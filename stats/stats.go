package stats

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	cc "github.com/msackman/chancell"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/network"
	eng "goshawkdb.io/server/txnengine"
	"math/rand"
	"time"
)

type StatsPublisher struct {
	logger            log.Logger
	localConnection   *client.LocalConnection
	connectionManager *network.ConnectionManager
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(statsPublisherMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan statsPublisherMsg
	rng               *rand.Rand
	configPublisher
}

type statsPublisherMsg interface {
	witness() statsPublisherMsg
}

type statsPublisherMsgBasic struct{}

func (sp statsPublisherMsgBasic) witness() statsPublisherMsg { return sp }

type statsPublisherMsgShutdown struct{ statsPublisherMsgBasic }

func (sp *StatsPublisher) Shutdown() {
	if sp.enqueueQuery(statsPublisherMsgShutdown{}) {
		sp.cellTail.Wait()
	}
}

type statsPublisherExe func() error

func (spe statsPublisherExe) witness() statsPublisherMsg { return spe }

func (sp *StatsPublisher) exec(fun func() error) bool {
	return sp.enqueueQuery(statsPublisherExe(fun))
}

type statsPublisherQueryCapture struct {
	sp  *StatsPublisher
	msg statsPublisherMsg
}

func (spqc *statsPublisherQueryCapture) ccc(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
	return spqc.sp.enqueueQueryInner(spqc.msg, cell, spqc.ccc)
}

func (sp *StatsPublisher) enqueueQuery(msg statsPublisherMsg) bool {
	spqc := &statsPublisherQueryCapture{sp: sp, msg: msg}
	return sp.cellTail.WithCell(spqc.ccc)
}

func NewStatsPublisher(cm *network.ConnectionManager, lc *client.LocalConnection, logger log.Logger) *StatsPublisher {
	sp := &StatsPublisher{
		logger:            log.NewContext(logger).With("subsystem", "statsPublisher"),
		localConnection:   lc,
		connectionManager: cm,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	var head *cc.ChanCellHead
	head, sp.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan statsPublisherMsg, n)
			cell.Open = func() { sp.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			sp.enqueueQueryInner = func(msg statsPublisherMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
				if curCell == cell {
					select {
					case queryChan <- msg:
						return true, nil
					default:
						return false, nil
					}
				} else {
					return false, cont
				}
			}
		})
	go sp.actorLoop(head)
	return sp
}

func (sp *StatsPublisher) actorLoop(head *cc.ChanCellHead) {
	var (
		err       error
		queryChan <-chan statsPublisherMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = sp.queryChan, cell }
	head.WithCell(chanFun)

	sp.configPublisher.init(sp)

	terminate := err != nil
	for !terminate {
		if msg, ok := <-queryChan; ok {
			switch msgT := msg.(type) {
			case statsPublisherMsgShutdown:
				terminate = true
			case statsPublisherExe:
				err = msgT()
			default:
				err = fmt.Errorf("Fatal to StatsPublisher: Received unexpected message: %#v", msgT)
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		sp.logger.Log("msg", "Fatal error.", "error", err)
	}
	sp.cellTail.Terminate()
}

type configPublisher struct {
	*StatsPublisher
	vsn      *common.TxnId
	root     *configuration.Root
	topology *configuration.Topology
	json     []byte
	backoff  *server.BinaryBackoffEngine
}

func (cp *configPublisher) init(sp *StatsPublisher) {
	cp.StatsPublisher = sp
	cp.vsn = common.VersionZero
	topology := cp.connectionManager.AddTopologySubscriber(eng.MiscSubscriber, cp)
	cp.TopologyChanged(topology, func(bool) {})
}

func (cp *configPublisher) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	finished := make(chan struct{})
	enqueued := cp.exec(func() error {
		close(finished)
		return cp.maybePublishConfig(topology)
	})
	if enqueued {
		go func() {
			select {
			case <-finished:
				done(true)
			case <-cp.cellTail.Terminated:
				done(false)
			}
		}()
	} else {
		done(false)
	}
}

func (cp *configPublisher) maybePublishConfig(topology *configuration.Topology) error {
	cp.root = nil
	cp.topology = nil
	cp.backoff = nil
	cp.json = nil

	if topology.NextConfiguration != nil {
		// it's not safe to publish during topology changes.
		return nil
	}

	var root *configuration.Root
	for idx, rootName := range topology.Roots {
		if rootName == server.ConfigRootName {
			root = &topology.RootVarUUIds[idx]
			break
		}
	}
	if root == nil {
		return nil
	}
	json, err := topology.ToJSONString()
	if err != nil {
		return err
	}

	cp.root = root
	cp.topology = topology
	cp.json = json
	cp.backoff = server.NewBinaryBackoffEngine(cp.rng, server.SubmissionMinSubmitDelay, server.SubmissionMaxSubmitDelay)
	return cp.publishConfig()
}

func (cp *configPublisher) publishConfig() error {
	for {
		if cp.root == nil || cp.json == nil {
			return nil
		}
		seg := capn.NewBuffer(nil)
		ctxn := cmsgs.NewClientTxn(seg)
		ctxn.SetRetry(false)

		actions := cmsgs.NewClientActionList(seg, 1)

		action := actions.At(0)
		action.SetVarId(cp.root.VarUUId[:])
		action.SetReadwrite()
		rw := action.Readwrite()
		rw.SetVersion(cp.vsn[:])
		rw.SetValue(cp.json)
		rw.SetReferences(cmsgs.NewClientVarIdPosList(seg, 0))

		ctxn.SetActions(actions)

		varPosMap := make(map[common.VarUUId]*common.Positions)
		varPosMap[*cp.root.VarUUId] = cp.root.Positions

		server.Log("StatsPublisher: Publishing Config:", string(cp.json))
		_, result, err := cp.localConnection.RunClientTransaction(&ctxn, varPosMap, nil)
		if err != nil {
			return err
		} else if result == nil { // shutdown
			return nil
		} else if result.Which() == msgs.OUTCOME_COMMIT {
			server.Log("StatsPublisher: Publishing Config committed")
			return nil
		} else if abort := result.Abort(); abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
			server.Log("StatsPublisher: Publishing Config requires resubmit")
			backoff := cp.backoff
			cp.backoff.Advance()
			cp.backoff.After(func() {
				cp.exec(func() error {
					if cp.backoff == backoff {
						return cp.publishConfig()
					} else {
						return nil
					}
				})
			})
			return nil
		} else {
			server.Log("StatsPublisher: Publishing Config requires rerun")
			updates := abort.Rerun()
			found := false
			var value []byte
			for idx, l := 0, updates.Len(); idx < l && !found; idx++ {
				update := updates.At(idx)
				updateActions := eng.TxnActionsFromData(update.Actions(), true).Actions()
				for idy, m := 0, updateActions.Len(); idy < m && !found; idy++ {
					updateAction := updateActions.At(idy)
					if found = bytes.Equal(cp.root.VarUUId[:], updateAction.VarId()); found {
						if updateAction.Which() == msgs.ACTION_WRITE {
							cp.vsn = common.MakeTxnId(update.TxnId())
							updateWrite := updateAction.Write()
							value = updateWrite.Value()
						} else {
							// must be MISSING, which I'm really not sure should ever happen!
							cp.vsn = common.VersionZero
						}
					}
				}
			}
			if !found {
				return errors.New("Internal error: failed to find update for rerun of config publishing")
			}
			if len(value) > 0 {
				inDB := new(configuration.ConfigurationJSON)
				if err := json.Unmarshal(value, inDB); err != nil {
					return err
				}
				if inDB.Version > cp.topology.Version {
					server.Log("StatsPublisher: Existing copy in database is ahead of us. Nothing more to do.")
					return nil
				} else if inDB.Version == cp.topology.Version {
					server.Log("StatsPublisher: Existing copy in database is at least as up to date as us. Nothing more to do.")
					return nil
				}
			}
		}
	}
}
