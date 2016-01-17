package network

import (
	"bytes"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	cc "github.com/msackman/chancell"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"log"
)

type TopologyTransmogrifier struct {
	connectionManager *ConnectionManager
	localConnection   *client.LocalConnection
	activeTopology    *configuration.Topology
	hostRMIds         map[string]common.RMId
	activeConnections map[common.RMId]paxos.Connection
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(topologyTransmogrifierMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan topologyTransmogrifierMsg
}

type topologyTransmogrifierMsg interface {
	topologyTransmogrifierMsgWitness()
}

type topologyTransmogrifierMsgShutdown struct{}

func (ttms *topologyTransmogrifierMsgShutdown) topologyTransmogrifierMsgWitness() {}

var topologyTransmogrifierMsgShutdownInst = &topologyTransmogrifierMsgShutdown{}

type topologyTransmogrifierMsgSetActiveConnections map[common.RMId]paxos.Connection

func (ttmsac topologyTransmogrifierMsgSetActiveConnections) topologyTransmogrifierMsgWitness() {}

type topologyTransmogrifierMsgVarChanged configuration.Topology

func (ttmvc *topologyTransmogrifierMsgVarChanged) topologyTransmogrifierMsgWitness() {}

func (tt *TopologyTransmogrifier) Shutdown() {
	if tt.enqueueQuery(topologyTransmogrifierMsgShutdownInst) {
		tt.cellTail.Wait()
	}
}

func (tt *TopologyTransmogrifier) enqueueQuery(msg topologyTransmogrifierMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return tt.enqueueQueryInner(msg, cell, f)
	}
	return tt.cellTail.WithCell(f)
}

func NewTopologyTransmogrifier(cm *ConnectionManager, lc *client.LocalConnection, commandLineConfig *configuration.Configuration) (*TopologyTransmogrifier, error) {
	tt := &TopologyTransmogrifier{
		connectionManager: cm,
		localConnection:   lc,
		hostRMIds:         make(map[string]common.RMId),
	}

	var head *cc.ChanCellHead
	head, tt.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan topologyTransmogrifierMsg, n)
			cell.Open = func() { tt.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			tt.enqueueQueryInner = func(msg topologyTransmogrifierMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
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

	cm.Dispatchers.VarDispatcher.ApplyToVar(func(v *eng.Var, err error) {
		if err != nil {
			panic(fmt.Errorf("Error trying to subscribe to topology: %v", err))
		}
		v.AddWriteSubscriber(configuration.VersionOne,
			func(v *eng.Var, value []byte, refs *msgs.VarIdPos_List, txn *eng.Txn) {
				var rootVarPosPtr *msgs.VarIdPos
				if refs.Len() == 1 {
					root := refs.At(0)
					rootVarPosPtr = &root
				}
				topology, err := configuration.TopologyFromCap(txn.Id, rootVarPosPtr, value)
				if err != nil {
					panic(fmt.Errorf("Unable to deserialize new topology: %v", err))
				}
				tt.enqueueQuery((*topologyTransmogrifierMsgVarChanged)(topology))
			})
	}, true, configuration.TopologyVarUUId)

	err := tt.ensureLocalTopology(commandLineConfig)
	if err != nil {
		return nil, err
	}

	cm.AddSender(tt)
	go tt.actorLoop(head)
	return tt, nil
}

func (tt *TopologyTransmogrifier) actorLoop(head *cc.ChanCellHead) {
	var (
		err       error
		queryChan <-chan topologyTransmogrifierMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = tt.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		if msg, ok := <-queryChan; ok {
			switch msgT := msg.(type) {
			case *topologyTransmogrifierMsgShutdown:
				terminate = true
			case topologyTransmogrifierMsgSetActiveConnections:
				tt.activeConnections = msgT
			case *topologyTransmogrifierMsgVarChanged:
				tt.activeTopology = (*configuration.Topology)(msgT)
				tt.connectionManager.SetTopology(tt.activeTopology)
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		log.Println("TopologyTransmogrifier error:", err)
	}
	tt.connectionManager.RemoveSenderAsync(tt)
	tt.cellTail.Terminate()
}

func (tt *TopologyTransmogrifier) ensureLocalTopology(config *configuration.Configuration) error {
	topology, err := tt.getTopologyFromLocalDatabase()
	if err != nil {
		return err
	}

	if topology == nil && config == nil {
		return errors.New("No configuration supplied and no configuration found in local store. Cannot continue")
	} else if topology == nil {
		_, err := tt.createTopologyZero(config.ClusterId)
		return err
	}

	tt.enqueueQuery((*topologyTransmogrifierMsgVarChanged)(topology))

	return nil
}

func (tt *TopologyTransmogrifier) createTopologyTransaction(read, write *configuration.Topology, active, passive common.RMIds) *msgs.Txn {
	if write == nil && read != nil {
		panic("Topology transaction with nil write and non-nil read not supported")
	}

	seg := capn.NewBuffer(nil)
	txn := msgs.NewTxn(seg)
	txn.SetSubmitter(uint32(tt.connectionManager.RMId))
	txn.SetSubmitterBootCount(tt.connectionManager.BootCount)

	actions := msgs.NewActionList(seg, 1)
	txn.SetActions(actions)
	action := actions.At(0)
	action.SetVarId(configuration.TopologyVarUUId[:])

	switch {
	case write == nil && read == nil: // discovery
		action.SetRead()
		action.Read().SetVersion(common.VersionZero[:])

	case read == nil: // creation
		action.SetCreate()
		create := action.Create()
		create.SetValue(write.Serialize())
		create.SetReferences(msgs.NewVarIdPosList(seg, 0))
		// When we create, we're creating with the blank topology. Blank
		// topology has MaxRMCount = 0. But we never actually use
		// positions of the topology var anyway. So the following code
		// basically never does anything, and is just here for
		// completeness, but it's still all safe.
		positions := seg.NewUInt8List(int(write.MaxRMCount))
		create.SetPositions(positions)
		for idx, l := 0, positions.Len(); idx < l; idx++ {
			positions.Set(idx, uint8(idx))
		}

	default: // modification
		action.SetReadwrite()
		rw := action.Readwrite()
		rw.SetVersion(read.DBVersion[:])
		rw.SetValue(write.Serialize())
		refs := msgs.NewVarIdPosList(seg, 1)
		rw.SetReferences(refs)
		varIdPos := refs.At(0)
		varIdPos.SetId(write.Root.VarUUId[:])
		varIdPos.SetPositions((capn.UInt8List)(*write.Root.Positions))
	}

	allocs := msgs.NewAllocationList(seg, len(active)+len(passive))
	txn.SetAllocations(allocs)

	offset := 0
	for idx, rmIds := range []common.RMIds{active, passive} {
		for idy, rmId := range rmIds {
			alloc := allocs.At(idy + offset)
			alloc.SetRmId(uint32(rmId))
			if idx == 0 {
				alloc.SetActive(tt.activeConnections[rmId].BootCount())
			} else {
				alloc.SetActive(0)
			}
			indices := seg.NewUInt16List(1)
			alloc.SetActionIndices(indices)
			indices.Set(0, 0)
		}
		offset += len(rmIds)
	}

	txn.SetFInc(uint8(len(active)))
	if read == nil {
		txn.SetTopologyVersion(0)
	} else {
		txn.SetTopologyVersion(read.Version)
	}

	return &txn
}

func (tt *TopologyTransmogrifier) getTopologyFromLocalDatabase() (*configuration.Topology, error) {
	empty, err := tt.connectionManager.Dispatchers.IsDatabaseEmpty()
	if empty || err != nil {
		return nil, err
	}

	for {
		txn := tt.createTopologyTransaction(nil, nil, []common.RMId{tt.connectionManager.RMId}, nil)

		result, err := tt.localConnection.RunTransaction(txn, true, tt.connectionManager.RMId)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, nil // shutting down
		}
		if result.Which() == msgs.OUTCOME_COMMIT {
			return nil, fmt.Errorf("Internal error: read of topology version 0 failed to abort")
		}
		abort := result.Abort()
		if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
			continue
		}
		abortUpdates := abort.Rerun()
		if abortUpdates.Len() != 1 {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave multiple updates")
		}
		update := abortUpdates.At(0)
		dbversion := common.MakeTxnId(update.TxnId())
		updateActions := update.Actions()
		if updateActions.Len() != 1 {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave multiple actions: %v", updateActions.Len())
		}
		updateAction := updateActions.At(0)
		if !bytes.Equal(updateAction.VarId(), configuration.TopologyVarUUId[:]) {
			return nil, fmt.Errorf("Internal error: unable to find action for topology from read of topology version 0")
		}
		if updateAction.Which() != msgs.ACTION_WRITE {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave non-write action")
		}
		write := updateAction.Write()
		var rootPtr *msgs.VarIdPos
		if refs := write.References(); refs.Len() == 1 {
			root := refs.At(0)
			rootPtr = &root
		}
		return configuration.TopologyFromCap(dbversion, rootPtr, write.Value())
	}
}

func (tt *TopologyTransmogrifier) createTopologyZero(clusterId string) (*common.TxnId, error) {
	topology := configuration.BlankTopology(clusterId)
	txn := tt.createTopologyTransaction(nil, topology, []common.RMId{tt.connectionManager.RMId}, nil)
	txnId := configuration.VersionOne
	result, err := tt.localConnection.RunTransaction(txn, false, tt.connectionManager.RMId)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil // shutting down
	}
	if result.Which() == msgs.OUTCOME_COMMIT {
		return txnId, nil
	} else {
		return nil, fmt.Errorf("Internal error: unable to write initial topology to local data store")
	}
}

func (tt *TopologyTransmogrifier) chooseRMIdsForTopology(topology *configuration.Topology) ([]common.RMId, []common.RMId) {
	twoFInc := len(topology.Hosts)
	fInc := (twoFInc >> 1) + 1
	f := twoFInc - fInc
	if len(tt.hostRMIds) < twoFInc {
		return nil, nil
	}
	active := make([]common.RMId, 0, fInc)
	passive := make([]common.RMId, 0, f)
	for _, host := range topology.Hosts {
		if rmId, found := tt.hostRMIds[host]; found {
			if _, found := tt.activeConnections[rmId]; found && len(active) < cap(active) {
				active = append(active, rmId)
			} else if len(passive) < cap(passive) {
				passive = append(passive, rmId)
			} else {
				return nil, nil
			}
		} else {
			return nil, nil
		}
	}
	return active, passive
}

func (tt *TopologyTransmogrifier) maybeCreateRoot(topology *configuration.Topology) error {
	if topology.Root.VarUUId != nil {
		return nil
	}
	twoFInc := int(topology.TwoFInc)
	if topology.RMs().NonEmptyLen() < twoFInc {
		return nil
	}
	active := make([]common.RMId, 0, int(topology.FInc))
	passive := make([]common.RMId, 0, int(topology.F))
	// this range is valid only because root's positions are hardcoded
	for _, rmId := range topology.RMs().NonEmpty()[:twoFInc] {
		if _, found := tt.activeConnections[rmId]; found && len(active) < cap(active) {
			active = append(active, rmId)
		} else if len(passive) < cap(passive) {
			passive = append(passive, rmId)
		} else {
			return nil
		}
	}

	server.Log("Creating Root. Actives:", active, "; Passives:", passive)
	for {
		seg := capn.NewBuffer(nil)
		txn := msgs.NewTxn(seg)
		txn.SetSubmitter(uint32(tt.connectionManager.RMId))
		txn.SetSubmitterBootCount(tt.connectionManager.BootCount)
		actions := msgs.NewActionList(seg, 1)
		txn.SetActions(actions)
		action := actions.At(0)
		vUUId := tt.localConnection.NextVarUUId()
		action.SetVarId(vUUId[:])
		action.SetCreate()
		create := action.Create()
		positions := seg.NewUInt8List(int(topology.MaxRMCount))
		create.SetPositions(positions)
		for idx, l := 0, positions.Len(); idx < l; idx++ {
			positions.Set(idx, uint8(idx))
		}
		create.SetValue([]byte{})
		create.SetReferences(msgs.NewVarIdPosList(seg, 0))
		allocs := msgs.NewAllocationList(seg, twoFInc)
		txn.SetAllocations(allocs)
		offset := 0
		for idx, rmIds := range []common.RMIds{active, passive} {
			for idy, rmId := range rmIds {
				alloc := allocs.At(idy + offset)
				alloc.SetRmId(uint32(rmId))
				if idx == 0 {
					alloc.SetActive(tt.activeConnections[rmId].BootCount())
				} else {
					alloc.SetActive(0)
				}
				indices := seg.NewUInt16List(1)
				alloc.SetActionIndices(indices)
				indices.Set(0, 0)
			}
			offset += len(rmIds)
		}
		txn.SetFInc(topology.FInc)
		txn.SetTopologyVersion(topology.Version)
		result, err := tt.localConnection.RunTransaction(&txn, true, active...)
		if err != nil {
			return err
		}
		if result == nil {
			return nil
		}
		if result.Which() == msgs.OUTCOME_COMMIT {
			server.Log("Root created in", vUUId)
			topology.Root.VarUUId = vUUId
			topology.Root.Positions = (*common.Positions)(&positions)
			return nil
		}
		abort := result.Abort()
		if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
			continue
		}
		return fmt.Errorf("Internal error: creation of root gave rerun outcome")
	}
}

func (tt *TopologyTransmogrifier) ConnectedRMs(conns map[common.RMId]paxos.Connection) {
	tt.enqueueQuery(topologyTransmogrifierMsgSetActiveConnections(conns))
}

func (tt *TopologyTransmogrifier) ConnectionLost(rmId common.RMId, conns map[common.RMId]paxos.Connection) {
	tt.enqueueQuery(topologyTransmogrifierMsgSetActiveConnections(conns))
}

func (tt *TopologyTransmogrifier) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, conns map[common.RMId]paxos.Connection) {
	tt.enqueueQuery(topologyTransmogrifierMsgSetActiveConnections(conns))
}
