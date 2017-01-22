package client

import (
	"encoding/binary"
	"fmt"
	cc "github.com/msackman/chancell"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"log"
	"sync"
)

type LocalConnection struct {
	sync.Mutex
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(localConnectionMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan localConnectionMsg
	rmId              common.RMId
	connectionManager paxos.ConnectionManager
	namespace         []byte
	submitter         *SimpleTxnSubmitter
	nextTxnNumber     uint64
	nextVarNumber     uint64
	txnQuery          localConnectionTxnQuery
}

type localConnectionMsg interface {
	witness() localConnectionMsg
}

type localConnectionMsgBasic struct{}

func (lcmb localConnectionMsgBasic) witness() localConnectionMsg { return lcmb }

type localConnectionMsgShutdown struct{ localConnectionMsgBasic }

type localConnectionMsgStatus struct {
	localConnectionMsgBasic
	*server.StatusConsumer
}

type localConnectionMsgOutcomeReceived struct {
	localConnectionMsgBasic
	sender  common.RMId
	txn     *eng.TxnReader
	outcome *msgs.Outcome
}

type localConnectionMsgTopologyChanged struct {
	localConnectionMsgBasic
	localConnectionMsgSyncQuery
	topology *configuration.Topology
}

type localConnectionTxnQuery interface {
	errored(error)
}

type localConnectionMsgSyncQuery struct {
	resultChan chan struct{}
	err        error
}

func (lcmsq *localConnectionMsgSyncQuery) init() {
	lcmsq.resultChan = make(chan struct{})
}

func (lcmsq *localConnectionMsgSyncQuery) errored(err error) {
	lcmsq.err = err
	lcmsq.maybeClose()
}

func (lcmsq *localConnectionMsgSyncQuery) maybeClose() {
	select {
	case <-lcmsq.resultChan:
	default:
		close(lcmsq.resultChan)
	}
}

type localConnectionMsgRunClientTxn struct {
	localConnectionMsgBasic
	localConnectionMsgSyncQuery
	txn                 *cmsgs.ClientTxn
	varPosMap           map[common.VarUUId]*common.Positions
	translationCallback eng.TranslationCallback
	txnReader           *eng.TxnReader
	outcome             *msgs.Outcome
}

func (lcmrct *localConnectionMsgRunClientTxn) consumer(txn *eng.TxnReader, outcome *msgs.Outcome, err error) error {
	lcmrct.txnReader = txn
	lcmrct.outcome = outcome
	lcmrct.err = err
	lcmrct.maybeClose()
	return nil
}

type localConnectionMsgRunTxn struct {
	localConnectionMsgBasic
	localConnectionMsgSyncQuery
	txn       *msgs.Txn
	txnId     *common.TxnId
	activeRMs []common.RMId
	backoff   *server.BinaryBackoffEngine
	txnReader *eng.TxnReader
	outcome   *msgs.Outcome
}

func (lcmrt *localConnectionMsgRunTxn) consumer(txn *eng.TxnReader, outcome *msgs.Outcome, err error) error {
	lcmrt.txnReader = txn
	lcmrt.outcome = outcome
	lcmrt.err = err
	lcmrt.maybeClose()
	return nil
}

func (lc *LocalConnection) NextVarUUId() *common.VarUUId {
	lc.Lock()
	defer lc.Unlock()
	vUUId := common.MakeVarUUId(lc.namespace)
	binary.BigEndian.PutUint64(vUUId[0:8], lc.nextVarNumber)
	lc.nextVarNumber++
	return vUUId
}

func (lc *LocalConnection) enqueueQuery(msg localConnectionMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return lc.enqueueQueryInner(msg, cell, f)
	}
	return lc.cellTail.WithCell(f)
}

func (lc *LocalConnection) enqueueQuerySync(msg localConnectionMsg, resultChan chan struct{}) bool {
	if lc.enqueueQuery(msg) {
		select {
		case <-resultChan:
			return true
		case <-lc.cellTail.Terminated:
			return false
		}
	} else {
		return false
	}
}

func (lc *LocalConnection) Shutdown(sync paxos.Blocking) {
	if lc.enqueueQuery(localConnectionMsgShutdown{}) && sync == paxos.Sync {
		lc.cellTail.Wait()
	}
}

func (lc *LocalConnection) Status(sc *server.StatusConsumer) {
	lc.enqueueQuery(localConnectionMsgStatus{StatusConsumer: sc})
}

func (lc *LocalConnection) SubmissionOutcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) {
	server.Log("LC Received submission outcome for", txn.Id)
	lc.enqueueQuery(localConnectionMsgOutcomeReceived{
		sender:  sender,
		txn:     txn,
		outcome: outcome,
	})
}

func (lc *LocalConnection) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &localConnectionMsgTopologyChanged{topology: topology}
	msg.init()
	if lc.enqueueQuery(msg) {
		go func() {
			select {
			case <-msg.resultChan:
				done(true)
			case <-lc.cellTail.Terminated:
				done(false)
			}
		}()
	} else {
		done(false)
	}
}

func (lc *LocalConnection) RunClientTransaction(txn *cmsgs.ClientTxn, varPosMap map[common.VarUUId]*common.Positions, translationCallback eng.TranslationCallback) (*eng.TxnReader, *msgs.Outcome, error) {
	query := &localConnectionMsgRunClientTxn{
		txn:                 txn,
		varPosMap:           varPosMap,
		translationCallback: translationCallback,
	}
	query.init()
	if lc.enqueueQuerySync(query, query.resultChan) {
		return query.txnReader, query.outcome, query.err
	} else {
		return nil, nil, nil
	}
}

// txn must be root in its segment
func (lc *LocalConnection) RunTransaction(txn *msgs.Txn, txnId *common.TxnId, backoff *server.BinaryBackoffEngine, activeRMs ...common.RMId) (*eng.TxnReader, *msgs.Outcome, error) {
	query := &localConnectionMsgRunTxn{
		txn:       txn,
		txnId:     txnId,
		backoff:   backoff,
		activeRMs: activeRMs,
	}
	query.init()
	if lc.enqueueQuerySync(query, query.resultChan) {
		return query.txnReader, query.outcome, query.err
	} else {
		return nil, nil, nil
	}
}

type localConnectionMsgServerConnectionsChanged struct {
	servers map[common.RMId]paxos.Connection
	done    func()
}

func (lcmscc localConnectionMsgServerConnectionsChanged) witness() localConnectionMsg { return lcmscc }

func (lc *LocalConnection) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	lc.enqueueQuery(localConnectionMsgServerConnectionsChanged{servers: servers, done: func() {}})
}
func (lc *LocalConnection) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	lc.enqueueQuery(localConnectionMsgServerConnectionsChanged{servers: servers, done: func() {}})
}
func (lc *LocalConnection) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, servers map[common.RMId]paxos.Connection, done func()) {
	finished := make(chan struct{})
	enqueued := lc.enqueueQuery(localConnectionMsgServerConnectionsChanged{
		servers: servers,
		done:    func() { close(finished) },
	})
	if enqueued {
		go func() {
			select {
			case <-finished:
			case <-lc.cellTail.Terminated:
			}
			done()
		}()
	} else {
		done()
	}
}

func NewLocalConnection(rmId common.RMId, bootCount uint32, cm paxos.ConnectionManager) *LocalConnection {
	namespace := make([]byte, common.KeyLen)
	binary.BigEndian.PutUint32(namespace[12:16], bootCount)
	binary.BigEndian.PutUint32(namespace[16:20], uint32(rmId))
	lc := &LocalConnection{
		rmId:              rmId,
		connectionManager: cm,
		namespace:         namespace,
		submitter:         NewSimpleTxnSubmitter(rmId, bootCount, cm),
		nextTxnNumber:     0,
		nextVarNumber:     0,
	}
	var head *cc.ChanCellHead
	head, lc.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan localConnectionMsg, n)
			cell.Open = func() { lc.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			lc.enqueueQueryInner = func(msg localConnectionMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
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

	go lc.actorLoop(head)
	return lc
}

func (lc *LocalConnection) actorLoop(head *cc.ChanCellHead) {
	topology := lc.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, lc)
	defer lc.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, lc)
	servers := lc.connectionManager.ClientEstablished(0, lc)
	if servers == nil {
		panic("LocalConnection failed to register with ConnectionManager!")
	}
	defer lc.connectionManager.ClientLost(0, lc)
	lc.submitter.TopologyChanged(topology)
	lc.submitter.ServerConnectionsChanged(servers)
	var (
		err       error
		queryChan <-chan localConnectionMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = lc.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		if msg, ok := <-queryChan; ok {
			switch msgT := msg.(type) {
			case localConnectionMsgShutdown:
				terminate = true
			case *localConnectionMsgTopologyChanged:
				err = lc.submitter.TopologyChanged(msgT.topology)
				msgT.maybeClose()
			case *localConnectionMsgRunTxn:
				lc.runTransaction(msgT)
			case *localConnectionMsgRunClientTxn:
				err = lc.runClientTransaction(msgT)
			case localConnectionMsgOutcomeReceived:
				err = lc.submitter.SubmissionOutcomeReceived(msgT.sender, msgT.txn, msgT.outcome)
			case localConnectionMsgServerConnectionsChanged:
				err = lc.submitter.ServerConnectionsChanged(msgT.servers)
				msgT.done()
			case localConnectionMsgStatus:
				lc.status(msgT.StatusConsumer)
			default:
				err = fmt.Errorf("Fatal to LocalConnection: Received unexpected message: %#v", msgT)
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		log.Println("LocalConnection error:", err)
	}
	lc.submitter.Shutdown()
	lc.cellTail.Terminate()
}

func (lc *LocalConnection) runClientTransaction(txnQuery *localConnectionMsgRunClientTxn) error {
	txn := txnQuery.txn
	txnId := lc.getNextTxnId()
	txn.SetId(txnId[:])
	server.Log("LC starting client txn", txnId)
	if varPosMap := txnQuery.varPosMap; varPosMap != nil {
		lc.submitter.EnsurePositions(varPosMap)
	}
	return lc.submitter.SubmitClientTransaction(txnQuery.translationCallback, txn, txnId, txnQuery.consumer, nil, true, nil)
}

func (lc *LocalConnection) runTransaction(txnQuery *localConnectionMsgRunTxn) {
	txnId := txnQuery.txnId
	txn := txnQuery.txn
	if txnId == nil {
		txnId = lc.getNextTxnId()
		txn.SetId(txnId[:])
		server.Log("LC starting txn", txnId)
	}
	lc.submitter.SubmitTransaction(txn, txnId, txnQuery.activeRMs, txnQuery.consumer, txnQuery.backoff)
}

func (lc *LocalConnection) getNextTxnId() *common.TxnId {
	txnId := common.MakeTxnId(lc.namespace)
	binary.BigEndian.PutUint64(txnId[0:8], lc.nextTxnNumber)
	lc.nextTxnNumber += 1 + uint64(lc.submitter.rng.Intn(8))
	return txnId
}

func (lc *LocalConnection) status(sc *server.StatusConsumer) {
	sc.Emit("LocalConnection")
	lc.submitter.Status(sc.Fork())
	sc.Join()
}
