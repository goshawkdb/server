package client

import (
	"encoding/binary"
	cc "github.com/msackman/chancell"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	"goshawkdb.io/server/paxos"
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
	localConnectionMsgWitness()
}

type localConnectionMsgShutdown struct{}

func (lcms *localConnectionMsgShutdown) localConnectionMsgWitness() {}

var localConnectionMsgShutdownInst = &localConnectionMsgShutdown{}

type localConnectionMsgStatus server.StatusConsumer

func (lcms *localConnectionMsgStatus) localConnectionMsgWitness() {}

type localConnectionMsgOutcomeReceived func(*LocalConnection)

func (lcmor localConnectionMsgOutcomeReceived) localConnectionMsgWitness() {}

type localConnectionMsgTopologyChange struct {
	topology *server.Topology
	servers  map[common.RMId]paxos.Connection
}

func (lcmtc *localConnectionMsgTopologyChange) localConnectionMsgWitness() {}

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
	localConnectionMsgSyncQuery
	txn         *cmsgs.ClientTxn
	varPosMap   map[common.VarUUId]*common.Positions
	assignTxnId bool
	outcome     *msgs.Outcome
}

func (lcmrct *localConnectionMsgRunClientTxn) localConnectionMsgWitness() {}
func (lcmrct *localConnectionMsgRunClientTxn) consumer(txnId *common.TxnId, outcome *msgs.Outcome) {
	lcmrct.outcome = outcome
	lcmrct.maybeClose()
}

type localConnectionMsgRunTxn struct {
	localConnectionMsgSyncQuery
	txn         *msgs.Txn
	assignTxnId bool
	activeRMs   []common.RMId
	outcome     *msgs.Outcome
}

func (lcmrt *localConnectionMsgRunTxn) localConnectionMsgWitness() {}
func (lcmrt *localConnectionMsgRunTxn) consumer(txnId *common.TxnId, outcome *msgs.Outcome) {
	lcmrt.outcome = outcome
	lcmrt.maybeClose()
}

func NewLocalConnection(rmId common.RMId, bootCount uint32, topology *server.Topology, cm paxos.ConnectionManager) *LocalConnection {
	namespace := make([]byte, common.KeyLen)
	binary.BigEndian.PutUint32(namespace[12:16], bootCount)
	binary.BigEndian.PutUint32(namespace[16:20], uint32(rmId))
	lc := &LocalConnection{
		rmId:              rmId,
		connectionManager: cm,
		namespace:         namespace,
		submitter:         NewSimpleTxnSubmitter(rmId, bootCount, topology, cm),
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

func (lc *LocalConnection) enqueueSyncQuery(msg localConnectionMsg, resultChan chan struct{}) bool {
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

func (lc *LocalConnection) Shutdown() {
	if lc.enqueueQuery(localConnectionMsgShutdownInst) {
		lc.cellTail.Wait()
	}
}

func (lc *LocalConnection) Status(sc *server.StatusConsumer) {
	lc.enqueueQuery((*localConnectionMsgStatus)(sc))
}

func (lc *LocalConnection) SubmissionOutcomeReceived(sender common.RMId, txnId *common.TxnId, outcome *msgs.Outcome) {
	server.Log("LC Received submission outcome for", txnId)
	lc.enqueueQuery(localConnectionMsgOutcomeReceived(func(lc *LocalConnection) {
		lc.submitter.SubmissionOutcomeReceived(sender, txnId, outcome)
	}))
}

func (lc *LocalConnection) TopologyChange(topology *server.Topology, servers map[common.RMId]paxos.Connection) {
	lc.enqueueQuery(&localConnectionMsgTopologyChange{
		topology: topology,
		servers:  servers,
	})
}

func (lc *LocalConnection) RunClientTransaction(txn *cmsgs.ClientTxn, varPosMap map[common.VarUUId]*common.Positions, assignTxnId bool) (*msgs.Outcome, error) {
	query := &localConnectionMsgRunClientTxn{
		txn:         txn,
		varPosMap:   varPosMap,
		assignTxnId: assignTxnId,
	}
	query.init()
	if lc.enqueueSyncQuery(query, query.resultChan) {
		return query.outcome, query.err
	} else {
		return nil, nil
	}
}

func (lc *LocalConnection) RunTransaction(txn *msgs.Txn, assignTxnId bool, activeRMs ...common.RMId) (*msgs.Outcome, error) {
	query := &localConnectionMsgRunTxn{
		txn:         txn,
		assignTxnId: assignTxnId,
		activeRMs:   activeRMs,
	}
	query.init()
	if lc.enqueueSyncQuery(query, query.resultChan) {
		return query.outcome, query.err
	} else {
		return nil, nil
	}
}

type localConnectionMsgDisableHashCodes map[common.RMId]paxos.Connection

func (lcmdhc localConnectionMsgDisableHashCodes) localConnectionMsgWitness() {}

func (lc *LocalConnection) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	lc.enqueueQuery(localConnectionMsgDisableHashCodes(servers))
}
func (lc *LocalConnection) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	lc.enqueueQuery(localConnectionMsgDisableHashCodes(servers))
}
func (lc *LocalConnection) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, servers map[common.RMId]paxos.Connection) {
	lc.enqueueQuery(localConnectionMsgDisableHashCodes(servers))
}

func (lc *LocalConnection) actorLoop(head *cc.ChanCellHead) {
	lc.connectionManager.AddSender(lc)
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
			case *localConnectionMsgShutdown:
				terminate = true
			case *localConnectionMsgTopologyChange:
				lc.submitter.TopologyChange(msgT.topology, msgT.servers)
			case *localConnectionMsgRunTxn:
				lc.runTransaction(msgT)
			case *localConnectionMsgRunClientTxn:
				lc.runClientTransaction(msgT)
			case localConnectionMsgOutcomeReceived:
				msgT(lc)
			case localConnectionMsgDisableHashCodes:
				lc.submitter.TopologyChange(nil, msgT)
			case *localConnectionMsgStatus:
				lc.status((*server.StatusConsumer)(msgT))
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		log.Println("LocalConnection error:", err)
	}
	lc.connectionManager.RemoveSenderAsync(lc)
	lc.submitter.Shutdown()
	lc.cellTail.Terminate()
}

func (lc *LocalConnection) runClientTransaction(txnQuery *localConnectionMsgRunClientTxn) {
	txn := txnQuery.txn
	if txnQuery.assignTxnId {
		txnId := lc.getNextTxnId()
		txn.SetId(txnId[:])
		server.Log("LC starting client txn", txnId)
	}
	if varPosMap := txnQuery.varPosMap; varPosMap != nil {
		lc.submitter.EnsurePositions(varPosMap)
	}
	err := lc.submitter.SubmitClientTransaction(txn, txnQuery.consumer, 0)
	if err != nil {
		txnQuery.errored(err)
	}
}

func (lc *LocalConnection) runTransaction(txnQuery *localConnectionMsgRunTxn) {
	txn := txnQuery.txn
	if txnQuery.assignTxnId {
		txnId := lc.getNextTxnId()
		txn.SetId(txnId[:])
		server.Log("LC starting txn", txnId)
	}
	lc.submitter.SubmitTransaction(txn, txnQuery.activeRMs, txnQuery.consumer, 0)
}

func (lc *LocalConnection) getNextTxnId() *common.TxnId {
	txnId := common.MakeTxnId(lc.namespace)
	binary.BigEndian.PutUint64(txnId[0:8], lc.nextTxnNumber)
	lc.nextTxnNumber++
	return txnId
}

func (lc *LocalConnection) status(sc *server.StatusConsumer) {
	sc.Emit("LocalConnection")
	lc.submitter.Status(sc.Fork())
	sc.Join()
}
