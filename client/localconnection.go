package client

import (
	"encoding/binary"
	"errors"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"sync"
)

type LocalConnection struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	connectionManager paxos.ConnectionManager
	lock              sync.Mutex
	namespace         []byte
	nextVarNumber     uint64
	nextTxnNumber     uint64
	submitter         *SimpleTxnSubmitter

	inner localConnectionInner
}

type localConnectionInner struct {
	*LocalConnection
	*actor.BasicServerInner
}

func NewLocalConnection(rmId common.RMId, bootCount uint32, cm paxos.ConnectionManager, logger log.Logger) *LocalConnection {
	namespace := make([]byte, common.KeyLen)
	binary.BigEndian.PutUint32(namespace[12:16], bootCount)
	binary.BigEndian.PutUint32(namespace[16:20], uint32(rmId))

	logger = log.With(logger, "subsystem", "localConnection")

	lc := &LocalConnection{
		connectionManager: cm,
		namespace:         namespace,
		nextTxnNumber:     0,
		nextVarNumber:     0,
	}
	lc.submitter = NewSimpleTxnSubmitter(rmId, bootCount, cm, lc, logger)

	lci := &lc.inner
	lci.LocalConnection = lc
	lci.BasicServerInner = actor.NewBasicServerInner(logger)

	_, err := actor.Spawn(lci)
	if err != nil {
		panic(err)
	}

	lc.EnqueueMsg(localConnectionMsgInit{LocalConnection: lc})

	return lc
}

type localConnectionMsgInit struct {
	*LocalConnection
}

func (msg localConnectionMsgInit) Exec() (bool, error) {
	servers, _ := msg.connectionManager.ClientEstablished(0, msg.LocalConnection)
	if servers == nil {
		return false, errors.New("LocalConnection failed to register with ConnectionManager!")
	}
	topology := msg.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, msg.LocalConnection)
	if err := msg.submitter.TopologyChanged(topology); err != nil {
		return false, err
	}
	if err := msg.submitter.ServerConnectionsChanged(servers); err != nil {
		return false, err
	}
	msg.connectionManager.AddServerConnectionSubscriber(msg.LocalConnection)
	return false, nil
}

func (lc *LocalConnection) NextVarUUId() *common.VarUUId {
	lc.lock.Lock()
	defer lc.lock.Unlock()
	vUUId := common.MakeVarUUId(lc.namespace)
	binary.BigEndian.PutUint64(vUUId[0:8], lc.nextVarNumber)
	lc.nextVarNumber++
	return vUUId
}

type localConnectionMsgStatus struct {
	*LocalConnection
	sc *server.StatusConsumer
}

func (msg localConnectionMsgStatus) Exec() (bool, error) {
	msg.sc.Emit("LocalConnection")
	msg.submitter.Status(msg.sc.Fork())
	msg.sc.Join()
	return false, nil
}

func (lc *LocalConnection) Status(sc *server.StatusConsumer) {
	lc.EnqueueMsg(localConnectionMsgStatus{LocalConnection: lc, sc: sc})
}

type localConnectionMsgOutcomeReceived struct {
	*LocalConnection
	sender  common.RMId
	txn     *eng.TxnReader
	outcome *msgs.Outcome
}

func (msg localConnectionMsgOutcomeReceived) Exec() (bool, error) {
	server.DebugLog(msg.inner.Logger, "debug", "Received submission outcome.", "TxnId", msg.txn.Id)
	return false, msg.submitter.SubmissionOutcomeReceived(msg.sender, msg.txn, msg.outcome)
}

func (lc *LocalConnection) SubmissionOutcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) {
	lc.EnqueueMsg(localConnectionMsgOutcomeReceived{
		LocalConnection: lc,
		sender:          sender,
		txn:             txn,
		outcome:         outcome,
	})
}

type localConnectionMsgTopologyChanged struct {
	actor.MsgSyncQuery
	*LocalConnection
	topology *configuration.Topology
}

func (msg localConnectionMsgTopologyChanged) Exec() (bool, error) {
	defer msg.MustClose()
	return false, msg.submitter.TopologyChanged(msg.topology)
}

func (lc *LocalConnection) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &localConnectionMsgTopologyChanged{LocalConnection: lc, topology: topology}
	msg.InitMsg(lc)
	if lc.EnqueueMsg(msg) {
		go done(msg.Wait())
	} else {
		done(false)
	}
}

type localConnectionMsgRunClientTxn struct {
	actor.MsgSyncQuery
	*LocalConnection
	txn                 *cmsgs.ClientTxn
	isTopologyTxn       bool
	varPosMap           map[common.VarUUId]*common.Positions
	translationCallback eng.TranslationCallback
	txnReader           *eng.TxnReader
	outcome             *msgs.Outcome
	err                 error
}

func (msg *localConnectionMsgRunClientTxn) setOutcomeError(txn *eng.TxnReader, outcome *msgs.Outcome, err error) error {
	msg.txnReader = txn
	msg.outcome = outcome
	msg.err = err
	msg.MustClose()
	return nil
}

func (msg *localConnectionMsgRunClientTxn) Exec() (bool, error) {
	txn := msg.txn
	txnId := msg.inner.nextTxnId()
	txn.SetId(txnId[:])
	server.DebugLog(msg.inner.Logger, "debug", "Starting client txn.", "TxnId", txnId)
	if varPosMap := msg.varPosMap; varPosMap != nil {
		msg.submitter.EnsurePositions(varPosMap)
	}
	return false, msg.submitter.SubmitClientTransaction(msg.translationCallback, txn, txnId, msg.setOutcomeError, nil, msg.isTopologyTxn, nil)
}

func (lc *LocalConnection) RunClientTransaction(txn *cmsgs.ClientTxn, isTopologyTxn bool, varPosMap map[common.VarUUId]*common.Positions, translationCallback eng.TranslationCallback) (*eng.TxnReader, *msgs.Outcome, error) {
	msg := &localConnectionMsgRunClientTxn{
		LocalConnection:     lc,
		txn:                 txn,
		isTopologyTxn:       isTopologyTxn,
		varPosMap:           varPosMap,
		translationCallback: translationCallback,
	}
	msg.InitMsg(lc)
	if lc.EnqueueMsg(msg) && msg.Wait() {
		return msg.txnReader, msg.outcome, msg.err
	} else {
		return nil, nil, nil
	}
}

type localConnectionMsgRunTxn struct {
	actor.MsgSyncQuery
	*LocalConnection
	txn       *msgs.Txn
	txnId     *common.TxnId
	activeRMs []common.RMId
	backoff   *server.BinaryBackoffEngine
	txnReader *eng.TxnReader
	outcome   *msgs.Outcome
	err       error
}

func (msg *localConnectionMsgRunTxn) setOutcomeError(txn *eng.TxnReader, outcome *msgs.Outcome, err error) error {
	msg.txnReader = txn
	msg.outcome = outcome
	msg.err = err
	msg.MustClose()
	return nil
}

func (msg *localConnectionMsgRunTxn) Exec() (bool, error) {
	txn := msg.txn
	txnId := msg.txnId
	if txnId == nil {
		txnId = msg.inner.nextTxnId()
		txn.SetId(txnId[:])
	}
	server.DebugLog(msg.inner.Logger, "debug", "Starting txn.", "TxnId", txnId)
	msg.submitter.SubmitTransaction(txn, txnId, msg.activeRMs, msg.setOutcomeError, msg.backoff)
	return false, nil
}

// txn must be root in its segment
func (lc *LocalConnection) RunTransaction(txn *msgs.Txn, txnId *common.TxnId, backoff *server.BinaryBackoffEngine, activeRMs ...common.RMId) (*eng.TxnReader, *msgs.Outcome, error) {
	msg := &localConnectionMsgRunTxn{
		LocalConnection: lc,
		txn:             txn,
		txnId:           txnId,
		activeRMs:       activeRMs,
		backoff:         backoff,
	}
	msg.InitMsg(lc)
	if lc.EnqueueMsg(msg) && msg.Wait() {
		return msg.txnReader, msg.outcome, msg.err
	} else {
		return nil, nil, nil
	}
}

type localConnectionMsgServerConnectionsChanged struct {
	actor.MsgSyncQuery
	*LocalConnection
	servers map[common.RMId]paxos.Connection
}

func (msg localConnectionMsgServerConnectionsChanged) Exec() (bool, error) {
	defer msg.MustClose()
	return false, msg.submitter.ServerConnectionsChanged(msg.servers)
}

func (lc *LocalConnection) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	msg := &localConnectionMsgServerConnectionsChanged{LocalConnection: lc, servers: servers}
	msg.InitMsg(lc)
	lc.EnqueueMsg(msg)
}

func (lc *LocalConnection) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	msg := &localConnectionMsgServerConnectionsChanged{LocalConnection: lc, servers: servers}
	msg.InitMsg(lc)
	lc.EnqueueMsg(msg)
}

func (lc *LocalConnection) ConnectionEstablished(rmId common.RMId, c paxos.Connection, servers map[common.RMId]paxos.Connection, done func()) {
	msg := &localConnectionMsgServerConnectionsChanged{LocalConnection: lc, servers: servers}
	msg.InitMsg(lc)
	if lc.EnqueueMsg(msg) {
		go func() {
			msg.Wait()
			done()
		}()
	} else {
		done()
	}
}

func (lc *localConnectionInner) Init(self *actor.Actor) (bool, error) {
	terminate, err := lc.BasicServerInner.Init(self)
	if terminate || err != nil {
		return terminate, err
	}

	lc.Mailbox = self.Mailbox
	lc.BasicServerOuter = actor.NewBasicServerOuter(self.Mailbox)

	return false, nil
}

func (lc *localConnectionInner) HandleShutdown(err error) bool {
	lc.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, lc)
	lc.connectionManager.ClientLost(0, lc)
	lc.connectionManager.RemoveServerConnectionSubscriber(lc)
	return lc.BasicServerInner.HandleShutdown(err)
}

func (lc *localConnectionInner) nextTxnId() *common.TxnId {
	txnId := common.MakeTxnId(lc.namespace)
	binary.BigEndian.PutUint64(txnId[0:8], lc.nextTxnNumber)
	lc.nextTxnNumber += 1 + uint64(lc.submitter.rng.Intn(8))
	return txnId
}
