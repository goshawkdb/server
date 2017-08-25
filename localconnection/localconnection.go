package localconnection

import (
	"encoding/binary"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/types/connectionmanager"
	sconn "goshawkdb.io/server/types/connections/server"
	loco "goshawkdb.io/server/types/localconnection"
	topo "goshawkdb.io/server/types/topology"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/binarybackoff"
	"goshawkdb.io/server/utils/status"
	"goshawkdb.io/server/utils/txnreader"
	"math/rand"
	"sync"
	"time"
)

type LocalConnection struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	connectionManager connectionmanager.ConnectionManager
	lock              sync.Mutex
	namespace         []byte
	nextVarNumber     uint64
	nextTxnNumber     uint64
	rng               *rand.Rand
	submitter         *client.SimpleTxnSubmitter

	inner localConnectionInner
}

type localConnectionInner struct {
	*LocalConnection
	*actor.BasicServerInner
}

func NewLocalConnection(rmId common.RMId, bootCount uint32, cm connectionmanager.ConnectionManager, logger log.Logger) *LocalConnection {
	namespace := make([]byte, common.KeyLen)
	binary.BigEndian.PutUint32(namespace[12:16], bootCount)
	binary.BigEndian.PutUint32(namespace[16:20], uint32(rmId))

	logger = log.With(logger, "subsystem", "localConnection")

	lc := &LocalConnection{
		connectionManager: cm,
		namespace:         namespace,
		nextTxnNumber:     0,
		nextVarNumber:     0,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	lc.submitter = client.NewSimpleTxnSubmitter(rmId, bootCount, cm, lc, lc.rng, logger)

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
		panic("LocalConnection failed to register with ConnectionManager!")
	}
	topology := msg.connectionManager.AddTopologySubscriber(topo.ConnectionSubscriber, msg.LocalConnection)
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
	sc *status.StatusConsumer
}

func (msg localConnectionMsgStatus) Exec() (bool, error) {
	msg.sc.Emit("LocalConnection")
	msg.submitter.Status(msg.sc.Fork())
	msg.sc.Join()
	return false, nil
}

func (lc *LocalConnection) Status(sc *status.StatusConsumer) {
	lc.EnqueueMsg(localConnectionMsgStatus{LocalConnection: lc, sc: sc})
}

type localConnectionMsgOutcomeReceived struct {
	*LocalConnection
	sender  common.RMId
	txn     *txnreader.TxnReader
	outcome *msgs.Outcome
}

func (msg localConnectionMsgOutcomeReceived) Exec() (bool, error) {
	utils.DebugLog(msg.inner.Logger, "debug", "Received submission outcome.", "TxnId", msg.txn.Id)
	return false, msg.submitter.SubmissionOutcomeReceived(msg.sender, msg.txn, msg.outcome)
}

func (lc *LocalConnection) SubmissionOutcomeReceived(sender common.RMId, txn *txnreader.TxnReader, outcome *msgs.Outcome) {
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
	translationCallback loco.TranslationCallback
	txnReader           *txnreader.TxnReader
	outcome             *msgs.Outcome
	err                 error
}

func (msg *localConnectionMsgRunClientTxn) setOutcomeError(txn *txnreader.TxnReader, outcome *msgs.Outcome, err error) error {
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
	utils.DebugLog(msg.inner.Logger, "debug", "Starting client txn.", "TxnId", txnId)
	if varPosMap := msg.varPosMap; varPosMap != nil {
		msg.submitter.EnsurePositions(varPosMap)
	}
	return false, msg.submitter.SubmitClientTransaction(msg.translationCallback, txn, txnId, msg.setOutcomeError, nil, msg.isTopologyTxn, nil)
}

func (lc *LocalConnection) RunClientTransaction(txn *cmsgs.ClientTxn, isTopologyTxn bool, varPosMap map[common.VarUUId]*common.Positions, translationCallback loco.TranslationCallback) (*txnreader.TxnReader, *msgs.Outcome, error) {
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
	backoff   *binarybackoff.BinaryBackoffEngine
	txnReader *txnreader.TxnReader
	outcome   *msgs.Outcome
	err       error
}

func (msg *localConnectionMsgRunTxn) setOutcomeError(txn *txnreader.TxnReader, outcome *msgs.Outcome, err error) error {
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
	utils.DebugLog(msg.inner.Logger, "debug", "Starting txn.", "TxnId", txnId)
	msg.submitter.SubmitTransaction(txn, txnId, msg.activeRMs, msg.setOutcomeError, msg.backoff)
	return false, nil
}

// txn must be root in its segment
func (lc *LocalConnection) RunTransaction(txn *msgs.Txn, txnId *common.TxnId, backoff *binarybackoff.BinaryBackoffEngine, activeRMs ...common.RMId) (*txnreader.TxnReader, *msgs.Outcome, error) {
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
	servers map[common.RMId]*sconn.ServerConnection
}

func (msg localConnectionMsgServerConnectionsChanged) Exec() (bool, error) {
	defer msg.MustClose()
	return false, msg.submitter.ServerConnectionsChanged(msg.servers)
}

func (lc *LocalConnection) ConnectedRMs(servers map[common.RMId]*sconn.ServerConnection) {
	msg := &localConnectionMsgServerConnectionsChanged{LocalConnection: lc, servers: servers}
	msg.InitMsg(lc)
	lc.EnqueueMsg(msg)
}

func (lc *LocalConnection) ConnectionLost(rmId common.RMId, servers map[common.RMId]*sconn.ServerConnection) {
	msg := &localConnectionMsgServerConnectionsChanged{LocalConnection: lc, servers: servers}
	msg.InitMsg(lc)
	lc.EnqueueMsg(msg)
}

func (lc *LocalConnection) ConnectionEstablished(c *sconn.ServerConnection, servers map[common.RMId]*sconn.ServerConnection, done func()) {
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
	lc.connectionManager.RemoveTopologySubscriberAsync(topo.ConnectionSubscriber, lc)
	lc.connectionManager.ClientLost(0, lc)
	lc.connectionManager.RemoveServerConnectionSubscriber(lc)
	return lc.BasicServerInner.HandleShutdown(err)
}

func (lc *localConnectionInner) nextTxnId() *common.TxnId {
	txnId := common.MakeTxnId(lc.namespace)
	binary.BigEndian.PutUint64(txnId[0:8], lc.nextTxnNumber)
	lc.nextTxnNumber += 1 + uint64(lc.rng.Intn(8))
	return txnId
}
