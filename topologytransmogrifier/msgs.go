package topologytransmogrifier

import (
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/binarybackoff"
	"goshawkdb.io/server/utils/txnreader"
	"goshawkdb.io/server/utils/vectorclock"
)

type topologyTransmogrifierMsgRequestConfigChange struct {
	*TopologyTransmogrifier
	config *configuration.Configuration
}

func (msg topologyTransmogrifierMsgRequestConfigChange) Exec() (bool, error) {
	utils.DebugLog(msg.inner.Logger, "debug", "Topology change request.", "config", msg.config)
	nonFatalErr := msg.setTarget(msg.config)
	// because this is definitely not the cmd-line config, an error here is non-fatal
	if nonFatalErr != nil {
		msg.inner.Logger.Log("msg", "Ignoring requested configuration change.", "error", nonFatalErr)
	}
	return false, nil
}

func (tt *TopologyTransmogrifier) RequestConfigurationChange(config *configuration.Configuration) {
	tt.EnqueueMsg(topologyTransmogrifierMsgRequestConfigChange{TopologyTransmogrifier: tt, config: config})
}

type topologyTransmogrifierMsgSetActiveConnections struct {
	actor.MsgSyncQuery
	*TopologyTransmogrifier
	servers map[common.RMId]*sconn.ServerConnection
}

func (msg *topologyTransmogrifierMsgSetActiveConnections) Exec() (bool, error) {
	defer msg.MustClose()
	msg.activeConnections = msg.servers

	msg.hostToConnection = make(map[string]*sconn.ServerConnection, len(msg.activeConnections))
	for _, cd := range msg.activeConnections {
		msg.hostToConnection[cd.Host] = cd
	}

	return msg.maybeTick()
}

func (tt *TopologyTransmogrifier) ConnectedRMs(conns map[common.RMId]*sconn.ServerConnection) {
	msg := &topologyTransmogrifierMsgSetActiveConnections{TopologyTransmogrifier: tt, servers: conns}
	msg.InitMsg(tt)
	tt.EnqueueMsg(msg)
}

func (tt *TopologyTransmogrifier) ConnectionLost(rmId common.RMId, conns map[common.RMId]*sconn.ServerConnection) {
	msg := &topologyTransmogrifierMsgSetActiveConnections{TopologyTransmogrifier: tt, servers: conns}
	msg.InitMsg(tt)
	tt.EnqueueMsg(msg)
}

func (tt *TopologyTransmogrifier) ConnectionEstablished(conn *sconn.ServerConnection, conns map[common.RMId]*sconn.ServerConnection, done func()) {
	msg := &topologyTransmogrifierMsgSetActiveConnections{TopologyTransmogrifier: tt, servers: conns}
	msg.InitMsg(tt)
	if tt.EnqueueMsg(msg) {
		go func() {
			msg.Wait()
			done()
		}()
	} else {
		done()
	}
}

type topologyTransmogrifierMsgTopologyObserved struct {
	*TopologyTransmogrifier
	topology *configuration.Topology
}

func (msg topologyTransmogrifierMsgTopologyObserved) Exec() (bool, error) {
	utils.DebugLog(msg.inner.Logger, "debug", "New topology observed.", "topology", msg.topology)
	return msg.setActiveTopology(msg.topology)
}

type topologyTransmogrifierMsgRunTransaction struct {
	*transmogrificationTask
	task    Task
	backoff *binarybackoff.BinaryBackoffEngine
	txn     *msgs.Txn
	target  *configuration.Topology
	active  common.RMIds
	passive common.RMIds
}

func (msg *topologyTransmogrifierMsgRunTransaction) Exec() (bool, error) {
	if msg.runTxnMsg != msg || msg.currentTask != msg.task {
		return false, nil
	}
	go msg.runTxn()
	msg.backoff.Advance()
	msg.backoff.After(func() { msg.EnqueueMsg(msg) })
	return false, nil
}

func (msg *topologyTransmogrifierMsgRunTransaction) runTxn() {
	topologyBadRead, txnId, resubmit, err := msg.submitTopologyTransaction(msg.txn, nil, msg.active, msg.passive)
	switch {
	case err != nil:
		msg.EnqueueFuncAsync(func() (bool, error) { return false, err })
	case resubmit:
		// do nothing - just rely on the backoff to resubmit the txn
	default:
		// either it's commit or rerun-abort-badread
		msg.EnqueueFuncAsync(func() (bool, error) {
			if msg.currentTask == msg.task && msg.runTxnMsg == msg {
				msg.runTxnMsg = nil
			}
			// Basically, we don't really care about the potential for
			// calls to setActiveTopology to be "out of order" wrt the
			// real topology. This is because the worst that will happen
			// is that another txn will be run against an old topology,
			// which will fail with badRead, so eventually everything
			// should sort itself out.
			if topologyBadRead == nil { // it committed
				target := msg.target.Clone()
				target.DBVersion = txnId
				return msg.setActiveTopology(target)
			} else {
				return msg.setActiveTopology(topologyBadRead)
			}
		})
	}
}

type topologyTransmogrifierMsgCreateRoots struct {
	*transmogrificationTask
	task          *installTargetOld
	backoff       *binarybackoff.BinaryBackoffEngine
	rootsRequired int
	target        *configuration.Topology
}

func (msg *topologyTransmogrifierMsgCreateRoots) Exec() (bool, error) {
	if msg.runTxnMsg != msg || msg.currentTask != msg.task {
		return false, nil
	}
	go msg.runTxn()
	msg.backoff.Advance()
	msg.backoff.After(func() { msg.EnqueueMsg(msg) })
	return false, nil
}

func (msg *topologyTransmogrifierMsgCreateRoots) runTxn() {
	resubmit, roots, err := msg.attemptCreateRoots(msg.rootsRequired)
	switch {
	case err != nil:
		msg.EnqueueFuncAsync(func() (bool, error) { return false, err })
	case resubmit:
		// do nothing - just rely on the backoff to resubmit the txn
	default:
		// seeing as we're just creating objs, this must be a commit.
		msg.target.RootVarUUIds = append(msg.target.RootVarUUIds, roots...)
		msg.EnqueueFuncAsync(func() (bool, error) {
			if msg.currentTask == msg.task && msg.runTxnMsg == msg {
				msg.runTxnMsg = nil
				return msg.task.installTargetOld(msg.target)
			}
			return false, nil
		})
	}
}

type topologyTransmogrifierMsgAddSubscription struct {
	*transmogrificationTask
	task    *subscribe
	backoff *binarybackoff.BinaryBackoffEngine
	txn     *msgs.Txn
	target  *configuration.Topology
	active  common.RMIds
	passive common.RMIds
	client.VerClock
}

func (msg *topologyTransmogrifierMsgAddSubscription) Exec() (bool, error) {
	if msg.subscriptionMsg != msg || msg.runTxnMsg != msg || msg.currentTask != msg.task {
		return false, nil
	}
	go msg.runTxn()
	msg.backoff.Advance()
	msg.backoff.After(func() { msg.EnqueueMsg(msg) })
	return false, nil
}

func (msg *topologyTransmogrifierMsgAddSubscription) runTxn() {
	topologyBadRead, txnId, resubmit, err := msg.submitTopologyTransaction(msg.txn, msg.SubscriptionConsumer, msg.active, msg.passive)
	switch {
	case err != nil:
		msg.EnqueueFuncAsync(func() (bool, error) { return false, err })
	case resubmit:
		// do nothing - just rely on the backoff to resubmit the txn
	default:
		// either it's commit or rerun-abort-badread
		msg.EnqueueFuncAsync(func() (bool, error) {
			if msg.subscriptionMsg != msg || msg.runTxnMsg != msg || msg.currentTask != msg.task {
				return false, nil
			}
			if topologyBadRead == nil {
				msg.runTxnMsg = nil
				msg.subscribed = true
				target := msg.target.Clone()
				target.DBVersion = txnId
				return msg.setActiveTopology(target)
			} else {
				return msg.setActiveTopology(topologyBadRead)
			}
		})
	}
}

func (msg *topologyTransmogrifierMsgAddSubscription) SubscriptionConsumer(sm *client.SubscriptionManager, txn *txnreader.TxnReader, outcome *msgs.Outcome) error {
	// here we are in the localConnection thread
	actions := txn.Actions(true).Actions()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		vUUId := common.MakeVarUUId(action.VarId())
		if configuration.TopologyVarUUId.Compare(vUUId) != common.EQ {
			continue
		}
		if value := action.Value(); value.Which() != msgs.ACTIONVALUE_EXISTING {
			continue
		} else if modify := value.Existing().Modify(); modify.Which() != msgs.ACTIONVALUEEXISTINGMODIFY_WRITE {
			continue
		} else {
			txnId := txn.Id
			clock := vectorclock.VectorClockFromData(outcome.Commit(), false)
			clockElem := clock.At(vUUId)
			cmp := msg.Version.Compare(txnId)
			if clockElem > msg.ClockElem || (clockElem == msg.ClockElem && cmp == common.LT) {
				write := modify.Write()
				value := write.Value()
				refs := write.References()
				topology, err := configuration.TopologyFromCap(txnId, &refs, value)
				msg.EnqueueFuncAsync(func() (bool, error) {
					if err != nil {
						return false, err
					} else if msg.subscriptionMsg != msg {
						// TODO: unsubscribe
						return false, nil
					} else {
						return msg.setActiveTopology(topology)
					}
				})
			}
		}

		break
	}

	return nil
}
