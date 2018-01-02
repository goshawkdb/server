package topologytransmogrifier

import (
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/binarybackoff"
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
	topologyBadRead, resubmit, err := msg.submitTopologyTransaction(msg.txn, msg.active, msg.passive)
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
				return msg.setActiveTopology(msg.target)
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
	task    Task
	backoff *binarybackoff.BinaryBackoffEngine
	txn     *msgs.Txn
	target  *configuration.Topology
	active  common.RMIds
	passive common.RMIds
}

func (msg *topologyTransmogrifierMsgAddSubscription) Exec() (bool, error) {
	if msg.runTxnMsg != msg || msg.currentTask != msg.task {
		return false, nil
	}
	go msg.runTxn()
	msg.backoff.Advance()
	msg.backoff.After(func() { msg.EnqueueMsg(msg) })
	return false, nil
}

func (msg *topologyTransmogrifierMsgAddSubscription) runTxn() {
	topologyBadRead, resubmit, err := msg.submitTopologyTransaction(msg.txn, msg.active, msg.passive)
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
			if topologyBadRead == nil {
				msg.subscribed = true
				return msg.setActiveTopology(msg.target)
			} else {
				return msg.setActiveTopology(topologyBadRead)
			}
		})
	}
}
