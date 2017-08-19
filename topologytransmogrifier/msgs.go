package topologyTransmogrifier

import (
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
)

type topologyTransmogrifierMsgRequestConfigChange struct {
	*TopologyTransmogrifier
	config *configuration.Configuration
}

func (msg *topologyTransmogrifierMsgRequestConfigChange) Exec() (bool, error) {
	utils.DebugLog(msg.inner.Logger, "debug", "Topology change request.", "config", msg.config)
	nonFatalErr := msg.setTarget(&configuration.NextConfiguration{Configuration: msg.config})
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
	servers map[common.RMId]sconn.ServerConnection
}

func (msg *topologyTransmogrifierMsgSetActiveConnections) Exec() (bool, error) {
	defer msg.MustClose()
	msg.activeConnections = msg.servers

	msg.hostToConnection = make(map[string]sconn.ServerConnection, len(msg.activeConnections))
	for _, cd := range msg.activeConnections {
		msg.hostToConnection[cd.Host()] = cd
	}

	return msg.maybeTick()
}

func (tt *TopologyTransmogrifier) ConnectedRMs(conns map[common.RMId]sconn.ServerConnection) {
	msg := &topologyTransmogrifierMsgSetActiveConnections{TopologyTransmogrifier: tt, servers: conns}
	msg.InitMsg(tt)
	tt.EnqueueMsg(msg)
}

func (tt *TopologyTransmogrifier) ConnectionLost(rmId common.RMId, conns map[common.RMId]sconn.ServerConnection) {
	msg := &topologyTransmogrifierMsgSetActiveConnections{TopologyTransmogrifier: tt, servers: conns}
	msg.InitMsg(tt)
	tt.EnqueueMsg(msg)
}

func (tt *TopologyTransmogrifier) ConnectionEstablished(rmId common.RMId, conn sconn.ServerConnection, conns map[common.RMId]sconn.ServerConnection, done func()) {
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
	backoff *utils.BinaryBackoffEngine
	txn     *msgs.Txn
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
	_, resubmit, err := msg.rewriteTopology(msg.txn, msg.active, msg.passive)
	switch {
	case err != nil:
		msg.EnqueueFuncAsync(func() (bool, error) { return false, err })
	case resubmit:
		// do nothing - just rely on the backoff to resubmit the txn
	default:
		// either it's commit or rerun-abort-badread in which case we
		// should receive the updated topology via the subscriber.
		msg.EnqueueFuncAsync(func() (bool, error) {
			if msg.runTxnMsg == msg {
				msg.runTxnMsg = nil
			}
			return false, nil
		})
	}
}

type topologyTransmogrifierMsgCreateRoots struct {
	*transmogrificationTask
	task           *installTargetOld
	backoff        *utils.BinaryBackoffEngine
	rootsRequired  int
	targetTopology *configuration.Topology
	active         common.RMIds
	passive        common.RMIds
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
		msg.targetTopology.RootVarUUIds = append(msg.targetTopology.RootVarUUIds, roots...)
		msg.EnqueueFuncAsync(func() (bool, error) {
			if msg.currentTask == msg.task && msg.runTxnMsg == msg {
				return msg.task.installTargetOld(msg.targetTopology, msg.active, msg.passive)
			}
			return false, nil
		})
	}
}
