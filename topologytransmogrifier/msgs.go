package topologyTransmogrifier

import (
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
)

type topologyTransmogrifierMsgRequestConfigChange struct {
	*TopologyTransmogrifier
	config *configuration.Configuration
}

func (msg *topologyTransmogrifierMsgRequestConfigChange) Exec() (bool, error) {
	server.DebugLog(msg.inner.Logger, "debug", "Topology change request.", "config", msg.config)
	nonFatalErr := msg.selectGoal(&configuration.NextConfiguration{Configuration: msg.config})
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
	servers map[common.RMId]paxos.Connection
}

func (msg *topologyTransmogrifierMsgSetActiveConnections) Exec() (bool, error) {
	defer msg.MustClose()
	msg.activeConnections = msg.servers

	msg.hostToConnection = make(map[string]paxos.Connection, len(msg.activeConnections))
	for _, cd := range msg.activeConnections {
		msg.hostToConnection[cd.Host()] = cd
	}

	return msg.maybeTick()
}

func (tt *TopologyTransmogrifier) ConnectedRMs(conns map[common.RMId]paxos.Connection) {
	msg := &topologyTransmogrifierMsgSetActiveConnections{TopologyTransmogrifier: tt, servers: conns}
	msg.InitMsg(tt)
	tt.EnqueueMsg(msg)
}

func (tt *TopologyTransmogrifier) ConnectionLost(rmId common.RMId, conns map[common.RMId]paxos.Connection) {
	msg := &topologyTransmogrifierMsgSetActiveConnections{TopologyTransmogrifier: tt, servers: conns}
	msg.InitMsg(tt)
	tt.EnqueueMsg(msg)
}

func (tt *TopologyTransmogrifier) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, conns map[common.RMId]paxos.Connection, done func()) {
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
