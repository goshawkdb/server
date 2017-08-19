package connectionmanager

import (
	"goshawkdb.io/common"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/types/connections/client"
	"goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/types/topology"
)

type ConnectionManager interface {
	server.ServerConnectionPublisher
	topology.TopologyPublisher
	ClientEstablished(connNumber uint32, conn client.ClientConnection) (map[common.RMId]server.ServerConnection, *client.ClientTxnMetrics)
	ClientLost(connNumber uint32, conn client.ClientConnection)
	GetClient(bootNumber, connNumber uint32) client.ClientConnection
	ServerConnectionFlushed(sender common.RMId)
	SetTopology(topology *configuration.Topology, callbacks map[topology.TopologyChangeSubscriberType]func(), localhost string, remoteHosts []string)
}
