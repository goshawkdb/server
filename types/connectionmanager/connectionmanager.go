package connectionmanager

import (
	"github.com/prometheus/client_golang/prometheus"
	"goshawkdb.io/common"
	"goshawkdb.io/common/certs"
	"goshawkdb.io/server/configuration"
	cconn "goshawkdb.io/server/types/connections/client"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/types/topology"
)

type ConnectionManager interface {
	sconn.ServerConnectionPublisher
	topology.TopologyPublisher

	ClientEstablished(connNumber uint32, conn cconn.ClientConnection) (map[common.RMId]*sconn.ServerConnection, *cconn.ClientTxnMetrics)
	ClientLost(connNumber uint32, conn cconn.ClientConnection)
	GetClient(bootNumber, connNumber uint32) cconn.ClientConnection

	ServerEstablished(server *sconn.ServerConnection)
	ServerLost(server *sconn.ServerConnection, restarting bool)

	LocalHost() string

	ServerConnectionFlushed(sender common.RMId)

	SetTopology(topology *configuration.Topology, callbacks map[topology.TopologyChangeSubscriberType]func(), localhost string, remoteHosts []string)

	NodeCertificatePrivateKeyPair() *certs.NodeCertificatePrivateKeyPair

	SetMetrics(client, server prometheus.Gauge, clientTxnMetrics *cconn.ClientTxnMetrics)
}
