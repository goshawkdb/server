package paxos

import (
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	eng "goshawkdb.io/server/txnengine"
)

type ConnectionManager interface {
	ServerConnectionPublisher
	eng.TopologyPublisher
	ClientEstablished(connNumber uint32, conn ClientConnection) (map[common.RMId]Connection, *ClientTxnMetrics)
	ClientLost(connNumber uint32, conn ClientConnection)
	GetClient(bootNumber, connNumber uint32) ClientConnection
}

type ClientTxnMetrics struct {
	TxnSubmit   prometheus.Counter
	TxnLatency  prometheus.Observer
	TxnResubmit prometheus.Counter
	TxnRerun    prometheus.Counter
}

type ClientConnection interface {
	Shutdownable
	ServerConnectionSubscriber
	Status(*server.StatusConsumer)
	SubmissionOutcomeReceived(common.RMId, *eng.TxnReader, *msgs.Outcome)
}

type Shutdownable interface {
	ShutdownSync()
}
