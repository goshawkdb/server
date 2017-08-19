package client

import (
	"github.com/prometheus/client_golang/prometheus"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
)

type ClientTxnMetrics struct {
	TxnSubmit   prometheus.Counter
	TxnLatency  prometheus.Observer
	TxnResubmit prometheus.Counter
	TxnRerun    prometheus.Counter
}

type ClientConnection interface {
	actor.ShutdownableActor
	server.ServerConnectionSubscriber
	Status(*utils.StatusConsumer)
	SubmissionOutcomeReceived(common.RMId, *utils.TxnReader, *msgs.Outcome)
}
