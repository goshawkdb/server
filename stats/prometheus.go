package stats

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	ghttp "goshawkdb.io/server/network/http"
	"goshawkdb.io/server/paxos"
	"goshawkdb.io/server/router"
	"goshawkdb.io/server/types/connectionmanager"
	cconn "goshawkdb.io/server/types/connections/client"
	"goshawkdb.io/server/types/topology"
	"net/http"
	"sync"
)

type PrometheusListener struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	self                common.RMId
	connectionManager   connectionmanager.ConnectionManager
	router              *router.Router
	mux                 *ghttp.HttpListenerWithMux
	topology            *configuration.Topology
	topologyLock        sync.RWMutex
	clientConnsVec      *prometheus.GaugeVec
	serverConnsVec      *prometheus.GaugeVec
	txnSubmitVec        *prometheus.CounterVec
	txnLatencyVec       *prometheus.HistogramVec
	txnResubmitVec      *prometheus.CounterVec
	txnRerunVec         *prometheus.CounterVec
	acceptorLifespanVec *prometheus.HistogramVec
	acceptorsVec        *prometheus.GaugeVec
	proposerLifespanVec *prometheus.HistogramVec
	proposersVec        *prometheus.GaugeVec

	inner prometheusListenerInner
}

type prometheusListenerInner struct {
	*PrometheusListener
	*actor.BasicServerInner
}

func NewPrometheusListener(mux *ghttp.HttpListenerWithMux, rmId common.RMId, cm connectionmanager.ConnectionManager, router *router.Router, logger log.Logger) *PrometheusListener {
	pl := &PrometheusListener{
		self:              rmId,
		connectionManager: cm,
		router:            router,
		mux:               mux,
	}

	pli := &pl.inner
	pli.PrometheusListener = pl
	pli.BasicServerInner = actor.NewBasicServerInner(log.With(logger, "subsystem", "prometheusListener"))

	_, err := actor.Spawn(pli)
	if err != nil {
		panic(err) // "impossible"
	}

	return pl
}

type prometheusListenerMsgTopologyChanged struct {
	actor.MsgSyncQuery
	*PrometheusListener
	topology *configuration.Topology
}

func (msg prometheusListenerMsgTopologyChanged) Exec() (bool, error) {
	defer msg.MustClose()
	msg.putTopology(msg.topology)
	return false, nil
}

func (pl *PrometheusListener) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &prometheusListenerMsgTopologyChanged{PrometheusListener: pl, topology: topology}
	msg.InitMsg(pl)
	if pl.EnqueueMsg(msg) {
		go func() { done(msg.Wait()) }()
	} else {
		done(false)
	}
}

func (pl *prometheusListenerInner) Init(self *actor.Actor) (bool, error) {
	terminate, err := pl.BasicServerInner.Init(self)
	if terminate || err != nil {
		return terminate, err
	}

	pl.Mailbox = self.Mailbox
	pl.BasicServerOuter = actor.NewBasicServerOuter(self.Mailbox)

	pl.initMetrics()

	topology := pl.connectionManager.AddTopologySubscriber(topology.ConnectionSubscriber, pl)
	pl.putTopology(topology)
	pl.configureMux()

	return false, nil
}

func (pl *prometheusListenerInner) HandleShutdown(err error) bool {
	pl.connectionManager.RemoveTopologySubscriberAsync(topology.ConnectionSubscriber, pl)
	return pl.BasicServerInner.HandleShutdown(err)
}

func (pl *PrometheusListener) putTopology(topology *configuration.Topology) {
	pl.topologyLock.Lock()
	pl.topology = topology
	pl.topologyLock.Unlock()

	if topology == nil {
		return
	}

	labels := prometheus.Labels{
		"ClusterId": topology.ClusterId,
		"RMId":      fmt.Sprint(pl.self),
	}

	clientConns := pl.clientConnsVec.With(labels)
	serverConns := pl.serverConnsVec.With(labels)

	txnSubmit := pl.txnSubmitVec.With(labels)
	txnLatency := pl.txnLatencyVec.With(labels)
	txnResubmit := pl.txnResubmitVec.With(labels)
	txnRerun := pl.txnRerunVec.With(labels)

	pl.connectionManager.SetMetrics(clientConns, serverConns,
		&cconn.ClientTxnMetrics{
			TxnSubmit:   txnSubmit,
			TxnLatency:  txnLatency,
			TxnResubmit: txnResubmit,
			TxnRerun:    txnRerun,
		})

	acceptorsGauge := pl.acceptorsVec.With(labels)
	acceptorLifespan := pl.acceptorLifespanVec.With(labels)
	pl.router.AcceptorDispatcher.SetMetrics(
		&paxos.AcceptorMetrics{
			Gauge:    acceptorsGauge,
			Lifespan: acceptorLifespan,
		})

	proposersGauge := pl.proposersVec.With(labels)
	proposerLifespan := pl.proposerLifespanVec.With(labels)
	pl.router.ProposerDispatcher.SetMetrics(
		&paxos.ProposerMetrics{
			Gauge:    proposersGauge,
			Lifespan: proposerLifespan,
		})
}

func (pl *PrometheusListener) getTopology() *configuration.Topology {
	pl.topologyLock.RLock()
	defer pl.topologyLock.RUnlock()
	return pl.topology
}

func (pl *PrometheusListener) configureMux() {
	promHandler := promhttp.Handler()

	pl.mux.HandleFunc(fmt.Sprintf("/%s", server.MetricsRootName), func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, _, roots := pl.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			if cap, found := roots[server.MetricsRootName]; found {
				if cap.CanRead() {
					promHandler.ServeHTTP(w, req)
					return
				}
			}
		}
		pl.inner.Logger.Log("type", "client", "authentication", "failure")
		w.WriteHeader(http.StatusForbidden)
	})

	pl.mux.Done()
}

func (pl *PrometheusListener) initMetrics() {
	// cm
	pl.clientConnsVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: common.ProductName,
		Name:      "client_connections_count",
		Help:      "Current count of live client connections.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(pl.clientConnsVec)

	pl.serverConnsVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: common.ProductName,
		Name:      "server_connections_count",
		Help:      "Current count of live server connections.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(pl.serverConnsVec)

	// txns
	pl.txnSubmitVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: common.ProductName,
		Name:      "transaction_client_submit_count",
		Help:      "Number of transactions submitted by clients.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(pl.txnSubmitVec)

	pl.txnLatencyVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: common.ProductName,
		Name:      "transaction_submit_duration_seconds",
		Help:      "Time taken to determine transaction outcome.",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 1.2, 55),
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(pl.txnLatencyVec)

	pl.txnResubmitVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: common.ProductName,
		Name:      "transaction_internal_submit_count",
		Help:      "Number of transactions submitted internally.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(pl.txnResubmitVec)

	pl.txnRerunVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: common.ProductName,
		Name:      "transaction_rerun_count",
		Help:      "Number of times each transaction is returned to the client to be rerun.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(pl.txnRerunVec)

	// acceptors
	pl.acceptorsVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: common.ProductName,
		Name:      "acceptors_count",
		Help:      "Current count of acceptors.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(pl.acceptorsVec)

	pl.acceptorLifespanVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: common.ProductName,
		Name:      "acceptor_life_duration_seconds",
		Help:      "Duration for which each acceptor is alive.",
		Buckets:   prometheus.ExponentialBuckets(0.002, 1.2, 55),
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(pl.acceptorLifespanVec)

	// proposers
	pl.proposersVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: common.ProductName,
		Name:      "proposers_count",
		Help:      "Current count of proposers.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(pl.proposersVec)

	pl.proposerLifespanVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: common.ProductName,
		Name:      "proposer_life_duration_seconds",
		Help:      "Duration for which each proposer is alive.",
		Buckets:   prometheus.ExponentialBuckets(0.002, 1.2, 55),
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(pl.proposerLifespanVec)
}
