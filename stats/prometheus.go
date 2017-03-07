package stats

import (
	"fmt"
	"github.com/go-kit/kit/log"
	cc "github.com/msackman/chancell"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/network"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"net/http"
	"sync"
)

type PrometheusListener struct {
	logger              log.Logger
	cellTail            *cc.ChanCellTail
	enqueueQueryInner   func(prometheusListenerMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan           <-chan prometheusListenerMsg
	connectionManager   *network.ConnectionManager
	mux                 *network.HttpListenerWithMux
	topology            *configuration.Topology
	topologyLock        *sync.RWMutex
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
}

type prometheusListenerMsg interface {
	prometheusListenerMsgWitness()
}

type prometheusListenerMsgShutdown struct{}

func (lms *prometheusListenerMsgShutdown) prometheusListenerMsgWitness() {}

var prometheusListenerMsgShutdownInst = &prometheusListenerMsgShutdown{}

func (l *PrometheusListener) Shutdown() {
	if l.enqueueQuery(prometheusListenerMsgShutdownInst) {
		l.cellTail.Wait()
	}
}

type prometheusListenerMsgTopologyChanged struct {
	topology   *configuration.Topology
	resultChan chan struct{}
}

func (lmtc *prometheusListenerMsgTopologyChanged) prometheusListenerMsgWitness() {}

func (lmtc *prometheusListenerMsgTopologyChanged) maybeClose() {
	select {
	case <-lmtc.resultChan:
	default:
		close(lmtc.resultChan)
	}
}

func (l *PrometheusListener) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &prometheusListenerMsgTopologyChanged{
		resultChan: make(chan struct{}),
		topology:   topology,
	}
	if l.enqueueQuery(msg) {
		go func() {
			select {
			case <-msg.resultChan:
			case <-l.cellTail.Terminated:
			}
			done(true)
		}()
	} else {
		done(true)
	}
}

type prometheusListenerQueryCapture struct {
	wl  *PrometheusListener
	msg prometheusListenerMsg
}

func (wlqc *prometheusListenerQueryCapture) ccc(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
	return wlqc.wl.enqueueQueryInner(wlqc.msg, cell, wlqc.ccc)
}

func (l *PrometheusListener) enqueueQuery(msg prometheusListenerMsg) bool {
	wlqc := &prometheusListenerQueryCapture{wl: l, msg: msg}
	return l.cellTail.WithCell(wlqc.ccc)
}

func NewPrometheusListener(mux *network.HttpListenerWithMux, cm *network.ConnectionManager, logger log.Logger) *PrometheusListener {
	l := &PrometheusListener{
		logger:            log.With(logger, "subsystem", "prometheusListener"),
		mux:               mux,
		connectionManager: cm,
		topologyLock:      new(sync.RWMutex),
	}
	var head *cc.ChanCellHead
	head, l.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan prometheusListenerMsg, n)
			cell.Open = func() { l.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			l.enqueueQueryInner = func(msg prometheusListenerMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
				if curCell == cell {
					select {
					case queryChan <- msg:
						return true, nil
					default:
						return false, nil
					}
				} else {
					return false, cont
				}
			}
		})

	go l.actorLoop(head)
	return l
}

func (l *PrometheusListener) actorLoop(head *cc.ChanCellHead) {
	l.initMetrics()

	topology := l.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, l)
	defer l.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, l)
	l.putTopology(topology)
	l.configureMux()

	var (
		err       error
		queryChan <-chan prometheusListenerMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = l.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		if msg, ok := <-queryChan; ok {
			switch msgT := msg.(type) {
			case *prometheusListenerMsgShutdown:
				terminate = true
			case *prometheusListenerMsgTopologyChanged:
				l.putTopology(msgT.topology)
				msgT.maybeClose()
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		l.logger.Log("msg", "Fatal error.", "error", err)
	}
	l.cellTail.Terminate()
}

func (l *PrometheusListener) putTopology(topology *configuration.Topology) {
	l.topologyLock.Lock()
	l.topology = topology
	l.topologyLock.Unlock()

	labels := prometheus.Labels{
		"ClusterId": topology.ClusterId,
		"RMId":      fmt.Sprint(l.connectionManager.RMId),
	}

	clientConns := l.clientConnsVec.With(labels)
	serverConns := l.serverConnsVec.With(labels)

	txnSubmit := l.txnSubmitVec.With(labels)
	txnLatency := l.txnLatencyVec.With(labels)
	txnResubmit := l.txnResubmitVec.With(labels)
	txnRerun := l.txnRerunVec.With(labels)

	l.connectionManager.SetMetrics(clientConns, serverConns,
		&paxos.ClientTxnMetrics{
			TxnSubmit:   txnSubmit,
			TxnLatency:  txnLatency,
			TxnResubmit: txnResubmit,
			TxnRerun:    txnRerun,
		})

	acceptorsGauge := l.acceptorsVec.With(labels)
	acceptorLifespan := l.acceptorLifespanVec.With(labels)
	l.connectionManager.Dispatchers.AcceptorDispatcher.SetMetrics(
		&paxos.AcceptorMetrics{
			Gauge:    acceptorsGauge,
			Lifespan: acceptorLifespan,
		})

	proposersGauge := l.proposersVec.With(labels)
	proposerLifespan := l.proposerLifespanVec.With(labels)
	l.connectionManager.Dispatchers.ProposerDispatcher.SetMetrics(
		&paxos.ProposerMetrics{
			Gauge:    proposersGauge,
			Lifespan: proposerLifespan,
		})
}

func (l *PrometheusListener) getTopology() *configuration.Topology {
	l.topologyLock.RLock()
	defer l.topologyLock.RUnlock()
	return l.topology
}

func (l *PrometheusListener) configureMux() {
	promHandler := promhttp.Handler()

	l.mux.HandleFunc(fmt.Sprintf("/%s", server.MetricsRootName), func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, _, roots := l.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			if _, found := roots[server.MetricsRootName]; found {
				promHandler.ServeHTTP(w, req)
				return
			}
		}
		l.logger.Log("type", "client", "authentication", "failure")
		w.WriteHeader(http.StatusForbidden)
	})

	l.mux.Done()
}

func (l *PrometheusListener) initMetrics() {
	// cm
	l.clientConnsVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: common.ProductName,
		Name:      "client_connections_count",
		Help:      "Current count of live client connections.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.clientConnsVec)

	l.serverConnsVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: common.ProductName,
		Name:      "server_connections_count",
		Help:      "Current count of live server connections.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.serverConnsVec)

	// txns
	l.txnSubmitVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: common.ProductName,
		Name:      "transaction_client_submit_count",
		Help:      "Number of transactions submitted by clients.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.txnSubmitVec)

	l.txnLatencyVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: common.ProductName,
		Name:      "transaction_submit_duration_seconds",
		Help:      "Time taken to determine transaction outcome.",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 1.2, 55),
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.txnLatencyVec)

	l.txnResubmitVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: common.ProductName,
		Name:      "transaction_internal_submit_count",
		Help:      "Number of transactions submitted internally.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.txnResubmitVec)

	l.txnRerunVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: common.ProductName,
		Name:      "transaction_rerun_count",
		Help:      "Number of times each transaction is returned to the client to be rerun.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.txnRerunVec)

	// acceptors
	l.acceptorsVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: common.ProductName,
		Name:      "acceptors_count",
		Help:      "Current count of acceptors.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.acceptorsVec)

	l.acceptorLifespanVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: common.ProductName,
		Name:      "acceptor_life_duration_seconds",
		Help:      "Duration for which each acceptor is alive.",
		Buckets:   prometheus.ExponentialBuckets(0.002, 1.2, 55),
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.acceptorLifespanVec)

	// proposers
	l.proposersVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: common.ProductName,
		Name:      "proposers_count",
		Help:      "Current count of proposers.",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.proposersVec)

	l.proposerLifespanVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: common.ProductName,
		Name:      "proposer_life_duration_seconds",
		Help:      "Duration for which each proposer is alive.",
		Buckets:   prometheus.ExponentialBuckets(0.002, 1.2, 55),
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.proposerLifespanVec)
}
