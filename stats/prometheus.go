package stats

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/go-kit/kit/log"
	cc "github.com/msackman/chancell"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"goshawkdb.io/common"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/network"
	eng "goshawkdb.io/server/txnengine"
	"net"
	"net/http"
	"sync"
)

type PrometheusListener struct {
	logger              log.Logger
	cellTail            *cc.ChanCellTail
	enqueueQueryInner   func(prometheusListenerMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan           <-chan prometheusListenerMsg
	connectionManager   *network.ConnectionManager
	listener            *net.TCPListener
	topology            *configuration.Topology
	topologyLock        *sync.RWMutex
	clientConnsGaugeVec *prometheus.GaugeVec
	serverConnsGaugeVec *prometheus.GaugeVec
}

type prometheusListenerMsg interface {
	prometheusListenerMsgWitness()
}

type prometheusListenerMsgAcceptError struct {
	listener *net.TCPListener
	err      error
}

func (lae prometheusListenerMsgAcceptError) prometheusListenerMsgWitness() {}

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

func NewPrometheusListener(listenPort uint16, cm *network.ConnectionManager, logger log.Logger) (*PrometheusListener, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%v", listenPort))
	if err != nil {
		return nil, err
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}
	l := &PrometheusListener{
		logger:            log.NewContext(logger).With("subsystem", "prometheusListener"),
		listener:          ln,
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
	return l, nil
}

func (l *PrometheusListener) actorLoop(head *cc.ChanCellHead) {
	l.initMetrics()

	topology := l.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, l)
	defer l.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, l)
	l.putTopology(topology)
	go l.acceptLoop()

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
			case prometheusListenerMsgAcceptError:
				if msgT.listener == l.listener {
					err = msgT.err
				}
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
	l.listener.Close()
}

func (l *PrometheusListener) putTopology(topology *configuration.Topology) {
	l.topologyLock.Lock()
	l.topology = topology
	l.topologyLock.Unlock()

	clientConnsGauge := l.clientConnsGaugeVec.With(prometheus.Labels{"ClusterId": topology.ClusterId, "RMId": fmt.Sprint(l.connectionManager.RMId)})

	serverConnsGauge := l.serverConnsGaugeVec.With(prometheus.Labels{"ClusterId": topology.ClusterId, "RMId": fmt.Sprint(l.connectionManager.RMId)})

	l.connectionManager.SetGauges(clientConnsGauge, serverConnsGauge)
}

func (l *PrometheusListener) getTopology() *configuration.Topology {
	l.topologyLock.RLock()
	defer l.topologyLock.RUnlock()
	return l.topology
}

func (l *PrometheusListener) acceptLoop() {
	nodeCertPrivKeyPair := l.connectionManager.NodeCertificatePrivateKeyPair()
	roots := x509.NewCertPool()
	roots.AddCert(nodeCertPrivKeyPair.CertificateRoot)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{
			tls.Certificate{
				Certificate: [][]byte{nodeCertPrivKeyPair.Certificate},
				PrivateKey:  nodeCertPrivKeyPair.PrivateKey,
			},
		},
		CipherSuites:             []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
		ClientCAs:                roots,
		RootCAs:                  roots,
		ClientAuth:               tls.RequireAnyClientCert,
		NextProtos:               []string{"h2", "http/1.1"},
	}

	promHandler := promhttp.Handler()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, _, _ := l.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			promHandler.ServeHTTP(w, req)
		} else {
			l.logger.Log("type", "client", "authentication", "failure")
			w.WriteHeader(http.StatusForbidden)
		}
	})

	s := &http.Server{
		ReadTimeout:  common.HeartbeatInterval * 2,
		WriteTimeout: common.HeartbeatInterval * 2,
		Handler:      mux,
	}

	listener := l.listener
	tlsListener := tls.NewListener(&wrappedPrometheusListener{TCPListener: listener}, tlsConfig)
	l.enqueueQuery(prometheusListenerMsgAcceptError{
		err:      s.Serve(tlsListener),
		listener: listener,
	})
}

type wrappedPrometheusListener struct {
	*net.TCPListener
}

func (pl *wrappedPrometheusListener) Accept() (net.Conn, error) {
	socket, err := pl.AcceptTCP()
	if err != nil {
		return nil, err
	}
	if err = common.ConfigureSocket(socket); err != nil {
		return nil, err
	}
	return socket, nil
}

func (l *PrometheusListener) initMetrics() {
	l.clientConnsGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: common.ProductName,
		Name:      "client_connections",
		Help:      "Current count of live client connections",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.clientConnsGaugeVec)
	l.serverConnsGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: common.ProductName,
		Name:      "server_connections",
		Help:      "Current count of live server connections",
	}, []string{"ClusterId", "RMId"})
	prometheus.MustRegister(l.serverConnsGaugeVec)
}
