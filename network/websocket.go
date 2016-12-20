package network

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/gorilla/websocket"
	cc "github.com/msackman/chancell"
	"github.com/tinylib/msgp/msgp"
	"goshawkdb.io/common"
	capcmsgs "goshawkdb.io/common/capnp"
	cmsgs "goshawkdb.io/common/msgpack"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type WebsocketListener struct {
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(websocketListenerMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan websocketListenerMsg
	connectionManager *ConnectionManager
	listener          *net.TCPListener
	topology          *configuration.Topology
	topologyLock      *sync.RWMutex
}

type websocketListenerMsg interface {
	websocketListenerMsgWitness()
}

type websocketListenerMsgAcceptError struct {
	listener *net.TCPListener
	err      error
}

func (lae websocketListenerMsgAcceptError) websocketListenerMsgWitness() {}

type websocketListenerMsgShutdown struct{}

func (lms *websocketListenerMsgShutdown) websocketListenerMsgWitness() {}

var websocketListenerMsgShutdownInst = &websocketListenerMsgShutdown{}

func (l *WebsocketListener) Shutdown() {
	if l.enqueueQuery(websocketListenerMsgShutdownInst) {
		l.cellTail.Wait()
	}
}

type websocketListenerMsgTopologyChanged struct {
	topology   *configuration.Topology
	resultChan chan struct{}
}

func (lmtc *websocketListenerMsgTopologyChanged) websocketListenerMsgWitness() {}

func (lmtc *websocketListenerMsgTopologyChanged) maybeClose() {
	select {
	case <-lmtc.resultChan:
	default:
		close(lmtc.resultChan)
	}
}

func (l *WebsocketListener) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &websocketListenerMsgTopologyChanged{
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

func (l *WebsocketListener) enqueueQuery(msg websocketListenerMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return l.enqueueQueryInner(msg, cell, f)
	}
	return l.cellTail.WithCell(f)
}

func NewWebsocketListener(listenPort uint16, cm *ConnectionManager) (*WebsocketListener, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%v", listenPort))
	if err != nil {
		return nil, err
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}
	l := &WebsocketListener{
		connectionManager: cm,
		listener:          ln,
		topologyLock:      new(sync.RWMutex),
	}
	var head *cc.ChanCellHead
	head, l.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan websocketListenerMsg, n)
			cell.Open = func() { l.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			l.enqueueQueryInner = func(msg websocketListenerMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
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

func (l *WebsocketListener) actorLoop(head *cc.ChanCellHead) {
	topology := l.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, l)
	defer l.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, l)
	l.putTopology(topology)
	go l.acceptLoop()

	var (
		err       error
		queryChan <-chan websocketListenerMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = l.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		if msg, ok := <-queryChan; ok {
			switch msgT := msg.(type) {
			case *websocketListenerMsgShutdown:
				terminate = true
			case websocketListenerMsgAcceptError:
				if msgT.listener == l.listener {
					err = msgT.err
				}
			case *websocketListenerMsgTopologyChanged:
				l.putTopology(msgT.topology)
				msgT.maybeClose()
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		log.Println("WSS Listen error:", err)
	}
	l.cellTail.Terminate()
	l.listener.Close()
}

func (l *WebsocketListener) putTopology(topology *configuration.Topology) {
	l.topologyLock.Lock()
	defer l.topologyLock.Unlock()
	l.topology = topology
}

func (l *WebsocketListener) getTopology() *configuration.Topology {
	l.topologyLock.RLock()
	defer l.topologyLock.RUnlock()
	return l.topology
}

func (l *WebsocketListener) acceptLoop() {
	nodeCertPrivKeyPair := l.connectionManager.NodeCertificatePrivateKeyPair
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

	connCount := uint32(0)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, _, _ := l.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte(fmt.Sprintf("GoshawkDB Server version %v. Websocket available at /ws", server.ServerVersion)))
		} else {
			log.Println("WSS Not authenticated!")
			w.WriteHeader(http.StatusForbidden)
		}
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, hashsum, roots := l.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			log.Printf("WSS User '%s' authenticated", hex.EncodeToString(hashsum[:]))
			connNumber := 2*atomic.AddUint32(&connCount, 1) + 1
			wsHandler(l.connectionManager, connNumber, w, req, peerCerts, roots)
		} else {
			log.Println("WSS Not authenticated!")
			w.WriteHeader(http.StatusForbidden)
		}
	})

	s := &http.Server{
		ReadTimeout:  common.HeartbeatInterval * 2,
		WriteTimeout: common.HeartbeatInterval * 2,
		Handler:      mux,
	}

	listener := l.listener
	tlsListener := tls.NewListener(&wrappedWebsocketListener{TCPListener: listener}, tlsConfig)
	l.enqueueQuery(websocketListenerMsgAcceptError{
		err:      s.Serve(tlsListener),
		listener: listener,
	})
}

type wrappedWebsocketListener struct {
	*net.TCPListener
}

func (wl *wrappedWebsocketListener) Accept() (net.Conn, error) {
	socket, err := wl.AcceptTCP()
	if err != nil {
		return nil, err
	}
	if err = common.ConfigureSocket(socket); err != nil {
		return nil, err
	}
	return socket, nil

}

var upgrader = websocket.Upgrader{
	// allow everything for now!
	CheckOrigin: func(r *http.Request) bool { return true },
}

func wsHandler(cm *ConnectionManager, connNumber uint32, w http.ResponseWriter, r *http.Request, peerCerts []*x509.Certificate, roots map[string]*common.Capability) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err == nil {
		log.Printf("WSS upgrade success for connection to %v", c.RemoteAddr())
		yesman := &wssMsgPackClient{
			remoteHost:        fmt.Sprintf("%v", c.RemoteAddr()),
			connectionNumber:  connNumber,
			socket:            c,
			peerCerts:         peerCerts,
			roots:             roots,
			connectionManager: cm,
		}
		NewConnectionWithHandshaker(yesman, cm)

	} else {
		log.Printf("WSS upgrade failure for connection to %v: %v", r.RemoteAddr, err)
	}
}

type wssMsgPackClient struct {
	*Connection
	remoteHost        string
	connectionNumber  uint32
	socket            *websocket.Conn
	peerCerts         []*x509.Certificate
	roots             map[string]*common.Capability
	rootsVar          map[common.VarUUId]*common.Capability
	namespace         []byte
	connectionManager *ConnectionManager
	topology          *configuration.Topology
	submitter         *client.ClientTxnSubmitter
	submitterIdle     *connectionMsgTopologyChanged
	reader            *socketReader
	beater            *wssBeater
}

func (wmpc *wssMsgPackClient) send(msg msgp.Encodable) error {
	wc, err := wmpc.socket.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}
	if err = msgp.Encode(wc, msg); err != nil {
		return err
	}
	return wc.Close()
}

func (wmpc *wssMsgPackClient) readOne(msg msgp.Decodable) error {
	msgType, reader, err := wmpc.socket.NextReader()
	switch {
	case err != nil:
		return err
	case msgType != websocket.BinaryMessage:
		return errors.New(fmt.Sprintf("WSS Connection requires binary messages only. Received msg of type %d", msgType))
	default:
		return msgp.Decode(reader, msg)
	}
}

func (wmpc *wssMsgPackClient) PerformHandshake(topology *configuration.Topology) (Protocol, error) {
	wmpc.topology = topology

	hello := &cmsgs.Hello{
		Product: common.ProductName,
		Version: common.ProductVersion,
	}
	if err := wmpc.send(hello); err != nil {
		return nil, err
	}
	hello = &cmsgs.Hello{}
	if err := wmpc.readOne(hello); err == nil {
		if wmpc.verifyHello(hello) {

			if wmpc.topology.ClusterUUId == 0 {
				return nil, errors.New("Cluster not yet formed")
			} else if len(wmpc.topology.Roots) == 0 {
				return nil, errors.New("No roots: cluster not yet formed")
			}

			return wmpc, wmpc.send(wmpc.makeHelloClient())
		} else {
			product := hello.Product
			if l := len(common.ProductName); len(product) > l {
				product = product[:l] + "..."
			}
			version := hello.Version
			if l := len(common.ProductVersion); len(version) > l {
				version = version[:l] + "..."
			}
			return nil, fmt.Errorf("WSS Connection: Received erroneous hello from peer: received product name '%s' (expected '%s'), product version '%s' (expected '%s')",
				product, common.ProductName, version, common.ProductVersion)
		}
	} else {
		return nil, err
	}
}

func (wmpc *wssMsgPackClient) verifyHello(hello *cmsgs.Hello) bool {
	return hello.Product == common.ProductName &&
		hello.Version == common.ProductVersion
}

func (wmpc *wssMsgPackClient) makeHelloClient() *cmsgs.HelloClientFromServer {
	namespace := make([]byte, common.KeyLen-8)
	binary.BigEndian.PutUint32(namespace[0:4], wmpc.connectionNumber)
	binary.BigEndian.PutUint32(namespace[4:8], wmpc.connectionManager.BootCount())
	binary.BigEndian.PutUint32(namespace[8:], uint32(wmpc.connectionManager.RMId))
	wmpc.namespace = namespace

	roots := make([]*cmsgs.Root, 0, len(wmpc.roots))
	rootsVar := make(map[common.VarUUId]*common.Capability, len(wmpc.roots))
	for idx, name := range wmpc.topology.Roots {
		if capability, found := wmpc.roots[name]; found {
			vUUId := wmpc.topology.RootVarUUIds[idx].VarUUId
			root := &cmsgs.Root{
				Name:       name,
				VarId:      vUUId[:],
				Capability: &cmsgs.Capability{},
			}
			root.Capability.FromCapnp(capability.Capability)
			roots = append(roots, root)
			rootsVar[*vUUId] = capability
		}
	}

	wmpc.rootsVar = rootsVar
	return &cmsgs.HelloClientFromServer{
		Namespace: namespace,
		Roots:     roots,
	}
}

func (wmpc *wssMsgPackClient) RestartDialer() Dialer {
	wmpc.InternalShutdown()
	return nil // client connections are never restarted
}

func (wmpc *wssMsgPackClient) Run(conn *Connection) error {
	wmpc.Connection = conn
	servers := wmpc.connectionManager.ClientEstablished(wmpc.connectionNumber, wmpc)
	if servers == nil {
		return errors.New("Not ready for client connections")

	} else {
		log.Printf("WSS Client connection established from %v\n", wmpc.remoteHost)

		wmpc.createBeater()
		wmpc.createReader()

		wmpc.submitter = client.NewClientTxnSubmitter(wmpc.connectionManager.RMId, wmpc.connectionManager.BootCount(), wmpc.rootsVar, wmpc.namespace, wmpc.connectionManager)
		wmpc.submitter.TopologyChanged(wmpc.topology)
		wmpc.submitter.ServerConnectionsChanged(servers)
		return nil
	}
}

func (wmpc *wssMsgPackClient) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	if si := wmpc.submitterIdle; si != nil {
		wmpc.submitterIdle = nil
		server.Log("WSS Connection", wmpc, "topologyChanged:", tc, "clearing old:", si)
		si.maybeClose()
	}

	topology := tc.topology
	wmpc.topology = topology

	if topology != nil {
		if authenticated, _, roots := wmpc.topology.VerifyPeerCerts(wmpc.peerCerts); !authenticated {
			server.Log("WSS Connection", wmpc, "topologyChanged", tc, "(client unauthed)")
			tc.maybeClose()
			return errors.New("WSS Client connection closed: No client certificate known")
		} else if len(roots) == len(wmpc.roots) {
			for name, capsOld := range wmpc.roots {
				if capsNew, found := roots[name]; !found || !capsNew.Equal(capsOld) {
					server.Log("WSS Connection", wmpc, "topologyChanged", tc, "(roots changed)")
					tc.maybeClose()
					return errors.New("WSS Client connection closed: roots have changed")
				}
			}
		} else {
			server.Log("WSS Connection", wmpc, "topologyChanged", tc, "(roots changed)")
			tc.maybeClose()
			return errors.New("WSS Client connection closed: roots have changed")
		}
	}
	if err := wmpc.submitter.TopologyChanged(topology); err != nil {
		tc.maybeClose()
		return err
	}
	if wmpc.submitter.IsIdle() {
		server.Log("WSS Connection", wmpc, "topologyChanged", tc, "(client, submitter is idle)")
		tc.maybeClose()
	} else {
		server.Log("WSS Connection", wmpc, "topologyChanged", tc, "(client, submitter not idle)")
		wmpc.submitterIdle = tc
	}

	return nil
}

func (wmpc *wssMsgPackClient) InternalShutdown() {
	if wmpc.reader != nil {
		wmpc.reader.stop()
		wmpc.reader = nil
	}
	if wmpc.submitter != nil {
		wmpc.submitter.Shutdown()
		wmpc.submitter = nil
	}
	if wmpc.beater != nil {
		wmpc.beater.stop()
		wmpc.beater = nil
	}
	if wmpc.socket != nil {
		wmpc.socket.Close()
		wmpc.socket = nil
	}
	wmpc.connectionManager.ClientLost(wmpc.connectionNumber, wmpc)
}

func (wmpc *wssMsgPackClient) SubmissionOutcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) {
	wmpc.enqueueQuery(connectionExec(func() error {
		return wmpc.outcomeReceived(sender, txn, outcome)
	}))
}

func (wmpc *wssMsgPackClient) outcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) error {
	err := wmpc.submitter.SubmissionOutcomeReceived(sender, txn, outcome)
	if wmpc.submitterIdle != nil && wmpc.submitter.IsIdle() {
		si := wmpc.submitterIdle
		wmpc.submitterIdle = nil
		server.Log("Connection", wmpc, "outcomeReceived", si, "(submitterIdle)")
		si.maybeClose()
	}
	return err
}

func (wmpc *wssMsgPackClient) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	wmpc.enqueueQuery(connectionExec(func() error {
		return wmpc.serverConnectionsChanged(servers)
	}))
}
func (wmpc *wssMsgPackClient) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	wmpc.enqueueQuery(connectionExec(func() error {
		return wmpc.serverConnectionsChanged(servers)
	}))
}
func (wmpc *wssMsgPackClient) ConnectionEstablished(rmId common.RMId, c paxos.Connection, servers map[common.RMId]paxos.Connection, done func()) {
	wmpc.enqueueQuery(connectionExec(func() error {
		if done != nil {
			defer done()
		}
		return wmpc.serverConnectionsChanged(servers)
	}))
}

func (wmpc *wssMsgPackClient) serverConnectionsChanged(servers map[common.RMId]paxos.Connection) error {
	return wmpc.submitter.ServerConnectionsChanged(servers)
}

func (wmpc *wssMsgPackClient) readAndHandleOneMsg() error {
	msg := &cmsgs.ClientMessage{}
	if err := wmpc.readOne(msg); err != nil {
		return err
	}
	switch {
	case msg.ClientTxnSubmission != nil:
		wmpc.enqueueQuery(connectionExec(func() error {
			return wmpc.submitTransaction(msg.ClientTxnSubmission)
		}))
		return nil
	default:
		return errors.New("Unexpected message type received from client.")
	}
}

func (wmpc *wssMsgPackClient) submitTransaction(ctxn *cmsgs.ClientTxn) error {
	origTxnId := common.MakeTxnId(ctxn.Id)
	seg := capn.NewBuffer(nil)
	ctxnCapn := ctxn.ToCapnp(seg)
	return wmpc.submitter.SubmitClientTransaction(&ctxnCapn, func(clientOutcome *capcmsgs.ClientTxnOutcome, err error) error {
		switch {
		case err != nil: // error is non-fatal to connection
			return wmpc.beater.sendMessage(wmpc.clientTxnError(ctxn, err, origTxnId))
		case clientOutcome == nil: // shutdown
			return nil
		default:
			msg := &cmsgs.ClientMessage{ClientTxnOutcome: &cmsgs.ClientTxnOutcome{}}
			msg.ClientTxnOutcome.FromCapnp(*clientOutcome)
			return wmpc.send(msg)
		}
	})
}

func (wmpc *wssMsgPackClient) clientTxnError(ctxn *cmsgs.ClientTxn, err error, origTxnId *common.TxnId) msgp.Encodable {
	msg := &cmsgs.ClientMessage{
		ClientTxnOutcome: &cmsgs.ClientTxnOutcome{
			Id:      origTxnId[:],
			FinalId: ctxn.Id,
			Error:   err.Error(),
		},
	}
	return msg
}

func (wmpc *wssMsgPackClient) createReader() {
	if wmpc.reader == nil {
		wmpc.reader = &socketReader{
			conn:             wmpc.Connection,
			socketMsgHandler: wmpc,
			terminate:        make(chan struct{}),
			terminated:       make(chan struct{}),
		}
		wmpc.reader.start()
	}
}

func (wmpc *wssMsgPackClient) createBeater() {
	if wmpc.beater == nil {
		beater := &wssBeater{
			wssMsgPackClient: wmpc,
			conn:             wmpc.Connection,
			terminate:        make(chan struct{}),
			terminated:       make(chan struct{}),
			ticker:           time.NewTicker(common.HeartbeatInterval),
		}
		beater.start()
		wmpc.beater = beater
	}
}

// WSS Beater

type wssBeater struct {
	*wssMsgPackClient
	conn         *Connection
	terminate    chan struct{}
	terminated   chan struct{}
	ticker       *time.Ticker
	mustSendBeat bool
}

func (b *wssBeater) start() {
	if b != nil {
		go b.tick()
	}
}

func (b *wssBeater) stop() {
	if b != nil {
		b.wssMsgPackClient = nil
		select {
		case <-b.terminated:
		default:
			close(b.terminate)
			<-b.terminated
		}
	}
}

func (b *wssBeater) tick() {
	defer func() {
		b.ticker.Stop()
		b.ticker = nil
		close(b.terminated)
	}()
	for {
		select {
		case <-b.terminate:
			return
		case <-b.ticker.C:
			if !b.conn.enqueueQuery(connectionExec(b.beat)) {
				return
			}
		}
	}
}

func (b *wssBeater) beat() error {
	if b != nil && b.wssMsgPackClient != nil {
		if b.mustSendBeat {
			// do not set it back to false here!
			return b.socket.WriteControl(websocket.PingMessage, nil, time.Time{})
		} else {
			b.mustSendBeat = true
		}
	}
	return nil
}

func (b *wssBeater) sendMessage(msg msgp.Encodable) error {
	if b != nil {
		b.mustSendBeat = false
		return b.send(msg)
	}
	return nil
}
