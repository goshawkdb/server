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
		log.Println("WSS Handler")
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
	log.Println("WSS Upgrading Connection")
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
		log.Println("started wss connection")

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
	connectionManager *ConnectionManager
	topology          *configuration.Topology
	submitter         *client.ClientTxnSubmitter
	submitterIdle     *connectionMsgTopologyChanged
	reader            *socketReader
}

func (wmpc *wssMsgPackClient) send(msg msgp.Encodable) error {
	wc, err := wmpc.socket.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}
	if err = msgp.Encode(wc, msg); err != nil {
		return err
	}
	log.Println("WSS did write")
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

		wmpc.createReader()

		wmpc.submitter = client.NewClientTxnSubmitter(wmpc.connectionManager.RMId, wmpc.connectionManager.BootCount(), wmpc.rootsVar, wmpc.connectionManager)
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
	// TODO - reader?
	if wmpc.submitter != nil {
		wmpc.submitter.Shutdown()
		wmpc.submitter = nil
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
			return wmpc.send(wmpc.clientTxnError(ctxn, err, origTxnId))
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

// OLD:
/*
type topologyHolder struct {
	sync.RWMutex
	*configuration.Topology
}

func (th *topologyHolder) topology() *configuration.Topology {
	th.RLock()
	defer th.RUnlock()
	return th.Topology
}

func StartListener(listenPort uint16, cm *network.ConnectionManager) {
	go func() {
		th := &topologyHolder{}
		th.Lock()
		th.Topology = cm.AddTopologySubscriber(eng.ConnectionSubscriber, th)
		th.Unlock()
		defer cm.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, th)

		nodeCertPrivKeyPair := cm.NodeCertificatePrivateKeyPair
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
		}

		connCount := uint32(1 << 31)

		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
			peerCerts := req.TLS.PeerCertificates
			if authenticated, _ := th.topology().VerifyPeerCerts(peerCerts); authenticated {
				w.Header().Set("Content-Type", "text/plain")
				w.Write([]byte(fmt.Sprintf("GoshawkDB Server version %v. Websocket available at /ws", server.ServerVersion)))
			} else {
				w.WriteHeader(http.StatusForbidden)
			}
		})
		mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
			peerCerts := req.TLS.PeerCertificates
			if authenticated, hashsum := th.topology().VerifyPeerCerts(peerCerts); authenticated {
				log.Printf("WSS User '%s' authenticated", hex.EncodeToString(hashsum[:]))
				connNumber := atomic.AddUint32(&connCount, 1)
				wsHandler(w, req, th, cm, peerCerts, connNumber)
			} else {
				w.WriteHeader(http.StatusForbidden)
			}
		})

		s := &http.Server{
			Addr:         fmt.Sprintf(":%d", listenPort),
			ReadTimeout:  common.HeartbeatInterval * 2,
			WriteTimeout: common.HeartbeatInterval * 2,
			TLSConfig:    tlsConfig,
			TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler)),
			Handler:      mux,
		}
		log.Println(s.ListenAndServeTLS("", ""))
	}()
}

func wsHandler(w http.ResponseWriter, r *http.Request, th *topologyHolder, cm *network.ConnectionManager, peerCerts []*x509.Certificate, connNumber uint32) {
	remoteAddr := r.RemoteAddr
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WSS upgrade failure for connection to %v: %v", remoteAddr, err)
		return
	}
	log.Printf("WSS upgrade success for connection to %v", remoteAddr)
	defer c.Close()
	wsc := &wsConnection{
		remoteHost:        remoteAddr,
		connectionManager: cm,
		socket:            c,
		ConnectionNumber:  connNumber,
		peerCerts:         peerCerts,
	}
	wsc.run() // blocks
	log.Printf("WSS connection to %v closed.", remoteAddr)
}

type connectionMsg interface {
	witness() connectionMsg
}

type connectionMsgBasic struct{}

func (cmb connectionMsgBasic) witness() connectionMsg { return cmb }

type connectionMsgShutdown struct{ connectionMsgBasic }

func (wsc *wsConnection) Shutdown(sync paxos.Blocking) {
	if wsc.enqueueQuery(connectionMsgShutdown{}) && sync == paxos.Sync {
		wsc.cellTail.Wait()
	}
}

type connectionMsgSend []byte

func (cms connectionMsgSend) witness() connectionMsg { return cms }

func (wsc *wsConnection) Send(msg []byte) {
	wsc.enqueueQuery(connectionMsgSend(msg))
}

type connectionMsgOutcomeReceived struct {
	connectionMsgBasic
	sender  common.RMId
	txnId   *common.TxnId
	outcome *msgs.Outcome
}

func (wsc *wsConnection) SubmissionOutcomeReceived(sender common.RMId, txnId *common.TxnId, outcome *msgs.Outcome) {
	wsc.enqueueQuery(connectionMsgOutcomeReceived{
		sender:  sender,
		txnId:   txnId,
		outcome: outcome,
	})
}

type connectionMsgTopologyChanged struct {
	connectionMsgBasic
	topology   *configuration.Topology
	resultChan chan struct{}
}

func (cmtc *connectionMsgTopologyChanged) Done() {
	close(cmtc.resultChan)
}

func (wsc *wsConnection) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &connectionMsgTopologyChanged{
		resultChan: make(chan struct{}),
		topology:   topology,
	}
	if wsc.enqueueQuery(msg) {
		go func() {
			select {
			case <-msg.resultChan:
			case <-wsc.cellTail.Terminated:
			}
			done(true) // connection drop is not a problem
		}()
	} else {
		done(true)
	}
}

type connectionMsgServerConnectionsChanged map[common.RMId]paxos.Connection

func (cmdhc connectionMsgServerConnectionsChanged) witness() connectionMsg { return cmdhc }

func (wsc *wsConnection) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	wsc.enqueueQuery(connectionMsgServerConnectionsChanged(servers))
}
func (wsc *wsConnection) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	wsc.enqueueQuery(connectionMsgServerConnectionsChanged(servers))
}
func (wsc *wsConnection) ConnectionEstablished(rmId common.RMId, c paxos.Connection, servers map[common.RMId]paxos.Connection) {
	wsc.enqueueQuery(connectionMsgServerConnectionsChanged(servers))
}

func (wsc *wsConnection) enqueueQuery(msg connectionMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return wsc.enqueueQueryInner(msg, cell, f)
	}
	return wsc.cellTail.WithCell(f)
}

type wsConnection struct {
	remoteHost        string
	connectionManager *network.ConnectionManager
	topology          *configuration.Topology
	submitter         *client.ClientTxnSubmitter
	peerCerts         []*x509.Certificate
	socket            *websocket.Conn
	ConnectionNumber  uint32
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(connectionMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan connectionMsg
	currentState      connectionStateMachineComponent
	connectionAwaitHandshake
	connectionAwaitClientHandshake
	connectionRun
}

func (wsc *wsConnection) run() {
	var head *cc.ChanCellHead
	head, wsc.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan connectionMsg, n)
			cell.Open = func() { wsc.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			wsc.enqueueQueryInner = func(msg connectionMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
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

	wsc.connectionAwaitHandshake.init(wsc)
	wsc.connectionAwaitClientHandshake.init(wsc)
	wsc.connectionRun.init(wsc)

	wsc.currentState = &wsc.connectionAwaitHandshake

	wsc.actorLoop(head)
}

func (wsc *wsConnection) actorLoop(head *cc.ChanCellHead) {
	wsc.topology = wsc.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, wsc)
	defer wsc.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, wsc)

	var (
		err       error
		oldState  connectionStateMachineComponent
		queryChan <-chan connectionMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = wsc.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		if oldState != wsc.currentState {
			oldState = wsc.currentState
			err = wsc.currentState.start()
		} else if msg, ok := <-queryChan; ok {
			terminate, err = wsc.handleMsg(msg)
		} else {
			head.Next(queryCell, chanFun)
		}
		terminate = terminate || err != nil
	}
	wsc.cellTail.Terminate()
	wsc.handleShutdown(err)
	log.Println("Connection terminated")
}

func (wsc *wsConnection) handleMsg(msg connectionMsg) (terminate bool, err error) {
	switch msgT := msg.(type) {
	case connectionMsgShutdown:
		terminate = true
	case connectionReadError:
		err = msgT.error
	case *connectionReadClientMessage:
		err = wsc.handleMsgFromClient((*cmsgs.ClientMessage)(msgT))
	case connectionMsgSend:
		err = wsc.sendMessage(msgT)
	case connectionMsgOutcomeReceived:
		wsc.outcomeReceived(msgT)
	case *connectionMsgTopologyChanged:
		err = wsc.topologyChanged(msgT)
	case connectionMsgServerConnectionsChanged:
		wsc.serverConnectionsChanged(msgT)
	default:
		err = fmt.Errorf("Fatal to wsConnection: Received unexpected message: %#v", msgT)
	}
	return
}

func (wsc *wsConnection) handleShutdown(err error) {
	if err != nil {
		log.Println(err)
	}
	wsc.maybeStopReaderAndCloseSocket()
	if wsc.isClient {
		wsc.connectionManager.ClientLost(wsc.ConnectionNumber, wsc)
		if wsc.submitter != nil {
			wsc.submitter.Shutdown()
		}
	}
}

func (wsc *wsConnection) nextState() {
	switch wsc.currentState {
	case &wsc.connectionAwaitHandshake:
		wsc.currentState = &wsc.connectionAwaitClientHandshake
	case &wsc.connectionAwaitClientHandshake:
		wsc.currentState = &wsc.connectionRun
	default:
		panic(fmt.Sprintf("Unexpected current state for nextState: %v", wsc.currentState))
	}
}

type connectionStateMachineComponent interface {
	init(*wsConnection)
	start() error
	connectionStateMachineComponentWitness()
}

type connectionAwaitHandshake struct {
	*wsConnection
}

func (cah *connectionAwaitHandshake) connectionStateMachineComponentWitness() {}
func (cah *connectionAwaitHandshake) String() string                          { return "ConnectionAwaitHandshake" }

func (cah *connectionAwaitHandshake) init(conn *wsConnection) {
	cah.wsConnection = conn
}

func (cah *connectionAwaitHandshake) start() error {
	helloSeg := cah.makeHello()
	if err := cah.send(server.SegToBytes(helloSeg)); err != nil {
		return err
	}
	if seg, err := cah.readOne(); err == nil {
		hello := cmsgs.ReadRootHello(seg)
		if cah.verifyHello(&hello) {
			if hello.IsClient() {
				cah.isClient = true
				cah.nextState()
				return nil
			} else {
				return errors.New("wsConnection: websocket connection must be to a client")
			}
		} else {
			return fmt.Errorf("Received erroneous hello from peer")
		}
	} else {
		return err
	}
}

func (cah *connectionAwaitHandshake) makeHello() *capn.Segment {
	seg := capn.NewBuffer(nil)
	hello := cmsgs.NewRootHello(seg)
	hello.SetProduct(common.ProductName)
	hello.SetVersion(common.ProductVersion)
	hello.SetIsClient(false)
	return seg
}

func (cah *connectionAwaitHandshake) verifyHello(hello *cmsgs.Hello) bool {
	return hello.Product() == common.ProductName &&
		hello.Version() == common.ProductVersion
}

func (cah *connectionAwaitHandshake) send(msg []byte) error {
	return cah.socket.WriteMessage(websocket.BinaryMessage, msg)
}

func (cah *connectionAwaitHandshake) readOne() (*capn.Segment, error) {
	msgType, reader, err := cah.socket.NextReader()
	switch {
	case err != nil:
		return nil, err
	case msgType != websocket.BinaryMessage:
		return nil, errors.New(fmt.Sprintf("wsConnection requires binary messages only. Received msg of type %d", msgType))
	default:
		return capn.ReadFromStream(reader, nil)
	}
}

// Await Client Handshake

type connectionAwaitClientHandshake struct {
	*wsConnection
	isClient bool
}

func (cach *connectionAwaitClientHandshake) connectionStateMachineComponentWitness() {}
func (cach *connectionAwaitClientHandshake) String() string                          { return "ConnectionAwaitClientHandshake" }

func (cach *connectionAwaitClientHandshake) init(conn *wsConnection) {
	cach.wsConnection = conn
}

func (cach *connectionAwaitClientHandshake) start() error {
	if cach.topology.Root.VarUUId == nil {
		return errors.New("Root not yet known")
	}

	helloFromServer := cach.makeHelloClientFromServer(cach.topology)
	if err := cach.send(server.SegToBytes(helloFromServer)); err != nil {
		return err
	}

	cach.nextState()
	return nil
}

func (cach *connectionAwaitClientHandshake) makeHelloClientFromServer(topology *configuration.Topology) *capn.Segment {
	seg := capn.NewBuffer(nil)
	hello := cmsgs.NewRootHelloClientFromServer(seg)
	namespace := make([]byte, common.KeyLen-8)
	binary.BigEndian.PutUint32(namespace[0:4], cach.ConnectionNumber)
	binary.BigEndian.PutUint32(namespace[4:8], cach.connectionManager.BootCount)
	binary.BigEndian.PutUint32(namespace[8:], uint32(cach.connectionManager.RMId))
	hello.SetNamespace(namespace)
	if topology.Root.VarUUId != nil {
		hello.SetRootId(topology.Root.VarUUId[:])
	}
	return seg
}

// Run

type connectionRun struct {
	*wsConnection
	submitterIdle *connectionMsgTopologyChanged
	reader        *connectionReader
}

func (cr *connectionRun) connectionStateMachineComponentWitness() {}
func (cr *connectionRun) String() string                          { return "ConnectionRun" }

func (cr *connectionRun) init(conn *wsConnection) {
	cr.wsConnection = conn
}

func (cr *connectionRun) start() error {
	servers := cr.connectionManager.ClientEstablished(cr.ConnectionNumber, cr.wsConnection)
	cr.submitter = client.NewClientTxnSubmitter(cr.connectionManager.RMId, cr.connectionManager.BootCount, cr.connectionManager)
	cr.submitter.TopologyChanged(cr.topology)
	cr.submitter.ServerConnectionsChanged(servers)

	cr.reader = newConnectionReader(cr.wsConnection)
	go cr.reader.readClient()
	return nil
}

func (cr *connectionRun) topologyChanged(tc *connectionMsgTopologyChanged) error {
	if si := cr.submitterIdle; si != nil {
		cr.submitterIdle = nil
		server.Log("wsConnection", cr.wsConnection, "topologyChanged:", tc, "clearing old:", si)
		si.Done()
	}
	topology := tc.topology
	cr.topology = topology
	if topology != nil {
		if authenticated, _ := topology.VerifyPeerCerts(cr.peerCerts); !authenticated {
			server.Log("wsConnection", cr.wsConnection, "topologyChanged", tc, "(client unauthed)")
			tc.Done()
			return errors.New("Client wsConnection closed: No client certificate known")
		}
	}
	if cr.submitter == nil {
		tc.Done()
	} else {
		cr.submitter.TopologyChanged(topology)
		if cr.submitter.IsIdle() {
			server.Log("wsConnection", cr.wsConnection, "topologyChanged", tc, "(client, submitter is idle)")
			tc.Done()
		} else {
			server.Log("wsConnection", cr.wsConnection, "topologyChanged", tc, "(client, submitter not idle)")
			cr.submitterIdle = tc
		}
	}
	return nil
}

func (cr *connectionRun) handleMsgFromClient(msg *cmsgs.ClientMessage) error {
	if cr.currentState != cr {
		// probably just draining the queue from the reader after a restart
		return nil
	}
	switch which := msg.Which(); which {
	case cmsgs.CLIENTMESSAGE_HEARTBEAT:
		// do nothing; should not exist!
	case cmsgs.CLIENTMESSAGE_CLIENTTXNSUBMISSION:
		ctxn := msg.ClientTxnSubmission()
		origTxnId := common.MakeTxnId(ctxn.Id())
		cr.submitter.SubmitClientTransaction(&ctxn, func(clientOutcome *cmsgs.ClientTxnOutcome, err error) {
			switch {
			case err != nil:
				cr.clientTxnError(&ctxn, err, origTxnId)
			case clientOutcome == nil: // shutdown
				return
			default:
				seg := capn.NewBuffer(nil)
				msg := cmsgs.NewRootClientMessage(seg)
				msg.SetClientTxnOutcome(*clientOutcome)
				cr.sendMessage(server.SegToBytes(msg.Segment))
			}
		})
	default:
		return fmt.Errorf("Unexpected message type received from client: %v", which)
	}
	return nil
}

func (cr *connectionRun) sendMessage(msg []byte) error {
	if cr.currentState == cr {
		return cr.send(msg)
	}
	return nil
}

func (cr *connectionRun) serverConnectionsChanged(servers map[common.RMId]paxos.Connection) {
	if cr.submitter != nil {
		cr.submitter.ServerConnectionsChanged(servers)
	}
}

func (cr *connectionRun) outcomeReceived(out connectionMsgOutcomeReceived) {
	if cr.currentState != cr {
		return
	}
	cr.submitter.SubmissionOutcomeReceived(out.sender, out.txnId, out.outcome)
	if cr.submitterIdle != nil && cr.submitter.IsIdle() {
		si := cr.submitterIdle
		cr.submitterIdle = nil
		server.Log("wsConnection", cr.wsConnection, "outcomeReceived", si, "(submitterIdle)")
		si.Done()
	}
}

func (cr *connectionRun) clientTxnError(ctxn *cmsgs.ClientTxn, err error, origTxnId *common.TxnId) error {
	seg := capn.NewBuffer(nil)
	msg := cmsgs.NewRootClientMessage(seg)
	outcome := cmsgs.NewClientTxnOutcome(seg)
	msg.SetClientTxnOutcome(outcome)
	if origTxnId == nil {
		outcome.SetId(ctxn.Id())
	} else {
		outcome.SetId(origTxnId[:])
	}
	outcome.SetFinalId(ctxn.Id())
	outcome.SetError(err.Error())
	return cr.sendMessage(server.SegToBytes(seg))
}

func (cr *connectionRun) maybeStopReaderAndCloseSocket() {
	if cr.reader != nil {
		close(cr.reader.terminate)
		if cr.socket != nil {
			if err := cr.socket.Close(); err != nil {
				log.Println(err)
			}
		}
		cr.reader.terminated.Wait()
		cr.reader = nil
	}

	if cr.socket != nil {
		if err := cr.socket.Close(); err != nil {
			log.Println(err)
		}
		cr.socket = nil
	}
}

// Reader

type connectionReadClientMessage cmsgs.ClientMessage

func (crcm *connectionReadClientMessage) witness() connectionMsg { return crcm }

type connectionReadError struct {
	connectionMsgBasic
	error
}

type connectionReader struct {
	*wsConnection
	terminate  chan struct{}
	terminated *sync.WaitGroup
}

func newConnectionReader(conn *wsConnection) *connectionReader {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	return &connectionReader{
		wsConnection: conn,
		terminate:    make(chan struct{}),
		terminated:   wg,
	}
}

func (cr *connectionReader) readClient() {
	cr.read(func(seg *capn.Segment) bool {
		msg := cmsgs.ReadRootClientMessage(seg)
		return cr.enqueueQuery((*connectionReadClientMessage)(&msg))
	})
}

func (cr *connectionReader) read(fun func(*capn.Segment) bool) {
	defer cr.terminated.Done()
	for {
		select {
		case <-cr.terminate:
			return
		default:
			if seg, err := cr.readOne(); err == nil {
				if !fun(seg) {
					return
				}
			} else {
				cr.enqueueQuery(connectionReadError{error: err})
				return
			}
		}
	}
}

*/
/*
	for {
		mt, message, err := c.ReadMessage()
		if err != nil {
			break
		}
		err = c.WriteMessage(mt, message)
		if err != nil {
			break
		}
	}
*/
