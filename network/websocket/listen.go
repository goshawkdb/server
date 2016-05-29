package websocket

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
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/network"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
)

type topologyHolder struct {
	sync.RWMutex
	*configuration.Topology
}

func (th *topologyHolder) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	th.Lock()
	defer th.Unlock()
	th.Topology = topology
	done(true)
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

var upgrader = websocket.Upgrader{
	// allow everything for now!
	CheckOrigin: func(r *http.Request) bool { return true },
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
