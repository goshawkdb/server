package network

import (
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
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
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type WebsocketListener struct {
	logger            log.Logger
	parentLogger      log.Logger
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(websocketListenerMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan websocketListenerMsg
	connectionManager *ConnectionManager
	mux               *HttpListenerWithMux
	topology          *configuration.Topology
	topologyLock      *sync.RWMutex
}

type websocketListenerMsg interface {
	websocketListenerMsgWitness()
}

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

type websocketListenerQueryCapture struct {
	wl  *WebsocketListener
	msg websocketListenerMsg
}

func (wlqc *websocketListenerQueryCapture) ccc(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
	return wlqc.wl.enqueueQueryInner(wlqc.msg, cell, wlqc.ccc)
}

func (l *WebsocketListener) enqueueQuery(msg websocketListenerMsg) bool {
	wlqc := &websocketListenerQueryCapture{wl: l, msg: msg}
	return l.cellTail.WithCell(wlqc.ccc)
}

func NewWebsocketListener(mux *HttpListenerWithMux, cm *ConnectionManager, logger log.Logger) *WebsocketListener {
	l := &WebsocketListener{
		logger:            log.With(logger, "subsystem", "websocketListener"),
		parentLogger:      logger,
		connectionManager: cm,
		mux:               mux,
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
	return l
}

func (l *WebsocketListener) actorLoop(head *cc.ChanCellHead) {
	topology := l.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, l)
	defer l.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, l)
	l.putTopology(topology)
	l.configureMux()

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
		l.logger.Log("msg", "Fatal error.", "error", err)
	}
	l.cellTail.Terminate()
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

func (l *WebsocketListener) configureMux() {
	connCount := uint32(0)

	l.mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, _, _ := l.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte(fmt.Sprintf("GoshawkDB Server version %v. Websocket available at /ws", server.ServerVersion)))
		} else {
			l.logger.Log("type", "client", "authentication", "failure")
			w.WriteHeader(http.StatusForbidden)
		}
	})
	l.mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, hashsum, roots := l.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			connNumber := 2*atomic.AddUint32(&connCount, 1) + 1
			l.logger.Log("type", "client", "authentication", "success", "fingerprint", hex.EncodeToString(hashsum[:]), "connNumber", connNumber)
			wsHandler(l.connectionManager, connNumber, w, req, peerCerts, roots, l.parentLogger)
		} else {
			l.logger.Log("type", "client", "authentication", "failure")
			w.WriteHeader(http.StatusForbidden)
		}
	})
	l.mux.Done()
}

var upgrader = websocket.Upgrader{
	// allow everything for now!
	CheckOrigin: func(r *http.Request) bool { return true },
}

func wsHandler(cm *ConnectionManager, connNumber uint32, w http.ResponseWriter, r *http.Request, peerCerts []*x509.Certificate, roots map[string]*common.Capability, logger log.Logger) {
	logger = log.With(logger, "subsystem", "connection", "dir", "incoming", "protocol", "websocket", "type", "client", "connNumber", connNumber)
	c, err := upgrader.Upgrade(w, r, nil)
	if err == nil {
		logger.Log("msg", "WSS upgrade success.", "remoteHost", c.RemoteAddr())
		yesman := &wssMsgPackClient{
			logger:            logger,
			remoteHost:        fmt.Sprintf("%v", c.RemoteAddr()),
			connectionNumber:  connNumber,
			socket:            c,
			peerCerts:         peerCerts,
			roots:             roots,
			connectionManager: cm,
		}
		NewConnection(yesman, cm, logger)

	} else {
		logger.Log("msg", "WSS upgrade failure.", "remoteHost", r.RemoteAddr, "error", err)
	}
}

type wssMsgPackClient struct {
	*Connection
	logger            log.Logger
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
	reader            *common.SocketReader
	beater            *wssBeater
}

func (wmpc *wssMsgPackClient) Send(msg msgp.Encodable) error {
	wc, err := wmpc.socket.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}
	if err = msgp.Encode(wc, msg); err != nil {
		return err
	}
	return wc.Close()
}

func (wmpc *wssMsgPackClient) ReadOne(msg msgp.Decodable) error {
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

func (wmpc *wssMsgPackClient) Dial() error {
	return nil
}

func (wmpc *wssMsgPackClient) PerformHandshake(topology *configuration.Topology) (Protocol, error) {
	wmpc.topology = topology

	hello := &cmsgs.Hello{
		Product: common.ProductName,
		Version: common.ProductVersion,
	}
	if err := wmpc.Send(hello); err != nil {
		return nil, err
	}
	hello = &cmsgs.Hello{}
	if err := wmpc.ReadOne(hello); err == nil {
		if wmpc.verifyHello(hello) {

			if wmpc.topology.ClusterUUId == 0 {
				return nil, errors.New("Cluster not yet formed")
			} else if len(wmpc.topology.Roots) == 0 {
				return nil, errors.New("No roots: cluster not yet formed")
			}

			return wmpc, wmpc.Send(wmpc.makeHelloClient())
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
	binary.BigEndian.PutUint32(namespace[4:8], wmpc.connectionManager.BootCount)
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

func (wmpc *wssMsgPackClient) Restart() bool {
	return false // client connections are never restarted
}

func (wmpc *wssMsgPackClient) Run(conn *Connection) error {
	wmpc.Connection = conn
	servers, metrics := wmpc.connectionManager.ClientEstablished(wmpc.connectionNumber, wmpc)
	if servers == nil {
		return errors.New("Not ready for client connections")

	} else {
		wmpc.logger.Log("msg", "Connection established.", "remoteHost", wmpc.remoteHost)

		wmpc.createBeater()
		wmpc.createReader()

		cm := wmpc.connectionManager
		wmpc.submitter = client.NewClientTxnSubmitter(cm.RMId, cm.BootCount, wmpc.rootsVar, wmpc.namespace,
			paxos.NewServerConnectionPublisherProxy(wmpc.Connection, cm, wmpc.logger), wmpc.Connection,
			wmpc.logger, metrics)
		wmpc.submitter.TopologyChanged(wmpc.topology)
		wmpc.submitter.ServerConnectionsChanged(servers)
		return nil
	}
}

func (wmpc *wssMsgPackClient) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	topology := tc.topology
	wmpc.topology = topology

	server.DebugLog(wmpc.logger, "debug", "TopologyChanged", "topology", topology)

	if topology != nil {
		if authenticated, _, roots := wmpc.topology.VerifyPeerCerts(wmpc.peerCerts); !authenticated {
			server.DebugLog(wmpc.logger, "debug", "TopologyChanged. Client Unauthed.", "topology", topology)
			tc.maybeClose()
			return errors.New("WSS Client connection closed: No client certificate known")
		} else if len(roots) == len(wmpc.roots) {
			for name, capsOld := range wmpc.roots {
				if capsNew, found := roots[name]; !found || !capsNew.Equal(capsOld) {
					server.DebugLog(wmpc.logger, "debug", "TopologyChanged. Roots changed.", "topology", topology)
					tc.maybeClose()
					return errors.New("WSS Client connection closed: roots have changed")
				}
			}
		} else {
			server.DebugLog(wmpc.logger, "debug", "TopologyChanged. Roots changed.", "topology", topology)
			tc.maybeClose()
			return errors.New("WSS Client connection closed: roots have changed")
		}
	}
	if err := wmpc.submitter.TopologyChanged(topology); err != nil {
		tc.maybeClose()
		return err
	}
	tc.maybeClose()

	return nil
}

func (wmpc *wssMsgPackClient) InternalShutdown() {
	if wmpc.reader != nil {
		wmpc.reader.Stop()
		wmpc.reader = nil
	}
	cont := func() {
		wmpc.connectionManager.ClientLost(wmpc.connectionNumber, wmpc)
		wmpc.shutdownComplete()
	}
	if wmpc.submitter == nil {
		cont()
	} else {
		wmpc.submitter.Shutdown(cont)
	}
	if wmpc.beater != nil {
		wmpc.beater.Stop()
		wmpc.beater = nil
	}
	if wmpc.socket != nil {
		wmpc.socket.Close()
		wmpc.socket = nil
	}
}

func (wmpc *wssMsgPackClient) SubmissionOutcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) {
	wmpc.EnqueueError(func() error {
		return wmpc.outcomeReceived(sender, txn, outcome)
	})
}

func (wmpc *wssMsgPackClient) outcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) error {
	return wmpc.submitter.SubmissionOutcomeReceived(sender, txn, outcome)
}

func (wmpc *wssMsgPackClient) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	wmpc.EnqueueError(func() error {
		return wmpc.serverConnectionsChanged(servers)
	})
}
func (wmpc *wssMsgPackClient) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	wmpc.EnqueueError(func() error {
		return wmpc.serverConnectionsChanged(servers)
	})
}
func (wmpc *wssMsgPackClient) ConnectionEstablished(rmId common.RMId, c paxos.Connection, servers map[common.RMId]paxos.Connection, done func()) {
	wmpc.EnqueueError(func() error {
		if done != nil {
			defer done()
		}
		return wmpc.serverConnectionsChanged(servers)
	})
}

func (wmpc *wssMsgPackClient) serverConnectionsChanged(servers map[common.RMId]paxos.Connection) error {
	return wmpc.submitter.ServerConnectionsChanged(servers)
}

func (wmpc *wssMsgPackClient) ReadAndHandleOneMsg() error {
	msg := &cmsgs.ClientMessage{}
	if err := wmpc.ReadOne(msg); err != nil {
		return err
	}
	switch {
	case msg.ClientTxnSubmission != nil:
		wmpc.EnqueueError(func() error {
			return wmpc.submitTransaction(msg.ClientTxnSubmission)
		})
		return nil
	default:
		return errors.New("Unexpected message type received from client.")
	}
}

func (wmpc *wssMsgPackClient) submitTransaction(ctxn *cmsgs.ClientTxn) error {
	origTxnId := common.MakeTxnId(ctxn.Id)
	seg := capn.NewBuffer(nil)
	ctxnCapn, err := ctxn.ToCapnp(seg)
	if err != nil { // error is non-fatal to connection
		return wmpc.beater.SendMessage(wmpc.clientTxnError(ctxn, err, origTxnId))
	}
	return wmpc.submitter.SubmitClientTransaction(ctxnCapn, func(clientOutcome *capcmsgs.ClientTxnOutcome, err error) error {
		switch {
		case err != nil:
			return wmpc.beater.SendMessage(wmpc.clientTxnError(ctxn, err, origTxnId))
		case clientOutcome == nil: // shutdown
			return nil
		default:
			msg := &cmsgs.ClientMessage{ClientTxnOutcome: &cmsgs.ClientTxnOutcome{}}
			msg.ClientTxnOutcome.FromCapnp(*clientOutcome)
			return wmpc.Send(msg)
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
		wmpc.reader = common.NewSocketReader(wmpc.Connection, wmpc)
		wmpc.reader.Start()
	}
}

func (wmpc *wssMsgPackClient) createBeater() {
	if wmpc.beater == nil {
		wmpc.beater = NewWssBeater(wmpc, wmpc.Connection)
		wmpc.beater.Start()
	}
}

// WSS Beater

type wssBeater struct {
	*wssMsgPackClient
	conn         common.ConnectionActor
	terminate    chan struct{}
	terminated   chan struct{}
	ticker       *time.Ticker
	mustSendBeat bool
}

func NewWssBeater(wmpc *wssMsgPackClient, conn common.ConnectionActor) *wssBeater {
	return &wssBeater{
		wssMsgPackClient: wmpc,
		conn:             conn,
		terminate:        make(chan struct{}),
		terminated:       make(chan struct{}),
		ticker:           time.NewTicker(common.HeartbeatInterval),
	}
}

func (b *wssBeater) Start() {
	if b != nil {
		go b.tick()
	}
}

func (b *wssBeater) Stop() {
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
			if !b.conn.EnqueueError(b.beat) {
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

func (b *wssBeater) SendMessage(msg msgp.Encodable) error {
	if b != nil {
		b.mustSendBeat = false
		return b.Send(msg)
	}
	return nil
}
