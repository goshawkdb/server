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
	"github.com/tinylib/msgp/msgp"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
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
	*actor.Mailbox
	*actor.BasicServerOuter

	parentLogger      log.Logger
	connectionManager *ConnectionManager
	mux               *HttpListenerWithMux
	topology          *configuration.Topology
	topologyLock      sync.RWMutex

	inner websocketListenerInner
}

type websocketListenerInner struct {
	*WebsocketListener
	*actor.BasicServerInner
}

func NewWebsocketListener(mux *HttpListenerWithMux, cm *ConnectionManager, logger log.Logger) *WebsocketListener {
	l := &WebsocketListener{
		parentLogger:      logger,
		connectionManager: cm,
		mux:               mux,
	}

	li := &l.inner
	li.WebsocketListener = l
	li.BasicServerInner = actor.NewBasicServerInner(log.With(logger, "subsystem", "websocketListener"))

	_, err := actor.Spawn(li)
	if err != nil {
		panic(err) // "impossible"
	}

	return l
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

type websocketListenerMsgTopologyChanged struct {
	actor.MsgSyncQuery
	*WebsocketListener
	topology *configuration.Topology
}

func (msg websocketListenerMsgTopologyChanged) Exec() (bool, error) {
	msg.putTopology(msg.topology)
	msg.MustClose()
	return false, nil
}

func (l *WebsocketListener) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &websocketListenerMsgTopologyChanged{WebsocketListener: l, topology: topology}
	msg.InitMsg(l)
	if l.EnqueueMsg(msg) {
		go func() {
			msg.Wait()
			done(true) // connection drop is not a problem
		}()
	} else {
		done(true) // connection drop is not a problem
	}
}

func (l *websocketListenerInner) Init(self *actor.Actor) (bool, error) {
	terminate, err := l.BasicServerInner.Init(self)
	if terminate || err != nil {
		return terminate, err
	}

	l.Mailbox = self.Mailbox
	l.BasicServerOuter = actor.NewBasicServerOuter(self.Mailbox)

	topology := l.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, l)
	l.putTopology(topology)
	l.configureMux()
	return false, nil
}

func (l *websocketListenerInner) HandleShutdown(err error) bool {
	l.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, l)
	return l.BasicServerInner.HandleShutdown(err)
}

func (l *websocketListenerInner) configureMux() {
	connCount := uint32(0)

	l.mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, _, _ := l.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte(fmt.Sprintf("GoshawkDB Server version %v. Websocket available at /ws", server.ServerVersion)))
		} else {
			l.Logger.Log("type", "client", "authentication", "failure")
			w.WriteHeader(http.StatusForbidden)
		}
	})
	l.mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		peerCerts := req.TLS.PeerCertificates
		if authenticated, hashsum, roots := l.getTopology().VerifyPeerCerts(peerCerts); authenticated {
			connNumber := 2*atomic.AddUint32(&connCount, 1) + 1
			l.Logger.Log("type", "client", "authentication", "success", "fingerprint", hex.EncodeToString(hashsum[:]), "connNumber", connNumber)
			wsHandler(l.connectionManager, connNumber, w, req, peerCerts, roots, l.parentLogger)
		} else {
			l.Logger.Log("type", "client", "authentication", "failure")
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
	*connectionInner
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
			cm, wmpc.Connection, wmpc.logger, metrics)
		if err := wmpc.submitter.TopologyChanged(wmpc.topology); err != nil {
			return err
		}
		if err := wmpc.submitter.ServerConnectionsChanged(servers); err != nil {
			return err
		}
		cm.AddServerConnectionSubscriber(wmpc)
		return nil
	}
}

func (wmpc *wssMsgPackClient) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	defer tc.Close()
	topology := tc.topology
	wmpc.topology = topology

	server.DebugLog(wmpc.logger, "debug", "TopologyChanged", "topology", topology)

	if topology != nil {
		if authenticated, _, roots := wmpc.topology.VerifyPeerCerts(wmpc.peerCerts); !authenticated {
			server.DebugLog(wmpc.logger, "debug", "TopologyChanged. Client Unauthed.", "topology", topology)
			return errors.New("WSS Client connection closed: No client certificate known")
		} else if len(roots) == len(wmpc.roots) {
			for name, capsOld := range wmpc.roots {
				if capsNew, found := roots[name]; !found || !capsNew.Equal(capsOld) {
					server.DebugLog(wmpc.logger, "debug", "TopologyChanged. Roots changed.", "topology", topology)
					return errors.New("WSS Client connection closed: roots have changed")
				}
			}
		} else {
			server.DebugLog(wmpc.logger, "debug", "TopologyChanged. Roots changed.", "topology", topology)
			return errors.New("WSS Client connection closed: roots have changed")
		}
	}
	if err := wmpc.submitter.TopologyChanged(topology); err != nil {
		return err
	}
	return nil
}

func (wmpc *wssMsgPackClient) InternalShutdown() {
	if wmpc.reader != nil {
		wmpc.reader.Stop()
		wmpc.reader = nil
	}
	cont := func() {
		wmpc.connectionManager.ClientLost(wmpc.connectionNumber, wmpc)
		wmpc.connectionManager.RemoveServerConnectionSubscriber(wmpc)
		wmpc.shutdownCompleted()
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
	wmpc.EnqueueFuncAsync(func() (bool, error) {
		return false, wmpc.submitter.SubmissionOutcomeReceived(sender, txn, outcome)
	})
}

func (wmpc *wssMsgPackClient) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	msg := &serverConnectionsChanged{submitter: wmpc.submitter, servers: servers}
	msg.InitMsg(wmpc)
	wmpc.EnqueueMsg(msg)
}

func (wmpc *wssMsgPackClient) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	msg := &serverConnectionsChanged{submitter: wmpc.submitter, servers: servers}
	msg.InitMsg(wmpc)
	wmpc.EnqueueMsg(msg)
}

func (wmpc *wssMsgPackClient) ConnectionEstablished(rmId common.RMId, c paxos.Connection, servers map[common.RMId]paxos.Connection, done func()) {
	msg := &serverConnectionsChanged{submitter: wmpc.submitter, servers: servers}
	msg.InitMsg(wmpc)
	if wmpc.EnqueueMsg(msg) {
		go func() {
			msg.Wait()
			done()
		}()
	} else {
		done()
	}
}

func (wmpc *wssMsgPackClient) ReadAndHandleOneMsg() error {
	msg := &cmsgs.ClientMessage{}
	if err := wmpc.ReadOne(msg); err != nil {
		return err
	}
	switch {
	case msg.ClientTxnSubmission != nil:
		wmpc.EnqueueFuncAsync(func() (bool, error) {
			return false, wmpc.submitTransaction(msg.ClientTxnSubmission)
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
	conn         actor.EnqueueMsgActor
	terminate    chan struct{}
	terminated   chan struct{}
	ticker       *time.Ticker
	mustSendBeat bool
}

func NewWssBeater(wmpc *wssMsgPackClient, conn actor.EnqueueMsgActor) *wssBeater {
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
			if !b.conn.EnqueueMsg(b) {
				return
			}
		}
	}
}

func (b *wssBeater) Exec() (bool, error) {
	if b != nil && b.wssMsgPackClient != nil {
		if b.mustSendBeat {
			// do not set it back to false here!
			return false, b.socket.WriteControl(websocket.PingMessage, nil, time.Time{})
		} else {
			b.mustSendBeat = true
		}
	}
	return false, nil
}

func (b *wssBeater) SendMessage(msg msgp.Encodable) error {
	if b != nil {
		b.mustSendBeat = false
		return b.Send(msg)
	}
	return nil
}
