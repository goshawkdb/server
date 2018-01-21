package websocketmsgpack

import (
	"crypto/x509"
	"encoding/binary"
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
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/network"
	"goshawkdb.io/server/types"
	"goshawkdb.io/server/types/connectionmanager"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/types/localconnection"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/txnreader"
	"net/http"
	"time"
)

var upgrader = websocket.Upgrader{
	// allow everything for now!
	CheckOrigin: func(r *http.Request) bool { return true },
}

type wssMsgPackClient struct {
	*network.Connection
	logger            log.Logger
	remoteHost        string
	connectionNumber  uint32
	self              common.RMId
	bootCount         uint32
	socket            *websocket.Conn
	peerCerts         []*x509.Certificate
	roots             map[string]common.Capability
	rootsPosCapVer    map[common.VarUUId]*types.PosCapVer
	namespace         []byte
	connectionManager connectionmanager.ConnectionManager
	localConnection   localconnection.LocalConnection
	topology          *configuration.Topology
	submitter         *client.RemoteTransactionSubmitter
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

func (wmpc *wssMsgPackClient) PerformHandshake(topology *configuration.Topology) (network.Protocol, error) {
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
	binary.BigEndian.PutUint32(namespace[4:8], wmpc.bootCount)
	binary.BigEndian.PutUint32(namespace[8:], uint32(wmpc.self))
	wmpc.namespace = namespace

	cRoots := make([]*cmsgs.Root, 0, len(wmpc.roots))
	rootsPosCapVer := make(map[common.VarUUId]*types.PosCapVer, len(wmpc.roots))
	for idx, name := range wmpc.topology.Roots {
		if capability, found := wmpc.roots[name]; found {
			root := wmpc.topology.RootVarUUIds[idx]
			cRoot := &cmsgs.Root{
				Name:       name,
				VarId:      root.VarUUId[:],
				Capability: &cmsgs.Capability{},
			}
			cRoot.Capability.FromCapnp(capability.AsMsg())
			cRoots = append(cRoots, cRoot)
			rootsPosCapVer[*root.VarUUId] = &types.PosCapVer{
				Positions:  root.Positions,
				Capability: capability,
				Version:    common.VersionZero,
			}
		}
	}

	wmpc.rootsPosCapVer = rootsPosCapVer
	return &cmsgs.HelloClientFromServer{
		Namespace: namespace,
		Roots:     cRoots,
	}
}

func (wmpc *wssMsgPackClient) Restart() bool {
	return false // client connections are never restarted
}

func (wmpc *wssMsgPackClient) Run(conn *network.Connection) error {
	wmpc.Connection = conn
	servers, metrics := wmpc.connectionManager.ClientEstablished(wmpc.connectionNumber, wmpc)
	if servers == nil {
		return errors.New("Not ready for client connections")

	} else {
		wmpc.logger.Log("msg", "Connection established.", "remoteHost", wmpc.remoteHost)

		wmpc.createBeater()
		wmpc.createReader()

		wmpc.submitter = client.NewRemoteTransactionSubmitter(wmpc.self, wmpc.bootCount, wmpc.connectionManager, wmpc.Connection, wmpc.Rng, wmpc.logger, wmpc.rootsPosCapVer, metrics)
		if err := wmpc.submitter.TopologyChanged(wmpc.topology); err != nil {
			return err
		}
		if err := wmpc.submitter.ServerConnectionsChanged(servers); err != nil {
			return err
		}
		wmpc.connectionManager.AddServerConnectionSubscriber(wmpc)
		return nil
	}
}

func (wmpc *wssMsgPackClient) TopologyChanged(tc *network.ConnectionMsgTopologyChanged) error {
	defer tc.Close()
	topology := tc.Topology
	wmpc.topology = topology

	utils.DebugLog(wmpc.logger, "debug", "TopologyChanged", "topology", topology)

	if topology != nil {
		if authenticated, _, roots := wmpc.topology.VerifyPeerCerts(wmpc.peerCerts); !authenticated {
			utils.DebugLog(wmpc.logger, "debug", "TopologyChanged. Client Unauthed.", "topology", topology)
			return errors.New("WSS Client connection closed: No client certificate known")
		} else if len(roots) == len(wmpc.roots) {
			for name, capsOld := range wmpc.roots {
				if capsNew, found := roots[name]; !found || capsNew != capsOld {
					utils.DebugLog(wmpc.logger, "debug", "TopologyChanged. Roots changed.", "topology", topology)
					return errors.New("WSS Client connection closed: roots have changed")
				}
			}
		} else {
			utils.DebugLog(wmpc.logger, "debug", "TopologyChanged. Roots changed.", "topology", topology)
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
	// The submitter, and indeed this entire connection (albeit without
	// the socket), must hang around until all the txns within the
	// submitter have been completed - one way or another. Otherwise,
	// if this connection dies but this node stays up, we could create
	// dangling transactions.
	onceEmpty := func(subs []*client.SubscriptionManager) {
		wmpc.connectionManager.ClientLost(wmpc.connectionNumber, wmpc)
		wmpc.connectionManager.RemoveServerConnectionSubscriber(wmpc)
		for _, sm := range subs {
			sm.Unsubscribe(wmpc.localConnection)
		}
		wmpc.ShutdownCompleted()
	}
	if wmpc.submitter == nil {
		onceEmpty(nil)
	} else {
		wmpc.submitter.Shutdown(onceEmpty)
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

func (wmpc *wssMsgPackClient) SubmissionOutcomeReceived(sender common.RMId, subId *common.TxnId, txn *txnreader.TxnReader, outcome *msgs.Outcome) {
	wmpc.EnqueueFuncAsync(func() (bool, error) {
		return false, wmpc.submitter.SubmissionOutcomeReceived(sender, subId, txn, outcome)
	})
}

type serverConnectionsChanged struct {
	actor.MsgSyncQuery
	submitter *client.RemoteTransactionSubmitter
	servers   map[common.RMId]*sconn.ServerConnection
}

func (msg *serverConnectionsChanged) Exec() (bool, error) {
	defer msg.MustClose()
	return false, msg.submitter.ServerConnectionsChanged(msg.servers)
}

func (wmpc *wssMsgPackClient) ConnectedRMs(servers map[common.RMId]*sconn.ServerConnection) {
	msg := &serverConnectionsChanged{submitter: wmpc.submitter, servers: servers}
	msg.InitMsg(wmpc)
	wmpc.EnqueueMsg(msg)
}

func (wmpc *wssMsgPackClient) ConnectionLost(rmId common.RMId, servers map[common.RMId]*sconn.ServerConnection) {
	msg := &serverConnectionsChanged{submitter: wmpc.submitter, servers: servers}
	msg.InitMsg(wmpc)
	wmpc.EnqueueMsg(msg)
}

func (wmpc *wssMsgPackClient) ConnectionEstablished(c *sconn.ServerConnection, servers map[common.RMId]*sconn.ServerConnection, done func()) {
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
	return wmpc.submitter.SubmitRemoteClientTransaction(origTxnId, ctxnCapn, func(clientOutcome *capcmsgs.ClientTxnOutcome, err error) error {
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
