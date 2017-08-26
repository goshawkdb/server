package tcpcapnproto

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/network"
	"goshawkdb.io/server/router"
	"goshawkdb.io/server/types/connectionmanager"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/txnreader"
	"net"
	"time"
)

// TLS Capnp Handshaker

type TLSCapnpHandshaker struct {
	*common.TLSCapnpHandshakerBase
	logger            log.Logger
	connectionNumber  uint32
	self              common.RMId
	bootCount         uint32
	restartable       bool
	router            *router.Router
	connectionManager connectionmanager.ConnectionManager
	topology          *configuration.Topology
	serverRemote      *sconn.ServerConnection
}

func NewTLSCapnpHandshaker(dialer common.Dialer, logger log.Logger, count uint32, rmId common.RMId, bootCount uint32, router *router.Router, cm connectionmanager.ConnectionManager, serverRemote *sconn.ServerConnection) *TLSCapnpHandshaker {
	return &TLSCapnpHandshaker{
		TLSCapnpHandshakerBase: common.NewTLSCapnpHandshakerBase(dialer),
		logger:                 logger,
		connectionNumber:       count,
		self:                   rmId,
		bootCount:              bootCount,
		restartable:            count == 0,
		router:                 router,
		connectionManager:      cm,
		serverRemote:           serverRemote,
	}
}

func (tch *TLSCapnpHandshaker) PerformHandshake(topology *configuration.Topology) (network.Protocol, error) {
	tch.topology = topology

	helloSeg := tch.makeHello()
	if err := tch.Send(common.SegToBytes(helloSeg)); err != nil {
		return nil, err
	}

	if seg, err := tch.ReadExactlyOne(); err == nil {
		hello := cmsgs.ReadRootHello(seg)
		if tch.verifyHello(&hello) {
			if tch.connectionNumber == 0 && hello.IsClient() { // we dialed, so it had better be a server.
				return nil, errors.New("Received erroneous hello from peer: expected to find server, but found client.")
			}
			if hello.IsClient() {
				tcc := tch.newTLSCapnpClient()
				return tcc, tcc.finishHandshake()

			} else {
				tcs := tch.newTLSCapnpServer()
				return tcs, tcs.finishHandshake()
			}

		} else {
			product := hello.Product()
			if l := len(common.ProductName); len(product) > l {
				product = product[:l] + "..."
			}
			version := hello.Version()
			if l := len(common.ProductVersion); len(version) > l {
				version = version[:l] + "..."
			}
			return nil, fmt.Errorf("Received erroneous hello from peer: received product name '%s' (expected '%s'), product version '%s' (expected '%s')",
				product, common.ProductName, version, common.ProductVersion)
		}
	} else {
		return nil, err
	}
}

func (tch *TLSCapnpHandshaker) Restart() bool {
	tch.InternalShutdown()
	tch.Reset()
	return tch.restartable
}

func (tch *TLSCapnpHandshaker) String() string {
	if tch.connectionNumber == 0 {
		return fmt.Sprintf("TLSCapnpHandshaker to %s", tch.RemoteHost())
	} else {
		return fmt.Sprintf("TLSCapnpHandshaker %d from remote", tch.connectionNumber)
	}
}

func (tch *TLSCapnpHandshaker) makeHello() *capn.Segment {
	seg := capn.NewBuffer(nil)
	hello := cmsgs.NewRootHello(seg)
	hello.SetProduct(common.ProductName)
	hello.SetVersion(common.ProductVersion)
	hello.SetIsClient(false)
	return seg
}

func (tch *TLSCapnpHandshaker) verifyHello(hello *cmsgs.Hello) bool {
	return hello.Product() == common.ProductName &&
		hello.Version() == common.ProductVersion
}

func (tch *TLSCapnpHandshaker) newTLSCapnpClient() *TLSCapnpClient {
	return &TLSCapnpClient{
		TLSCapnpHandshaker: tch,
		logger:             log.With(tch.logger, "type", "client", "connNumber", tch.connectionNumber),
	}
}

func (tch *TLSCapnpHandshaker) newTLSCapnpServer() *TLSCapnpServer {
	// If the remote node is removed from the cluster then dialer is
	// set to nil to stop us recreating this connection when it
	// disconnects. If this connection came from the listener
	// (i.e. dialer == nil) then we never restart it anyway.
	return &TLSCapnpServer{
		TLSCapnpHandshaker: tch,
		logger:             log.With(tch.logger, "type", "server"),
		remote:             tch.serverRemote,
	}
}

func (tch *TLSCapnpHandshaker) baseTLSConfig() *tls.Config {
	nodeCertPrivKeyPair := tch.connectionManager.NodeCertificatePrivateKeyPair()
	if nodeCertPrivKeyPair == nil {
		return nil
	}
	roots := x509.NewCertPool()
	roots.AddCert(nodeCertPrivKeyPair.CertificateRoot)

	return &tls.Config{
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
	}
}

func (tch *TLSCapnpHandshaker) serverError(err error) error {
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	msg.SetConnectionError(err.Error())
	// ignoring the possible error from tch.send - it's a best effort
	// basis at this point.
	tch.Send(common.SegToBytes(seg))
	return err
}

// TLS Capnp Server

type TLSCapnpServer struct {
	*TLSCapnpHandshaker
	remote *sconn.ServerConnection
	logger log.Logger
	conn   *network.Connection
	reader *common.SocketReader
}

func (tcs *TLSCapnpServer) finishHandshake() error {

	// TLS seems to require us to pick one end as the client and one
	// end as the server even though in a server-server connection we
	// really don't care which is which.
	config := tcs.baseTLSConfig()
	if tcs.connectionNumber == 0 {
		// We dialed, so we're going to act as the client
		config.InsecureSkipVerify = true
		socket := tls.Client(tcs.Socket(), config)
		if err := socket.SetDeadline(time.Time{}); err != nil {
			return err
		}
		tcs.Dialer = common.NewTCPDialer(socket, tcs.RemoteHost(), tcs.logger)

		// This is nuts: as a server, we can demand the client cert and
		// verify that without any concept of a client name. But as the
		// client, if we don't have a server name, then we have to do
		// the verification ourself. Why is TLS asymmetric?!

		if err := socket.Handshake(); err != nil {
			tcs.logger.Log("authentication", "failure", "error", err)
			return err
		}

		opts := x509.VerifyOptions{
			Roots:         config.RootCAs,
			DNSName:       "", // disable server name checking
			Intermediates: x509.NewCertPool(),
		}
		certs := socket.ConnectionState().PeerCertificates
		for i, cert := range certs {
			if i == 0 {
				continue
			}
			opts.Intermediates.AddCert(cert)
		}
		if _, err := certs[0].Verify(opts); err != nil {
			tcs.logger.Log("authentication", "failure", "error", err)
			return err
		}

	} else {
		// We came from the listener, so we're going to act as the server.
		config.ClientAuth = tls.RequireAndVerifyClientCert
		socket := tls.Server(tcs.Socket(), config)
		if err := socket.SetDeadline(time.Time{}); err != nil {
			return err
		}
		tcs.Dialer = common.NewTCPDialer(socket, tcs.RemoteHost(), tcs.logger)

		if err := socket.Handshake(); err != nil {
			tcs.logger.Log("authentication", "failure", "error", err)
			return err
		}
	}
	tcs.logger.Log("authentication", "success")

	hello := tcs.makeHelloServer()
	if err := tcs.TLSCapnpHandshaker.Send(common.SegToBytes(hello)); err != nil {
		return err
	}

	if seg, err := tcs.ReadOne(); err == nil {
		hello := msgs.ReadRootHelloServerFromServer(seg)
		tcs.remote.Host = hello.LocalHost()
		tcs.remote.RMId = common.RMId(hello.RmId())
		if tcs.verifyTopology(&hello) {
			if _, found := tcs.topology.RMsRemoved[tcs.remote.RMId]; found {
				tcs.restartable = false
				return tcs.serverError(
					fmt.Errorf("%v has been removed from topology and may not rejoin.", tcs.remote.RMId))
			}

			tcs.remote.ClusterUUId = hello.ClusterUUId()
			tcs.remote.BootCount = hello.BootCount()
			return nil
		} else {
			return fmt.Errorf("Unequal remote topology (%v, %v)", tcs.remote.Host, tcs.remote.RMId)
		}
	} else {
		return err
	}
}

func (tcs *TLSCapnpServer) makeHelloServer() *capn.Segment {
	seg := capn.NewBuffer(nil)
	hello := msgs.NewRootHelloServerFromServer(seg)
	localHost := tcs.connectionManager.LocalHost()
	hello.SetLocalHost(localHost)
	hello.SetRmId(uint32(tcs.self))
	hello.SetBootCount(tcs.bootCount)
	hello.SetClusterId(tcs.topology.ClusterId)
	hello.SetClusterUUId(tcs.topology.ClusterUUId)
	return seg
}

func (tcs *TLSCapnpServer) verifyTopology(remote *msgs.HelloServerFromServer) bool {
	if tcs.topology.ClusterId == remote.ClusterId() {
		remoteUUId := remote.ClusterUUId()
		localUUId := tcs.topology.ClusterUUId
		return remoteUUId == 0 || localUUId == 0 || remoteUUId == localUUId
	}
	return false
}

func (tcs *TLSCapnpServer) Run(conn *network.Connection) error {
	tcs.conn = conn
	tcs.logger.Log("msg", "Connection established.", "remoteHost", tcs.remote.Host, "remoteRMId", tcs.remote.RMId)
	tcs.remote.Sender = tcs
	flushSeg := capn.NewBuffer(nil)
	flushMsg := msgs.NewRootMessage(flushSeg)
	flushMsg.SetFlushed()
	flushBytes := common.SegToBytes(flushSeg)
	tcs.remote.Flushed = func() { tcs.Send(flushBytes) }
	tcs.remote.ShutdownSync = tcs.conn.ShutdownSync
	tcs.remote.Status = tcs.conn.Status

	seg := capn.NewBuffer(nil)
	message := msgs.NewRootMessage(seg)
	message.SetHeartbeat()
	tcs.CreateBeater(conn, common.SegToBytes(seg))
	tcs.createReader()

	tcs.connectionManager.ServerEstablished(tcs.remote)

	return nil
}

func (tcs *TLSCapnpServer) TopologyChanged(tc *network.ConnectionMsgTopologyChanged) error {
	defer tc.MustClose()

	topology := tc.Topology
	tcs.topology = topology

	utils.DebugLog(tcs.logger, "debug", "TopologyChanged.", "topology", topology)
	if topology != nil && tcs.restartable {
		if _, found := topology.RMsRemoved[tcs.remote.RMId]; found {
			tcs.restartable = false
		}
	}

	return nil
}

func (tcs *TLSCapnpServer) Send(msg []byte) {
	tcs.conn.EnqueueFuncAsync(func() (bool, error) { return false, tcs.SendMessage(msg) })
}

func (tcs *TLSCapnpServer) Restart() bool {
	tcs.connectionManager.ServerLost(tcs.remote, tcs.restartable)
	tcs.internalShutdown()

	return tcs.TLSCapnpHandshaker.Restart()
}

func (tcs *TLSCapnpServer) InternalShutdown() {
	tcs.connectionManager.ServerLost(tcs.remote, false)
	tcs.TLSCapnpHandshaker.InternalShutdown()
	tcs.internalShutdown()
	tcs.Reset()
	tcs.conn.ShutdownCompleted()
}

func (tcs *TLSCapnpServer) internalShutdown() {
	if tcs.reader != nil {
		tcs.reader.Stop()
		tcs.reader = nil
	}
}

func (tcs *TLSCapnpServer) ReadAndHandleOneMsg() error {
	seg, err := tcs.ReadOne()
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("Missed too many connection heartbeats. (%v)", netErr)
		} else {
			return err
		}
	}
	msg := msgs.ReadRootMessage(seg)
	switch which := msg.Which(); which {
	case msgs.MESSAGE_HEARTBEAT:
		return nil // do nothing
	case msgs.MESSAGE_CONNECTIONERROR:
		return fmt.Errorf("Error received from %v: \"%s\"", tcs.remote.RMId, msg.ConnectionError())
	default:
		tcs.router.Dispatch(tcs.remote.RMId, which, msg)
		return nil
	}
}

func (tcs *TLSCapnpServer) String() string {
	if tcs.connectionNumber == 0 {
		return fmt.Sprintf("TLSCapnpServer for %v(%d) to %s", tcs.remote.RMId, tcs.remote.BootCount, tcs.remote.Host)
	} else {
		return fmt.Sprintf("TLSCapnpServer for %v(%d) from %s", tcs.remote.RMId, tcs.remote.BootCount, tcs.remote.Host)
	}
}

func (tcs *TLSCapnpServer) createReader() {
	if tcs.reader == nil {
		tcs.reader = common.NewSocketReader(tcs.conn, tcs)
		tcs.reader.Start()
	}
}

// TLS Capnp Client

type TLSCapnpClient struct {
	*TLSCapnpHandshaker
	*network.Connection
	remoteHost string
	logger     log.Logger
	peerCerts  []*x509.Certificate
	roots      map[string]*common.Capability
	rootsVar   map[common.VarUUId]*common.Capability
	namespace  []byte
	submitter  *client.ClientTxnSubmitter
	reader     *common.SocketReader
}

func (tcc *TLSCapnpClient) finishHandshake() error {
	config := tcc.baseTLSConfig()
	if config == nil {
		return errors.New("Cluster not yet formed")
	}
	config.ClientAuth = tls.RequireAnyClientCert
	socket := tls.Server(tcc.Socket(), config)
	if err := socket.SetDeadline(time.Time{}); err != nil {
		return err
	}
	tcc.Dialer = common.NewTCPDialer(socket, tcc.Dialer.RemoteHost(), tcc.logger)
	if err := socket.Handshake(); err != nil {
		return err
	}

	if tcc.topology.ClusterUUId == 0 {
		return errors.New("Cluster not yet formed")
	} else if len(tcc.topology.Roots) == 0 {
		return errors.New("No roots: cluster not yet formed")
	}

	peerCerts := socket.ConnectionState().PeerCertificates
	if authenticated, hashsum, roots := tcc.topology.VerifyPeerCerts(peerCerts); authenticated {
		tcc.peerCerts = peerCerts
		tcc.roots = roots
		tcc.logger.Log("authentication", "success", "fingerprint", hex.EncodeToString(hashsum[:]))
		helloFromServer := tcc.makeHelloClient()
		if err := tcc.Send(common.SegToBytes(helloFromServer)); err != nil {
			return err
		}
		tcc.remoteHost = tcc.Socket().RemoteAddr().String()
		return nil
	} else {
		tcc.logger.Log("authentication", "failure")
		return errors.New("Client connection rejected: No client certificate known")
	}
}

func (tcc *TLSCapnpClient) makeHelloClient() *capn.Segment {
	seg := capn.NewBuffer(nil)
	hello := cmsgs.NewRootHelloClientFromServer(seg)
	namespace := make([]byte, common.KeyLen-8)
	binary.BigEndian.PutUint32(namespace[0:4], tcc.connectionNumber)
	binary.BigEndian.PutUint32(namespace[4:8], tcc.TLSCapnpHandshaker.bootCount)
	binary.BigEndian.PutUint32(namespace[8:], uint32(tcc.self))
	tcc.namespace = namespace
	hello.SetNamespace(namespace)
	rootsCap := cmsgs.NewRootList(seg, len(tcc.roots))
	idy := 0
	rootsVar := make(map[common.VarUUId]*common.Capability, len(tcc.roots))
	for idx, name := range tcc.topology.Roots {
		if capability, found := tcc.roots[name]; found {
			rootCap := rootsCap.At(idy)
			idy++
			vUUId := tcc.topology.RootVarUUIds[idx].VarUUId
			rootCap.SetName(name)
			rootCap.SetVarId(vUUId[:])
			rootCap.SetCapability(capability.Capability)
			rootsVar[*vUUId] = capability
		}
	}
	hello.SetRoots(rootsCap)
	tcc.rootsVar = rootsVar
	return seg
}

func (tcc *TLSCapnpClient) Run(conn *network.Connection) error {
	tcc.Connection = conn
	servers, metrics := tcc.TLSCapnpHandshaker.connectionManager.ClientEstablished(tcc.connectionNumber, tcc)
	if servers == nil {
		return errors.New("Not ready for client connections")

	} else {
		tcc.logger.Log("msg", "Connection established.", "remoteHost", tcc.remoteHost)

		seg := capn.NewBuffer(nil)
		message := cmsgs.NewRootClientMessage(seg)
		message.SetHeartbeat()
		tcc.CreateBeater(conn, common.SegToBytes(seg))
		tcc.createReader()

		cm := tcc.TLSCapnpHandshaker.connectionManager
		tcc.submitter = client.NewClientTxnSubmitter(tcc.self, tcc.bootCount, tcc.rootsVar, tcc.namespace,
			cm, tcc.Connection, tcc.logger, metrics)
		if err := tcc.submitter.TopologyChanged(tcc.topology); err != nil {
			return err
		}
		if err := tcc.submitter.ServerConnectionsChanged(servers); err != nil {
			return err
		}
		cm.AddServerConnectionSubscriber(tcc)
		return nil
	}
}

func (tcc *TLSCapnpClient) TopologyChanged(tc *network.ConnectionMsgTopologyChanged) error {
	defer tc.MustClose()
	topology := tc.Topology
	tcc.topology = topology

	utils.DebugLog(tcc.logger, "debug", "TopologyChanged", "topology", topology)

	if topology != nil {
		if authenticated, _, roots := tcc.topology.VerifyPeerCerts(tcc.peerCerts); !authenticated {
			utils.DebugLog(tcc.logger, "debug", "TopologyChanged. Client Unauthed.", "topology", topology)
			return errors.New("Client connection closed: No client certificate known")
		} else if len(roots) == len(tcc.roots) {
			for name, capsOld := range tcc.roots {
				if capsNew, found := roots[name]; !found || !capsNew.Equal(capsOld) {
					utils.DebugLog(tcc.logger, "debug", "TopologyChanged. Roots Changed.", "topology", topology)
					return errors.New("Client connection closed: roots have changed")
				}
			}
		} else {
			utils.DebugLog(tcc.logger, "debug", "TopologyChanged. Roots Changed.", "topology", topology)
			return errors.New("Client connection closed: roots have changed")
		}
	}
	if err := tcc.submitter.TopologyChanged(topology); err != nil {
		return err
	}
	return nil
}

func (tcc *TLSCapnpClient) Restart() bool {
	return false // client connections are never restarted
}

func (tcc *TLSCapnpClient) InternalShutdown() {
	if tcc.reader != nil {
		tcc.reader.Stop()
		tcc.reader = nil
	}
	cont := func() {
		tcc.TLSCapnpHandshaker.connectionManager.ClientLost(tcc.connectionNumber, tcc)
		tcc.TLSCapnpHandshaker.connectionManager.RemoveServerConnectionSubscriber(tcc)
		tcc.ShutdownCompleted()
	}
	if tcc.submitter == nil {
		cont()
	} else {
		tcc.submitter.Shutdown(cont)
	}
	tcc.TLSCapnpHandshaker.InternalShutdown()
}

func (tcc *TLSCapnpClient) String() string {
	return fmt.Sprintf("TLSCapnpClient %d from %s", tcc.connectionNumber, tcc.remoteHost)
}

func (tcc *TLSCapnpClient) SubmissionOutcomeReceived(sender common.RMId, txn *txnreader.TxnReader, outcome *msgs.Outcome) {
	tcc.EnqueueFuncAsync(func() (bool, error) {
		return false, tcc.submitter.SubmissionOutcomeReceived(sender, txn, outcome)
	})
}

type serverConnectionsChanged struct {
	actor.MsgSyncQuery
	submitter *client.ClientTxnSubmitter
	servers   map[common.RMId]*sconn.ServerConnection
}

func (msg *serverConnectionsChanged) Exec() (bool, error) {
	defer msg.MustClose()
	return false, msg.submitter.ServerConnectionsChanged(msg.servers)
}

func (tcc *TLSCapnpClient) ConnectedRMs(servers map[common.RMId]*sconn.ServerConnection) {
	msg := &serverConnectionsChanged{submitter: tcc.submitter, servers: servers}
	msg.InitMsg(tcc)
	tcc.EnqueueMsg(msg)
}

func (tcc *TLSCapnpClient) ConnectionLost(rmId common.RMId, servers map[common.RMId]*sconn.ServerConnection) {
	msg := &serverConnectionsChanged{submitter: tcc.submitter, servers: servers}
	msg.InitMsg(tcc)
	tcc.EnqueueMsg(msg)
}

func (tcc *TLSCapnpClient) ConnectionEstablished(c *sconn.ServerConnection, servers map[common.RMId]*sconn.ServerConnection, done func()) {
	msg := &serverConnectionsChanged{submitter: tcc.submitter, servers: servers}
	msg.InitMsg(tcc)
	if tcc.EnqueueMsg(msg) {
		go func() {
			msg.Wait()
			done()
		}()
	} else {
		done()
	}
}

func (tcc *TLSCapnpClient) ReadAndHandleOneMsg() error {
	seg, err := tcc.ReadOne()
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("Missed too many connection heartbeats. (%v)", netErr)
		} else {
			return err
		}
	}
	msg := cmsgs.ReadRootClientMessage(seg)
	switch which := msg.Which(); which {
	case cmsgs.CLIENTMESSAGE_HEARTBEAT:
		return nil // do nothing
	case cmsgs.CLIENTMESSAGE_CLIENTTXNSUBMISSION:
		// submitter is accessed from the connection go routine, so we must relay this
		tcc.EnqueueFuncAsync(func() (bool, error) {
			return false, tcc.submitTransaction(msg.ClientTxnSubmission())
		})
		return nil
	default:
		return fmt.Errorf("Unexpected message type received from client: %v", which)
	}
}

func (tcc *TLSCapnpClient) submitTransaction(ctxn cmsgs.ClientTxn) error {
	origTxnId := common.MakeTxnId(ctxn.Id())
	return tcc.submitter.SubmitClientTransaction(&ctxn, func(clientOutcome *cmsgs.ClientTxnOutcome, err error) error {
		switch {
		case err != nil: // error is non-fatal to connection
			return tcc.SendMessage(tcc.clientTxnError(&ctxn, err, origTxnId))
		case clientOutcome == nil: // shutdown
			return nil
		default:
			seg := capn.NewBuffer(nil)
			msg := cmsgs.NewRootClientMessage(seg)
			msg.SetClientTxnOutcome(*clientOutcome)
			return tcc.SendMessage(common.SegToBytes(msg.Segment))
		}
	})
}

func (tcc *TLSCapnpClient) clientTxnError(ctxn *cmsgs.ClientTxn, err error, origTxnId *common.TxnId) []byte {
	seg := capn.NewBuffer(nil)
	msg := cmsgs.NewRootClientMessage(seg)
	outcome := cmsgs.NewClientTxnOutcome(seg)
	msg.SetClientTxnOutcome(outcome)
	outcome.SetId(origTxnId[:])
	outcome.SetFinalId(ctxn.Id())
	outcome.SetError(err.Error())
	return common.SegToBytes(seg)
}

func (tcc *TLSCapnpClient) createReader() {
	if tcc.reader == nil {
		tcc.reader = common.NewSocketReader(tcc.Connection, tcc)
		tcc.reader.Start()
	}
}
