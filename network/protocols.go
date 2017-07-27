package network

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
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"io"
	"math/rand"
	"net"
	"time"
)

type Dialer interface {
	Dial() (Handshaker, error)
}

type Handshaker interface {
	PerformHandshake(*configuration.Topology) (Protocol, error)
	RestartDialer() Dialer
	InternalShutdown()
}

type Protocol interface {
	Run(*Connection) error
	TopologyChanged(*connectionMsgTopologyChanged) error
	RestartDialer() Dialer
	InternalShutdown()
}

// TCP TLS Capnp dialer

type TCPDialer struct {
	logger           log.Logger
	remoteHost       string
	handshakeBuilder HandshakeBuilder
}

type HandshakeBuilder func(Dialer, *net.TCPConn) Handshaker

func NewTCPDialerForTLSCapnp(remoteHost string, handshakeBuilder HandshakeBuilder, logger log.Logger) *TCPDialer {
	if remoteHost == "" {
		panic("Empty host")
	}
	dialer := &TCPDialer{
		logger:           logger,
		remoteHost:       remoteHost,
		handshakeBuilder: handshakeBuilder,
	}
	return dialer
}

func (td *TCPDialer) Dial() (Handshaker, error) {
	td.logger.Log("msg", "Attempting connection.", "remoteHost", td.remoteHost)
	tcpAddr, err := net.ResolveTCPAddr("tcp", td.remoteHost)
	if err != nil {
		return nil, err
	}
	socket, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		if socket != nil {
			socket.Close()
		}
		return nil, err
	}
	return td.handshakeBuilder(td, socket), nil
}

func (td *TCPDialer) String() string {
	return fmt.Sprintf("TCPDialer to %s", td.remoteHost)
}

// TLS Capnp Handshaker Base

type TLSCapnpHandshakerBase struct {
	dialer         Dialer
	logger         log.Logger
	remoteHost     string
	socket         net.Conn
	rng            *rand.Rand
	beater         *TLSCapnpBeater
	buf            []byte
	bufWriteOffset int64
}

func (tchb *TLSCapnpHandshakerBase) InternalShutdown() {
	if tchb.beater != nil {
		tchb.beater.Stop()
		tchb.beater = nil
	}
	if tchb.socket != nil {
		tchb.socket.Close()
		tchb.socket = nil
	}
}

func (tchb *TLSCapnpHandshakerBase) Send(msg []byte) error {
	l := len(msg)
	for l > 0 {
		switch w, err := tchb.socket.Write(msg); {
		case err != nil:
			return err
		case w == l:
			return nil
		default:
			msg = msg[w:]
			l -= w
		}
	}
	return nil
}

// this exists for handshake so that we don't accidentally read bits of the TLS handshake and break that!
func (tchb *TLSCapnpHandshakerBase) ReadExactlyOne() (*capn.Segment, error) {
	if err := tchb.socket.SetReadDeadline(time.Now().Add(common.HeartbeatInterval << 1)); err != nil {
		return nil, err
	}
	return capn.ReadFromStream(tchb.socket, nil)
}

func (tchb *TLSCapnpHandshakerBase) ReadOne() (*capn.Segment, error) {
	if tchb.bufWriteOffset > 0 {
		// still have data in the buffer, so first try decoding that
		if seg, err := tchb.attemptCapnpDecode(); seg != nil || err != nil {
			return seg, err
		}
	}
	for {
		if int64(len(tchb.buf)) == tchb.bufWriteOffset { // run out of buf space
			tchb.increaseBuffer()
		}
		if err := tchb.socket.SetReadDeadline(time.Now().Add(common.HeartbeatInterval << 1)); err != nil {
			return nil, err
		}
		if readCount, err := tchb.socket.Read(tchb.buf[tchb.bufWriteOffset:]); err != nil {
			return nil, err
		} else if readCount > 0 { // we read something; try to parse
			tchb.bufWriteOffset += int64(readCount)
			if seg, err := tchb.attemptCapnpDecode(); seg != nil || err != nil {
				return seg, err
			}
		}
	}
}

func (tchb *TLSCapnpHandshakerBase) increaseBuffer() {
	size := int64(common.ConnectionBufferSize)
	req := tchb.bufWriteOffset
	for size <= req {
		size *= 2
	}
	buf := make([]byte, size)
	copy(buf[:req], tchb.buf[:req])
	tchb.buf = buf
}

func (tchb *TLSCapnpHandshakerBase) attemptCapnpDecode() (*capn.Segment, error) {
	seg, used, err := capn.ReadFromMemoryZeroCopy(tchb.buf[:tchb.bufWriteOffset])
	if used > 0 {
		tchb.buf = tchb.buf[used:]
		tchb.bufWriteOffset -= used
	}
	if err == io.EOF {
		// ignore it - it just means we don't have enough data from the socket yet
		return nil, nil
	} else {
		return seg, err
	}
}

func (tchb *TLSCapnpHandshakerBase) CreateBeater(conn ConnectionActor, beatBytes []byte) {
	if tchb.beater == nil {
		tchb.beater = NewTLSCapnpBeater(tchb, conn, beatBytes)
		tchb.beater.Start()
	}
}

// TLS Capnp Handshaker Server

type TLSCapnpHandshakerServer struct {
	TLSCapnpHandshakerBase
	connectionNumber  uint32
	connectionManager *ConnectionManager
	topology          *configuration.Topology
}

func NewTLSCapnpHandshakerServerBuilder(cm *ConnectionManager, count uint32, remoteHost string, logger log.Logger) HandshakeBuilder {
	return func(dialer Dialer, socket *net.TCPConn) Handshaker {
		return NewTLSCapnpHandshakerServer(dialer, socket, cm, count, remoteHost, logger)
	}
}

func NewTLSCapnpHandshakerServer(dialer Dialer, socket *net.TCPConn, cm *ConnectionManager, count uint32, remoteHost string, logger log.Logger) Handshaker {
	if err := common.ConfigureSocket(socket); err != nil {
		logger.Log("msg", "Error when configuring socket", "error", err)
		return nil
	}
	return &TLSCapnpHandshakerServer{
		TLSCapnpHandshakerBase: TLSCapnpHandshakerBase{
			dialer:     dialer,
			logger:     logger,
			remoteHost: remoteHost,
			socket:     socket,
			rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		},
		connectionNumber:  count,
		connectionManager: cm,
	}
}

func (tchs *TLSCapnpHandshakerServer) PerformHandshake(topology *configuration.Topology) (Protocol, error) {
	tchs.topology = topology

	helloSeg := tchs.makeHello()
	if err := tchs.Send(server.SegToBytes(helloSeg)); err != nil {
		return nil, err
	}

	if seg, err := tchs.ReadExactlyOne(); err == nil {
		hello := cmsgs.ReadRootHello(seg)
		if tchs.verifyHello(&hello) {
			if hello.IsClient() {
				tcc := tchs.newTLSCapnpClient()
				return tcc, tcc.finishHandshake()

			} else {
				tcs := tchs.newTLSCapnpServer()
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

func (tchs *TLSCapnpHandshakerServer) RestartDialer() Dialer {
	tchs.InternalShutdown()
	return tchs.dialer
}

func (tchs *TLSCapnpHandshakerServer) String() string {
	if tchs.dialer == nil {
		return fmt.Sprintf("TLSCapnpHandshakerServer %d from remote", tchs.connectionNumber)
	} else {
		return fmt.Sprintf("TLSCapnpHandshakerServer to %s", tchs.remoteHost)
	}
}

func (tchs *TLSCapnpHandshakerServer) makeHello() *capn.Segment {
	seg := capn.NewBuffer(nil)
	hello := cmsgs.NewRootHello(seg)
	hello.SetProduct(common.ProductName)
	hello.SetVersion(common.ProductVersion)
	hello.SetIsClient(false)
	return seg
}

func (tchs *TLSCapnpHandshakerServer) verifyHello(hello *cmsgs.Hello) bool {
	return hello.Product() == common.ProductName &&
		hello.Version() == common.ProductVersion
}

func (tchs *TLSCapnpHandshakerServer) newTLSCapnpClient() *TLSCapnpClient {
	return &TLSCapnpClient{
		TLSCapnpHandshakerServer: tchs,
		logger: log.With(tchs.logger, "type", "client", "connNumber", tchs.connectionNumber),
	}
}

func (tchs *TLSCapnpHandshakerServer) newTLSCapnpServer() *TLSCapnpServer {
	// If the remote node is removed from the cluster then dialer is
	// set to nil to stop us recreating this connection when it
	// disconnects. If this connection came from the listener
	// (i.e. dialer == nil) then we never restart it anyway.
	return &TLSCapnpServer{
		TLSCapnpHandshakerServer: tchs,
		logger: log.With(tchs.logger, "type", "server"),
	}
}

func (tchs *TLSCapnpHandshakerServer) baseTLSConfig() *tls.Config {
	nodeCertPrivKeyPair := tchs.connectionManager.NodeCertificatePrivateKeyPair()
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

func (tchs *TLSCapnpHandshakerServer) serverError(err error) error {
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	msg.SetConnectionError(err.Error())
	// ignoring the possible error from tchs.send - it's a best effort
	// basis at this point.
	tchs.Send(server.SegToBytes(seg))
	return err
}

// TLS Capnp Server

type TLSCapnpServer struct {
	*TLSCapnpHandshakerServer
	logger            log.Logger
	conn              *Connection
	remoteRMId        common.RMId
	remoteClusterUUId uint64
	remoteBootCount   uint32
	reader            *SocketReader
}

func (tcs *TLSCapnpServer) finishHandshake() error {
	// TLS seems to require us to pick one end as the client and one
	// end as the server even though in a server-server connection we
	// really don't care which is which.
	config := tcs.baseTLSConfig()
	if tcs.dialer == nil {
		// We came from the listener, so we're going to act as the server.
		config.ClientAuth = tls.RequireAndVerifyClientCert
		socket := tls.Server(tcs.socket, config)
		if err := socket.SetDeadline(time.Time{}); err != nil {
			return err
		}
		tcs.socket = socket

		if err := socket.Handshake(); err != nil {
			tcs.logger.Log("authentication", "failure", "error", err)
			return err
		}

	} else {
		// We dialed, so we're going to act as the client
		config.InsecureSkipVerify = true
		socket := tls.Client(tcs.socket, config)
		if err := socket.SetDeadline(time.Time{}); err != nil {
			return err
		}
		tcs.socket = socket

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
	}
	tcs.logger.Log("authentication", "success")

	hello := tcs.makeHelloServer()
	if err := tcs.TLSCapnpHandshakerServer.Send(server.SegToBytes(hello)); err != nil {
		return err
	}

	if seg, err := tcs.ReadOne(); err == nil {
		hello := msgs.ReadRootHelloServerFromServer(seg)
		tcs.remoteHost = hello.LocalHost()
		tcs.remoteRMId = common.RMId(hello.RmId())
		if tcs.verifyTopology(&hello) {
			if _, found := tcs.topology.RMsRemoved[tcs.remoteRMId]; found {
				return tcs.serverError(
					fmt.Errorf("%v has been removed from topology and may not rejoin.", tcs.remoteRMId))
			}

			tcs.remoteClusterUUId = hello.ClusterUUId()
			tcs.remoteBootCount = hello.BootCount()
			return nil
		} else {
			return fmt.Errorf("Unequal remote topology (%v, %v)", tcs.remoteHost, tcs.remoteRMId)
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
	hello.SetRmId(uint32(tcs.connectionManager.RMId))
	hello.SetBootCount(tcs.connectionManager.BootCount)
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

func (tcs *TLSCapnpServer) Run(conn *Connection) error {
	tcs.conn = conn
	tcs.logger.Log("msg", "Connection established.", "remoteHost", tcs.remoteHost, "remoteRMId", tcs.remoteRMId)

	seg := capn.NewBuffer(nil)
	message := msgs.NewRootMessage(seg)
	message.SetHeartbeat()
	tcs.CreateBeater(conn, server.SegToBytes(seg))
	tcs.createReader()

	flushSeg := capn.NewBuffer(nil)
	flushMsg := msgs.NewRootMessage(flushSeg)
	flushMsg.SetFlushed()
	flushBytes := server.SegToBytes(flushSeg)
	tcs.connectionManager.ServerEstablished(tcs, tcs.remoteHost, tcs.remoteRMId, tcs.remoteBootCount, tcs.remoteClusterUUId, func() { tcs.Send(flushBytes) })

	return nil
}

func (tcs *TLSCapnpServer) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	defer tc.maybeClose()

	topology := tc.topology
	tcs.topology = topology

	server.DebugLog(tcs.logger, "debug", "TopologyChanged.", "topology", topology)
	if topology != nil && tcs.dialer != nil {
		if _, found := topology.RMsRemoved[tcs.remoteRMId]; found {
			tcs.dialer = nil
		}
	}

	return nil
}

func (tcs *TLSCapnpServer) Send(msg []byte) {
	tcs.conn.EnqueueError(func() error { return tcs.beater.SendMessage(msg) })
}

func (tcs *TLSCapnpServer) RestartDialer() Dialer {
	tcs.internalShutdown()
	if restarting := tcs.dialer != nil; restarting {
		tcs.connectionManager.ServerLost(tcs, tcs.remoteHost, tcs.remoteRMId, restarting)
		tcs.TLSCapnpHandshakerServer.InternalShutdown()
	}

	return tcs.dialer
}

func (tcs *TLSCapnpServer) InternalShutdown() {
	tcs.internalShutdown()
	tcs.connectionManager.ServerLost(tcs, tcs.remoteHost, tcs.remoteRMId, false)
	tcs.TLSCapnpHandshakerServer.InternalShutdown()
	tcs.conn.shutdownComplete()
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
		return fmt.Errorf("Error received from %v: \"%s\"", tcs.remoteRMId, msg.ConnectionError())
	case msgs.MESSAGE_TOPOLOGYCHANGEREQUEST:
		configCap := msg.TopologyChangeRequest()
		config := configuration.ConfigurationFromCap(&configCap)
		tcs.connectionManager.RequestConfigurationChange(config)
		return nil
	default:
		tcs.connectionManager.DispatchMessage(tcs.remoteRMId, which, msg)
		return nil
	}
}

func (tcs *TLSCapnpServer) String() string {
	if tcs.dialer == nil {
		return fmt.Sprintf("TLSCapnpServer for %v(%d) from %s", tcs.remoteRMId, tcs.remoteBootCount, tcs.remoteHost)
	} else {
		return fmt.Sprintf("TLSCapnpServer for %v(%d) to %s", tcs.remoteRMId, tcs.remoteBootCount, tcs.remoteHost)
	}
}

func (tcs *TLSCapnpServer) createReader() {
	if tcs.reader == nil {
		tcs.reader = NewSocketReader(tcs.conn, tcs)
		tcs.reader.Start()
	}
}

// TLS Capnp Client

type TLSCapnpClient struct {
	*TLSCapnpHandshakerServer
	*Connection
	logger    log.Logger
	peerCerts []*x509.Certificate
	roots     map[string]*common.Capability
	rootsVar  map[common.VarUUId]*common.Capability
	namespace []byte
	submitter *client.ClientTxnSubmitter
	reader    *SocketReader
}

func (tcc *TLSCapnpClient) finishHandshake() error {
	config := tcc.baseTLSConfig()
	if config == nil {
		return errors.New("Cluster not yet formed")
	}
	config.ClientAuth = tls.RequireAnyClientCert
	socket := tls.Server(tcc.socket, config)
	tcc.socket = socket
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
		if err := tcc.Send(server.SegToBytes(helloFromServer)); err != nil {
			return err
		}
		tcc.remoteHost = tcc.socket.RemoteAddr().String()
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
	binary.BigEndian.PutUint32(namespace[4:8], tcc.TLSCapnpHandshakerServer.connectionManager.BootCount)
	binary.BigEndian.PutUint32(namespace[8:], uint32(tcc.TLSCapnpHandshakerServer.connectionManager.RMId))
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

func (tcc *TLSCapnpClient) Run(conn *Connection) error {
	tcc.Connection = conn
	servers, metrics := tcc.TLSCapnpHandshakerServer.connectionManager.ClientEstablished(tcc.connectionNumber, tcc)
	if servers == nil {
		return errors.New("Not ready for client connections")

	} else {
		tcc.logger.Log("msg", "Connection established.", "remoteHost", tcc.remoteHost)

		seg := capn.NewBuffer(nil)
		message := cmsgs.NewRootClientMessage(seg)
		message.SetHeartbeat()
		tcc.CreateBeater(conn, server.SegToBytes(seg))
		tcc.createReader()

		cm := tcc.TLSCapnpHandshakerServer.connectionManager
		tcc.submitter = client.NewClientTxnSubmitter(cm.RMId, cm.BootCount, tcc.rootsVar, tcc.namespace,
			paxos.NewServerConnectionPublisherProxy(tcc.Connection, cm, tcc.logger), tcc.Connection,
			tcc.logger, metrics)
		tcc.submitter.TopologyChanged(tcc.topology)
		tcc.submitter.ServerConnectionsChanged(servers)
		return nil
	}
}

func (tcc *TLSCapnpClient) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	topology := tc.topology
	tcc.topology = topology

	server.DebugLog(tcc.logger, "debug", "TopologyChanged", "topology", topology)

	if topology != nil {
		if authenticated, _, roots := tcc.topology.VerifyPeerCerts(tcc.peerCerts); !authenticated {
			server.DebugLog(tcc.logger, "debug", "TopologyChanged. Client Unauthed.", "topology", topology)
			tc.maybeClose()
			return errors.New("Client connection closed: No client certificate known")
		} else if len(roots) == len(tcc.roots) {
			for name, capsOld := range tcc.roots {
				if capsNew, found := roots[name]; !found || !capsNew.Equal(capsOld) {
					server.DebugLog(tcc.logger, "debug", "TopologyChanged. Roots Changed.", "topology", topology)
					tc.maybeClose()
					return errors.New("Client connection closed: roots have changed")
				}
			}
		} else {
			server.DebugLog(tcc.logger, "debug", "TopologyChanged. Roots Changed.", "topology", topology)
			tc.maybeClose()
			return errors.New("Client connection closed: roots have changed")
		}
	}
	if err := tcc.submitter.TopologyChanged(topology); err != nil {
		tc.maybeClose()
		return err
	}
	tc.maybeClose()

	return nil
}

func (tcc *TLSCapnpClient) RestartDialer() Dialer {
	return nil // client connections are never restarted
}

func (tcc *TLSCapnpClient) InternalShutdown() {
	if tcc.reader != nil {
		tcc.reader.Stop()
		tcc.reader = nil
	}
	cont := func() {
		tcc.TLSCapnpHandshakerServer.connectionManager.ClientLost(tcc.connectionNumber, tcc)
		tcc.shutdownComplete()
	}
	if tcc.submitter == nil {
		cont()
	} else {
		tcc.submitter.Shutdown(cont)
	}
	tcc.TLSCapnpHandshakerServer.InternalShutdown()
}

func (tcc *TLSCapnpClient) String() string {
	return fmt.Sprintf("TLSCapnpClient %d from %s", tcc.connectionNumber, tcc.remoteHost)
}

func (tcc *TLSCapnpClient) SubmissionOutcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) {
	tcc.EnqueueError(func() error {
		return tcc.outcomeReceived(sender, txn, outcome)
	})
}

func (tcc *TLSCapnpClient) outcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) error {
	return tcc.submitter.SubmissionOutcomeReceived(sender, txn, outcome)
}

func (tcc *TLSCapnpClient) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	tcc.EnqueueError(func() error {
		return tcc.serverConnectionsChanged(servers)
	})
}
func (tcc *TLSCapnpClient) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	tcc.EnqueueError(func() error {
		return tcc.serverConnectionsChanged(servers)
	})
}
func (tcc *TLSCapnpClient) ConnectionEstablished(rmId common.RMId, c paxos.Connection, servers map[common.RMId]paxos.Connection, done func()) {
	finished := make(chan struct{})
	enqueued := tcc.EnqueueError(func() error {
		defer close(finished)
		return tcc.serverConnectionsChanged(servers)
	})

	if enqueued {
		go tcc.WithTerminatedChan(func(terminated chan struct{}) {
			select {
			case <-finished:
			case <-terminated:
			}
			done()
		})
	} else {
		done()
	}
}

func (tcc *TLSCapnpClient) serverConnectionsChanged(servers map[common.RMId]paxos.Connection) error {
	return tcc.submitter.ServerConnectionsChanged(servers)
}

func (tcc *TLSCapnpClient) ReadAndHandleOneMsg() error {
	seg, err := tcc.ReadOne()
	if err != nil {
		return err
	}
	msg := cmsgs.ReadRootClientMessage(seg)
	switch which := msg.Which(); which {
	case cmsgs.CLIENTMESSAGE_HEARTBEAT:
		return nil // do nothing
	case cmsgs.CLIENTMESSAGE_CLIENTTXNSUBMISSION:
		// submitter is accessed from the connection go routine, so we must relay this
		tcc.EnqueueError(func() error {
			return tcc.submitTransaction(msg.ClientTxnSubmission())
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
			return tcc.beater.SendMessage(tcc.clientTxnError(&ctxn, err, origTxnId))
		case clientOutcome == nil: // shutdown
			return nil
		default:
			seg := capn.NewBuffer(nil)
			msg := cmsgs.NewRootClientMessage(seg)
			msg.SetClientTxnOutcome(*clientOutcome)
			return tcc.beater.SendMessage(server.SegToBytes(msg.Segment))
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
	return server.SegToBytes(seg)
}

func (tcc *TLSCapnpClient) createReader() {
	if tcc.reader == nil {
		tcc.reader = NewSocketReader(tcc.Connection, tcc)
		tcc.reader.Start()
	}
}

// TLSCapnpBeater

type TLSCapnpBeater struct {
	*TLSCapnpHandshakerBase
	conn         ConnectionActor
	beatBytes    []byte
	terminate    chan struct{}
	terminated   chan struct{}
	ticker       *time.Ticker
	mustSendBeat bool
}

func NewTLSCapnpBeater(tchb *TLSCapnpHandshakerBase, conn ConnectionActor, beatBytes []byte) *TLSCapnpBeater {
	return &TLSCapnpBeater{
		TLSCapnpHandshakerBase: tchb,
		beatBytes:              beatBytes,
		conn:                   conn,
		terminate:              make(chan struct{}),
		terminated:             make(chan struct{}),
		ticker:                 time.NewTicker(common.HeartbeatInterval >> 1),
		mustSendBeat:           true,
	}
}

func (b *TLSCapnpBeater) Start() {
	if b != nil {
		go b.tick()
	}
}

func (b *TLSCapnpBeater) Stop() {
	if b != nil {
		b.TLSCapnpHandshakerBase = nil
		select {
		case <-b.terminated:
		default:
			close(b.terminate)
			<-b.terminated
		}
	}
}

func (b *TLSCapnpBeater) tick() {
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

func (b *TLSCapnpBeater) beat() error {
	if b != nil && b.TLSCapnpHandshakerBase != nil {
		if b.mustSendBeat {
			return b.SendMessage(b.beatBytes)
		} else {
			b.mustSendBeat = true
		}
		// Useful for testing recovery from network brownouts
		/*
			if b.rng.Intn(15) == 0 && b.dialer != nil {
				return fmt.Errorf("Random death. Restarting connection.")
			}
		*/
	}
	return nil
}

func (b *TLSCapnpBeater) SendMessage(msg []byte) error {
	if b != nil {
		b.mustSendBeat = false
		return b.Send(msg)
	}
	return nil
}

// Reader

type SocketReader struct {
	conn ConnectionActor
	SocketMsgHandler
	terminate  chan struct{}
	terminated chan struct{}
}

type SocketMsgHandler interface {
	ReadAndHandleOneMsg() error
}

func NewSocketReader(conn ConnectionActor, smh SocketMsgHandler) *SocketReader {
	return &SocketReader{
		conn:             conn,
		SocketMsgHandler: smh,
		terminate:        make(chan struct{}),
		terminated:       make(chan struct{}),
	}
}

func (sr *SocketReader) Start() {
	if sr != nil {
		go sr.read()
	}
}

func (sr *SocketReader) Stop() {
	if sr != nil {
		select {
		case <-sr.terminated:
		default:
			close(sr.terminate)
			<-sr.terminated
		}
	}
}

func (sr *SocketReader) read() {
	defer close(sr.terminated)
	for {
		select {
		case <-sr.terminate:
			return
		default:
			if err := sr.ReadAndHandleOneMsg(); err != nil {
				sr.conn.EnqueueError(func() error { return err }) // connectionReadError{error: err})
				return
			}
		}
	}
}

type ConnectionActor interface {
	EnqueueError(func() error) bool
}
