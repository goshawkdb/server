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
	handshakeBuilder func(*net.TCPConn) *TLSCapnpHandshaker
}

func NewTCPDialerForTLSCapnp(remoteHost string, cm *ConnectionManager, logger log.Logger) *TCPDialer {
	if remoteHost == "" {
		panic("Empty host")
	}
	dialer := &TCPDialer{
		logger:     logger,
		remoteHost: remoteHost,
	}
	dialer.handshakeBuilder = func(socket *net.TCPConn) *TLSCapnpHandshaker {
		return NewTLSCapnpHandshaker(dialer, socket, cm, 0, remoteHost, dialer.logger)
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
	return td.handshakeBuilder(socket), nil
}

func (td *TCPDialer) String() string {
	return fmt.Sprintf("TCPDialer to %s", td.remoteHost)
}

// TLS Capnp Handshaker

type TLSCapnpHandshaker struct {
	dialer            Dialer
	logger            log.Logger
	remoteHost        string
	connectionNumber  uint32
	socket            net.Conn
	connectionManager *ConnectionManager
	rng               *rand.Rand
	topology          *configuration.Topology
	beater            *beater
	buf               []byte
	bufWriteOffset    int64
}

func NewTLSCapnpHandshaker(dialer Dialer, socket *net.TCPConn, cm *ConnectionManager, count uint32, remoteHost string, logger log.Logger) *TLSCapnpHandshaker {
	if err := common.ConfigureSocket(socket); err != nil {
		logger.Log("msg", "Error when configuring socket", "error", err)
		return nil
	}
	return &TLSCapnpHandshaker{
		dialer:            dialer,
		logger:            logger,
		remoteHost:        remoteHost,
		connectionNumber:  count,
		socket:            socket,
		connectionManager: cm,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (tch *TLSCapnpHandshaker) PerformHandshake(topology *configuration.Topology) (Protocol, error) {
	tch.topology = topology

	helloSeg := tch.makeHello()
	if err := tch.send(server.SegToBytes(helloSeg)); err != nil {
		return nil, err
	}

	if seg, err := tch.readExactlyOne(); err == nil {
		hello := cmsgs.ReadRootHello(seg)
		if tch.verifyHello(&hello) {
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

func (tch *TLSCapnpHandshaker) RestartDialer() Dialer {
	tch.InternalShutdown()
	return tch.dialer
}

func (tch *TLSCapnpHandshaker) InternalShutdown() {
	if tch.beater != nil {
		tch.beater.stop()
		tch.beater = nil
	}
	if tch.socket != nil {
		tch.socket.Close()
		tch.socket = nil
	}
}

func (tch *TLSCapnpHandshaker) String() string {
	if tch.dialer == nil {
		return fmt.Sprintf("TLSCapnpHandshaker %d from remote", tch.connectionNumber)
	} else {
		return fmt.Sprintf("TLSCapnpHandshaker to %s", tch.remoteHost)
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

func (tch *TLSCapnpHandshaker) send(msg []byte) error {
	l := len(msg)
	for l > 0 {
		switch w, err := tch.socket.Write(msg); {
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
func (tch *TLSCapnpHandshaker) readExactlyOne() (*capn.Segment, error) {
	if err := tch.socket.SetReadDeadline(time.Now().Add(common.HeartbeatInterval << 1)); err != nil {
		return nil, err
	}
	return capn.ReadFromStream(tch.socket, nil)
}

func (tch *TLSCapnpHandshaker) readOne() (*capn.Segment, error) {
	if tch.bufWriteOffset > 0 {
		// still have data in the buffer, so first try decoding that
		if seg, err := tch.attemptCapnpDecode(); seg != nil || err != nil {
			return seg, err
		}
	}
	for {
		if int64(len(tch.buf)) == tch.bufWriteOffset { // run out of buf space
			tch.increaseBuffer()
		}
		if err := tch.socket.SetReadDeadline(time.Now().Add(common.HeartbeatInterval << 1)); err != nil {
			return nil, err
		}
		if readCount, err := tch.socket.Read(tch.buf[tch.bufWriteOffset:]); err != nil {
			return nil, err
		} else if readCount > 0 { // we read something; try to parse
			tch.bufWriteOffset += int64(readCount)
			if seg, err := tch.attemptCapnpDecode(); seg != nil || err != nil {
				return seg, err
			}
		}
	}
}

func (tch *TLSCapnpHandshaker) increaseBuffer() {
	size := int64(common.ConnectionBufferSize)
	req := tch.bufWriteOffset
	for size <= req {
		size *= 2
	}
	buf := make([]byte, size)
	copy(buf[:req], tch.buf[:req])
	tch.buf = buf
}

func (tch *TLSCapnpHandshaker) attemptCapnpDecode() (*capn.Segment, error) {
	seg, used, err := capn.ReadFromMemoryZeroCopy(tch.buf[:tch.bufWriteOffset])
	if used > 0 {
		tch.buf = tch.buf[used:]
		tch.bufWriteOffset -= used
	}
	if err == io.EOF {
		// ignore it - it just means we don't have enough data from the socket yet
		return nil, nil
	} else {
		return seg, err
	}
}

func (tch *TLSCapnpHandshaker) verifyHello(hello *cmsgs.Hello) bool {
	return hello.Product() == common.ProductName &&
		hello.Version() == common.ProductVersion
}

func (tch *TLSCapnpHandshaker) newTLSCapnpClient() *TLSCapnpClient {
	return &TLSCapnpClient{
		TLSCapnpHandshaker: tch,
		logger:             log.NewContext(tch.logger).With("type", "client", "connNumber", tch.connectionNumber),
	}
}

func (tch *TLSCapnpHandshaker) newTLSCapnpServer() *TLSCapnpServer {
	// If the remote node is removed from the cluster then dialer is
	// set to nil to stop us recreating this connection when it
	// disconnects. If this connection came from the listener
	// (i.e. dialer == nil) then we never restart it anyway.
	return &TLSCapnpServer{
		TLSCapnpHandshaker: tch,
		logger:             log.NewContext(tch.logger).With("type", "server"),
	}
}

func (tch *TLSCapnpHandshaker) baseTLSConfig() *tls.Config {
	nodeCertPrivKeyPair := tch.connectionManager.NodeCertificatePrivateKeyPair()
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
	tch.send(server.SegToBytes(seg))
	return err
}

func (tch *TLSCapnpHandshaker) createBeater(conn *Connection, beatBytes []byte) {
	if tch.beater == nil {
		beater := &beater{
			TLSCapnpHandshaker: tch,
			beatBytes:          beatBytes,
			conn:               conn,
			terminate:          make(chan struct{}),
			terminated:         make(chan struct{}),
			ticker:             time.NewTicker(common.HeartbeatInterval >> 1),
			mustSendBeat:       true,
		}
		beater.start()
		tch.beater = beater
	}
}

// TLS Capnp Server

type TLSCapnpServer struct {
	*TLSCapnpHandshaker
	logger            log.Logger
	Connection        *Connection
	remoteRMId        common.RMId
	remoteClusterUUId uint64
	remoteBootCount   uint32
	reader            *socketReader
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
	if err := tcs.send(server.SegToBytes(hello)); err != nil {
		return err
	}

	if seg, err := tcs.readOne(); err == nil {
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
	tcs.Connection = conn
	tcs.logger.Log("msg", "Connection established.", "remoteHost", tcs.remoteHost, "remoteRMId", tcs.remoteRMId)

	seg := capn.NewBuffer(nil)
	message := msgs.NewRootMessage(seg)
	message.SetHeartbeat()
	tcs.createBeater(conn, server.SegToBytes(seg))
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

	server.DebugLog(tcs.logger, "debug", "TopologyChanged.", "topology", tc)
	if topology != nil && tcs.dialer != nil {
		if _, found := topology.RMsRemoved[tcs.remoteRMId]; found {
			tcs.dialer = nil
		}
	}

	return nil
}

func (tcs *TLSCapnpServer) Send(msg []byte) {
	tcs.Connection.enqueueQuery(connectionMsgExecError(func() error { return tcs.beater.sendMessage(msg) }))
}

func (tcs *TLSCapnpServer) RestartDialer() Dialer {
	tcs.internalShutdown()
	if restarting := tcs.dialer != nil; restarting {
		tcs.connectionManager.ServerLost(tcs, tcs.remoteHost, tcs.remoteRMId, restarting)
		tcs.TLSCapnpHandshaker.InternalShutdown()
	}

	return tcs.dialer
}

func (tcs *TLSCapnpServer) InternalShutdown() {
	tcs.internalShutdown()
	tcs.connectionManager.ServerLost(tcs, tcs.remoteHost, tcs.remoteRMId, false)
	tcs.TLSCapnpHandshaker.InternalShutdown()
}

func (tcs *TLSCapnpServer) internalShutdown() {
	if tcs.reader != nil {
		tcs.reader.stop()
		tcs.reader = nil
	}
}

func (tcs *TLSCapnpServer) readAndHandleOneMsg() error {
	seg, err := tcs.readOne()
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
		tcs.reader = &socketReader{
			conn:             tcs.Connection,
			socketMsgHandler: tcs,
			terminate:        make(chan struct{}),
			terminated:       make(chan struct{}),
		}
		tcs.reader.start()
	}
}

// TLS Capnp Client

type TLSCapnpClient struct {
	*TLSCapnpHandshaker
	*Connection
	logger        log.Logger
	peerCerts     []*x509.Certificate
	roots         map[string]*common.Capability
	rootsVar      map[common.VarUUId]*common.Capability
	namespace     []byte
	submitter     *client.ClientTxnSubmitter
	submitterIdle *connectionMsgTopologyChanged
	reader        *socketReader
}

func (tcc *TLSCapnpClient) finishHandshake() error {
	config := tcc.baseTLSConfig()
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
		if err := tcc.send(server.SegToBytes(helloFromServer)); err != nil {
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
	binary.BigEndian.PutUint32(namespace[4:8], tcc.TLSCapnpHandshaker.connectionManager.BootCount)
	binary.BigEndian.PutUint32(namespace[8:], uint32(tcc.TLSCapnpHandshaker.connectionManager.RMId))
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
	servers, txnLatency, txnResubmit, txnRerun, txnSubmit := tcc.TLSCapnpHandshaker.connectionManager.ClientEstablished(tcc.connectionNumber, tcc)
	if servers == nil {
		return errors.New("Not ready for client connections")

	} else {
		tcc.logger.Log("msg", "Connection established.", "remoteHost", tcc.remoteHost)

		seg := capn.NewBuffer(nil)
		message := cmsgs.NewRootClientMessage(seg)
		message.SetHeartbeat()
		tcc.createBeater(conn, server.SegToBytes(seg))
		tcc.createReader()

		cm := tcc.TLSCapnpHandshaker.connectionManager
		tcc.submitter = client.NewClientTxnSubmitter(cm.RMId, cm.BootCount, tcc.rootsVar, tcc.namespace,
			paxos.NewServerConnectionPublisherProxy(tcc.Connection, cm, tcc.logger), tcc.Connection,
			tcc.logger, txnLatency, txnResubmit, txnRerun, txnSubmit)
		tcc.submitter.TopologyChanged(tcc.topology)
		tcc.submitter.ServerConnectionsChanged(servers)
		return nil
	}
}

func (tcc *TLSCapnpClient) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	if si := tcc.submitterIdle; si != nil {
		tcc.submitterIdle = nil
		server.DebugLog(tcc.logger, "debug", "TopologyChanged.", "topology", tc, "clearingSI", si)
		si.maybeClose()
	}

	topology := tc.topology
	tcc.topology = topology

	if topology != nil {
		if authenticated, _, roots := tcc.topology.VerifyPeerCerts(tcc.peerCerts); !authenticated {
			server.DebugLog(tcc.logger, "debug", "TopologyChanged. Client Unauthed.", "topology", tc)
			tc.maybeClose()
			return errors.New("Client connection closed: No client certificate known")
		} else if len(roots) == len(tcc.roots) {
			for name, capsOld := range tcc.roots {
				if capsNew, found := roots[name]; !found || !capsNew.Equal(capsOld) {
					server.DebugLog(tcc.logger, "debug", "TopologyChanged. Roots Changed.", "topology", tc)
					tc.maybeClose()
					return errors.New("Client connection closed: roots have changed")
				}
			}
		} else {
			server.DebugLog(tcc.logger, "debug", "TopologyChanged. Roots Changed.", "topology", tc)
			tc.maybeClose()
			return errors.New("Client connection closed: roots have changed")
		}
	}
	if err := tcc.submitter.TopologyChanged(topology); err != nil {
		tc.maybeClose()
		return err
	}
	if tcc.submitter.IsIdle() {
		server.DebugLog(tcc.logger, "debug", "TopologyChanged. Submitter is idle.", "topology", tc)
		tc.maybeClose()
	} else {
		server.DebugLog(tcc.logger, "debug", "TopologyChanged. Submitter not idle.", "topology", tc)
		tcc.submitterIdle = tc
	}

	return nil
}

func (tcc *TLSCapnpClient) RestartDialer() Dialer {
	return nil // client connections are never restarted
}

func (tcc *TLSCapnpClient) InternalShutdown() {
	if tcc.reader != nil {
		tcc.reader.stop()
		tcc.reader = nil
	}
	if tcc.submitter != nil {
		tcc.submitter.Shutdown()
	}
	tcc.TLSCapnpHandshaker.connectionManager.ClientLost(tcc.connectionNumber, tcc)
	tcc.TLSCapnpHandshaker.InternalShutdown()
}

func (tcc *TLSCapnpClient) String() string {
	return fmt.Sprintf("TLSCapnpClient %d from %s", tcc.connectionNumber, tcc.remoteHost)
}

func (tcc *TLSCapnpClient) SubmissionOutcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) {
	tcc.enqueueQuery(connectionMsgExecError(func() error {
		return tcc.outcomeReceived(sender, txn, outcome)
	}))
}

func (tcc *TLSCapnpClient) outcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) error {
	err := tcc.submitter.SubmissionOutcomeReceived(sender, txn, outcome)
	if tcc.submitterIdle != nil && tcc.submitter.IsIdle() {
		si := tcc.submitterIdle
		tcc.submitterIdle = nil
		server.DebugLog(tcc.logger, "debug", "OutcomeReceived.", "submitterIdle", si)
		si.maybeClose()
	}
	return err
}

func (tcc *TLSCapnpClient) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	tcc.enqueueQuery(connectionMsgExecError(func() error {
		return tcc.serverConnectionsChanged(servers)
	}))
}
func (tcc *TLSCapnpClient) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	tcc.enqueueQuery(connectionMsgExecError(func() error {
		return tcc.serverConnectionsChanged(servers)
	}))
}
func (tcc *TLSCapnpClient) ConnectionEstablished(rmId common.RMId, c paxos.Connection, servers map[common.RMId]paxos.Connection, done func()) {
	finished := make(chan struct{})
	enqueued := tcc.enqueueQuery(connectionMsgExecError(func() error {
		defer close(finished)
		return tcc.serverConnectionsChanged(servers)
	}))

	if enqueued {
		go func() {
			select {
			case <-finished:
			case <-tcc.cellTail.Terminated:
			}
			done()
		}()
	} else {
		done()
	}
}

func (tcc *TLSCapnpClient) serverConnectionsChanged(servers map[common.RMId]paxos.Connection) error {
	return tcc.submitter.ServerConnectionsChanged(servers)
}

func (tcc *TLSCapnpClient) readAndHandleOneMsg() error {
	seg, err := tcc.readOne()
	if err != nil {
		return err
	}
	msg := cmsgs.ReadRootClientMessage(seg)
	switch which := msg.Which(); which {
	case cmsgs.CLIENTMESSAGE_HEARTBEAT:
		return nil // do nothing
	case cmsgs.CLIENTMESSAGE_CLIENTTXNSUBMISSION:
		// submitter is accessed from the connection go routine, so we must relay this
		tcc.enqueueQuery(connectionMsgExecError(func() error {
			return tcc.submitTransaction(msg.ClientTxnSubmission())
		}))
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
			return tcc.beater.sendMessage(tcc.clientTxnError(&ctxn, err, origTxnId))
		case clientOutcome == nil: // shutdown
			return nil
		default:
			seg := capn.NewBuffer(nil)
			msg := cmsgs.NewRootClientMessage(seg)
			msg.SetClientTxnOutcome(*clientOutcome)
			return tcc.beater.sendMessage(server.SegToBytes(msg.Segment))
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
		tcc.reader = &socketReader{
			conn:             tcc.Connection,
			socketMsgHandler: tcc,
			terminate:        make(chan struct{}),
			terminated:       make(chan struct{}),
		}
		tcc.reader.start()
	}
}

// Beater

type beater struct {
	*TLSCapnpHandshaker
	conn         *Connection
	beatBytes    []byte
	terminate    chan struct{}
	terminated   chan struct{}
	ticker       *time.Ticker
	mustSendBeat bool
}

func (b *beater) start() {
	if b != nil {
		go b.tick()
	}
}

func (b *beater) stop() {
	if b != nil {
		b.TLSCapnpHandshaker = nil
		select {
		case <-b.terminated:
		default:
			close(b.terminate)
			<-b.terminated
		}
	}
}

func (b *beater) tick() {
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
			if !b.conn.enqueueQuery(connectionMsgExecError(b.beat)) {
				return
			}
		}
	}
}

func (b *beater) beat() error {
	if b != nil && b.TLSCapnpHandshaker != nil {
		if b.mustSendBeat {
			return b.sendMessage(b.beatBytes)
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

func (b *beater) sendMessage(msg []byte) error {
	if b != nil {
		b.mustSendBeat = false
		return b.send(msg)
	}
	return nil
}

// Reader

type socketReader struct {
	conn *Connection
	socketMsgHandler
	terminate  chan struct{}
	terminated chan struct{}
}

type socketMsgHandler interface {
	readAndHandleOneMsg() error
}

func (sr *socketReader) start() {
	if sr != nil {
		go sr.read()
	}
}

func (sr *socketReader) stop() {
	if sr != nil {
		select {
		case <-sr.terminated:
		default:
			close(sr.terminate)
			<-sr.terminated
		}
	}
}

func (sr *socketReader) read() {
	defer close(sr.terminated)
	for {
		select {
		case <-sr.terminate:
			return
		default:
			if err := sr.readAndHandleOneMsg(); err != nil {
				sr.conn.enqueueQuery(connectionReadError{error: err})
				return
			}
		}
	}
}
