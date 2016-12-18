package network

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"log"
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
	SendMessage([]byte) error
	RestartDialer() Dialer
	InternalShutdown()
}

// TCP TLS Capnp dialer

type TCPDialer struct {
	remoteHost       string
	handshakeBuilder func(*net.TCPConn) *TLSCapnpHandshaker
}

func NewTCPDialerForTLSCapnp(remoteHost string, cm *ConnectionManager) *TCPDialer {
	if remoteHost == "" {
		panic("Empty host")
	}
	dialer := &TCPDialer{remoteHost: remoteHost}
	dialer.handshakeBuilder = func(socket *net.TCPConn) *TLSCapnpHandshaker {
		return NewTLSCapnpHandshaker(dialer, socket, cm, 0, remoteHost)
	}
	return dialer
}

func (td *TCPDialer) Dial() (Handshaker, error) {
	log.Printf("Attempting connection to %s", td.remoteHost)
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
	remoteHost        string
	connectionNumber  uint32
	socket            net.Conn
	connectionManager *ConnectionManager
	rng               *rand.Rand
	topology          *configuration.Topology
	beater            *beater
}

func NewTLSCapnpHandshaker(dialer Dialer, socket *net.TCPConn, cm *ConnectionManager, count uint32, remoteHost string) *TLSCapnpHandshaker {
	if err := common.ConfigureSocket(socket); err != nil {
		log.Println("Error when configuring socket:", err)
		return nil
	}
	return &TLSCapnpHandshaker{
		dialer:            dialer,
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

	if seg, err := tch.readOne(); err == nil {
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

func (tch *TLSCapnpHandshaker) SendMessage(msg []byte) error {
	return tch.beater.sendMessage(msg)
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

func (tch *TLSCapnpHandshaker) readOne() (*capn.Segment, error) {
	if err := tch.socket.SetReadDeadline(time.Now().Add(common.HeartbeatInterval * 2)); err != nil {
		return nil, err
	}
	return capn.ReadFromStream(tch.socket, nil)
}

func (tch *TLSCapnpHandshaker) verifyHello(hello *cmsgs.Hello) bool {
	return hello.Product() == common.ProductName &&
		hello.Version() == common.ProductVersion
}

func (tch *TLSCapnpHandshaker) newTLSCapnpClient() *TLSCapnpClient {
	return &TLSCapnpClient{
		TLSCapnpHandshaker: tch,
	}
}

func (tch *TLSCapnpHandshaker) newTLSCapnpServer() *TLSCapnpServer {
	// If the remote node is removed from the cluster then dialer is
	// set to nil to stop us recreating this connection when it
	// disconnects. If this connection came from the listener
	// (i.e. dialer == nil) then we never restart it anyway.
	return &TLSCapnpServer{
		TLSCapnpHandshaker: tch,
	}
}

func (tch *TLSCapnpHandshaker) baseTLSConfig() *tls.Config {
	nodeCertPrivKeyPair := tch.connectionManager.NodeCertificatePrivateKeyPair
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
			ticker:             time.NewTicker(common.HeartbeatInterval),
		}
		beater.start()
		tch.beater = beater
	}
}

// TLS Capnp Server

type TLSCapnpServer struct {
	*TLSCapnpHandshaker
	conn              *Connection
	remoteRMId        common.RMId
	remoteClusterUUId uint64
	remoteBootCount   uint32
	combinedTieBreak  uint32
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
			return err
		}
	}

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
			tcs.combinedTieBreak = tcs.combinedTieBreak ^ hello.TieBreak()
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
	hello.SetBootCount(tcs.connectionManager.BootCount())
	tieBreak := tcs.rng.Uint32()
	tcs.combinedTieBreak = tieBreak
	hello.SetTieBreak(tieBreak)
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
	log.Printf("Server connection established to %v (%v)\n", tcs.remoteHost, tcs.remoteRMId)

	seg := capn.NewBuffer(nil)
	message := msgs.NewRootMessage(seg)
	message.SetHeartbeat()
	tcs.createBeater(conn, server.SegToBytes(seg))
	tcs.createReader()

	flushSeg := capn.NewBuffer(nil)
	flushMsg := msgs.NewRootMessage(flushSeg)
	flushMsg.SetFlushed()
	flushBytes := server.SegToBytes(flushSeg)
	tcs.connectionManager.ServerEstablished(tcs.conn, tcs.remoteHost, tcs.remoteRMId, tcs.remoteBootCount, tcs.combinedTieBreak, tcs.remoteClusterUUId, func() { tcs.conn.Send(flushBytes) })

	return nil
}

func (tcs *TLSCapnpServer) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	topology := tc.topology
	tcs.topology = topology

	server.Log("Connection", tcs, "topologyChanged", tc, "(isServer)")
	tc.maybeClose()
	if topology != nil && tcs.dialer != nil {
		if _, found := topology.RMsRemoved[tcs.remoteRMId]; found {
			tcs.dialer = nil
		}
	}

	return nil
}

func (tcs *TLSCapnpServer) RestartDialer() Dialer {
	tcs.InternalShutdown()
	return tcs.dialer
}

func (tcs *TLSCapnpServer) InternalShutdown() {
	if tcs.reader != nil {
		tcs.reader.stop()
		tcs.reader = nil
	}
	tcs.connectionManager.ServerLost(tcs.conn, tcs.remoteRMId, false)
	tcs.TLSCapnpHandshaker.InternalShutdown()
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
			conn:             tcs.conn,
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
	conn          *Connection
	peerCerts     []*x509.Certificate
	roots         map[string]*common.Capability
	rootsVar      map[common.VarUUId]*common.Capability
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
	if authenticated, hashsum, roots := tcc.verifyPeerCerts(peerCerts); authenticated {
		tcc.peerCerts = peerCerts
		tcc.roots = roots
		log.Printf("User '%s' authenticated", hex.EncodeToString(hashsum[:]))
		helloFromServer := tcc.makeHelloClient()
		if err := tcc.send(server.SegToBytes(helloFromServer)); err != nil {
			return err
		}
		tcc.remoteHost = tcc.socket.RemoteAddr().String()
		return nil
	} else {
		return errors.New("Client connection rejected: No client certificate known")
	}
}

func (tcc *TLSCapnpClient) verifyPeerCerts(peerCerts []*x509.Certificate) (authenticated bool, hashsum [sha256.Size]byte, roots map[string]*common.Capability) {
	fingerprints := tcc.topology.Fingerprints
	for _, cert := range peerCerts {
		hashsum = sha256.Sum256(cert.Raw)
		if roots, found := fingerprints[hashsum]; found {
			return true, hashsum, roots
		}
	}
	return false, hashsum, nil
}

func (tcc *TLSCapnpClient) makeHelloClient() *capn.Segment {
	seg := capn.NewBuffer(nil)
	hello := cmsgs.NewRootHelloClientFromServer(seg)
	namespace := make([]byte, common.KeyLen-8)
	binary.BigEndian.PutUint32(namespace[0:4], tcc.connectionNumber)
	binary.BigEndian.PutUint32(namespace[4:8], tcc.connectionManager.BootCount())
	binary.BigEndian.PutUint32(namespace[8:], uint32(tcc.connectionManager.RMId))
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
	tcc.conn = conn
	servers := tcc.connectionManager.ClientEstablished(tcc.connectionNumber, tcc)
	if servers == nil {
		return errors.New("Not ready for client connections")

	} else {
		log.Printf("Client connection established from %v\n", tcc.remoteHost)

		seg := capn.NewBuffer(nil)
		message := cmsgs.NewRootClientMessage(seg)
		message.SetHeartbeat()
		tcc.createBeater(conn, server.SegToBytes(seg))
		tcc.createReader()

		tcc.submitter = client.NewClientTxnSubmitter(tcc.connectionManager.RMId, tcc.connectionManager.BootCount(), tcc.rootsVar, tcc.connectionManager)
		tcc.submitter.TopologyChanged(tcc.topology)
		tcc.submitter.ServerConnectionsChanged(servers)
		return nil
	}
}

func (tcc *TLSCapnpClient) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	if si := tcc.submitterIdle; si != nil {
		tcc.submitterIdle = nil
		server.Log("Connection", tcc, "topologyChanged:", tc, "clearing old:", si)
		si.maybeClose()
	}

	topology := tc.topology
	tcc.topology = topology

	if topology != nil {
		if authenticated, _, roots := tcc.verifyPeerCerts(tcc.peerCerts); !authenticated {
			server.Log("Connection", tcc, "topologyChanged", tc, "(client unauthed)")
			tc.maybeClose()
			return errors.New("Client connection closed: No client certificate known")
		} else if len(roots) == len(tcc.roots) {
			for name, capsOld := range tcc.roots {
				if capsNew, found := roots[name]; !found || !capsNew.Equal(capsOld) {
					server.Log("Connection", tcc, "topologyChanged", tc, "(roots changed)")
					tc.maybeClose()
					return errors.New("Client connection closed: roots have changed")
				}
			}
		} else {
			server.Log("Connection", tcc, "topologyChanged", tc, "(roots changed)")
			tc.maybeClose()
			return errors.New("Client connection closed: roots have changed")
		}
	}
	if err := tcc.submitter.TopologyChanged(topology); err != nil {
		tc.maybeClose()
		return err
	}
	if tcc.submitter.IsIdle() {
		server.Log("Connection", tcc, "topologyChanged", tc, "(client, submitter is idle)")
		tc.maybeClose()
	} else {
		server.Log("Connection", tcc, "topologyChanged", tc, "(client, submitter not idle)")
		tcc.submitterIdle = tc
	}

	return nil
}

func (tcc *TLSCapnpClient) RestartDialer() Dialer {
	tcc.InternalShutdown()
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
	tcc.connectionManager.ClientLost(tcc.connectionNumber, tcc)
	tcc.TLSCapnpHandshaker.InternalShutdown()
}

func (tcc *TLSCapnpClient) String() string {
	return fmt.Sprintf("TLSCapnpClient %d from %s", tcc.connectionNumber, tcc.remoteHost)
}

func (tcc *TLSCapnpClient) Shutdown(sync paxos.Blocking) {
	tcc.conn.Shutdown(sync)
}

func (tcc *TLSCapnpClient) SubmissionOutcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) {
	tcc.conn.enqueueQuery(connectionExec(func() error {
		return tcc.outcomeReceived(sender, txn, outcome)
	}))
}

func (tcc *TLSCapnpClient) outcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) error {
	err := tcc.submitter.SubmissionOutcomeReceived(sender, txn, outcome)
	if tcc.submitterIdle != nil && tcc.submitter.IsIdle() {
		si := tcc.submitterIdle
		tcc.submitterIdle = nil
		server.Log("Connection", tcc, "outcomeReceived", si, "(submitterIdle)")
		si.maybeClose()
	}
	return err
}

func (tcc *TLSCapnpClient) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	tcc.conn.enqueueQuery(connectionExec(func() error {
		return tcc.serverConnectionsChanged(servers)
	}))
}
func (tcc *TLSCapnpClient) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	tcc.conn.enqueueQuery(connectionExec(func() error {
		return tcc.serverConnectionsChanged(servers)
	}))
}
func (tcc *TLSCapnpClient) ConnectionEstablished(rmId common.RMId, c paxos.Connection, servers map[common.RMId]paxos.Connection, done func()) {
	tcc.conn.enqueueQuery(connectionExec(func() error {
		if done != nil {
			defer done()
		}
		return tcc.serverConnectionsChanged(servers)
	}))
}

func (tcc *TLSCapnpClient) serverConnectionsChanged(servers map[common.RMId]paxos.Connection) error {
	return tcc.submitter.ServerConnectionsChanged(servers)
}

func (tcc *TLSCapnpClient) Status(sc *server.StatusConsumer) {
	tcc.conn.Status(sc)
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
		tcc.conn.enqueueQuery(connectionExec(func() error {
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
			return tcc.SendMessage(tcc.clientTxnError(&ctxn, err, origTxnId))
		case clientOutcome == nil: // shutdown
			return nil
		default:
			seg := capn.NewBuffer(nil)
			msg := cmsgs.NewRootClientMessage(seg)
			msg.SetClientTxnOutcome(*clientOutcome)
			return tcc.SendMessage(server.SegToBytes(msg.Segment))
		}
	})
}

func (tcc *TLSCapnpClient) clientTxnError(ctxn *cmsgs.ClientTxn, err error, origTxnId *common.TxnId) []byte {
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
	return server.SegToBytes(seg)
}

func (tcc *TLSCapnpClient) createReader() {
	if tcc.reader == nil {
		tcc.reader = &socketReader{
			conn:             tcc.conn,
			socketMsgHandler: tcc,
			terminate:        make(chan struct{}),
			terminated:       make(chan struct{}),
		}
		tcc.reader.start()
	}
}

// WebSocketMsgPackClient

type WebsocketMsgPackClient struct {
}

func (wmc *WebsocketMsgPackClient) PerformHandshake(*configuration.Topology) (*WebsocketMsgPackClient, error) {
	return wmc, nil
}

func (wmc *WebsocketMsgPackClient) RestartDialer() Dialer {
	return nil // client connections are never restarted
}

func (wmc *WebsocketMsgPackClient) Run() error {
	return nil
}

func (wmc *WebsocketMsgPackClient) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	return nil
}

// Beater

type beater struct {
	connectionMsgBasic
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
			if !b.conn.enqueueQuery(connectionExec(b.beat)) {
				return
			}
		}
	}
}

func (b *beater) beat() error {
	if b != nil && b.TLSCapnpHandshaker != nil {
		if b.mustSendBeat {
			// do not set it back to false here!
			return b.send(b.beatBytes)
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
