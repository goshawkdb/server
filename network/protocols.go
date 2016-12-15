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
	"log"
	"math/rand"
	"net"
	"time"
)

type Handshaker interface {
	PerformHandshake() (Protocol, error)
}

type Protocol interface {
	RestartHandshake() Handshaker
	Run() error
	HandleMsg(connectionMsg) error
	TopologyChanged(*connectionMsgTopologyChanged) error
	ServerConnectionsChanged(map[common.RMId]paxos.Connection) error
}

type TLSCapnpHandshaker struct {
	conn              *Connection
	dialed            bool
	remoteHost        string
	connectionNumber  uint32
	socket            net.Conn
	connectionManager *ConnectionManager
	rng               *rand.Rand
	topology          *configuration.Topology
	beater            *beater
}

type TLSCapnpServer struct {
	*TLSCapnpHandshaker
	remoteRMId        common.RMId
	remoteClusterUUId uint64
	remoteBootCount   uint32
	combinedTieBreak  uint32
	restart           bool
	reader            *socketReader
}

type TLSCapnpClient struct {
	*TLSCapnpHandshaker
	peerCerts     []*x509.Certificate
	roots         map[string]*common.Capability
	rootsVar      map[common.VarUUId]*common.Capability
	submitter     *client.ClientTxnSubmitter
	submitterIdle *connectionMsgTopologyChanged
	reader        *socketReader
}

type WebsocketMsgPackClient struct {
}

func NewTLSCapnpHandshaker(conn *Connection, rng *rand.Rand, socket *net.TCPConn, cm *ConnectionManager, count uint32, remoteHost string) *TLSCapnpHandshaker {
	if err := common.ConfigureSocket(socket); err != nil {
		log.Println("Error when configuring socket:", err)
		return nil
	}
	return &TLSCapnpHandshaker{
		conn:              conn,
		dialed:            len(remoteHost) > 0,
		remoteHost:        remoteHost,
		connectionNumber:  count,
		socket:            socket,
		connectionManager: cm,
		rng:               rng,
	}
}

func (tch *TLSCapnpHandshaker) String() string {
	if tch.dialed {
		return fmt.Sprintf("Connection (TLSCapnpHandshaker) to %s", tch.remoteHost)
	} else {
		return fmt.Sprintf("Connection (TLSCapnpHandshaker) from remote")
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

func (tch *TLSCapnpHandshaker) PerformHandshake() (Protocol, error) {
	helloSeg := tch.makeHello()
	if err := tch.send(server.SegToBytes(helloSeg)); err != nil {
		return nil, err
	}

	if seg, err := tch.readOne(); err == nil {
		hello := cmsgs.ReadRootHello(seg)
		if tch.verifyHello(&hello) {
			if hello.IsClient() {
				tcc := tch.newTLSCapnpClient()
				if err = tcc.finishHandshake(); err == nil {
					return tcc, nil
				} else {
					return nil, err
				}

			} else {
				tcs := tch.newTLSCapnpServer()
				if err = tcs.finishHandshake(); err == nil {
					return tcs, nil
				} else if tcs.dialed {
					return tcs, err
				} else {
					return tcs, nil
				}
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

func (tch *TLSCapnpHandshaker) newTLSCapnpClient() *TLSCapnpClient {
	return &TLSCapnpClient{
		TLSCapnpHandshaker: tch,
	}
}

func (tch *TLSCapnpHandshaker) newTLSCapnpServer() *TLSCapnpServer {
	// If the remote node is removed from the cluster then restart is
	// set to false to stop us recreating this connection when it
	// disconnects. If this connection came from the listener
	// (i.e. !dialed) then we never restart it anyway.
	return &TLSCapnpServer{
		TLSCapnpHandshaker: tch,
		restart:            tch.dialed,
	}
}

func (tch *TLSCapnpHandshaker) createBeater(beatBytes []byte) {
	if tch.beater == nil {
		tch.beater = &beater{
			TLSCapnpHandshaker: tch,
			beatBytes:          beatBytes,
			conn:               tch.conn,
			terminate:          make(chan struct{}),
			terminated:         make(chan struct{}),
			ticker:             time.NewTicker(common.HeartbeatInterval),
		}
		tch.beater.start()
	}
}

func (tch *TLSCapnpHandshaker) serverError(err error) error {
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	msg.SetConnectionError(err.Error())
	tch.send(server.SegToBytes(seg))
	return err
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

// CapnpServer

func (tcs *TLSCapnpServer) String() string {
	if tcs.dialed {
		return fmt.Sprintf("Connection (TLSCapnpServer) to %s", tcs.remoteHost)
	} else {
		return fmt.Sprintf("Connection (TLSCapnpServer) from %s", tcs.remoteHost)
	}
}

func (tcs *TLSCapnpServer) finishHandshake() error {
	// TLS seems to require us to pick one end as the client and one
	// end as the server even though in a server-server connection we
	// really don't care which is which.
	config := tcs.baseTLSConfig()
	if tcs.dialed {
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

	} else {
		// We came from the listener, so we're going to act as the server.
		config.ClientAuth = tls.RequireAndVerifyClientCert
		socket := tls.Server(tcs.socket, config)
		if err := socket.SetDeadline(time.Time{}); err != nil {
			return err
		}
		tcs.socket = socket
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

func (tcs *TLSCapnpServer) RestartHandshake() Handshaker {
	tcs.beater.stop()
	tcs.beater = nil
	if tcs.restart {
		return tcs.TLSCapnpHandshaker
	} else {
		return nil
	}
}

func (tcs *TLSCapnpServer) Run() error {
	seg := capn.NewBuffer(nil)
	message := msgs.NewRootMessage(seg)
	message.SetHeartbeat()
	tcs.createBeater(server.SegToBytes(seg))
	tcs.createReader()

	flushSeg := capn.NewBuffer(nil)
	flushMsg := msgs.NewRootMessage(flushSeg)
	flushMsg.SetFlushed()
	flushBytes := server.SegToBytes(flushSeg)
	tcs.connectionManager.ServerEstablished(tcs.conn, tcs.remoteHost, tcs.remoteRMId, tcs.remoteBootCount, tcs.combinedTieBreak, tcs.remoteClusterUUId, func() { tcs.conn.Send(flushBytes) })

	return nil
}

func (tcs *TLSCapnpServer) HandleMsg(msg connectionMsg) error {
	switch msgT := msg.(type) {
	case *beater:
		return tcs.beater.beat()
	default:
		return fmt.Errorf("Fatal to Connection: Received unexpected message: %#v", msgT)
	}
}

func (tcs *TLSCapnpServer) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	topology := tc.topology
	tcs.topology = topology

	server.Log("Connection", tcs, "topologyChanged", tc, "(isServer)")
	tc.maybeClose()
	if topology != nil && tcs.restart {
		if _, found := topology.RMsRemoved[tcs.remoteRMId]; found {
			tcs.restart = false
		}
	}

	return nil
}

func (tcs *TLSCapnpServer) ServerConnectionsChanged(map[common.RMId]paxos.Connection) error {
	return nil
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

func (tcs *TLSCapnpServer) readAndHandleOneMsg() error {
	seg, err := tcs.readOne()
	if err != nil {
		return err
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

// CapnpClient

func (tcc *TLSCapnpClient) String() string {
	if tcc.dialed {
		return fmt.Sprintf("Connection (TLSCapnpClient) to %s", tcc.remoteHost)
	} else {
		return fmt.Sprintf("Connection (TLSCapnpClient) from %s", tcc.remoteHost)
	}
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

func (tcc *TLSCapnpClient) RestartHandshake() Handshaker {
	tcc.beater.stop()
	tcc.beater = nil
	return nil // client connections are never restarted
}

func (tcc *TLSCapnpClient) Run() error {
	servers := tcc.connectionManager.ClientEstablished(tcc.connectionNumber, tcc.conn)
	if servers == nil {
		return errors.New("Not ready for client connections")
	} else {
		seg := capn.NewBuffer(nil)
		message := cmsgs.NewRootClientMessage(seg)
		message.SetHeartbeat()
		tcc.createBeater(server.SegToBytes(seg))
		tcc.createReader()

		tcc.submitter = client.NewClientTxnSubmitter(tcc.connectionManager.RMId, tcc.connectionManager.BootCount(), tcc.rootsVar, tcc.connectionManager)
		tcc.submitter.TopologyChanged(tcc.topology)
		tcc.submitter.ServerConnectionsChanged(servers)
		return nil
	}
}

func (tcc *TLSCapnpClient) HandleMsg(msg connectionMsg) error {
	switch msgT := msg.(type) {
	case connectionMsgOutcomeReceived:
		return tcc.outcomeReceived(msgT)
	case *beater:
		return tcc.beater.beat()
	default:
		return fmt.Errorf("Fatal to Connection: Received unexpected message: %#v", msgT)
	}
}

func (tcc *TLSCapnpClient) outcomeReceived(out connectionMsgOutcomeReceived) error {
	err := tcc.submitter.SubmissionOutcomeReceived(out.sender, out.txn, out.outcome)
	if tcc.submitterIdle != nil && tcc.submitter.IsIdle() {
		si := tcc.submitterIdle
		tcc.submitterIdle = nil
		server.Log("Connection", tcc, "outcomeReceived", si, "(submitterIdle)")
		si.maybeClose()
	}
	return err
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

func (tcc *TLSCapnpClient) ServerConnectionsChanged(servers map[common.RMId]paxos.Connection) error {
	return tcc.submitter.ServerConnectionsChanged(servers)
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
		tcc.conn.enqueueQuery(connectionSubmitTransaction(msg))
		return nil
	default:
		return fmt.Errorf("Unexpected message type received from client: %v", which)
	}
}

// WebSocketMsgPackClient

func (wmc *WebsocketMsgPackClient) PerformHandshake() (*WebsocketMsgPackClient, error) {
	return wmc, nil
}

func (wmc *WebsocketMsgPackClient) RestartHandshake() Handshaker {
	return nil // client connections are never restarted
}

func (wmc *WebsocketMsgPackClient) Run() error {
	return nil
}

func (wmc *WebsocketMsgPackClient) HandleMsg(msg connectionMsg) error {
	return nil
}

func (wmc *WebsocketMsgPackClient) TopologyChanged(tc *connectionMsgTopologyChanged) error {
	return nil
}

func (wmc *WebsocketMsgPackClient) ServerConnectionsChanged(map[common.RMId]paxos.Connection) error {
	return nil
}

// beater

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
		select {
		case <-b.terminate:
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
			if !b.conn.enqueueQuery(b) {
				return
			}
		}
	}
}

func (b *beater) beat() error {
	if b != nil {
		b.mustSendBeat = !b.mustSendBeat
		if !b.mustSendBeat {
			return b.send(b.beatBytes)
		}
	}
	return nil
}

// reader

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
		case <-sr.terminate:
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
