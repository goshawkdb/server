package network

import (
	cr "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	cc "github.com/msackman/chancell"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/nacl/box"
	"golang.org/x/crypto/nacl/secretbox"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/paxos"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

type Connection struct {
	sync.RWMutex
	established       bool
	remoteHost        string
	remoteRMId        common.RMId
	remoteBootCount   uint32
	combinedTieBreak  uint32
	remoteTopology    *server.Topology
	socket            *net.TCPConn
	ConnectionNumber  uint32
	connectionManager *ConnectionManager
	submitter         *client.ClientTxnSubmitter
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(connectionMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan connectionMsg
	rng               *rand.Rand
	currentState      connectionStateMachineComponent
	connectionDelay
	connectionDial
	connectionAwaitHandshake
	connectionAwaitClientHandshake
	connectionAwaitServerHandshake
	connectionRun
}

type connectionMsg interface {
	connectionMsgWitness()
}

type connectionMsgShutdown struct{}

func (cms *connectionMsgShutdown) connectionMsgWitness() {}

var connectionMsgShutdownInst = &connectionMsgShutdown{}

type connectionMsgSend []byte

func (cms connectionMsgSend) connectionMsgWitness() {}

type connectionMsgOutcomeReceived func(*connectionRun)

func (cor connectionMsgOutcomeReceived) connectionMsgWitness() {}

type connectionMsgTopologyChange struct {
	topology *server.Topology
	servers  map[common.RMId]paxos.Connection
}

func (ctc *connectionMsgTopologyChange) connectionMsgWitness() {}

type connectionMsgStatus server.StatusConsumer

func (cms *connectionMsgStatus) connectionMsgWitness() {}

type connectionMsgDisableHashCodes map[common.RMId]paxos.Connection

func (cmdhc connectionMsgDisableHashCodes) connectionMsgWitness() {}

func (conn *Connection) Shutdown(sync bool) {
	if conn.enqueueQuery(connectionMsgShutdownInst) && sync {
		conn.cellTail.Wait()
	}
}

func (conn *Connection) Send(msg []byte) {
	conn.enqueueQuery(connectionMsgSend(msg))
}

func (conn *Connection) SubmissionOutcomeReceived(sender common.RMId, txnId *common.TxnId, outcome *msgs.Outcome) {
	conn.enqueueQuery(connectionMsgOutcomeReceived(func(cr *connectionRun) {
		cr.submitter.SubmissionOutcomeReceived(sender, txnId, outcome)
	}))
}

func (conn *Connection) TopologyChange(topology *server.Topology, servers map[common.RMId]paxos.Connection) {
	conn.enqueueQuery(&connectionMsgTopologyChange{
		topology: topology,
		servers:  servers,
	})
}

func (conn *Connection) Status(sc *server.StatusConsumer) {
	conn.enqueueQuery((*connectionMsgStatus)(sc))
}

func (conn *Connection) RemoteDetails() (bool, string, common.RMId, uint32, uint32, *server.Topology) {
	conn.RLock()
	defer conn.RUnlock()
	return conn.established, conn.remoteHost, conn.remoteRMId, conn.remoteBootCount, conn.combinedTieBreak, conn.remoteTopology
}

func (conn *Connection) IsServer() bool {
	conn.RLock()
	defer conn.RUnlock()
	return conn.isServer
}

func (conn *Connection) ConnectedRMs(servers map[common.RMId]paxos.Connection) {
	conn.enqueueQuery(connectionMsgDisableHashCodes(servers))
}
func (conn *Connection) ConnectionLost(rmId common.RMId, servers map[common.RMId]paxos.Connection) {
	conn.enqueueQuery(connectionMsgDisableHashCodes(servers))
}
func (conn *Connection) ConnectionEstablished(rmId common.RMId, c paxos.Connection, servers map[common.RMId]paxos.Connection) {
	conn.enqueueQuery(connectionMsgDisableHashCodes(servers))
}

func (conn *Connection) enqueueQuery(msg connectionMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return conn.enqueueQueryInner(msg, cell, f)
	}
	return conn.cellTail.WithCell(f)
}

func NewConnectionToDial(host string, cm *ConnectionManager) *Connection {
	conn := &Connection{
		remoteHost:        host,
		connectionManager: cm,
	}
	conn.start()
	return conn
}

func NewConnectionFromTCPConn(socket *net.TCPConn, cm *ConnectionManager, count uint32) *Connection {
	conn := &Connection{
		socket:            socket,
		connectionManager: cm,
		ConnectionNumber:  count,
	}
	conn.start()
	return conn
}

func (conn *Connection) start() {
	var head *cc.ChanCellHead
	head, conn.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan connectionMsg, n)
			cell.Open = func() { conn.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			conn.enqueueQueryInner = func(msg connectionMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
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

	conn.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	conn.established = false

	conn.connectionDelay.init(conn)
	conn.connectionDial.init(conn)
	conn.connectionAwaitHandshake.init(conn)
	conn.connectionAwaitServerHandshake.init(conn)
	conn.connectionAwaitClientHandshake.init(conn)
	conn.connectionRun.init(conn)

	if conn.socket == nil {
		conn.currentState = &conn.connectionDial
	} else {
		conn.currentState = &conn.connectionAwaitHandshake
	}

	go conn.actorLoop(head)
}

func (conn *Connection) actorLoop(head *cc.ChanCellHead) {
	var (
		err          error
		stateChanged bool
		oldState     connectionStateMachineComponent
		queryChan    <-chan connectionMsg
		queryCell    *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = conn.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		stateChanged = oldState != conn.currentState
		if stateChanged {
			oldState = conn.currentState
			terminate, err = conn.currentState.start()
		} else {
			if msg, ok := <-queryChan; ok {
				terminate, err = conn.handleMsg(msg)
			} else {
				head.Next(queryCell, chanFun)
			}
		}
		terminate = terminate || err != nil
	}
	conn.cellTail.Terminate()
	conn.handleShutdown(err)
	log.Println("Connection terminated")
}

func (conn *Connection) handleMsg(msg connectionMsg) (terminate bool, err error) {
	switch msgT := msg.(type) {
	case *connectionMsgShutdown:
		terminate = true
		conn.currentState = nil
	case *connectionDelay:
		msgT.received()
	case *connectionBeater:
		err = conn.beat()
	case *connectionReadError:
		conn.reader = nil
		err = conn.connectionRun.maybeRestartConnection(msgT)
	case *connectionReadMessage:
		err = conn.handleMsgFromServer((*msgs.Message)(msgT))
	case *connectionReadClientMessage:
		err = conn.handleMsgFromClient((*cmsgs.ClientMessage)(msgT))
	case connectionMsgSend:
		err = conn.sendMessage(msgT)
	case connectionMsgOutcomeReceived:
		conn.outcomeReceived(msgT)
	case *connectionMsgTopologyChange:
		conn.topologyChange(msgT)
	case connectionMsgDisableHashCodes:
		conn.disableHashCodes(msgT)
	case *connectionMsgStatus:
		conn.status((*server.StatusConsumer)(msgT))
	default:
		err = fmt.Errorf("Fatal to Connection: Received unexpected message: %v", msgT)
	}
	return
}

func (conn *Connection) handleShutdown(err error) {
	conn.Lock()
	conn.established = false
	conn.Unlock()
	if err != nil {
		log.Println(err)
	}
	conn.maybeStopBeater()
	conn.maybeStopReaderAndCloseSocket()
	if conn.isClient {
		conn.connectionManager.ClientLost(conn.ConnectionNumber)
		conn.connectionManager.RemoveSenderAsync(conn)
		if conn.submitter != nil {
			conn.submitter.Shutdown()
		}
	}
	if conn.isServer {
		conn.connectionManager.ServerLost(conn)
	}
}

// state machine

type connectionStateMachineComponent interface {
	init(*Connection)
	start() (bool, error)
	connectionStateMachineComponentWitness()
}

func (conn *Connection) nextState(requestedState connectionStateMachineComponent) {
	if requestedState == nil {
		switch conn.currentState {
		case &conn.connectionDelay:
			conn.currentState = &conn.connectionDial
		case &conn.connectionDial:
			conn.currentState = &conn.connectionAwaitHandshake
		case &conn.connectionAwaitClientHandshake:
			conn.currentState = &conn.connectionRun
		case &conn.connectionAwaitServerHandshake:
			conn.currentState = &conn.connectionRun
		default:
			panic(fmt.Sprintf("Unexpected current state for nextState: %v", conn.currentState))
		}
	} else {
		conn.currentState = requestedState
	}
}

func (conn *Connection) status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("Connection to %v (%v, %v)", conn.remoteHost, conn.remoteRMId, conn.remoteBootCount))
	sc.Emit(fmt.Sprintf("- Current State: %v", conn.currentState))
	sc.Emit(fmt.Sprintf("- Established? %v", conn.established))
	sc.Emit(fmt.Sprintf("- IsServer? %v", conn.isServer))
	sc.Emit(fmt.Sprintf("- IsClient? %v", conn.isClient))
	if conn.submitter != nil {
		conn.submitter.Status(sc.Fork())
	}
	sc.Join()
}

// Delay

type connectionDelay struct {
	*Connection
	delay *time.Timer
}

func (cd *connectionDelay) connectionStateMachineComponentWitness() {}
func (cd *connectionDelay) String() string                          { return "ConnectionDelay" }

func (cd *connectionDelay) init(conn *Connection) {
	cd.Connection = conn
}

func (cd *connectionDelay) start() (bool, error) {
	cd.maybeStopReaderAndCloseSocket()
	cd.maybeStopBeater()
	cd.Lock()
	cd.established = false
	cd.privateKey = nil
	cd.sessionKey = nil
	cd.nonce = 0
	cd.nonceAryOut[0] = 0
	cd.nonceAryIn[0] = 0
	cd.isServer = false
	cd.isClient = false
	cd.Unlock()
	if cd.delay == nil {
		delay := server.ConnectionRestartDelayMin + time.Duration(cd.rng.Intn(server.ConnectionRestartDelayRangeMS))*time.Millisecond
		cd.delay = time.AfterFunc(delay, func() {
			cd.enqueueQuery(cd)
		})
	}
	return false, nil
}

func (cd *connectionDelay) connectionMsgWitness() {}

func (cd *connectionDelay) received() {
	if cd.currentState == cd {
		cd.delay = nil
		cd.nextState(nil)
	}
}

// Connect

type connectionDial struct {
	*Connection
}

func (cc *connectionDial) connectionStateMachineComponentWitness() {}
func (cc *connectionDial) String() string                          { return "ConnectionDial" }

func (cc *connectionDial) init(conn *Connection) {
	cc.Connection = conn
}

func (cc *connectionDial) start() (bool, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", cc.remoteHost)
	if err != nil {
		log.Println(err)
		cc.nextState(&cc.connectionDelay)
		return false, nil
	}
	socket, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Println(err)
		cc.nextState(&cc.connectionDelay)
		return false, nil
	}
	socket.SetKeepAlive(true)
	socket.SetKeepAlivePeriod(time.Second)
	cc.socket = socket
	cc.nextState(nil)
	return false, nil
}

// Await Handshake

type connectionAwaitHandshake struct {
	*Connection
	privateKey  *[32]byte
	sessionKey  *[32]byte
	nonce       uint64
	nonceAryIn  *[24]byte
	nonceAryOut *[24]byte
	isServer    bool
	isClient    bool
	outBuff     []byte
	inBuff      []byte
}

func (cah *connectionAwaitHandshake) connectionStateMachineComponentWitness() {}
func (cah *connectionAwaitHandshake) String() string                          { return "ConnectionAwaitHandshake" }

func (cah *connectionAwaitHandshake) init(conn *Connection) {
	cah.Connection = conn
	nonceAryIn := [24]byte{}
	cah.nonceAryIn = &nonceAryIn
	nonceAryOut := [24]byte{}
	cah.nonceAryOut = &nonceAryOut
	cah.inBuff = make([]byte, 16)
}

func (cah *connectionAwaitHandshake) start() (bool, error) {
	helloSeg, err := cah.makeHello()
	if err != nil {
		return cah.maybeRestartConnection(err)
	}
	if err := cah.send(server.SegToBytes(helloSeg)); err != nil {
		return cah.maybeRestartConnection(err)
	}
	cah.nonce = 0

	if seg, err := capn.ReadFromStream(cah.socket, nil); err == nil {
		hello := cmsgs.ReadRootHello(seg)
		if cah.verifyHello(&hello) {
			sessionKey := [32]byte{}
			remotePublicKey := [32]byte{}
			copy(remotePublicKey[:], hello.PublicKey())
			box.Precompute(&sessionKey, &remotePublicKey, cah.privateKey)

			if hello.IsClient() {
				cah.Lock()
				cah.isClient = true
				cah.sessionKey = &sessionKey
				cah.Unlock()
				cah.nonceAryIn[0] = 128
				cah.nextState(&cah.connectionAwaitClientHandshake)

			} else {
				extendedKey := make([]byte, 64)
				copy(extendedKey[:32], sessionKey[:])
				copy(extendedKey[32:], cah.connectionManager.passwordHash[:])
				sessionKey = sha256.Sum256(extendedKey)
				cah.Lock()
				cah.isServer = true
				cah.sessionKey = &sessionKey
				cah.Unlock()
				if cah.remoteHost == "" {
					cah.nonceAryIn[0] = 128
				} else {
					cah.nonceAryOut[0] = 128
				}
				cah.nextState(&cah.connectionAwaitServerHandshake)
			}
			return false, nil
		} else {
			return cah.maybeRestartConnection(fmt.Errorf("Received erroneous hello from peer"))
		}
	} else {
		return cah.maybeRestartConnection(err)
	}
}

func (cah *connectionAwaitHandshake) makeHello() (*capn.Segment, error) {
	seg := capn.NewBuffer(nil)
	hello := cmsgs.NewRootHello(seg)
	hello.SetProduct(common.ProductName)
	hello.SetVersion(common.ProductVersion)
	publicKey, privateKey, err := box.GenerateKey(cr.Reader)
	if err != nil {
		return nil, err
	}
	cah.privateKey = privateKey
	hello.SetPublicKey(publicKey[:])
	hello.SetIsClient(false)
	return seg, nil
}

func (cah *connectionAwaitHandshake) send(msg []byte) (err error) {
	if cah.sessionKey == nil {
		_, err = cah.socket.Write(msg)
	} else {
		reqLen := len(msg) + secretbox.Overhead + 16
		if cah.outBuff == nil {
			cah.outBuff = make([]byte, 16, reqLen)
		} else if l := len(cah.outBuff); l < reqLen {
			cah.outBuff = make([]byte, 16, l*(1+(reqLen/l)))
		} else {
			cah.outBuff = cah.outBuff[:16]
		}
		cah.nonce++
		binary.BigEndian.PutUint64(cah.outBuff[:8], cah.nonce)
		binary.BigEndian.PutUint64(cah.nonceAryOut[16:], cah.nonce)
		secretbox.Seal(cah.outBuff[16:], msg, cah.nonceAryOut, cah.sessionKey)
		binary.BigEndian.PutUint64(cah.outBuff[8:16], uint64(reqLen-16))
		_, err = cah.socket.Write(cah.outBuff[:reqLen])
	}
	return
}

func (cah *connectionAwaitHandshake) readAndDecryptOne() (*capn.Segment, error) {
	if cah.sessionKey == nil {
		return capn.ReadFromStream(cah.socket, nil)
	}
	read, err := cah.socket.Read(cah.inBuff)
	if err != nil {
		return nil, err
	} else if read < len(cah.inBuff) {
		return nil, fmt.Errorf("Only read %v bytes, wanted %v", read, len(cah.inBuff))
	}
	copy(cah.nonceAryIn[16:], cah.inBuff[:8])
	msgLen := binary.BigEndian.Uint64(cah.inBuff[8:16])
	plainLen := msgLen - secretbox.Overhead
	msgBuf := make([]byte, plainLen+msgLen)
	for recvBuf := msgBuf[plainLen:]; len(recvBuf) != 0; {
		read, err = cah.socket.Read(recvBuf)
		if err != nil {
			return nil, err
		} else {
			recvBuf = recvBuf[read:]
		}
	}
	plaintext, ok := secretbox.Open(msgBuf[:0], msgBuf[plainLen:], cah.nonceAryIn, cah.sessionKey)
	if !ok {
		return nil, fmt.Errorf("Unable to decrypt message")
	}
	seg, _, err := capn.ReadFromMemoryZeroCopy(plaintext)
	return seg, err
}

func (cah *connectionAwaitHandshake) verifyHello(hello *cmsgs.Hello) bool {
	return hello.Product() == common.ProductName &&
		hello.Version() == common.ProductVersion
}

func (cah *connectionAwaitHandshake) maybeRestartConnection(err error) (bool, error) {
	if cah.remoteHost == "" {
		// we came from the listener and don't know who the remote is, so have to shutdown
		return false, err
	} else {
		log.Println(err)
		cah.nextState(&cah.connectionDelay)
		return false, nil
	}
}

func (cah *connectionAwaitServerHandshake) makeHelloFromServer(topology *server.Topology) *capn.Segment {
	seg := capn.NewBuffer(nil)
	hello := cmsgs.NewRootHelloFromServer(seg)
	localHost := cah.connectionManager.LocalHost()
	hello.SetLocalHost(localHost)
	namespace := make([]byte, common.KeyLen-8)
	binary.BigEndian.PutUint32(namespace[0:4], cah.ConnectionNumber)
	binary.BigEndian.PutUint32(namespace[4:8], cah.connectionManager.BootCount)
	binary.BigEndian.PutUint32(namespace[8:], uint32(cah.connectionManager.RMId))
	hello.SetNamespace(namespace)
	if cah.isServer {
		tieBreak := cah.rng.Uint32()
		cah.Lock()
		cah.combinedTieBreak = tieBreak
		cah.Unlock()
		hello.SetTieBreak(tieBreak)
		hello.SetTopologyDBVersion(topology.DBVersion[:])
		hello.SetTopology(topology.AddToSegAutoRoot(seg))
	}
	if topology.RootVarUUId != nil {
		varIdPos := cmsgs.NewVarIdPos(seg)
		hello.SetRoot(varIdPos)
		varIdPos.SetId(topology.RootVarUUId[:])
		varIdPos.SetPositions((capn.UInt8List)(*topology.RootPositions))
	}
	return seg
}

// Await Server Handshake

type connectionAwaitServerHandshake struct {
	*Connection
}

func (cash *connectionAwaitServerHandshake) connectionStateMachineComponentWitness() {}
func (cash *connectionAwaitServerHandshake) String() string                          { return "ConnectionAwaitServerHandshake" }

func (cash *connectionAwaitServerHandshake) init(conn *Connection) {
	cash.Connection = conn
}

func (cash *connectionAwaitServerHandshake) start() (bool, error) {
	topology := cash.connectionManager.Topology()
	helloFromServer := cash.makeHelloFromServer(topology)
	if err := cash.send(server.SegToBytes(helloFromServer)); err != nil {
		return cash.connectionAwaitHandshake.maybeRestartConnection(err)
	}

	if seg, err := cash.readAndDecryptOne(); err == nil {
		hello := cmsgs.ReadRootHelloFromServer(seg)
		if verified, remoteTopology := cash.verifyTopology(topology, &hello); verified {
			cash.Lock()
			cash.established = true
			cash.remoteHost = hello.LocalHost()
			ns := hello.Namespace()
			cash.remoteBootCount = binary.BigEndian.Uint32(ns[4:8])
			cash.remoteRMId = common.RMId(binary.BigEndian.Uint32(ns[8:12]))
			cash.combinedTieBreak = cash.combinedTieBreak ^ hello.TieBreak()
			cash.remoteTopology = remoteTopology
			cash.Unlock()
			cash.nextState(nil)
			return false, nil
		} else {
			return cash.connectionAwaitHandshake.maybeRestartConnection(fmt.Errorf("Unequal remote topology"))
		}
	} else {
		return cash.connectionAwaitHandshake.maybeRestartConnection(err)
	}
}

func (cash *connectionAwaitServerHandshake) verifyTopology(topology *server.Topology, remote *cmsgs.HelloFromServer) (bool, *server.Topology) {
	remoteTopologyDBVersion := common.MakeTxnId(remote.TopologyDBVersion())
	remoteTopologyCap := remote.Topology()
	remoteRoot := remote.Root()
	remoteTopology := server.TopologyFromCap(remoteTopologyDBVersion, &remoteRoot, &remoteTopologyCap)
	return topology.Configuration.Equal(remoteTopology.Configuration), remoteTopology
}

// Await Client Handshake

type connectionAwaitClientHandshake struct {
	*Connection
}

func (cach *connectionAwaitClientHandshake) connectionStateMachineComponentWitness() {}
func (cach *connectionAwaitClientHandshake) String() string                          { return "ConnectionAwaitClientHandshake" }

func (cach *connectionAwaitClientHandshake) init(conn *Connection) {
	cach.Connection = conn
}

func (cach *connectionAwaitClientHandshake) start() (bool, error) {
	if seg, err := cach.readAndDecryptOne(); err == nil {
		hello := cmsgs.ReadRootHelloFromClient(seg)
		topology := cach.connectionManager.Topology()

		un := hello.Username()
		if pw, found := topology.Accounts[un]; !found {
			return false, fmt.Errorf("Unknown user '%s'", un)
		} else if err = bcrypt.CompareHashAndPassword([]byte(pw), hello.Password()); err != nil {
			return false, fmt.Errorf("Incorrect password for '%s': %v", un, err)
		} else {
			log.Printf("User '%s' authenticated", un)
		}

		helloFromServer := cach.makeHelloFromServer(topology)
		if err := cach.send(server.SegToBytes(helloFromServer)); err != nil {
			return cach.connectionAwaitHandshake.maybeRestartConnection(err)
		}
		cach.Lock()
		cach.established = true
		cach.remoteHost = cach.socket.RemoteAddr().String()
		cach.Unlock()
		cach.nextState(nil)
		return false, nil

	} else {
		return cach.connectionAwaitHandshake.maybeRestartConnection(err)
	}
}

// Run

type connectionRun struct {
	*Connection
	beater       *connectionBeater
	reader       *connectionReader
	mustSendBeat bool
	missingBeats int
	beatBytes    []byte
}

func (cr *connectionRun) connectionStateMachineComponentWitness() {}
func (cr *connectionRun) String() string                          { return "ConnectionRun" }

func (cr *connectionRun) init(conn *Connection) {
	cr.Connection = conn
}

func (cr *connectionRun) outcomeReceived(f func(*connectionRun)) {
	if cr.currentState != cr {
		return
	}
	f(cr)
}

func (cr *connectionRun) start() (bool, error) {
	log.Printf("Connection established to %v (%v)\n", cr.remoteHost, cr.remoteRMId)

	seg := capn.NewBuffer(nil)
	if cr.isClient {
		message := cmsgs.NewRootClientMessage(seg)
		message.SetHeartbeat()
	} else {
		message := msgs.NewRootMessage(seg)
		message.SetHeartbeat()
	}
	cr.beatBytes = server.SegToBytes(seg)

	if cr.isServer {
		cr.connectionManager.ServerEstablished(cr.Connection)
	}
	if cr.isClient {
		topology, servers := cr.connectionManager.ClientEstablished(cr.ConnectionNumber, cr.Connection)
		cr.connectionManager.AddSender(cr.Connection)
		cr.submitter = client.NewClientTxnSubmitter(cr.connectionManager.RMId, cr.connectionManager.BootCount, topology, cr.connectionManager)
		cr.submitter.TopologyChange(nil, servers)
	}
	cr.mustSendBeat = true
	cr.missingBeats = 0

	cr.beater = newConnectionBeater(cr.Connection)
	go cr.beater.beat()

	cr.reader = newConnectionReader(cr.Connection)
	if cr.isClient {
		go cr.reader.readClient()
	} else {
		go cr.reader.readServer()
	}

	return false, nil
}

func (cr *connectionRun) topologyChange(tChange *connectionMsgTopologyChange) {
	if cr.currentState != cr || !cr.isClient {
		return
	}
	cr.submitter.TopologyChange(tChange.topology, tChange.servers)
}

func (cr *connectionRun) disableHashCodes(servers map[common.RMId]paxos.Connection) {
	if cr.currentState != cr || !cr.isClient {
		return
	}
	cr.submitter.TopologyChange(nil, servers)
}

func (cr *connectionRun) handleMsgFromClient(msg *cmsgs.ClientMessage) error {
	if cr.currentState != cr {
		// probably just draining the queue from the reader after a restart
		return nil
	}
	cr.missingBeats = 0
	switch which := msg.Which(); which {
	case cmsgs.CLIENTMESSAGE_HEARTBEAT:
		// do nothing
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
		cr.maybeRestartConnection(fmt.Errorf("Unexpected message type received from client: %v", which))
	}
	return nil
}

func (cr *connectionRun) handleMsgFromServer(msg *msgs.Message) error {
	if cr.currentState != cr {
		// probably just draining the queue from the reader after a restart
		return nil
	}
	cr.missingBeats = 0
	if which := msg.Which(); which != msgs.MESSAGE_HEARTBEAT {
		cr.connectionManager.Dispatchers.DispatchMessage(cr.remoteRMId, which, msg)
	}
	return nil
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

func (cr *connectionRun) maybeRestartConnection(err error) error {
	switch {
	case err == nil || cr.currentState != cr:
		return nil

	case cr.isServer:
		log.Printf("Error on server connection to %v: %v", cr.remoteRMId, err)
		cr.nextState(&cr.connectionDelay)
		cr.connectionManager.ServerLost(cr.Connection)
		return nil

	case cr.isClient:
		log.Printf("Error on client connection to %v: %v", cr.remoteHost, err)
		cr.connectionManager.ClientLost(cr.ConnectionNumber)
		return err

	default:
		return err
	}
}

func (cr *connectionRun) sendMessage(msg []byte) error {
	if cr.currentState == cr {
		cr.mustSendBeat = false
		return cr.maybeRestartConnection(cr.send(msg))
	}
	return nil
}

func (cr *connectionRun) beat() error {
	if cr.currentState != cr {
		return nil
	}
	if cr.missingBeats == 2 {
		return cr.maybeRestartConnection(
			fmt.Errorf("Missed too many connection heartbeats. Restarting connection."))
	}
	// Useful for testing recovery from network brownouts
	/*
		if cr.rng.Intn(15) == 0 && cr.isServer {
			return cr.maybeRestartConnection(
				fmt.Errorf("Random death. Restarting connection."))
		}
	*/
	cr.missingBeats++
	if cr.mustSendBeat {
		return cr.maybeRestartConnection(cr.send(cr.beatBytes))
	} else {
		cr.mustSendBeat = true
	}
	return nil
}

func (cr *connectionRun) maybeStopBeater() {
	if cr.beater != nil {
		close(cr.beater.terminate)
		cr.beater.terminated.Wait()
		cr.beater = nil
	}
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
		cr.socket = nil

	} else if cr.socket != nil {
		if err := cr.socket.Close(); err != nil {
			log.Println(err)
		}
		cr.socket = nil
	}
}

// Beater

type connectionBeater struct {
	*Connection
	terminate  chan struct{}
	terminated *sync.WaitGroup
	ticker     *time.Ticker
}

func newConnectionBeater(conn *Connection) *connectionBeater {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	return &connectionBeater{
		Connection: conn,
		terminate:  make(chan struct{}),
		terminated: wg,
		ticker:     time.NewTicker(common.HeartbeatInterval),
	}
}

func (cb *connectionBeater) beat() {
	defer func() {
		cb.ticker.Stop()
		cb.ticker = nil
		cb.terminated.Done()
	}()
	for {
		select {
		case <-cb.terminate:
			return
		case <-cb.ticker.C:
			if !cb.enqueueQuery(cb) {
				return
			}
		}
	}
}

func (cb *connectionBeater) connectionMsgWitness() {}

// Reader

type connectionReader struct {
	*Connection
	terminate  chan struct{}
	terminated *sync.WaitGroup
}

func newConnectionReader(conn *Connection) *connectionReader {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	return &connectionReader{
		Connection: conn,
		terminate:  make(chan struct{}),
		terminated: wg,
	}
}

func (cr *connectionReader) readServer() {
	cr.read(func (seg *capn.Segment) bool {
		msg := msgs.ReadRootMessage(seg)
		return cr.enqueueQuery((*connectionReadMessage)(&msg))
	})
}

func (cr *connectionReader) readClient() {
	cr.read(func (seg *capn.Segment) bool {
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
			if seg, err := cr.readAndDecryptOne(); err == nil {
				if !fun(seg) {
					return
				}
			} else {
				cr.enqueueQuery(&connectionReadError{err})
				return
			}
		}
	}
}

type connectionReadMessage msgs.Message

func (crm *connectionReadMessage) connectionMsgWitness() {}

type connectionReadClientMessage cmsgs.ClientMessage

func (crcm *connectionReadClientMessage) connectionMsgWitness() {}

type connectionReadError struct {
	error
}

func (cre *connectionReadError) connectionMsgWitness() {}
