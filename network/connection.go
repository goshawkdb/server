package network

import (
	"fmt"
	cc "github.com/msackman/chancell"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"log"
	"math/rand"
	"net"
	"time"
)

type Connection struct {
	connectionManager *ConnectionManager
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(connectionMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan connectionMsg
	rng               *rand.Rand
	currentState      connectionStateMachineComponent
	connectionDelay
	connectionDial
	connectionHandshake
	connectionRun
}

type connectionMsg interface {
	witness() connectionMsg
}

type connectionMsgBasic struct{}

func (cmb connectionMsgBasic) witness() connectionMsg { return cmb }

type connectionMsgShutdown struct{ connectionMsgBasic }

type connectionMsgSend []byte

func (cms connectionMsgSend) witness() connectionMsg { return cms }

type connectionMsgTopologyChanged struct {
	connectionMsgBasic
	topology   *configuration.Topology
	resultChan chan struct{}
}

func (cmtc *connectionMsgTopologyChanged) maybeClose() {
	select {
	case <-cmtc.resultChan:
	default:
		close(cmtc.resultChan)
	}
}

type connectionMsgStatus struct {
	connectionMsgBasic
	*server.StatusConsumer
}

func (conn *Connection) Shutdown(sync paxos.Blocking) {
	if conn.enqueueQuery(connectionMsgShutdown{}) && sync == paxos.Sync {
		conn.cellTail.Wait()
	}
}

func (conn *Connection) Send(msg []byte) {
	conn.enqueueQuery(connectionMsgSend(msg))
}

func (conn *Connection) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &connectionMsgTopologyChanged{
		resultChan: make(chan struct{}),
		topology:   topology,
	}
	if conn.enqueueQuery(msg) {
		go func() {
			select {
			case <-msg.resultChan:
			case <-conn.cellTail.Terminated:
			}
			done(true) // connection drop is not a problem
		}()
	} else {
		done(true)
	}
}

func (conn *Connection) Status(sc *server.StatusConsumer) {
	conn.enqueueQuery(connectionMsgStatus{StatusConsumer: sc})
}

func (conn *Connection) enqueueQuery(msg connectionMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return conn.enqueueQueryInner(msg, cell, f)
	}
	return conn.cellTail.WithCell(f)
}

func NewConnectionTCPTLSCapnpDialer(remoteHost string, cm *ConnectionManager) *Connection {
	dialer := NewTCPDialerForTLSCapnp(remoteHost, cm)
	return NewConnectionWithDialer(dialer, cm)
}

func NewConnectionTCPTLSCapnpHandshaker(socket *net.TCPConn, cm *ConnectionManager, count uint32) *Connection {
	yesman := NewTLSCapnpHandshaker(nil, socket, cm, count, "")
	return NewConnectionWithHandshaker(yesman, cm)
}

func NewConnectionWithHandshaker(yesman Handshaker, cm *ConnectionManager) *Connection {
	conn := &Connection{
		connectionManager: cm,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	conn.Handshaker = yesman
	conn.start()
	return conn
}

func NewConnectionWithDialer(phone Dialer, cm *ConnectionManager) *Connection {
	conn := &Connection{
		connectionManager: cm,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	conn.Dialer = phone
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

	conn.connectionDelay.init(conn)
	conn.connectionDial.init(conn)
	conn.connectionHandshake.init(conn)
	conn.connectionRun.init(conn)

	if conn.Dialer != nil {
		conn.currentState = &conn.connectionDial
	} else if conn.Handshaker != nil {
		conn.currentState = &conn.connectionHandshake
	}

	go conn.actorLoop(head)
}

func (conn *Connection) actorLoop(head *cc.ChanCellHead) {
	conn.topology = conn.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, conn)
	defer conn.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, conn)

	var (
		err       error
		oldState  connectionStateMachineComponent
		queryChan <-chan connectionMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = conn.queryChan, cell }
	head.WithCell(chanFun)
	if conn.topology == nil {
		panic("Nil topology on connection start!")
		// err = errors.New("No local topology, not ready for any connections")
	}

	terminate := err != nil
	for !terminate {
		if oldState != conn.currentState {
			oldState = conn.currentState
			terminate, err = conn.currentState.start()
		} else if msg, ok := <-queryChan; ok {
			terminate, err = conn.handleMsg(msg)
		} else {
			head.Next(queryCell, chanFun)
		}
		terminate = terminate || err != nil
	}
	conn.cellTail.Terminate()
	conn.handleShutdown(err)
	log.Println("Connection terminated")
}

func (conn *Connection) handleMsg(msg connectionMsg) (terminate bool, err error) {
	switch msgT := msg.(type) {
	case connectionMsgShutdown:
		terminate = true
		conn.currentState = nil
	case *connectionDelay:
		msgT.received()
	case connectionReadError:
		err = msgT.error
	case connectionExec:
		err = msgT()
	case connectionMsgSend:
		err = conn.sendMessage(msgT)
	case *connectionMsgTopologyChanged:
		err = conn.topologyChanged(msgT)
	case connectionMsgStatus:
		conn.status(msgT.StatusConsumer)
	default:
		err = fmt.Errorf("Fatal to Connection: Received unexpected message: %#v", msgT)
	}
	if err != nil && !terminate {
		err = conn.maybeRestartConnection(err)
	}
	return
}

func (conn *Connection) maybeRestartConnection(err error) error {
	if conn.Handshaker != nil {
		conn.Dialer = conn.Handshaker.RestartDialer()
	} else if conn.Protocol != nil {
		conn.Dialer = conn.Protocol.RestartDialer()
	}

	if conn.Dialer == nil {
		return err // it's fatal; actor loop will shutdown Protocol or Handshaker
	} else {
		log.Printf("Restarting connection due to error: %v", err)
		conn.nextState(&conn.connectionDelay)
		return nil
	}
}

func (conn *Connection) handleShutdown(err error) {
	if err != nil {
		log.Println(err)
	}
	if conn.Protocol != nil {
		conn.Protocol.InternalShutdown()
	} else if conn.Handshaker != nil {
		conn.Handshaker.InternalShutdown()
	}
	conn.Protocol = nil
	conn.Handshaker = nil
	conn.Dialer = nil
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
			conn.currentState = &conn.connectionHandshake
		case &conn.connectionHandshake:
			conn.currentState = &conn.connectionRun
		default:
			panic(fmt.Sprintf("Unexpected current state for nextState: %v", conn.currentState))
		}
	} else {
		conn.currentState = requestedState
	}
}

func (conn *Connection) status(sc *server.StatusConsumer) {
	/*
		sc.Emit(fmt.Sprintf("Connection to %v (%v, %v)", conn.remoteHost, conn.remoteRMId, conn.remoteBootCount))
		sc.Emit(fmt.Sprintf("- Current State: %v", conn.currentState))
		sc.Emit(fmt.Sprintf("- IsServer? %v", conn.isServer))
		sc.Emit(fmt.Sprintf("- IsClient? %v", conn.isClient))
		if conn.submitter != nil {
			conn.submitter.Status(sc.Fork())
		}
	*/
	sc.Join()
}

// Delay

type connectionDelay struct {
	connectionMsgBasic
	*Connection
	delay *time.Timer
}

func (cd *connectionDelay) connectionStateMachineComponentWitness() {}
func (cd *connectionDelay) String() string                          { return "ConnectionDelay" }

func (cd *connectionDelay) init(conn *Connection) {
	cd.Connection = conn
}

func (cd *connectionDelay) start() (bool, error) {
	cd.Handshaker = nil
	cd.Protocol = nil
	if cd.delay == nil {
		delay := server.ConnectionRestartDelayMin + time.Duration(cd.rng.Intn(server.ConnectionRestartDelayRangeMS))*time.Millisecond
		cd.delay = time.AfterFunc(delay, func() {
			cd.enqueueQuery(cd)
		})
	}
	return false, nil
}

func (cd *connectionDelay) received() {
	if cd.currentState == cd {
		cd.delay = nil
		cd.nextState(nil)
	}
}

// Dial

type connectionDial struct {
	*Connection
	Dialer
}

func (cc *connectionDial) connectionStateMachineComponentWitness() {}
func (cc *connectionDial) String() string                          { return "ConnectionDial" }

func (cc *connectionDial) init(conn *Connection) {
	cc.Connection = conn
}

func (cc *connectionDial) start() (bool, error) {
	yesman, err := cc.Dial()
	if err == nil {
		cc.Handshaker = yesman
		cc.nextState(nil)
	} else {
		log.Println(err)
		cc.nextState(&cc.connectionDelay)
	}
	return false, nil
}

// Handshake

type connectionHandshake struct {
	*Connection
	topology *configuration.Topology
	Handshaker
}

func (cah *connectionHandshake) connectionStateMachineComponentWitness() {}
func (cah *connectionHandshake) String() string                          { return "ConnectionHandshake" }

func (cah *connectionHandshake) init(conn *Connection) {
	cah.Connection = conn
}

func (cah *connectionHandshake) start() (bool, error) {
	protocol, err := cah.PerformHandshake(cah.topology)
	cah.Protocol = protocol
	if err == nil {
		cah.nextState(nil)
		return false, nil
	} else {
		return false, err
	}
}

// Run

type connectionRun struct {
	*Connection
	Protocol
}

func (cr *connectionRun) connectionStateMachineComponentWitness() {}
func (cr *connectionRun) String() string                          { return "ConnectionRun" }

func (cr *connectionRun) init(conn *Connection) {
	cr.Connection = conn
}

func (cr *connectionRun) start() (bool, error) {
	cr.Handshaker = nil
	return false, cr.Run(cr.Connection)
}

func (cr *connectionRun) topologyChanged(tc *connectionMsgTopologyChanged) error {
	if cr.Protocol == nil {
		return nil
	} else {
		return cr.Protocol.TopologyChanged(tc)
	}
}

func (cr *connectionRun) sendMessage(msg []byte) error {
	if cr.Protocol == nil {
		return nil
	} else {
		return cr.Protocol.SendMessage(msg)
	}
}

// Reader

type connectionReadError struct {
	connectionMsgBasic
	error
}

type connectionExec func() error

func (ce connectionExec) witness() connectionMsg { return ce }
