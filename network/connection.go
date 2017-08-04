package network

import (
	"errors"
	"fmt"
	"github.com/go-kit/kit/log"
	cc "github.com/msackman/chancell"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	eng "goshawkdb.io/server/txnengine"
	"math/rand"
	"net"
	"time"
)

type Connection struct {
	logger            log.Logger
	connectionManager *ConnectionManager
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(connectionMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan connectionMsg
	shutdownStarted   bool
	handshaker        Handshaker
	rng               *rand.Rand
	currentState      connectionStateMachineComponent
	connectionDelay
	connectionDial
	connectionHandshake
	connectionRun
}

type connectionMsg interface{}

type connectionMsgSync interface {
	connectionMsg
	common.MsgSync
}

type connectionMsgStartShutdown struct{}
type connectionMsgShutdownComplete struct{}

type connectionMsgTopologyChanged struct {
	common.MsgSyncQuery
	topology *configuration.Topology
}

type connectionMsgStatus struct {
	*server.StatusConsumer
}

// for paxos.Actorish
type connectionMsgExec func()

// is async
func (conn *Connection) Shutdown() {
	conn.enqueueQuery(connectionMsgStartShutdown{})
}

func (conn *Connection) shutdownComplete() {
	conn.enqueueQuery(connectionMsgShutdownComplete{})
}

func (conn *Connection) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &connectionMsgTopologyChanged{topology: topology}
	conn.enqueueQuerySync(msg)
	done(true)
}

func (conn *Connection) Status(sc *server.StatusConsumer) {
	conn.enqueueQuery(connectionMsgStatus{StatusConsumer: sc})
}

// This is for the paxos.Actorish interface
func (conn *Connection) Enqueue(fun func()) bool {
	return conn.enqueueQuery(connectionMsgExec(fun))
}

// This is for the paxos.Actorish interface
func (conn *Connection) WithTerminatedChan(fun func(chan struct{})) {
	fun(conn.cellTail.Terminated)
}

type connectionMsgExecFuncError func() error

func (conn *Connection) EnqueueFuncError(fun func() error) bool {
	return conn.enqueueQuery(connectionMsgExecFuncError(fun))
}

type connectionQueryCapture struct {
	conn *Connection
	msg  connectionMsg
}

func (cqc *connectionQueryCapture) ccc(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
	return cqc.conn.enqueueQueryInner(cqc.msg, cell, cqc.ccc)
}

func (conn *Connection) enqueueQuery(msg connectionMsg) bool {
	cqc := &connectionQueryCapture{conn: conn, msg: msg}
	return conn.cellTail.WithCell(cqc.ccc)
}

func (conn *Connection) enqueueQuerySync(msg connectionMsgSync) bool {
	resultChan := msg.Init()
	if conn.enqueueQuery(msg) {
		select {
		case <-resultChan:
			return true
		case <-conn.cellTail.Terminated:
			return false
		}
	} else {
		return false
	}
}

// we are dialing out to someone else
func NewConnectionTCPTLSCapnpDialer(remoteHost string, cm *ConnectionManager, logger log.Logger) *Connection {
	logger = log.With(logger, "subsystem", "connection", "dir", "outgoing", "protocol", "capnp")
	phone := common.NewTCPDialer(nil, remoteHost, logger)
	yesman := NewTLSCapnpHandshaker(phone, logger, 0, cm)
	return NewConnection(yesman, cm, logger)
}

// the socket is already established - we got it from the TCP listener
func NewConnectionTCPTLSCapnpHandshaker(socket *net.TCPConn, cm *ConnectionManager, count uint32, logger log.Logger) {
	logger = log.With(logger, "subsystem", "connection", "dir", "incoming", "protocol", "capnp")
	phone := common.NewTCPDialer(socket, "", logger)
	yesman := NewTLSCapnpHandshaker(phone, logger, count, cm)
	NewConnection(yesman, cm, logger)
}

func NewConnection(yesman Handshaker, cm *ConnectionManager, logger log.Logger) *Connection {
	conn := &Connection{
		logger:            logger,
		connectionManager: cm,
		handshaker:        yesman,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
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

	conn.connectionDelay.init(conn)
	conn.connectionDial.init(conn)
	conn.connectionHandshake.init(conn)
	conn.connectionRun.init(conn)

	conn.currentState = &conn.connectionDial

	go conn.actorLoop(head)
}

func (conn *Connection) actorLoop(head *cc.ChanCellHead) {
	conn.topology = conn.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, conn)
	defer conn.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, conn)

	defer func() {
		if r := recover(); r != nil {
			conn.logger.Log("msg", "Connection panicked!", "error", fmt.Sprint(r))
		}
	}()

	var (
		err       error
		oldState  connectionStateMachineComponent
		queryChan <-chan connectionMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = conn.queryChan, cell }
	head.WithCell(chanFun)
	if conn.topology == nil {
		// Most likely is that the connection manager has shutdown due
		// to some other error and so the sync enqueue failed.
		err = errors.New("No local topology, not ready for any connections.")
	}

	terminated := err != nil // have we stopped?
	terminating := false     // what should we do next?
	for !terminated {
		if oldState != conn.currentState {
			oldState = conn.currentState
			terminating, err = conn.currentState.start()
		} else if msg, ok := <-queryChan; ok {
			terminating, terminated, err = conn.handleMsg(msg)
		} else {
			head.Next(queryCell, chanFun)
		}
		terminating = terminating || err != nil
		if terminating {
			conn.startShutdown(err)
			err = nil
		}
	}
	conn.cellTail.Terminate()
	conn.handleShutdown(err)
	conn.logger.Log("msg", "Terminated.")
}

func (conn *Connection) handleMsg(msg connectionMsg) (terminating, terminated bool, err error) {
	switch msgT := msg.(type) {
	case connectionMsgStartShutdown:
		terminating = true
	case connectionMsgShutdownComplete:
		terminated = true
	case connectionMsgExec:
		msgT()
	case connectionMsgExecFuncError:
		err = msgT()
	case *connectionMsgTopologyChanged:
		err = conn.topologyChanged(msgT)
	case connectionMsgStatus:
		conn.status(msgT.StatusConsumer)
	default:
		panic(fmt.Sprintf("Received unexpected message: %#v", msgT))
	}
	if err != nil && !terminating {
		err = conn.maybeRestartConnection(err)
	}
	return
}

func (conn *Connection) maybeRestartConnection(err error) error {
	restartable := false
	if conn.protocol != nil {
		restartable = conn.protocol.Restart()
	} else if conn.handshaker != nil {
		restartable = conn.handshaker.Restart()
	}

	if restartable {
		conn.logger.Log("msg", "Restarting.", "error", err)
		conn.nextState(&conn.connectionDelay)
		return nil
	} else {
		return err // it's fatal; actor loop will shutdown Protocol or Handshaker
	}
}

func (conn *Connection) startShutdown(err error) {
	if err != nil {
		conn.logger.Log("error", err)
	}
	if !conn.shutdownStarted {
		conn.shutdownStarted = true
		if conn.protocol != nil {
			conn.protocol.InternalShutdown()
			conn.protocol = nil
		} else {
			conn.shutdownComplete()
		}
	}
}

func (conn *Connection) handleShutdown(err error) {
	if err != nil {
		conn.logger.Log("error", err)
	}
	if conn.protocol != nil {
		conn.protocol.InternalShutdown()
	} else if conn.handshaker != nil {
		conn.handshaker.InternalShutdown()
	}
	conn.currentState = nil
	conn.protocol = nil
	conn.handshaker = nil
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
	if conn.protocol != nil {
		sc.Emit(fmt.Sprintf("Connection %v", conn.protocol))
	} else if conn.handshaker != nil {
		sc.Emit(fmt.Sprintf("Connection %v", conn.handshaker))
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
	cd.protocol = nil
	if cd.delay == nil {
		delay := server.ConnectionRestartDelayMin + time.Duration(cd.rng.Intn(server.ConnectionRestartDelayRangeMS))*time.Millisecond
		cd.delay = time.AfterFunc(delay, func() {
			cd.Enqueue(cd.received)
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
}

func (cc *connectionDial) connectionStateMachineComponentWitness() {}
func (cc *connectionDial) String() string                          { return "ConnectionDial" }

func (cc *connectionDial) init(conn *Connection) {
	cc.Connection = conn
}

func (cc *connectionDial) start() (bool, error) {
	err := cc.handshaker.Dial()
	if err == nil {
		cc.nextState(nil)
	} else {
		cc.logger.Log("msg", "Error when dialing.", "error", err)
		cc.nextState(&cc.connectionDelay)
	}
	return false, nil
}

// Handshake

type connectionHandshake struct {
	*Connection
	topology *configuration.Topology
}

func (cah *connectionHandshake) connectionStateMachineComponentWitness() {}
func (cah *connectionHandshake) String() string                          { return "ConnectionHandshake" }

func (cah *connectionHandshake) init(conn *Connection) {
	cah.Connection = conn
}

func (cah *connectionHandshake) start() (bool, error) {
	protocol, err := cah.handshaker.PerformHandshake(cah.topology)
	if err == nil {
		cah.protocol = protocol
		cah.nextState(nil)
		return false, nil
	} else {
		return false, err
	}
}

// Run

type connectionRun struct {
	*Connection
	protocol Protocol
}

func (cr *connectionRun) connectionStateMachineComponentWitness() {}
func (cr *connectionRun) String() string                          { return "ConnectionRun" }

func (cr *connectionRun) init(conn *Connection) {
	cr.Connection = conn
}

func (cr *connectionRun) start() (bool, error) {
	return false, cr.protocol.Run(cr.Connection)
}

func (cr *connectionRun) topologyChanged(tc *connectionMsgTopologyChanged) error {
	switch {
	case cr.protocol != nil:
		cr.topology = tc.topology
		return cr.protocol.TopologyChanged(tc)
	default:
		tc.Close()
		return nil
	}
}
