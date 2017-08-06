package network

import (
	"errors"
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	eng "goshawkdb.io/server/txnengine"
	"math/rand"
	"net"
	"time"
)

type Connection struct {
	*actor.Mailbox
	*actor.BasicServerOuter
	inner *connectionInner
}

type connectionInner struct {
	*Connection
	*actor.BasicServerInner // super-type, essentially
	connectionManager       *ConnectionManager
	shuttingDown            bool
	handshaker              Handshaker
	rng                     *rand.Rand
	previousState           connectionStateMachineComponent
	currentState            connectionStateMachineComponent
	connectionDelay
	connectionDial
	connectionHandshake
	connectionRun
}

func (conn *Connection) shutdownCompleted() {
	conn.EnqueueFuncAsync(conn.inner.msgShutdownCompleted)
}

type connectionMsgTopologyChanged struct {
	actor.MsgSyncQuery
	conn     *connectionInner
	topology *configuration.Topology
}

func (msg *connectionMsgTopologyChanged) Exec() (bool, error) {
	msg.conn.topology = msg.topology
	switch {
	case msg.conn.protocol != nil:
		return false, msg.conn.protocol.TopologyChanged(msg) // This calls msg.MustClose
	default:
		msg.MustClose()
		return false, nil
	}
}

func (conn *Connection) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	msg := &connectionMsgTopologyChanged{topology: topology, conn: conn.inner}
	msg.InitMsg(conn)
	if conn.EnqueueMsg(msg) {
		go func() {
			msg.Wait()
			done(true) // connection drop is not a problem
		}()
	} else {
		done(true) // connection drop is not a problem
	}
}

type connectionMsgStatus struct {
	*connectionInner
	sc *server.StatusConsumer
}

func (msg connectionMsgStatus) Exec() (bool, error) {
	if msg.protocol != nil {
		msg.sc.Emit(fmt.Sprintf("Connection %v", msg.protocol))
	} else if msg.handshaker != nil {
		msg.sc.Emit(fmt.Sprintf("Connection %v", msg.handshaker))
	}
	msg.sc.Join()
	return false, nil
}

func (conn *Connection) Status(sc *server.StatusConsumer) {
	conn.EnqueueMsg(connectionMsgStatus{connectionInner: conn.inner, sc: sc})
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
	c := &Connection{}

	ci := &connectionInner{
		Connection:        c,
		BasicServerInner:  actor.NewBasicServerInner(logger),
		connectionManager: cm,
		handshaker:        yesman,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	mailbox, err := actor.Spawn(ci)
	if err != nil {
		panic(err) // "impossible"
	}

	c.Mailbox = mailbox
	c.BasicServerOuter = actor.NewBasicServerOuter(mailbox)
	c.inner = ci

	return c
}

func (conn *connectionInner) Init(self *actor.Actor) (bool, error) {
	conn.connectionDelay.init(conn)
	conn.connectionDial.init(conn)
	conn.connectionHandshake.init(conn)
	conn.connectionRun.init(conn)

	conn.topology = conn.connectionManager.AddTopologySubscriber(eng.ConnectionSubscriber, conn)
	if conn.topology == nil {
		// Most likely is that the connection manager has shutdown due
		// to some other error and so the sync enqueue failed.
		return false, errors.New("No local topology, not ready for any connections.")
	}

	conn.nextState(nil)
	return conn.BasicServerInner.Init(self)
}

func (conn *connectionInner) HandleShutdown(err error) bool {
	if conn.shuttingDown {
		conn.connectionManager.RemoveTopologySubscriberAsync(eng.ConnectionSubscriber, conn)
		return conn.BasicServerInner.HandleShutdown(err)

	} else if err != nil {
		err = conn.maybeRestartConnection(err)
	}

	if err != nil {
		conn.Logger.Log("error", err)
	}
	conn.shuttingDown = true
	if conn.protocol != nil {
		conn.protocol.InternalShutdown() // this will call shutdownCompleted
	} else {
		conn.shutdownCompleted()
		if conn.handshaker != nil {
			conn.handshaker.InternalShutdown()
		}
	}
	conn.currentState = nil
	conn.protocol = nil
	conn.handshaker = nil
	return false
}

func (conn *connectionInner) HandleBeat() (terminate bool, err error) {
	for conn.previousState != conn.currentState && !terminate && err == nil {
		conn.previousState = conn.currentState
		terminate, err = conn.currentState.start()
	}
	return
}

func (conn *connectionInner) msgShutdownCompleted() (bool, error) {
	return true, nil
}

func (conn *connectionInner) maybeRestartConnection(err error) error {
	restartable := false
	if conn.protocol != nil {
		restartable = conn.protocol.Restart()
	} else if conn.handshaker != nil {
		restartable = conn.handshaker.Restart()
	}

	if restartable {
		conn.Logger.Log("msg", "Restarting.", "error", err)
		conn.nextState(&conn.connectionDelay)
		return nil
	} else {
		return err // it's fatal; HandleShutdown will shutdown Protocol and Handshaker
	}
}

// state machine

type connectionStateMachineComponent interface {
	init(*connectionInner)
	start() (bool, error)
}

func (conn *connectionInner) nextState(requestedState connectionStateMachineComponent) {
	if requestedState == nil {
		switch conn.currentState {
		case nil, &conn.connectionDelay:
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

// Delay

type connectionDelay struct {
	*connectionInner
	delay *time.Timer
}

func (cd *connectionDelay) String() string { return "ConnectionDelay" }

func (cd *connectionDelay) init(conn *connectionInner) {
	cd.connectionInner = conn
}

func (cd *connectionDelay) start() (bool, error) {
	cd.protocol = nil
	if cd.delay == nil {
		delay := server.ConnectionRestartDelayMin + time.Duration(cd.rng.Intn(server.ConnectionRestartDelayRangeMS))*time.Millisecond
		cd.delay = time.AfterFunc(delay, func() {
			cd.EnqueueMsg(cd)
		})
	}
	return false, nil
}

func (cd *connectionDelay) Exec() (bool, error) {
	if cd.currentState == cd {
		cd.delay = nil
		cd.nextState(nil)
	}
	return false, nil
}

// Dial

type connectionDial struct {
	*connectionInner
}

func (cc *connectionDial) String() string { return "ConnectionDial" }

func (cc *connectionDial) init(conn *connectionInner) {
	cc.connectionInner = conn
}

func (cc *connectionDial) start() (bool, error) {
	err := cc.handshaker.Dial()
	if err == nil {
		cc.nextState(nil)
	} else {
		cc.Logger.Log("msg", "Error when dialing.", "error", err)
		cc.nextState(&cc.connectionDelay)
	}
	return false, nil
}

// Handshake

type connectionHandshake struct {
	*connectionInner
	topology *configuration.Topology
}

func (cah *connectionHandshake) String() string { return "ConnectionHandshake" }

func (cah *connectionHandshake) init(conn *connectionInner) {
	cah.connectionInner = conn
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
	*connectionInner
	protocol Protocol
}

func (cr *connectionRun) String() string { return "ConnectionRun" }

func (cr *connectionRun) init(conn *connectionInner) {
	cr.connectionInner = conn
}

func (cr *connectionRun) start() (bool, error) {
	return false, cr.protocol.Run(cr.connectionInner)
}
