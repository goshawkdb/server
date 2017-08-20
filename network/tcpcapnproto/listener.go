package tcpcapnproto

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server/network"
	"goshawkdb.io/server/router"
	"goshawkdb.io/server/types/connectionmanager"
	"net"
)

// we are dialing out to someone else
func NewConnectionTCPTLSCapnpDialer(remoteHost string, self common.RMId, bootcount uint32, router *router.Router, cm connectionmanager.ConnectionManager, logger log.Logger) *network.Connection {
	logger = log.With(logger, "subsystem", "connection", "dir", "outgoing", "protocol", "capnp")
	phone := common.NewTCPDialer(nil, remoteHost, logger)
	yesman := NewTLSCapnpHandshaker(phone, logger, 0, self, bootcount, router, cm)
	return network.NewConnection(yesman, cm, logger)
}

// the socket is already established - we got it from the TCP listener
func (l *listenerInner) NewConnectionTCPTLSCapnpHandshaker(socket *net.TCPConn, count uint32) {
	logger := log.With(l.parentLogger, "subsystem", "connection", "dir", "incoming", "protocol", "capnp")
	phone := common.NewTCPDialer(socket, "", logger)
	yesman := NewTLSCapnpHandshaker(phone, logger, count, l.self, l.bootcount, l.router, l.connectionManager)
	network.NewConnection(yesman, l.connectionManager, logger)
}

type Listener struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	parentLogger      log.Logger
	self              common.RMId
	bootcount         uint32
	router            *router.Router
	connectionManager connectionmanager.ConnectionManager
	listenPort        uint16
	listener          *net.TCPListener

	inner listenerInner
}

type listenerInner struct {
	*Listener
	*actor.BasicServerInner
}

func NewListener(listenPort uint16, rmId common.RMId, bootcount uint32, router *router.Router, cm connectionmanager.ConnectionManager, logger log.Logger) (*Listener, error) {
	l := &Listener{
		parentLogger:      logger,
		self:              rmId,
		bootcount:         bootcount,
		router:            router,
		connectionManager: cm,
		listenPort:        listenPort,
	}

	li := &l.inner
	li.Listener = l
	li.BasicServerInner = actor.NewBasicServerInner(log.With(logger, "subsystem", "tcpListener"))

	_, err := actor.Spawn(li)
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (l *listenerInner) Init(self *actor.Actor) (bool, error) {
	terminate, err := l.BasicServerInner.Init(self)
	if terminate || err != nil {
		return terminate, err
	}

	l.Mailbox = self.Mailbox
	l.BasicServerOuter = actor.NewBasicServerOuter(self.Mailbox)

	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%v", l.listenPort))
	if err != nil {
		return false, err
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return false, err
	}

	l.listener = ln

	go l.acceptLoop()
	return false, nil
}

func (l *listenerInner) acceptLoop() {
	connectionCount := uint32(0)
	for {
		conn, err := l.listener.AcceptTCP()
		if err == nil {
			connectionCount++
			cc := connectionCount * 2
			l.EnqueueFuncAsync(func() (bool, error) {
				l.NewConnectionTCPTLSCapnpHandshaker(conn, cc)
				return false, nil
			})

		} else {
			l.EnqueueFuncAsync(func() (bool, error) { return false, err })
			return
		}
	}
}
