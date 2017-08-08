package network

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common/actor"
	"net"
)

type Listener struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	parentLogger      log.Logger
	connectionManager *ConnectionManager
	listenPort        uint16
	listener          *net.TCPListener

	inner listenerInner
}

type listenerInner struct {
	*Listener
	*actor.BasicServerInner
}

func NewListener(listenPort uint16, cm *ConnectionManager, logger log.Logger) (*Listener, error) {
	l := &Listener{
		parentLogger:      logger,
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
				NewConnectionTCPTLSCapnpHandshaker(conn, l.connectionManager, cc, l.parentLogger)
				return false, nil
			})

		} else {
			l.EnqueueFuncAsync(func() (bool, error) { return false, err })
			return
		}
	}
}
