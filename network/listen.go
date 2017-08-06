package network

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common/actor"
	"net"
)

type Listener struct {
	*actor.BasicServerOuter
}

type listenerInner struct {
	*Listener
	*actor.Mailbox
	*actor.BasicServerInner
	parentLogger      log.Logger
	connectionManager *ConnectionManager
	listenPort        uint16
	listener          *net.TCPListener
}

func NewListener(listenPort uint16, cm *ConnectionManager, logger log.Logger) (*Listener, error) {
	l := &Listener{}

	li := &listenerInner{
		Listener:          l,
		BasicServerInner:  actor.NewBasicServerInner(log.With(logger, "subsystem", "tcpListener")),
		parentLogger:      logger,
		connectionManager: cm,
		listenPort:        listenPort,
	}

	mailbox, err := actor.Spawn(li)
	if err != nil {
		return nil, err
	}

	l.BasicServerOuter = actor.NewBasicServerOuter(mailbox)

	return l, nil
}

func (l *listenerInner) Init(self *actor.Actor) (bool, error) {
	l.Mailbox = self.Mailbox

	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%v", l.listenPort))
	if err != nil {
		return false, err
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return false, err
	}

	l.listener = ln

	l.BasicServerInner.Init(self)
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
