package paxos

import (
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/dispatcher"
	eng "goshawkdb.io/server/txnengine"
)

type Blocking bool

const (
	Async Blocking = false
	Sync  Blocking = true
)

type ConnectionManager interface {
	ServerConnectionPublisher
	eng.TopologyPublisher
	ClientEstablished(connNumber uint32, conn ClientConnection) map[common.RMId]Connection
	ClientLost(connNumber uint32, conn ClientConnection)
	GetClient(bootNumber, connNumber uint32) ClientConnection
}

type ServerConnectionPublisher interface {
	AddServerConnectionSubscriber(obs ServerConnectionSubscriber)
	RemoveServerConnectionSubscriber(obs ServerConnectionSubscriber)
}

type ServerConnectionSubscriber interface {
	ConnectedRMs(map[common.RMId]Connection)
	ConnectionLost(common.RMId, map[common.RMId]Connection)
	ConnectionEstablished(common.RMId, Connection, map[common.RMId]Connection)
}

type Connection interface {
	Host() string
	RMId() common.RMId
	BootCount() uint32
	TieBreak() uint32
	ClusterUUId() uint64
	Send(msg []byte)
}

type ClientConnection interface {
	Shutdownable
	ServerConnectionSubscriber
	SubmissionOutcomeReceived(common.RMId, *common.TxnId, *msgs.Outcome)
}

type Shutdownable interface {
	Shutdown(sync Blocking)
}

type serverConnectionPublisherProxy struct {
	exe      *dispatcher.Executor
	upstream ServerConnectionPublisher
	servers  map[common.RMId]Connection
	subs     map[ServerConnectionSubscriber]server.EmptyStruct
}

func NewServerConnectionPublisherProxy(exe *dispatcher.Executor, upstream ServerConnectionPublisher) ServerConnectionPublisher {
	pub := &serverConnectionPublisherProxy{
		exe:      exe,
		upstream: upstream,
		subs:     make(map[ServerConnectionSubscriber]server.EmptyStruct),
	}
	upstream.AddServerConnectionSubscriber(pub)
	return pub
}

func (pub *serverConnectionPublisherProxy) AddServerConnectionSubscriber(obs ServerConnectionSubscriber) {
	pub.subs[obs] = server.EmptyStructVal
	if pub.servers != nil {
		obs.ConnectedRMs(pub.servers)
	}
}

func (pub *serverConnectionPublisherProxy) RemoveServerConnectionSubscriber(obs ServerConnectionSubscriber) {
	delete(pub.subs, obs)
}

func (pub *serverConnectionPublisherProxy) ConnectedRMs(servers map[common.RMId]Connection) {
	pub.exe.Enqueue(func() {
		pub.servers = servers
		for sub := range pub.subs {
			sub.ConnectedRMs(servers)
		}
	})
}

func (pub *serverConnectionPublisherProxy) ConnectionLost(lost common.RMId, servers map[common.RMId]Connection) {
	pub.exe.Enqueue(func() {
		pub.servers = servers
		for sub := range pub.subs {
			sub.ConnectionLost(lost, servers)
		}
	})
}

func (pub *serverConnectionPublisherProxy) ConnectionEstablished(gained common.RMId, conn Connection, servers map[common.RMId]Connection) {
	pub.exe.Enqueue(func() {
		pub.servers = servers
		for sub := range pub.subs {
			sub.ConnectionEstablished(gained, conn, servers)
		}
	})
}

type OneShotSender struct {
	remaining map[common.RMId]server.EmptyStruct
	msg       []byte
	connPub   ServerConnectionPublisher
}

func NewOneShotSender(msg []byte, connPub ServerConnectionPublisher, recipients ...common.RMId) *OneShotSender {
	remaining := make(map[common.RMId]server.EmptyStruct, len(recipients))
	for _, rmId := range recipients {
		remaining[rmId] = server.EmptyStructVal
	}
	oss := &OneShotSender{
		remaining: remaining,
		msg:       msg,
		connPub:   connPub,
	}
	server.Log(oss, "Adding one shot sender with recipients", recipients)
	connPub.AddServerConnectionSubscriber(oss)
	return oss
}

func (s *OneShotSender) ConnectedRMs(conns map[common.RMId]Connection) {
	for recipient := range s.remaining {
		if conn, found := conns[recipient]; found {
			delete(s.remaining, recipient)
			conn.Send(s.msg)
		}
	}
	if len(s.remaining) == 0 {
		server.Log(s, "Removing one shot sender")
		s.connPub.RemoveServerConnectionSubscriber(s)
	}
}

func (s *OneShotSender) ConnectionLost(common.RMId, map[common.RMId]Connection) {}

func (s *OneShotSender) ConnectionEstablished(rmId common.RMId, conn Connection, conns map[common.RMId]Connection) {
	if _, found := s.remaining[rmId]; found {
		delete(s.remaining, rmId)
		conn.Send(s.msg)
		if len(s.remaining) == 0 {
			server.Log(s, "Removing one shot sender")
			s.connPub.RemoveServerConnectionSubscriber(s)
		}
	}
}

type RepeatingSender struct {
	recipients []common.RMId
	msg        []byte
}

func NewRepeatingSender(msg []byte, recipients ...common.RMId) *RepeatingSender {
	return &RepeatingSender{
		recipients: recipients,
		msg:        msg,
	}
}

func (s *RepeatingSender) ConnectedRMs(conns map[common.RMId]Connection) {
	for _, recipient := range s.recipients {
		if conn, found := conns[recipient]; found {
			conn.Send(s.msg)
		}
	}
}

func (s *RepeatingSender) ConnectionLost(common.RMId, map[common.RMId]Connection) {}

func (s *RepeatingSender) ConnectionEstablished(rmId common.RMId, conn Connection, conns map[common.RMId]Connection) {
	for _, recipient := range s.recipients {
		if recipient == rmId {
			conn.Send(s.msg)
			return
		}
	}
}

type RepeatingAllSender struct {
	msg []byte
}

func NewRepeatingAllSender(msg []byte) *RepeatingAllSender {
	return &RepeatingAllSender{
		msg: msg,
	}
}

func (s *RepeatingAllSender) ConnectedRMs(conns map[common.RMId]Connection) {
	for _, conn := range conns {
		conn.Send(s.msg)
	}
}

func (s *RepeatingAllSender) ConnectionLost(common.RMId, map[common.RMId]Connection) {}

func (s *RepeatingAllSender) ConnectionEstablished(rmId common.RMId, conn Connection, conns map[common.RMId]Connection) {
	conn.Send(s.msg)
}
