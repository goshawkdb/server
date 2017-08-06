package paxos

import (
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	eng "goshawkdb.io/server/txnengine"
)

type ConnectionManager interface {
	ServerConnectionPublisher
	eng.TopologyPublisher
	ClientEstablished(connNumber uint32, conn ClientConnection) (map[common.RMId]Connection, *ClientTxnMetrics)
	ClientLost(connNumber uint32, conn ClientConnection)
	GetClient(bootNumber, connNumber uint32) ClientConnection
}

type ClientTxnMetrics struct {
	TxnSubmit   prometheus.Counter
	TxnLatency  prometheus.Observer
	TxnResubmit prometheus.Counter
	TxnRerun    prometheus.Counter
}

type ServerConnectionPublisher interface {
	AddServerConnectionSubscriber(obs ServerConnectionSubscriber)
	RemoveServerConnectionSubscriber(obs ServerConnectionSubscriber)
}

type ServerConnectionSubscriber interface {
	ConnectedRMs(map[common.RMId]Connection)
	ConnectionLost(common.RMId, map[common.RMId]Connection)
	ConnectionEstablished(common.RMId, Connection, map[common.RMId]Connection, func())
}

type Connection interface {
	Host() string
	RMId() common.RMId
	BootCount() uint32
	ClusterUUId() uint64
	Send(msg []byte)
}

type ClientConnection interface {
	Shutdownable
	ServerConnectionSubscriber
	Status(*server.StatusConsumer)
	SubmissionOutcomeReceived(common.RMId, *eng.TxnReader, *msgs.Outcome)
}

type Shutdownable interface {
	ShutdownSync()
}

type EnqueueActor interface {
	actor.EnqueueMsgActor
	actor.EnqueueFuncActor
}

type serverConnectionPublisherProxy struct {
	logger   log.Logger
	exe      EnqueueActor
	upstream ServerConnectionPublisher
	servers  map[common.RMId]Connection
	subs     map[ServerConnectionSubscriber]server.EmptyStruct
}

func NewServerConnectionPublisherProxy(exe EnqueueActor, upstream ServerConnectionPublisher, logger log.Logger) ServerConnectionPublisher {
	pub := &serverConnectionPublisherProxy{
		logger:   logger,
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
	pub.exe.EnqueueFuncAsync(func() (bool, error) {
		pub.servers = servers
		for sub := range pub.subs {
			sub.ConnectedRMs(servers)
		}
		return false, nil
	})
}

func (pub *serverConnectionPublisherProxy) ConnectionLost(lost common.RMId, servers map[common.RMId]Connection) {
	pub.exe.EnqueueFuncAsync(func() (bool, error) {
		pub.servers = servers
		for sub := range pub.subs {
			sub.ConnectionLost(lost, servers)
		}
		return false, nil
	})
}

type scppConnectionEstablished struct {
	pub     *serverConnectionPublisherProxy
	gained  common.RMId
	conn    Connection
	servers map[common.RMId]Connection
	wg      *common.ChannelWaitGroup
}

func (ce *scppConnectionEstablished) Exec() (bool, error) {
	ce.pub.servers = ce.servers
	for sub := range ce.pub.subs {
		ce.wg.Add(1)
		sub.ConnectionEstablished(ce.gained, ce.conn, ce.servers, ce.wg.Done)
	}
	server.DebugLog(ce.pub.logger, "debug", "ServerConnEstablished Proxy expecting callbacks.")
	ce.wg.Done()
	return false, nil
}

func (pub *serverConnectionPublisherProxy) ConnectionEstablished(gained common.RMId, conn Connection, servers map[common.RMId]Connection, onDone func()) {
	ce := &scppConnectionEstablished{
		pub:     pub,
		gained:  gained,
		conn:    conn,
		servers: servers,
		wg:      common.NewChannelWaitGroup(),
	}
	// we do this because wg is edge triggered, so if pub.subs is
	// empty, we have to have something that goes from 1 to 0
	ce.wg.Add(1)
	if pub.exe.EnqueueMsg(ce) { // just because it was enqueued doesn't mean it'll be exec'd
		go func() {
			ce.wg.WaitUntilEither(ce.pub.exe.TerminatedChan())
			onDone()
		}()
	} else {
		onDone()
	}
}

type OneShotSender struct {
	logger    log.Logger
	remaining map[common.RMId]server.EmptyStruct
	msg       []byte
	connPub   ServerConnectionPublisher
}

func NewOneShotSender(logger log.Logger, msg []byte, connPub ServerConnectionPublisher, recipients ...common.RMId) *OneShotSender {
	remaining := make(map[common.RMId]server.EmptyStruct, len(recipients))
	for _, rmId := range recipients {
		remaining[rmId] = server.EmptyStructVal
	}
	oss := &OneShotSender{
		logger:    logger,
		remaining: remaining,
		msg:       msg,
		connPub:   connPub,
	}
	server.DebugLog(oss.logger, "debug", "Adding one shot sender.", "recipients", recipients)
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
		server.DebugLog(s.logger, "debug", "Removing one shot sender.")
		s.connPub.RemoveServerConnectionSubscriber(s)
	}
}

func (s *OneShotSender) ConnectionLost(common.RMId, map[common.RMId]Connection) {}

func (s *OneShotSender) ConnectionEstablished(rmId common.RMId, conn Connection, conns map[common.RMId]Connection, done func()) {
	if _, found := s.remaining[rmId]; found {
		delete(s.remaining, rmId)
		conn.Send(s.msg)
		if len(s.remaining) == 0 {
			server.DebugLog(s.logger, "debug", "Removing one shot sender.")
			s.connPub.RemoveServerConnectionSubscriber(s) // this is async
		}
	}
	done()
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

func (s *RepeatingSender) ConnectionEstablished(rmId common.RMId, conn Connection, conns map[common.RMId]Connection, done func()) {
	defer done()
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

func (s *RepeatingAllSender) ConnectionEstablished(rmId common.RMId, conn Connection, conns map[common.RMId]Connection, done func()) {
	conn.Send(s.msg)
	done()
}
