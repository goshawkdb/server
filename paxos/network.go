package paxos

import (
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
)

type ConnectionManager interface {
	AddServerConnectionObserver(sender ServerConnectionObserver)
	RemoveServerConnectionObserverSync(sender ServerConnectionObserver)
	RemoveServerConnectionObserverAsync(sender ServerConnectionObserver)
	GetClient(uint32, uint32) ClientConnection
}

type ServerConnectionObserver interface {
	ConnectedRMs(map[common.RMId]Connection)
	ConnectionLost(common.RMId, map[common.RMId]Connection)
	ConnectionEstablished(common.RMId, Connection, map[common.RMId]Connection)
}

type Connection interface {
	Host() string
	RMId() common.RMId
	BootCount() uint32
	TieBreak() uint32
	RootId() *common.VarUUId
	Send(msg []byte)
}

type ClientConnection interface {
	TopologyChange(*configuration.Topology, map[common.RMId]Connection)
	SubmissionOutcomeReceived(common.RMId, *common.TxnId, *msgs.Outcome)
}

type OneShotSender struct {
	remaining         map[common.RMId]server.EmptyStruct
	msg               []byte
	connectionManager ConnectionManager
}

func NewOneShotSender(msg []byte, cm ConnectionManager, recipients ...common.RMId) *OneShotSender {
	remaining := make(map[common.RMId]server.EmptyStruct, len(recipients))
	for _, rmId := range recipients {
		remaining[rmId] = server.EmptyStructVal
	}
	oss := &OneShotSender{
		remaining:         remaining,
		msg:               msg,
		connectionManager: cm,
	}
	server.Log(oss, "Adding one shot sender with recipients", recipients)
	cm.AddServerConnectionObserver(oss)
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
		s.connectionManager.RemoveServerConnectionObserverAsync(s)
	}
}

func (s *OneShotSender) ConnectionLost(common.RMId, map[common.RMId]Connection) {}

func (s *OneShotSender) ConnectionEstablished(rmId common.RMId, conn Connection, conns map[common.RMId]Connection) {
	if _, found := s.remaining[rmId]; found {
		delete(s.remaining, rmId)
		conn.Send(s.msg)
		if len(s.remaining) == 0 {
			server.Log(s, "Removing one shot sender")
			s.connectionManager.RemoveServerConnectionObserverAsync(s)
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
