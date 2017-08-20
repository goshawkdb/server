package senders

import (
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/server/types"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
)

type OneShotSender struct {
	logger    log.Logger
	msg       []byte
	connPub   sconn.ServerConnectionPublisher
	remaining map[common.RMId]types.EmptyStruct
}

func NewOneShotSender(logger log.Logger, msg []byte, connPub sconn.ServerConnectionPublisher, recipients ...common.RMId) *OneShotSender {
	remaining := make(map[common.RMId]types.EmptyStruct, len(recipients))
	for _, rmId := range recipients {
		remaining[rmId] = types.EmptyStructVal
	}
	oss := &OneShotSender{
		logger:    logger,
		msg:       msg,
		connPub:   connPub,
		remaining: remaining,
	}
	utils.DebugLog(oss.logger, "debug", "Adding one shot sender.", "recipients", recipients)
	connPub.AddServerConnectionSubscriber(oss)
	return oss
}

func (oss *OneShotSender) ConnectedRMs(conns map[common.RMId]*sconn.ServerConnection) {
	for recipient := range oss.remaining {
		if conn, found := conns[recipient]; found {
			delete(oss.remaining, recipient)
			conn.Send(oss.msg)
		}
	}
	if len(oss.remaining) == 0 {
		utils.DebugLog(oss.logger, "debug", "Removing one shot sender.")
		oss.connPub.RemoveServerConnectionSubscriber(oss)
	}
}

func (oss *OneShotSender) ConnectionLost(common.RMId, map[common.RMId]*sconn.ServerConnection) {}

func (oss *OneShotSender) ConnectionEstablished(conn *sconn.ServerConnection, conns map[common.RMId]*sconn.ServerConnection, done func()) {
	defer done()
	if _, found := oss.remaining[conn.RMId]; found {
		delete(oss.remaining, conn.RMId)
		conn.Send(oss.msg)
		if len(oss.remaining) == 0 {
			utils.DebugLog(oss.logger, "debug", "Removing one shot sender.")
			oss.connPub.RemoveServerConnectionSubscriber(oss)
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

func (rs *RepeatingSender) ConnectedRMs(conns map[common.RMId]*sconn.ServerConnection) {
	for _, recipient := range rs.recipients {
		if conn, found := conns[recipient]; found {
			conn.Send(rs.msg)
		}
	}
}

func (rs *RepeatingSender) ConnectionLost(common.RMId, map[common.RMId]*sconn.ServerConnection) {}

func (rs *RepeatingSender) ConnectionEstablished(conn *sconn.ServerConnection, conns map[common.RMId]*sconn.ServerConnection, done func()) {
	defer done()
	for _, recipient := range rs.recipients {
		if recipient == conn.RMId {
			conn.Send(rs.msg)
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

func (ras *RepeatingAllSender) ConnectedRMs(conns map[common.RMId]*sconn.ServerConnection) {
	for _, conn := range conns {
		conn.Send(ras.msg)
	}
}

func (ras *RepeatingAllSender) ConnectionLost(common.RMId, map[common.RMId]*sconn.ServerConnection) {}

func (ras *RepeatingAllSender) ConnectionEstablished(conn *sconn.ServerConnection, conns map[common.RMId]*sconn.ServerConnection, done func()) {
	conn.Send(ras.msg)
	done()
}
