package server

import (
	"goshawkdb.io/common"
	"goshawkdb.io/server/utils/status"
)

type ServerConnection struct {
	Host        string
	RMId        common.RMId
	BootCount   uint32
	ClusterUUId uint64
	Sender
	Flushed      func()
	ShutdownSync func()
	Status       func(*status.StatusConsumer)
}

type Sender interface {
	Send(msg []byte)
}

type ServerConnectionSubscriber interface {
	ConnectedRMs(map[common.RMId]*ServerConnection)
	ConnectionLost(common.RMId, map[common.RMId]*ServerConnection)
	ConnectionEstablished(*ServerConnection, map[common.RMId]*ServerConnection, func())
}

type ServerConnectionPublisher interface {
	AddServerConnectionSubscriber(obs ServerConnectionSubscriber)
	RemoveServerConnectionSubscriber(obs ServerConnectionSubscriber)
}
