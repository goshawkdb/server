package server

import (
	"goshawkdb.io/common"
)

type ServerConnection struct {
	Host         string
	RMId         common.RMId
	BootCount    uint32
	ClusterUUId  uint64
	Send         func(msg []byte)
	Flushed      func()
	ShutdownSync func()
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
