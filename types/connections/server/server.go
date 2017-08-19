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
	ShutdownSync func()
}

func (sc *ServerConnection) Clone() *ServerConnection {
	return &ServerConnection{
		Host:         sc.Host,
		RMId:         sc.RMId,
		BootCount:    sc.BootCount,
		ClusterUUId:  sc.ClusterUUId,
		Send:         sc.Send,
		ShutdownSync: sc.ShutdownSync,
	}
}

type ServerConnectionSubscriber interface {
	ConnectedRMs(map[common.RMId]*ServerConnection)
	ConnectionLost(common.RMId, map[common.RMId]*ServerConnection)
	ConnectionEstablished(common.RMId, *ServerConnection, map[common.RMId]*ServerConnection, func())
}

type ServerConnectionPublisher interface {
	AddServerConnectionSubscriber(obs ServerConnectionSubscriber)
	RemoveServerConnectionSubscriber(obs ServerConnectionSubscriber)
}
