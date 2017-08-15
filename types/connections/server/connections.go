package server

import (
	"goshawkdb.io/common"
)

type ServerConnection interface {
	Host() string
	RMId() common.RMId
	BootCount() uint32
	ClusterUUId() uint64
	Send(msg []byte)
}

type ServerConnectionSubscriber interface {
	ConnectedRMs(map[common.RMId]ServerConnection)
	ConnectionLost(common.RMId, map[common.RMId]ServerConnection)
	ConnectionEstablished(common.RMId, ServerConnection, map[common.RMId]ServerConnection, func())
}

type ServerConnectionPublisher interface {
	AddServerConnectionSubscriber(obs ServerConnectionSubscriber)
	RemoveServerConnectionSubscriber(obs ServerConnectionSubscriber)
}
