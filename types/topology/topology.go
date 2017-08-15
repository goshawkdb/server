package topology

import (
	"goshawkdb.io/server/configuration"
)

type TopologyPublisher interface {
	AddTopologySubscriber(TopologyChangeSubscriberType, TopologySubscriber) *configuration.Topology
	RemoveTopologySubscriberAsync(TopologyChangeSubscriberType, TopologySubscriber)
}

type TopologySubscriber interface {
	TopologyChanged(*configuration.Topology, func(bool))
}

type TopologyChangeSubscriberType uint8

const (
	VarSubscriber                     TopologyChangeSubscriberType = iota
	ProposerSubscriber                TopologyChangeSubscriberType = iota
	AcceptorSubscriber                TopologyChangeSubscriberType = iota
	ConnectionSubscriber              TopologyChangeSubscriberType = iota
	ConnectionManagerSubscriber       TopologyChangeSubscriberType = iota
	EmigratorSubscriber               TopologyChangeSubscriberType = iota
	MiscSubscriber                    TopologyChangeSubscriberType = iota
	TopologyChangeSubscriberTypeLimit int                          = iota
)
