package topology

import (
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
)

type TopologyTransmogrifier interface {
	RequestConfigurationChange(*configuration.Configuration)
	ImmigrationReceived(sender common.RMId, migration msgs.Migration)
	ImmigrationCompleteReceived(sender common.RMId, migrationComplete msgs.MigrationComplete)
}

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
