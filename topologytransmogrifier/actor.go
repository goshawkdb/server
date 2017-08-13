package topologyTransmogrifier

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"math/rand"
	"time"
)

type TopologyTransmogrifier struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	self              common.RMId
	db                *db.Databases
	connectionManager *ConnectionManager
	localConnection   *client.LocalConnection
	activeTopology    *configuration.Topology
	hostToConnection  map[string]paxos.Connection
	activeConnections map[common.RMId]paxos.Connection
	migrations        map[uint32]map[common.RMId]*int32

	currentTask Task

	listenPort        uint16
	rng               *rand.Rand
	shutdownSignaller ShutdownSignaller
	localEstablished  chan struct{}

	inner topologyTransmogrifierInner
}

type topologyTransmogrifierInner struct {
	*TopologyTransmogrifier
	*actor.BasicServerInner
	previousTask Task
}

func NewTopologyTransmogrifier(self common.RMId, db *db.Databases, cm *ConnectionManager, lc *client.LocalConnection, listenPort uint16, ss ShutdownSignaller, config *configuration.Configuration, logger log.Logger) (*TopologyTransmogrifier, <-chan struct{}) {

	localEstablished := make(chan struct{})
	tt := &TopologyTransmogrifier{
		self:              self,
		db:                db,
		connectionManager: cm,
		localConnection:   lc,
		migrations:        make(map[uint32]map[common.RMId]*int32),
		listenPort:        listenPort,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		shutdownSignaller: ss,
		localEstablished:  localEstablished,
	}
	tt.currentTask = tt.newTransmogrificationTask(&configuration.NextConfiguration{Configuration: config})
	tti := &tt.inner
	tti.TopologyTransmogrifier = tt
	tti.BasicServerInner = actor.NewBasicServerInner(log.With(logger, "subsystem", "topologyTransmogrifier"))

	_, err := actor.Spawn(tti)
	if err != nil {
		panic(err)
	}

	return tt, localEstablished
}

type topologyTransmogrifierMsgTopologyObserved struct {
	*TopologyTransmogrifier
	topology *configuration.Topology
}

func (msg topologyTransmogrifierMsgTopologyObserved) Exec() (bool, error) {
	server.DebugLog(msg.inner.Logger, "debug", "New topology observed.", "topology", msg.topology)
	return msg.setActiveTopology(msg.topology)
}

func (tt *topologyTransmogrifierInner) Init(self *actor.Actor) (bool, error) {
	terminate, err := tt.BasicServerInner.Init(self)
	if terminate || err != nil {
		return terminate, err
	}

	tt.Mailbox = self.Mailbox
	tt.BasicServerOuter = actor.NewBasicServerOuter(self.Mailbox)

	tt.connectionManager.AddServerConnectionSubscriber(tt.TopologyTransmogrifier)

	subscriberInstalled := make(chan struct{})
	tt.connectionManager.Dispatchers.VarDispatcher.ApplyToVar(func(v *eng.Var) {
		if v == nil {
			panic("Unable to create topology var!")
		}
		v.AddWriteSubscriber(configuration.VersionOne,
			&eng.VarWriteSubscriber{
				Observe: func(v *eng.Var, value []byte, refs *msgs.VarIdPos_List, txn *eng.Txn) {
					topology, err := configuration.TopologyFromCap(txn.Id, refs, value)
					if err != nil {
						panic(fmt.Errorf("Unable to deserialize new topology: %v", err))
					}
					server.DebugLog(tt.inner.Logger, "debug", "Observation enqueued.", "topology", topology)
					tt.EnqueueMsg(topologyTransmogrifierMsgTopologyObserved{
						TopologyTransmogrifier: tt.TopologyTransmogrifier,
						topology:               topology,
					})
				},
				Cancel: func(v *eng.Var) {
					panic("Subscriber on topology var has been cancelled!")
				},
			})
		close(subscriberInstalled)
	}, true, configuration.TopologyVarUUId)
	<-subscriberInstalled

	return false, nil
}

func (tt *topologyTransmogrifierInner) HandleBeat() (terminate bool, err error) {
	for tt.currentTask != nil && tt.previousTask != tt.currentTask && !terminate && err == nil {
		tt.previousTask = tt.currentTask
		terminate, err = tt.currentTask.Tick()
	}
	return
}

func (tt *topologyTransmogrifierInner) HandleShutdown(err error) bool {
	tt.connectionManager.RemoveServerConnectionSubscriber(tt.TopologyTransmogrifier)
	if tt.localEstablished != nil {
		close(tt.localEstablished)
		tt.localEstablished = nil
	}
	if err != nil {
		tt.inner.Logger.Log("msg", "Fatal error.", "error", err)
		tt.shutdownSignaller.SignalShutdown()
	}
	return tt.BasicServerInner.HandleShutdown(err)
}
