package topologytransmogrifier

import (
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/localconnection"
	"goshawkdb.io/server/router"
	"goshawkdb.io/server/types/connectionmanager"
	sconn "goshawkdb.io/server/types/connections/server"
	"math/rand"
	"time"
)

type TopologyTransmogrifier struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	self              common.RMId
	db                *db.Databases
	router            *router.Router
	connectionManager connectionmanager.ConnectionManager
	localConnection   *localconnection.LocalConnection
	activeTopology    *configuration.Topology
	hostToConnection  map[string]*sconn.ServerConnection
	activeConnections map[common.RMId]*sconn.ServerConnection
	migrations        map[uint32]map[common.RMId]*int32

	subscriber  *subscriber
	currentTask Task

	listenPort        uint16
	rng               *rand.Rand
	shutdownSignaller actor.ShutdownableActor
	localEstablished  chan struct{}

	inner topologyTransmogrifierInner
}

type topologyTransmogrifierInner struct {
	*TopologyTransmogrifier
	*actor.BasicServerInner
	previousTask Task
}

func NewTopologyTransmogrifier(self common.RMId, db *db.Databases, router *router.Router, cm connectionmanager.ConnectionManager, lc *localconnection.LocalConnection, listenPort uint16, ss actor.ShutdownableActor, config *configuration.Configuration, logger log.Logger) (*TopologyTransmogrifier, <-chan struct{}) {

	localEstablished := make(chan struct{})
	tt := &TopologyTransmogrifier{
		self:              self,
		db:                db,
		router:            router,
		connectionManager: cm,
		localConnection:   lc,
		migrations:        make(map[uint32]map[common.RMId]*int32),
		listenPort:        listenPort,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		shutdownSignaller: ss,
		localEstablished:  localEstablished,
	}
	tt.subscriber = &subscriber{
		TopologyTransmogrifier: tt,
	}
	router.Transmogrifier = tt
	tt.currentTask = tt.newTransmogrificationTask(config)
	tti := &tt.inner
	tti.TopologyTransmogrifier = tt
	tti.BasicServerInner = actor.NewBasicServerInner(log.With(logger, "subsystem", "topologyTransmogrifier"))

	_, err := actor.Spawn(tti)
	if err != nil {
		panic(err)
	}

	return tt, localEstablished
}

func (tt *topologyTransmogrifierInner) Init(self *actor.Actor) (bool, error) {
	terminate, err := tt.BasicServerInner.Init(self)
	if terminate || err != nil {
		return terminate, err
	}

	tt.Mailbox = self.Mailbox
	tt.BasicServerOuter = actor.NewBasicServerOuter(self.Mailbox)

	tt.connectionManager.AddServerConnectionSubscriber(tt.TopologyTransmogrifier)

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
		go tt.shutdownSignaller.ShutdownSync()
	}
	return tt.BasicServerInner.HandleShutdown(err)
}
