package network

import (
	"bytes"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"math/rand"
	"sync/atomic"
	"time"
)

type TopologyTransmogrifier struct {
	*actor.Mailbox
	*actor.BasicServerOuter

	db                *db.Databases
	connectionManager *ConnectionManager
	localConnection   *client.LocalConnection
	active            *configuration.Topology
	hostToConnection  map[string]paxos.Connection
	activeConnections map[common.RMId]paxos.Connection
	migrations        map[uint32]map[common.RMId]*int32

	currentTask  topologyTask
	previousTask topologyTask

	listenPort        uint16
	rng               *rand.Rand
	shutdownSignaller ShutdownSignaller
	localEstablished  chan struct{}

	inner topologyTransmogrifierInner
}

type topologyTransmogrifierInner struct {
	*TopologyTransmogrifier
	*actor.BasicServerInner
}

func NewTopologyTransmogrifier(db *db.Databases, cm *ConnectionManager, lc *client.LocalConnection, listenPort uint16, ss ShutdownSignaller, config *configuration.Configuration, logger log.Logger) (*TopologyTransmogrifier, <-chan struct{}) {

	localEstablished := make(chan struct{})
	tt := &TopologyTransmogrifier{
		db:                db,
		connectionManager: cm,
		localConnection:   lc,
		migrations:        make(map[uint32]map[common.RMId]*int32),
		listenPort:        listenPort,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		shutdownSignaller: ss,
		localEstablished:  localEstablished,
	}
	tt.currentTask = &targetConfig{
		TopologyTransmogrifier: tt,
		config:                 &configuration.NextConfiguration{Configuration: config},
	}
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

type topologyTransmogrifierMsgRequestConfigChange struct {
	*TopologyTransmogrifier
	config *configuration.Configuration
}

func (msg *topologyTransmogrifierMsgRequestConfigChange) Exec() (bool, error) {
	server.DebugLog(msg.inner.Logger, "debug", "Topology change request.", "config", msg.config)
	nonFatalErr := msg.selectGoal(&configuration.NextConfiguration{Configuration: msg.config})
	// because this is definitely not the cmd-line config, an error here is non-fatal
	if nonFatalErr != nil {
		msg.inner.Logger.Log("msg", "Ignoring requested configuration change.", "error", nonFatalErr)
	}
	return false, nil
}

func (tt *TopologyTransmogrifier) RequestConfigurationChange(config *configuration.Configuration) {
	tt.EnqueueMsg(topologyTransmogrifierMsgRequestConfigChange{TopologyTransmogrifier: tt, config: config})
}

type topologyTransmogrifierMsgMigrationReceived struct {
	*TopologyTransmogrifier
	migration *msgs.Migration
	sender    common.RMId
}

func (msg topologyTransmogrifierMsgMigrationReceived) Exec() (bool, error) {
	version := msg.migration.Version()
	if version <= msg.active.Version {
		// This topology change has been completed. Ignore this migration.
		return false, nil
	} else if next := msg.active.NextConfiguration; next != nil {
		if version < next.Version {
			// Whatever change that was for, it isn't happening any
			// more. Ignore.
			return false, nil
		} else if _, found := next.Pending[msg.connectionManager.RMId]; version == next.Version && !found {
			// Migration is for the current topology change, but we've
			// declared ourselves done, so Ignore.
			return false, nil
		}
	}

	senders, found := msg.migrations[version]
	if !found {
		senders = make(map[common.RMId]*int32)
		msg.migrations[version] = senders
	}
	sender := msg.sender
	inprogressPtr, found := senders[sender]
	if found {
		atomic.AddInt32(inprogressPtr, 1)
	} else {
		inprogress := int32(2)
		inprogressPtr = &inprogress
		senders[sender] = inprogressPtr
	}
	txnCount := int32(msg.migration.Elems().Len())
	lsc := msg.newTxnLSC(txnCount, inprogressPtr)
	msg.connectionManager.Dispatchers.ProposerDispatcher.ImmigrationReceived(msg.migration, lsc)
	return false, nil
}

func (tt *TopologyTransmogrifier) MigrationReceived(sender common.RMId, migration *msgs.Migration) {
	tt.EnqueueMsg(topologyTransmogrifierMsgMigrationReceived{
		TopologyTransmogrifier: tt,
		migration:              migration,
		sender:                 sender,
	})
}

type topologyTransmogrifierMsgMigrationComplete struct {
	*TopologyTransmogrifier
	complete *msgs.MigrationComplete
	sender   common.RMId
}

func (msg topologyTransmogrifierMsgMigrationComplete) Exec() (bool, error) {
	version := msg.complete.Version()
	sender := msg.sender
	server.DebugLog(msg.inner.Logger, "debug", "MCR received.", "sender", sender, "version", version)
	senders, found := msg.migrations[version]
	if !found {
		if version > msg.active.Version {
			senders = make(map[common.RMId]*int32)
			msg.migrations[version] = senders
		} else {
			return false, nil
		}
	}
	inprogress := int32(0)
	if inprogressPtr, found := senders[sender]; found {
		inprogress = atomic.AddInt32(inprogressPtr, -1)
	} else {
		inprogressPtr = &inprogress
		senders[sender] = inprogressPtr
	}
	// race here?!
	if inprogress == 0 {
		return msg.maybeTick()
	}
	return false, nil
}

func (tt *TopologyTransmogrifier) MigrationCompleteReceived(sender common.RMId, migrationComplete *msgs.MigrationComplete) {
	tt.EnqueueMsg(topologyTransmogrifierMsgMigrationComplete{
		TopologyTransmogrifier: tt,
		complete:               migrationComplete,
		sender:                 sender,
	})
}

type topologyTransmogrifierMsgSetActiveConnections struct {
	actor.MsgSyncQuery
	*TopologyTransmogrifier
	servers map[common.RMId]paxos.Connection
}

func (msg *topologyTransmogrifierMsgSetActiveConnections) Exec() (bool, error) {
	defer msg.MustClose()
	msg.activeConnections = msg.servers

	msg.hostToConnection = make(map[string]paxos.Connection, len(msg.activeConnections))
	for _, cd := range msg.activeConnections {
		msg.hostToConnection[cd.Host()] = cd
	}

	return msg.maybeTick()
}

func (tt *TopologyTransmogrifier) ConnectedRMs(conns map[common.RMId]paxos.Connection) {
	msg := &topologyTransmogrifierMsgSetActiveConnections{TopologyTransmogrifier: tt, servers: conns}
	msg.InitMsg(tt)
	tt.EnqueueMsg(msg)
}

func (tt *TopologyTransmogrifier) ConnectionLost(rmId common.RMId, conns map[common.RMId]paxos.Connection) {
	msg := &topologyTransmogrifierMsgSetActiveConnections{TopologyTransmogrifier: tt, servers: conns}
	msg.InitMsg(tt)
	tt.EnqueueMsg(msg)
}

func (tt *TopologyTransmogrifier) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, conns map[common.RMId]paxos.Connection, done func()) {
	msg := &topologyTransmogrifierMsgSetActiveConnections{TopologyTransmogrifier: tt, servers: conns}
	msg.InitMsg(tt)
	if tt.EnqueueMsg(msg) {
		go func() {
			msg.Wait()
			done()
		}()
	} else {
		done()
	}
}

type topologyTransmogrifierMsgTopologyObserved struct {
	*TopologyTransmogrifier
	topology *configuration.Topology
}

func (msg topologyTransmogrifierMsgTopologyObserved) Exec() (bool, error) {
	server.DebugLog(msg.inner.Logger, "debug", "New topology observed.", "topology", msg.topology)
	return msg.setActive(msg.topology)
}

func (tt *TopologyTransmogrifier) maybeTick() (bool, error) {
	if tt.currentTask == nil {
		return false, nil
	} else {
		return tt.currentTask.Tick()
	}
}

func (tt *TopologyTransmogrifier) setActive(topology *configuration.Topology) (bool, error) {
	server.DebugLog(tt.inner.Logger, "debug", "SetActive.", "topology", topology)
	if tt.active != nil {
		switch {
		case tt.active.ClusterId != topology.ClusterId && tt.active.ClusterId != "":
			return false, fmt.Errorf("Topology: Fatal: config with ClusterId change from '%s' to '%s'.",
				tt.active.ClusterId, topology.ClusterId)

		case topology.Version < tt.active.Version:
			tt.inner.Logger.Log("msg", "Ignoring config with version less than active version.",
				"goalVersion", topology.Version, "activeVersion", tt.active.Version)
			return false, nil

		case tt.active.Configuration.Equal(topology.Configuration):
			// silently ignore it
			return false, nil
		}
	}

	if _, found := topology.RMsRemoved[tt.connectionManager.RMId]; found {
		return false, errors.New("We have been removed from the cluster. Shutting down.")
	}
	tt.active = topology

	if tt.currentTask != nil {
		if terminate, err := tt.currentTask.Tick(); terminate || err != nil {
			return terminate, err
		}
	}

	if tt.currentTask == nil {
		if next := topology.NextConfiguration; next == nil {
			localHost, remoteHosts, err := tt.active.LocalRemoteHosts(tt.listenPort)
			if err != nil {
				return false, err
			}
			tt.installTopology(topology, nil, localHost, remoteHosts)
			tt.inner.Logger.Log("msg", "Topology change complete.", "localhost", localHost, "RMId", tt.connectionManager.RMId)

			future := tt.db.WithEnv(func(env *mdb.Env) (interface{}, error) {
				return nil, env.SetFlags(mdb.NOSYNC, topology.NoSync)
			})
			for version := range tt.migrations {
				if version <= topology.Version {
					delete(tt.migrations, version)
				}
			}

			_, err = future.ResultError()
			if err != nil {
				return false, err
			}

		} else {
			return false, tt.selectGoal(next)
		}
	}
	return false, nil
}

func (tt *TopologyTransmogrifier) installTopology(topology *configuration.Topology, callbacks map[eng.TopologyChangeSubscriberType]func() (bool, error), localHost string, remoteHosts []string) {
	server.DebugLog(tt.inner.Logger, "debug", "Installing topology to connection manager, et al.", "topology", topology)
	if tt.localEstablished != nil {
		if callbacks == nil {
			callbacks = make(map[eng.TopologyChangeSubscriberType]func() (bool, error))
		}
		origFun := callbacks[eng.ConnectionManagerSubscriber]
		callbacks[eng.ConnectionManagerSubscriber] = func() (bool, error) {
			if tt.localEstablished != nil {
				close(tt.localEstablished)
				tt.localEstablished = nil
			}
			if origFun == nil {
				return false, nil
			} else {
				return origFun()
			}
		}
	}
	wrapped := make(map[eng.TopologyChangeSubscriberType]func(), len(callbacks))
	for subType, cb := range callbacks {
		cbCopy := cb
		wrapped[subType] = func() { tt.EnqueueFuncAsync(cbCopy) }
	}
	tt.connectionManager.SetTopology(topology, wrapped, localHost, remoteHosts)
}

func (tt *TopologyTransmogrifier) selectGoal(goal *configuration.NextConfiguration) error {
	if tt.active != nil {
		activeClusterUUId, goalClusterUUId := tt.active.ClusterUUId, goal.ClusterUUId
		switch {
		case goal.Version == 0:
			return nil // done installing version0.

		case goal.ClusterId != tt.active.ClusterId && tt.active.ClusterId != "":
			return fmt.Errorf("Illegal config change: ClusterId should be '%s' instead of '%s'.",
				tt.active.ClusterId, goal.ClusterId)

		case goalClusterUUId != 0 && activeClusterUUId != 0 && goalClusterUUId != activeClusterUUId:
			return fmt.Errorf("Illegal config change: ClusterUUId should be '%v' instead of '%v'.",
				activeClusterUUId, goalClusterUUId)

		case goal.MaxRMCount != tt.active.MaxRMCount && tt.active.Version != 0:
			return fmt.Errorf("Illegal config change: Currently changes to MaxRMCount are not supported, sorry.")

		case goal.Version < tt.active.Version:
			return fmt.Errorf("Illegal config change: Ignoring config with version %v as newer version already active (%v).",
				goal.Version, tt.active.Version)

		case goal.Configuration.EqualExternally(tt.active.Configuration):
			tt.inner.Logger.Log("msg", "Config transition completed.", "activeVersion", goal.Version)
			return nil

		case goal.Version == tt.active.Version:
			return fmt.Errorf("Illegal config change: Config has changed but Version has not been increased (%v). Ignoring.", goal.Version)
		}
	}

	if tt.currentTask != nil {
		existingGoal := tt.currentTask.Goal()
		switch {
		case goal.ClusterId != existingGoal.ClusterId:
			return fmt.Errorf("Illegal config change: ClusterId should be '%s' instead of '%s'.",
				existingGoal.ClusterId, goal.ClusterId)

		case goal.Version < existingGoal.Version:
			return fmt.Errorf("Illegal config change: Ignoring config with version %v as newer version already targetted (%v).",
				goal.Version, existingGoal.Version)

		case goal.Configuration.EqualExternally(existingGoal.Configuration):
			tt.inner.Logger.Log("msg", "Config transition already in progress.", "goalVersion", goal.Version)
			return nil

		case goal.Version == existingGoal.Version:
			return fmt.Errorf("Illegal config change: Config has changed but Version has not been increased (%v). Ignoring.", goal.Version)

		default:
			server.DebugLog(tt.inner.Logger, "debug", "Abandoning old task.")
			tt.currentTask.Abandon()
			tt.currentTask = nil
		}
	}

	if tt.currentTask == nil {
		server.DebugLog(tt.inner.Logger, "debug", "Creating new task.")
		tt.currentTask = &targetConfig{
			TopologyTransmogrifier: tt,
			config:                 goal,
		}
	}
	return nil
}

func (tt *TopologyTransmogrifier) enqueueTick(task topologyTask, tc *targetConfig) {
	if !tc.tickEnqueued {
		tc.tickEnqueued = true
		tc.createOrAdvanceBackoff()
		tc.backoff.After(func() {
			tt.EnqueueFuncAsync(func() (bool, error) {
				tc.tickEnqueued = false
				if tt.currentTask == task {
					return tt.currentTask.Tick()
				}
				return false, nil
			})
		})
	}
}

func (tt *TopologyTransmogrifier) maybeTick2(task topologyTask, tc *targetConfig) func() bool {
	var i uint32 = 0
	closer := func() bool {
		return atomic.CompareAndSwapUint32(&i, 0, 1)
	}
	time.AfterFunc(2*time.Second, func() {
		if !closer() {
			return
		}
		tt.EnqueueFuncAsync(func() (bool, error) {
			tt.enqueueTick(task, tc)
			return false, nil
		})
	})
	return closer
}

func (tt *TopologyTransmogrifier) newTxnLSC(txnCount int32, inprogressPtr *int32) eng.TxnLocalStateChange {
	return &migrationTxnLocalStateChange{
		TopologyTransmogrifier: tt,
		pendingLocallyComplete: txnCount,
		inprogressPtr:          inprogressPtr,
	}
}

type migrationTxnLocalStateChange struct {
	*TopologyTransmogrifier
	pendingLocallyComplete int32
	inprogressPtr          *int32
}

func (mtlsc *migrationTxnLocalStateChange) TxnBallotsComplete(...*eng.Ballot) {
	panic("TxnBallotsComplete called on migrating txn.")
}

// Careful: we're in the proposer dispatcher go routine here!
func (mtlsc *migrationTxnLocalStateChange) TxnLocallyComplete(txn *eng.Txn) {
	txn.CompletionReceived()
	if atomic.AddInt32(&mtlsc.pendingLocallyComplete, -1) == 0 &&
		atomic.AddInt32(mtlsc.inprogressPtr, -1) == 0 {
		mtlsc.EnqueueFuncAsync(func() (bool, error) {
			if mtlsc.currentTask != nil {
				return mtlsc.currentTask.Tick()
			}
			return false, nil
		})
	}
}

func (mtlsc *migrationTxnLocalStateChange) TxnFinished(*eng.Txn) {}

// topologyTask

type topologyTask interface {
	Tick() (bool, error)
	Abandon()
	Goal() *configuration.NextConfiguration
}

// targetConfig

type targetConfig struct {
	*TopologyTransmogrifier
	config       *configuration.NextConfiguration
	sender       paxos.ServerConnectionSubscriber
	backoff      *server.BinaryBackoffEngine
	tickEnqueued bool
}

func (task *targetConfig) Tick() (bool, error) {
	task.backoff = nil
	task.tickEnqueued = false

	switch {
	case task.active == nil:
		task.inner.Logger.Log("msg", "Ensuring local topology.")
		task.currentTask = &ensureLocalTopology{task}

	case task.active.ClusterId == "":
		task.inner.Logger.Log("msg", "Attempting to join cluster.", "configuration", task.config)
		task.currentTask = &joinCluster{targetConfig: task}

	case task.active.NextConfiguration == nil || task.active.NextConfiguration.Version < task.config.Version:
		task.inner.Logger.Log("msg", "Attempting to install topology change target.", "configuration", task.config)
		task.currentTask = &installTargetOld{targetConfig: task}

	case task.active.NextConfiguration != nil && task.active.NextConfiguration.Version == task.config.Version:
		if !task.active.NextConfiguration.InstalledOnNew {
			task.inner.Logger.Log("msg", "Attempting to install topology change to new cluster.", "configuration", task.config)
			task.currentTask = &installTargetNew{targetConfig: task}

		} else if !task.active.NextConfiguration.QuietRMIds[task.connectionManager.RMId] {
			task.inner.Logger.Log("msg", "Waiting for quiet.", "configuration", task.config)
			task.currentTask = &quiet{targetConfig: task}

		} else if len(task.active.NextConfiguration.Pending) > 0 {
			task.inner.Logger.Log("msg", "Attempting to perform object migration for topology target.", "configuration", task.config)
			task.currentTask = &migrate{targetConfig: task}

		} else {
			task.inner.Logger.Log("msg", "Object migration completed, switching to new topology.", "configuration", task.config)
			task.currentTask = &installCompletion{targetConfig: task}
		}

	default:
		return false, fmt.Errorf("Topology: Confused about what to do. Active topology is: %v; goal is %v",
			task.active, task.config)
	}
	return false, nil
}

func (task *targetConfig) shareGoalWithAll() {
	if task.sender != nil {
		return
	}
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	msg.SetTopologyChangeRequest(task.config.AddToSegAutoRoot(seg))
	task.sender = paxos.NewRepeatingAllSender(common.SegToBytes(seg))
	task.connectionManager.AddServerConnectionSubscriber(task.sender)
}

func (task *targetConfig) ensureRemoveTaskSender() {
	if task.sender != nil {
		task.connectionManager.RemoveServerConnectionSubscriber(task.sender)
		task.sender = nil
	}
}

func (task *targetConfig) Abandon()                               { task.ensureRemoveTaskSender() }
func (task *targetConfig) Goal() *configuration.NextConfiguration { return task.config }

func (task *targetConfig) fatal(err error) (bool, error) {
	task.ensureRemoveTaskSender()
	task.currentTask = nil
	task.inner.Logger.Log("msg", "Fatal error.", "error", err)
	return false, err
}

func (task *targetConfig) error(err error) (bool, error) {
	task.ensureRemoveTaskSender()
	task.currentTask = nil
	task.inner.Logger.Log("msg", "Non-fatal error.", "error", err)
	return false, nil
}

func (task *targetConfig) completed() (bool, error) {
	task.ensureRemoveTaskSender()
	task.inner.Logger.Log("msg", "Task completed.")
	task.currentTask = nil
	return false, nil
}

// NB filters out empty RMIds so no need to pre-filter.
func (task *targetConfig) formActivePassive(activeCandidates, extraPassives common.RMIds) (active, passive common.RMIds) {
	active, passive = []common.RMId{}, []common.RMId{}
	for _, rmId := range activeCandidates {
		if rmId == common.RMIdEmpty {
			continue
		} else if _, found := task.activeConnections[rmId]; found {
			active = append(active, rmId)
		} else {
			passive = append(passive, rmId)
		}
	}

	if len(active) <= len(passive) {
		task.inner.Logger.Log("msg", "Can not make progress at this time due to too many failures.",
			"failures", fmt.Sprint(passive))
		return nil, nil
	}
	// Be careful with this maths. The topology object is on every
	// node, so we must use a majority of nodes. So if we have 6 nodes,
	// then we must use 4 as actives. So we're essentially treating
	// this as if it's a cluster of 7 with one failure.
	fInc := ((len(active) + len(passive)) >> 1) + 1
	active, passive = active[:fInc], append(active[fInc:], passive...)
	passive = append(passive, extraPassives...)
	return active, passive
}

func (task *targetConfig) verifyClusterUUIds(clusterUUId uint64, remoteHosts []string) (bool, error) {
	for _, host := range remoteHosts {
		if cd, found := task.hostToConnection[host]; found {
			switch remoteClusterUUId := cd.ClusterUUId(); {
			case remoteClusterUUId == 0:
				// they're joining
			case clusterUUId == remoteClusterUUId:
				// all good
			default:
				return false, errors.New("Attempt made to merge different logical clusters together, which is illegal. Aborting topology change.")
			}
		} else {
			return false, nil
		}
	}
	return true, nil
}

func (task *targetConfig) firstLocalHost(config *configuration.Configuration) (localHost string, err error) {
	for config != nil {
		localHost, _, err = config.LocalRemoteHosts(task.listenPort)
		if err == nil {
			return localHost, err
		}
		config = config.NextConfiguration.Configuration
	}
	return "", err
}

func (task *targetConfig) allHostsBarLocalHost(localHost string, next *configuration.NextConfiguration) []string {
	remoteHosts := make([]string, len(next.AllHosts))
	copy(remoteHosts, next.AllHosts)
	for idx, host := range remoteHosts {
		if host == localHost {
			remoteHosts = append(remoteHosts[:idx], remoteHosts[idx+1:]...)
			break
		}
	}
	return remoteHosts
}

func (task *targetConfig) isInRMs(rmIds common.RMIds) bool {
	for _, rmId := range rmIds {
		if rmId == task.connectionManager.RMId {
			return true
		}
	}
	return false
}

func (task *targetConfig) createOrAdvanceBackoff() {
	if task.backoff == nil {
		task.backoff = server.NewBinaryBackoffEngine(task.rng, server.SubmissionMinSubmitDelay, time.Duration(len(task.config.Hosts))*server.SubmissionMaxSubmitDelay)
	} else {
		task.backoff.Advance()
	}
}

// ensureLocalTopology

type ensureLocalTopology struct {
	*targetConfig
}

func (task *ensureLocalTopology) Tick() (bool, error) {
	if task.active != nil {
		// The fact we're here means we're done - there is a topology
		// discovered one way or another.
		if terminate, err := task.completed(); terminate || err != nil {
			return terminate, err
		}
		if task.config.Configuration == nil {
			// There was no config supplied on the command line, so just
			// pop what we've read in here.
			task.config.Configuration = task.active.Configuration
		}
		// However, just because we have a local config doesn't mean it
		// actually satisfies the goal, so we now need to reevaluate our
		// goal versus our loaded config.
		return false, task.selectGoal(task.config)
	}

	if _, found := task.activeConnections[task.connectionManager.RMId]; !found {
		return false, nil
	}

	topology, err := task.getTopologyFromLocalDatabase()
	if err != nil {
		return task.fatal(err)
	}

	if topology == nil && (task.config == nil || task.config.Configuration == nil || task.config.ClusterId == "") {
		return task.fatal(errors.New("No configuration supplied and no configuration found in local store. Cannot continue."))

	} else if topology == nil {
		_, err = task.createTopologyZero(task.config)
		if err != nil {
			return task.fatal(err)
		}
		// if err == nil, the create succeeded, so wait for observation
		return false, nil
	} else {
		// It's already on disk, we're not going to see it through the subscriber.
		return task.setActive(topology)
	}
}

// joinCluster

type joinCluster struct {
	*targetConfig
}

func (task *joinCluster) Tick() (bool, error) {
	if !(task.active.ClusterId == "") {
		if terminate, err := task.completed(); terminate || err != nil {
			return terminate, err
		}
		// Exactly the same logic as in ensureLocalTopology: the active
		// probably doesn't have a Next set; even if it does, it may
		// have no relationship to task.config.
		return false, task.selectGoal(task.config)
	}

	localHost, remoteHosts, err := task.config.LocalRemoteHosts(task.listenPort)
	if err != nil {
		// For joining, it's fatal if we can't find ourself in the
		// target.
		return task.fatal(err)
	}

	// Set up the ClusterId so that we can actually create some connections.
	active := task.active.Clone()
	active.ClusterId = task.config.ClusterId

	// Must install to connectionManager before launching any connections.
	// We may not have the youngest topology and there could be other
	// hosts who have connected to us who are trying to send us a more
	// up to date topology. So we shouldn't kill off those connections.
	task.installTopology(active, nil, localHost, remoteHosts)

	// It's possible that different members of our goal are trying to
	// achieve different goals, so in all cases, we should share our
	// goal with them. This is essential if it turns out that we're
	// trying to join into an existing cluster - we can't possibly know
	// that's what's happening at this stage.
	task.shareGoalWithAll()

	rmIds := make([]common.RMId, 0, len(task.config.Hosts))
	clusterUUId := uint64(0)
	for _, host := range task.config.Hosts {
		cd, found := task.hostToConnection[host]
		if !found {
			// We can only continue at this point if we really are
			// connected to everyone mentioned in the config.
			return false, nil
		}
		rmIds = append(rmIds, cd.RMId())
		switch theirClusterUUId := cd.ClusterUUId(); {
		case theirClusterUUId == 0:
			// they're joining too
		case clusterUUId == 0:
			clusterUUId = theirClusterUUId
		case clusterUUId == theirClusterUUId:
			// all good
		default:
			return task.fatal(
				errors.New("Attempt made to merge different logical clusters together, which is illegal. Aborting."))
		}
	}

	if allJoining := clusterUUId == 0; allJoining {
		// Note that the order of RMIds here matches the order of hosts.
		return task.allJoining(rmIds)

	} else {
		// If we're not allJoining then we need the previous config
		// because we need to make sure that everyone in the old config
		// learns of the change. The shareGoalWithAll() call above will
		// ensure this happens.

		task.inner.Logger.Log("msg", "Requesting help from existing cluster members for topology change.")
		return false, nil
	}
}

func (task *joinCluster) allJoining(allRMIds common.RMIds) (bool, error) {
	// NB: active never gets installed to the DB itself.
	config := task.config
	config1 := configuration.BlankConfiguration()
	config1.ClusterId = config.ClusterId
	config1.Hosts = config.Hosts
	config1.F = config.F
	config1.MaxRMCount = config.MaxRMCount
	config1.RMs = allRMIds

	active := task.active.Clone()
	active.SetConfiguration(config1)

	return task.setActive(active)
}

// installTargetOld
// Purpose is to do a txn using the current topology in which we set
// topology.Next to be the target topology. We calculate and store the
// migration strategy at this point too.

type installTargetOld struct {
	*targetConfig
}

func (task *installTargetOld) Tick() (bool, error) {
	if next := task.active.NextConfiguration; !(next == nil || next.Version < task.config.Version) {
		return task.completed()
	}

	if !task.isInRMs(task.active.RMs) {
		task.shareGoalWithAll()
		task.inner.Logger.Log("msg", "Awaiting existing cluster members.")
		// this step must be performed by the existing RMs
		return false, nil
	}
	// If we're in the old config, do not share with others just yet
	// because we may well have more information (new connections) than
	// the others so they might calculate different targets and then
	// we'd be racing.

	targetTopology, rootsRequired, terminate, err := task.calculateTargetTopology()
	if terminate || err != nil || targetTopology == nil {
		return terminate, err
	}

	// Here, we just want to use the RMs in the old topology only.
	// And add on all new (if there are any) as passives
	active, passive := task.formActivePassive(task.active.RMs, targetTopology.NextConfiguration.NewRMIds)
	if active == nil {
		return false, nil
	}

	task.inner.Logger.Log("msg", "Calculated target topology.", "configuration", targetTopology.NextConfiguration,
		"newRoots", rootsRequired, "active", fmt.Sprint(active), "passive", fmt.Sprint(passive))

	if rootsRequired != 0 {
		go func() {
			closer := task.maybeTick2(task, task.targetConfig)
			resubmit, roots, err := task.attemptCreateRoots(rootsRequired)
			if !closer() {
				return
			}
			task.EnqueueFuncAsync(func() (bool, error) {
				switch {
				case task.currentTask != task:
					return false, nil

				case err != nil:
					return task.fatal(err)

				case resubmit:
					task.enqueueTick(task, task.targetConfig)
					return false, nil

				default:
					targetTopology.RootVarUUIds = append(targetTopology.RootVarUUIds, roots...)
					return task.installTargetOld(targetTopology, active, passive)
				}
			})
		}()
	} else {
		return task.installTargetOld(targetTopology, active, passive)
	}
	return false, nil
}

func (task *installTargetOld) installTargetOld(targetTopology *configuration.Topology, active, passive common.RMIds) (bool, error) {
	// We use all the nodes in the old cluster as potential
	// acceptors. We will require a majority of them are alive, which
	// we've checked once above.
	twoFInc := uint16(task.active.RMs.NonEmptyLen())
	txn := task.createTopologyTransaction(task.active, targetTopology, twoFInc, active, passive)
	go task.runTopologyTransaction(task, txn, active, passive)
	return false, nil
}

func (task *installTargetOld) calculateTargetTopology() (*configuration.Topology, int, bool, error) {
	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		terminate, err := task.fatal(err)
		return nil, 0, terminate, err
	}

	hostsSurvived, hostsRemoved, hostsAdded :=
		make(map[string]common.RMId),
		make(map[string]common.RMId),
		make(map[string]paxos.Connection)

	allRemoteHosts := make([]string, 0, len(task.active.Hosts)+len(task.config.Hosts))

	// 1. Start by assuming all old hosts have been removed
	rmIdsOld := task.active.RMs.NonEmpty()
	// rely on hosts and rms being in the same order.
	hostsOld := task.active.Hosts
	for idx, host := range hostsOld {
		hostsRemoved[host] = rmIdsOld[idx]
		if host != localHost {
			allRemoteHosts = append(allRemoteHosts, host)
		}
	}

	// 2. For each new host, if it is in the removed set, it's
	// "survived". Else it's new. Don't care about correcting
	// hostsRemoved.
	for _, host := range task.config.Hosts {
		if rmId, found := hostsRemoved[host]; found {
			hostsSurvived[host] = rmId
		} else {
			hostsAdded[host] = nil
			if host != localHost {
				allRemoteHosts = append(allRemoteHosts, host)
			}
		}
	}

	task.installTopology(task.active, nil, localHost, allRemoteHosts)

	// the -1 is because allRemoteHosts will not include localHost
	hostsAddedList := allRemoteHosts[len(hostsOld)-1:]
	allAddedFound, err := task.verifyClusterUUIds(task.active.ClusterUUId, hostsAddedList)
	if err != nil {
		terminate, err := task.error(err)
		return nil, 0, terminate, err
	} else if !allAddedFound {
		return nil, 0, false, nil
	}

	// map(old -> new)
	rmIdsTranslation := make(map[common.RMId]common.RMId)
	connsAdded := make([]paxos.Connection, 0, len(hostsAdded))
	rmIdsSurvived := make([]common.RMId, 0, len(hostsSurvived))
	rmIdsLost := make([]common.RMId, 0, len(hostsRemoved))

	// 3. Assume all old RMIds have been removed (so map to RMIdEmpty)
	for _, rmId := range rmIdsOld {
		rmIdsTranslation[rmId] = common.RMIdEmpty
	}
	// 4. All new hosts must have new RMIds, and we must be connected
	// to them.
	for host := range hostsAdded {
		cd, found := task.hostToConnection[host]
		if !found {
			return nil, 0, false, nil
		}
		hostsAdded[host] = cd
		connsAdded = append(connsAdded, cd)
	}
	// 5. Problem is that hostsAdded may be missing entries for hosts
	// that have been wiped and thus changed RMId
	for host, rmIdOld := range hostsSurvived {
		cd, found := task.hostToConnection[host]
		if found && rmIdOld != cd.RMId() {
			// We have evidence the RMId has changed!
			rmIdNew := cd.RMId()
			rmIdsTranslation[rmIdOld] = rmIdNew
			hostsAdded[host] = cd
		} else {
			// No evidence it's changed RMId, so it maps to itself.
			rmIdsTranslation[rmIdOld] = rmIdOld
			rmIdsSurvived = append(rmIdsSurvived, rmIdOld)
		}
	}

	connsAddedCopy := connsAdded

	// Now construct the new RMId list.
	rmIdsNew := make([]common.RMId, 0, len(allRemoteHosts)+1)
	hostsNew := make([]string, 0, len(allRemoteHosts)+1)
	hostIdx := 0
	for _, rmIdOld := range task.active.RMs { // need the gaps!
		rmIdNew := rmIdsTranslation[rmIdOld]
		switch {
		case rmIdNew == common.RMIdEmpty && len(connsAddedCopy) > 0:
			cd := connsAddedCopy[0]
			connsAddedCopy = connsAddedCopy[1:]
			rmIdNew = cd.RMId()
			rmIdsNew = append(rmIdsNew, rmIdNew)
			hostsNew = append(hostsNew, cd.Host())
			if rmIdOld != common.RMIdEmpty {
				hostIdx++
				rmIdsLost = append(rmIdsLost, rmIdOld)
				rmIdsTranslation[rmIdOld] = rmIdNew
			}
		case rmIdNew == common.RMIdEmpty:
			rmIdsNew = append(rmIdsNew, rmIdNew)
			if rmIdOld != common.RMIdEmpty {
				hostIdx++
				rmIdsLost = append(rmIdsLost, rmIdOld)
			}
		case rmIdNew != rmIdOld: // via rmIdsTranslation, must be a reset RM
			rmIdsNew = append(rmIdsNew, rmIdNew)
			host := hostsOld[hostIdx]
			hostsNew = append(hostsNew, host)
			hostIdx++
			rmIdsLost = append(rmIdsLost, rmIdOld)
			connsAdded = append(connsAdded, hostsAdded[host]) // will not affect connsAddedCopy
		default:
			rmIdsNew = append(rmIdsNew, rmIdNew)
			hostsNew = append(hostsNew, hostsOld[hostIdx])
			hostIdx++
		}
	}
	// Finally, we may still have some new RMIds we never found space
	// for.
	for _, cd := range connsAddedCopy {
		rmIdsNew = append(rmIdsNew, cd.RMId())
		hostsNew = append(hostsNew, cd.Host())
	}

	targetTopology := task.active.Clone()
	next := task.config.Configuration.Clone()
	next.RMs = rmIdsNew
	next.Hosts = hostsNew

	// Pointer semantics, so we need to copy into our new set
	removed := make(map[common.RMId]server.EmptyStruct)
	alreadyRemoved := targetTopology.RMsRemoved
	for rmId := range alreadyRemoved {
		removed[rmId] = server.EmptyStructVal
	}
	for _, rmId := range rmIdsLost {
		removed[rmId] = server.EmptyStructVal
	}
	next.RMsRemoved = removed

	rmIdsAdded := make([]common.RMId, len(connsAdded))
	for idx, cd := range connsAdded {
		rmIdsAdded[idx] = cd.RMId()
	}
	conds := calculateMigrationConditions(rmIdsAdded, rmIdsLost, rmIdsSurvived, task.active.Configuration, next)

	// now figure out which roots have survived and how many new ones
	// we need to create.
	oldNamesList := targetTopology.Roots
	oldNamesCount := len(oldNamesList)
	oldNames := make(map[string]uint32, oldNamesCount)
	for idx, name := range oldNamesList {
		oldNames[name] = uint32(idx)
	}
	newNames := next.Roots
	rootsRequired := 0
	rootIndices := make([]uint32, len(newNames))
	for idx, name := range newNames {
		if index, found := oldNames[name]; found {
			rootIndices[idx] = index
		} else {
			rootIndices[idx] = uint32(oldNamesCount + rootsRequired)
			rootsRequired++
		}
	}
	targetTopology.RootVarUUIds = targetTopology.RootVarUUIds[:oldNamesCount]

	targetTopology.NextConfiguration = &configuration.NextConfiguration{
		Configuration:  next,
		AllHosts:       append(allRemoteHosts, localHost),
		NewRMIds:       rmIdsAdded,
		SurvivingRMIds: rmIdsSurvived,
		LostRMIds:      rmIdsLost,
		RootIndices:    rootIndices,
		InstalledOnNew: len(rmIdsAdded) == 0,
		Pending:        conds,
	}
	// This is the only time that we go from a topology with a nil
	// next, to one with a non-nil next. Therefore, we must ensure the
	// ClusterUUId is non-0 and consistent down the chain. Of course,
	// the txn will ensure only one such rewrite will win.
	targetTopology.EnsureClusterUUId(0)
	server.DebugLog(task.inner.Logger, "debug", "Set cluster uuid.", "uuid", targetTopology.ClusterUUId)

	return targetTopology, rootsRequired, false, nil
}

func calculateMigrationConditions(added, lost, survived []common.RMId, from, to *configuration.Configuration) configuration.Conds {
	conditions := configuration.Conds(make(map[common.RMId]*configuration.CondSuppliers))
	twoFIncOld := (uint16(from.F) << 1) + 1

	for _, rmIdNew := range added {
		conditions.DisjoinWith(rmIdNew, &configuration.Generator{
			RMId:     rmIdNew,
			UseNext:  true,
			Includes: true,
		})
	}

	if int(twoFIncOld) < from.RMs.NonEmptyLen() {
		if from.F < to.F || len(lost) > len(added) {
			for _, rmId := range survived {
				conditions.DisjoinWith(rmId, &configuration.Conjunction{
					Left: &configuration.Generator{
						RMId:     rmId,
						UseNext:  false,
						Includes: false,
					},
					Right: &configuration.Generator{
						RMId:     rmId,
						UseNext:  true,
						Includes: true,
					},
				})
			}
		}
	}
	return conditions
}

// installTargetNew
// Now that everyone in the old/current topology knows about the Next
// topology, we need to do a further txn to ensure everyone new who's
// joining the cluster gets told.

type installTargetNew struct {
	*targetConfig
}

func (task *installTargetNew) Tick() (bool, error) {
	next := task.active.NextConfiguration
	if !(next != nil && next.Version == task.config.Version && !next.InstalledOnNew) {
		return task.completed()
	}

	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	remoteHosts := task.allHostsBarLocalHost(localHost, next)
	task.installTopology(task.active, nil, localHost, remoteHosts)
	task.shareGoalWithAll()

	if !task.isInRMs(next.NewRMIds) {
		task.inner.Logger.Log("msg", "Awaiting new cluster members.")
		// this step must be performed by the new RMs
		return false, nil
	}

	// From this point onwards, we have the possibility that some
	// node-to-be-removed has rushed ahead and has shutdown. So we
	// can't rely on any to-be-removed node. So that means we can only
	// rely on the nodes in next.RMs, which means we need a majority of
	// them to be alive; and we use the removed RMs as extra passives.
	active, passive := task.formActivePassive(next.RMs, next.LostRMIds)
	if active == nil {
		return false, nil
	}

	twoFInc := uint16(next.RMs.NonEmptyLen())

	task.inner.Logger.Log("msg", "Installing on new cluster members.",
		"active", fmt.Sprint(active), "passive", fmt.Sprint(passive))

	topology := task.active.Clone()
	topology.NextConfiguration.InstalledOnNew = true

	txn := task.createTopologyTransaction(task.active, topology, twoFInc, active, passive)
	go task.runTopologyTransaction(task, txn, active, passive)
	return false, nil
}

// quiet

type quiet struct {
	*targetConfig
	installing *configuration.Configuration
	stage      uint8
}

func (task *quiet) Tick() (bool, error) {
	// The purpose of getting the vars to go quiet isn't just for
	// emigration; it's also to require that txn outcomes are decided
	// (consensus reached) before any acceptors get booted out. So we
	// go through all this even if len(pending) is 0.
	next := task.active.NextConfiguration
	if !(next != nil && next.Version == task.config.Version &&
		!next.QuietRMIds[task.connectionManager.RMId]) {
		return task.completed()
	}

	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	remoteHosts := task.allHostsBarLocalHost(localHost, next)

	activeNextConfig := next.Configuration
	if activeNextConfig != task.installing {
		task.installing = activeNextConfig
		task.stage = 0
		task.inner.Logger.Log("msg", "Quiet: new target topology detected; restarting.")
	}

	switch task.stage {
	case 0, 2:
		task.inner.Logger.Log("msg", fmt.Sprintf("Quiet: installing on to Proposers (%d of 3).", task.stage+1))
		// 0: Install to the proposerManagers. Once we know this is on
		// all our proposerManagers, we know that they will stop
		// accepting client txns.
		// 2: Install to the proposers again. This is to ensure that
		// TLCs have been written to disk.
		task.installTopology(task.active, map[eng.TopologyChangeSubscriberType]func() (bool, error){
			eng.ProposerSubscriber: func() (bool, error) {
				if activeNextConfig == task.installing {
					if task.stage == 0 || task.stage == 2 {
						task.stage++
					}
				}
				return task.maybeTick()
			},
		}, localHost, remoteHosts)

	case 1:
		task.inner.Logger.Log("msg", "Quiet: installing on to Vars (2 of 3).")
		// 1: Install to the varManagers. They only confirm back to us
		// once they've banned rolls, and ensured all active txns are
		// completed (though the TLC may not have gone to disk yet).
		task.installTopology(task.active, map[eng.TopologyChangeSubscriberType]func() (bool, error){
			eng.VarSubscriber: func() (bool, error) {
				if activeNextConfig == task.installing && task.stage == 1 {
					task.stage = 2
				}
				return task.maybeTick()
			},
		}, localHost, remoteHosts)

	case 3:
		// Now run a txn to record this.
		active, passive := task.formActivePassive(next.RMs, next.LostRMIds)
		if active == nil {
			return false, nil
		}

		twoFInc := uint16(next.RMs.NonEmptyLen())

		task.inner.Logger.Log("msg", "Quiet achieved, recording progress.", "pending", next.Pending,
			"active", fmt.Sprint(active), "passive", fmt.Sprint(passive))

		topology := task.active.Clone()
		topology.NextConfiguration.QuietRMIds[task.connectionManager.RMId] = true

		txn := task.createTopologyTransaction(task.active, topology, twoFInc, active, passive)
		go task.runTopologyTransaction(task, txn, active, passive)

	default:
		panic(fmt.Sprintf("Unexpected stage: %d", task.stage))
	}

	task.shareGoalWithAll()
	return false, nil
}

// migrate

type migrate struct {
	*targetConfig
	emigrator *emigrator
}

func (task *migrate) Tick() (bool, error) {
	next := task.active.NextConfiguration
	if !(next != nil && next.Version == task.config.Version && len(next.Pending) > 0) {
		return task.completed()
	}

	task.inner.Logger.Log("msg", "Migration: all quiet, ready to attempt migration.")

	// By this point, we know that our vars can be safely
	// migrated. They can still learn from other txns going on, but,
	// because any RM receiving immigration will get F+1 copies, we
	// guarantee that they will get at least one most-up-to-date copy
	// of each relevant var, so it does not cause any problems for us
	// if we receive learnt outcomes during emigration.
	if task.isInRMs(task.active.RMs) {
		// don't attempt any emigration unless we were in the old
		// topology
		task.ensureEmigrator()
	}

	if _, found := next.Pending[task.connectionManager.RMId]; !found {
		task.inner.Logger.Log("msg", "All migration into all this RM completed. Awaiting others.")
		return false, nil
	}

	senders, found := task.migrations[next.Version]
	if !found {
		return false, nil
	}
	maxSuppliers := task.active.RMs.NonEmptyLen() - int(task.active.F)
	if task.isInRMs(task.active.RMs) {
		// We were part of the old topology, so we have already supplied ourselves!
		maxSuppliers--
	}
	topology := task.active.Clone()
	next = topology.NextConfiguration
	changed := false
	for sender, inprogressPtr := range senders {
		if atomic.LoadInt32(inprogressPtr) == 0 {
			// Because we wait for locallyComplete, we know they've gone to disk.
			changed = next.Pending.SuppliedBy(task.connectionManager.RMId, sender, maxSuppliers) || changed
		}
	}
	// We track progress by updating the topology to remove RMs who
	// have completed sending to us.
	if !changed {
		return false, nil
	}

	active, passive := task.formActivePassive(next.RMs, next.LostRMIds)
	if active == nil {
		return false, nil
	}

	twoFInc := uint16(next.RMs.NonEmptyLen())

	task.inner.Logger.Log("msg", "Recording local immigration progress.", "pending", next.Pending,
		"active", fmt.Sprint(active), "passive", fmt.Sprint(passive))

	txn := task.createTopologyTransaction(task.active, topology, twoFInc, active, passive)
	go task.runTopologyTransaction(task, txn, active, passive)

	task.shareGoalWithAll()
	return false, nil
}

func (task *migrate) Abandon() {
	task.ensureStopEmigrator()
	task.targetConfig.Abandon()
}

func (task *migrate) completed() (bool, error) {
	task.ensureStopEmigrator()
	return task.targetConfig.completed()
}

func (task *migrate) ensureEmigrator() {
	if task.emigrator == nil {
		task.emigrator = newEmigrator(task)
	}
}

func (task *migrate) ensureStopEmigrator() {
	if task.emigrator != nil {
		task.emigrator.stopAsync()
		task.emigrator = nil
	}
}

// install Completion

type installCompletion struct {
	*targetConfig
}

func (task *installCompletion) Tick() (bool, error) {
	next := task.active.NextConfiguration
	if next == nil {
		task.inner.Logger.Log("msg", "Completion installed.")
		return task.completed()
	}

	if _, found := next.RMsRemoved[task.connectionManager.RMId]; found {
		task.inner.Logger.Log("msg", "We've been removed from cluster. Taking no further part.")
		return false, nil
	}

	noisyCount := 0
	for _, rmId := range task.active.RMs {
		if _, found := next.QuietRMIds[rmId]; !found {
			noisyCount++
			if noisyCount > int(task.active.F) {
				task.inner.Logger.Log("msg", "Awaiting more original RMIds to become quiet.",
					"originals", fmt.Sprint(task.active.RMs))
				return false, nil
			}
		}
	}

	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	remoteHosts := task.allHostsBarLocalHost(localHost, next)
	task.installTopology(task.active, nil, localHost, remoteHosts)
	task.shareGoalWithAll()

	active, passive := task.formActivePassive(next.RMs, next.LostRMIds)
	if active == nil {
		return false, nil
	}

	twoFInc := uint16(next.RMs.NonEmptyLen())

	topology := task.active.Clone()
	topology.SetConfiguration(next.Configuration)

	oldRoots := task.active.RootVarUUIds
	newRoots := make([]configuration.Root, len(next.RootIndices))
	for idx, index := range next.RootIndices {
		newRoots[idx] = oldRoots[index]
	}
	topology.RootVarUUIds = newRoots

	txn := task.createTopologyTransaction(task.active, topology, twoFInc, active, passive)
	go task.runTopologyTransaction(task, txn, active, passive)
	return false, nil
}

// utils

func (tc *targetConfig) runTopologyTransaction(task topologyTask, txn *msgs.Txn, active, passive common.RMIds) {
	closer := tc.maybeTick2(task, tc)
	_, resubmit, err := tc.rewriteTopology(txn, active, passive)
	if !closer() {
		return
	}
	tc.EnqueueFuncAsync(func() (bool, error) {
		switch {
		case tc.currentTask != task:
			return false, nil

		case err != nil:
			return tc.fatal(err)

		case resubmit:
			tc.enqueueTick(task, tc)
			return false, nil

		default:
			// Must be commit, or badread, which means again we should
			// receive the updated topology through the subscriber.
			return false, nil
		}
	})
}

func (task *targetConfig) createTopologyTransaction(read, write *configuration.Topology, twoFInc uint16, active, passive common.RMIds) *msgs.Txn {
	if write == nil && read != nil {
		panic("Topology transaction with nil write and non-nil read not supported")
	}

	seg := capn.NewBuffer(nil)
	txn := msgs.NewRootTxn(seg)

	actionsSeg := capn.NewBuffer(nil)
	actionsWrapper := msgs.NewRootActionListWrapper(actionsSeg)
	actions := msgs.NewActionList(actionsSeg, 1)
	actionsWrapper.SetActions(actions)
	action := actions.At(0)
	action.SetVarId(configuration.TopologyVarUUId[:])

	switch {
	case write == nil && read == nil: // discovery
		action.SetRead()
		action.Read().SetVersion(common.VersionZero[:])

	case read == nil: // creation
		action.SetCreate()
		create := action.Create()
		create.SetValue(write.Serialize())
		create.SetReferences(msgs.NewVarIdPosList(seg, 0))
		// When we create, we're creating with the blank topology. Blank
		// topology has MaxRMCount = 0. But we never actually use
		// positions of the topology var anyway. So the following code
		// basically never does anything, and is just here for
		// completeness, but it's still all safe.
		positions := seg.NewUInt8List(int(write.MaxRMCount))
		create.SetPositions(positions)
		for idx, l := 0, positions.Len(); idx < l; idx++ {
			positions.Set(idx, uint8(idx))
		}

	default: // modification
		action.SetReadwrite()
		rw := action.Readwrite()
		rw.SetVersion(read.DBVersion[:])
		rw.SetValue(write.Serialize())
		roots := write.RootVarUUIds
		refs := msgs.NewVarIdPosList(seg, len(roots))
		for idx, root := range roots {
			varIdPos := refs.At(idx)
			varIdPos.SetId(root.VarUUId[:])
			varIdPos.SetPositions((capn.UInt8List)(*root.Positions))
			varIdPos.SetCapability(common.MaxCapability.Capability)
		}
		rw.SetReferences(refs)
	}
	txn.SetActions(common.SegToBytes(actionsSeg))

	allocs := msgs.NewAllocationList(seg, len(active)+len(passive))
	txn.SetAllocations(allocs)

	offset := 0
	for idx, rmIds := range []common.RMIds{active, passive} {
		for idy, rmId := range rmIds {
			alloc := allocs.At(idy + offset)
			alloc.SetRmId(uint32(rmId))
			if idx == 0 {
				alloc.SetActive(task.activeConnections[rmId].BootCount())
			} else {
				alloc.SetActive(0)
			}
			indices := seg.NewUInt16List(1)
			alloc.SetActionIndices(indices)
			indices.Set(0, 0)
		}
		offset += len(rmIds)
	}

	txn.SetTwoFInc(twoFInc)
	if read == nil {
		txn.SetTopologyVersion(0)
	} else {
		txn.SetTopologyVersion(read.Version)
	}
	txn.SetIsTopology(true)

	return &txn
}

func (task *targetConfig) getTopologyFromLocalDatabase() (*configuration.Topology, error) {
	empty, err := task.connectionManager.Dispatchers.IsDatabaseEmpty()
	if empty || err != nil {
		return nil, err
	}

	backoff := server.NewBinaryBackoffEngine(task.rng, server.SubmissionMinSubmitDelay, server.SubmissionMaxSubmitDelay)
	for {
		txn := task.createTopologyTransaction(nil, nil, 1, []common.RMId{task.connectionManager.RMId}, nil)

		_, result, err := task.localConnection.RunTransaction(txn, nil, backoff, task.connectionManager.RMId)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, nil // shutting down
		}
		if result.Which() == msgs.OUTCOME_COMMIT {
			return nil, fmt.Errorf("Internal error: read of topology version 0 failed to abort")
		}
		abort := result.Abort()
		if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
			backoff.Advance()
			continue
		}
		abortUpdates := abort.Rerun()
		if abortUpdates.Len() != 1 {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave multiple updates")
		}
		update := abortUpdates.At(0)
		dbversion := common.MakeTxnId(update.TxnId())
		updateActions := eng.TxnActionsFromData(update.Actions(), true).Actions()
		if updateActions.Len() != 1 {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave multiple actions: %v", updateActions.Len())
		}
		updateAction := updateActions.At(0)
		if !bytes.Equal(updateAction.VarId(), configuration.TopologyVarUUId[:]) {
			return nil, fmt.Errorf("Internal error: unable to find action for topology from read of topology version 0")
		}
		if updateAction.Which() != msgs.ACTION_WRITE {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave non-write action")
		}
		write := updateAction.Write()
		refs := write.References()
		return configuration.TopologyFromCap(dbversion, &refs, write.Value())
	}
}

func (task *targetConfig) createTopologyZero(config *configuration.NextConfiguration) (*configuration.Topology, error) {
	topology := configuration.BlankTopology()
	topology.NextConfiguration = config
	txn := task.createTopologyTransaction(nil, topology, 1, []common.RMId{task.connectionManager.RMId}, nil)
	txnId := topology.DBVersion
	txn.SetId(txnId[:])
	// in general, we do backoff locally, so don't pass backoff through here
	_, result, err := task.localConnection.RunTransaction(txn, txnId, nil, task.connectionManager.RMId)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil // shutting down
	}
	if result.Which() == msgs.OUTCOME_COMMIT {
		return topology, nil
	} else {
		return nil, fmt.Errorf("Internal error: unable to write initial topology to local data store")
	}
}

func (task *targetConfig) rewriteTopology(txn *msgs.Txn, active, passive common.RMIds) (bool, bool, error) {
	// in general, we do backoff locally, so don't pass backoff through here
	server.DebugLog(task.inner.Logger, "debug", "Running transaction.", "active", active, "passive", passive)
	txnReader, result, err := task.localConnection.RunTransaction(txn, nil, nil, active...)
	if result == nil || err != nil {
		return false, false, err
	}
	txnId := txnReader.Id
	if result.Which() == msgs.OUTCOME_COMMIT {
		server.DebugLog(task.inner.Logger, "debug", "Txn Committed.", "TxnId", txnId)
		return true, false, nil
	}
	abort := result.Abort()
	server.DebugLog(task.inner.Logger, "debug", "Txn Aborted.", "TxnId", txnId)
	if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
		return false, true, nil
	}
	abortUpdates := abort.Rerun()
	if abortUpdates.Len() != 1 {
		return false, false,
			fmt.Errorf("Internal error: readwrite of topology gave %v updates (1 expected)",
				abortUpdates.Len())
	}
	update := abortUpdates.At(0)
	dbversion := common.MakeTxnId(update.TxnId())

	updateActions := eng.TxnActionsFromData(update.Actions(), true).Actions()
	if updateActions.Len() != 1 {
		return false, false,
			fmt.Errorf("Internal error: readwrite of topology gave update with %v actions instead of 1!",
				updateActions.Len())
	}
	updateAction := updateActions.At(0)
	if !bytes.Equal(updateAction.VarId(), configuration.TopologyVarUUId[:]) {
		return false, false,
			fmt.Errorf("Internal error: update action from readwrite of topology is not for topology! %v",
				common.MakeVarUUId(updateAction.VarId()))
	}
	if updateAction.Which() != msgs.ACTION_WRITE {
		return false, false,
			fmt.Errorf("Internal error: update action from readwrite of topology gave non-write action!")
	}
	writeAction := updateAction.Write()
	refs := writeAction.References()
	_, err = configuration.TopologyFromCap(dbversion, &refs, writeAction.Value())
	return false, false, err
}

func (task *targetConfig) attemptCreateRoots(rootCount int) (bool, configuration.Roots, error) {
	server.DebugLog(task.inner.Logger, "debug", "Creating Roots.", "count", rootCount)

	seg := capn.NewBuffer(nil)
	ctxn := cmsgs.NewClientTxn(seg)
	ctxn.SetRetry(false)
	roots := make([]configuration.Root, rootCount)
	actions := cmsgs.NewClientActionList(seg, rootCount)
	for idx := range roots {
		action := actions.At(idx)
		vUUId := task.localConnection.NextVarUUId()
		action.SetVarId(vUUId[:])
		action.SetCreate()
		create := action.Create()
		create.SetValue([]byte{})
		create.SetReferences(cmsgs.NewClientVarIdPosList(seg, 0))
		root := &roots[idx]
		root.VarUUId = vUUId
	}
	ctxn.SetActions(actions)
	txnReader, result, err := task.localConnection.RunClientTransaction(&ctxn, false, nil, nil)
	server.DebugLog(task.inner.Logger, "debug", "Created root.", "result", result, "error", err)
	if err != nil {
		return false, nil, err
	}
	if result == nil { // shutdown
		return false, nil, nil
	}
	if result.Which() == msgs.OUTCOME_COMMIT {
		actions := txnReader.Actions(true).Actions()
		for idx := range roots {
			root := &roots[idx]
			action := actions.At(idx)
			vUUId := common.MakeVarUUId(action.VarId())
			if vUUId.Compare(root.VarUUId) != common.EQ {
				return false, nil, fmt.Errorf("Internal error: actions changed order! At %v expecting %v, found %v", idx, root.VarUUId, vUUId)
			}
			if action.Which() != msgs.ACTION_CREATE {
				return false, nil, fmt.Errorf("Internal error: actions changed type! At %v expecting create, found %v", idx, action.Which())
			}
			positions := action.Create().Positions()
			root.Positions = (*common.Positions)(&positions)
		}
		server.DebugLog(task.inner.Logger, "debug", "Roots created.", "roots", roots)
		return false, roots, nil
	}
	if result.Abort().Which() == msgs.OUTCOMEABORT_RESUBMIT {
		return true, nil, nil
	}
	return false, nil, fmt.Errorf("Internal error: creation of root gave rerun outcome")
}

// emigrator

type emigrator struct {
	logger            log.Logger
	stop              int32
	db                *db.Databases
	connectionManager *ConnectionManager
	activeBatches     map[common.RMId]*sendBatch
	topology          *configuration.Topology
	conns             map[common.RMId]paxos.Connection
}

func newEmigrator(task *migrate) *emigrator {
	e := &emigrator{
		logger:            task.inner.Logger,
		db:                task.db,
		connectionManager: task.connectionManager,
		activeBatches:     make(map[common.RMId]*sendBatch),
	}
	e.topology = e.connectionManager.AddTopologySubscriber(eng.EmigratorSubscriber, e)
	e.connectionManager.AddServerConnectionSubscriber(e)
	return e
}

func (e *emigrator) stopAsync() {
	atomic.StoreInt32(&e.stop, 1)
	e.connectionManager.RemoveServerConnectionSubscriber(e)
	e.connectionManager.RemoveTopologySubscriberAsync(eng.EmigratorSubscriber, e)
}

func (e *emigrator) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	defer done(true)
	e.topology = topology
	e.startBatches()
}

func (e *emigrator) ConnectedRMs(conns map[common.RMId]paxos.Connection) {
	e.conns = conns
	e.startBatches()
}

func (e *emigrator) ConnectionLost(rmId common.RMId, conns map[common.RMId]paxos.Connection) {
	delete(e.activeBatches, rmId)
}

func (e *emigrator) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, conns map[common.RMId]paxos.Connection, done func()) {
	defer done()
	if rmId == e.connectionManager.RMId {
		return
	}
	e.conns = conns
	e.startBatches()
}

func (e *emigrator) startBatches() {
	pending := e.topology.NextConfiguration.Pending
	batchConds := make([]*sendBatch, 0, len(pending))
	for rmId, cond := range pending {
		if rmId == e.connectionManager.RMId {
			continue
		}
		if _, found := e.activeBatches[rmId]; found {
			continue
		}
		if conn, found := e.conns[rmId]; found {
			e.logger.Log("msg", "Starting emigration batch.", "RMId", rmId)
			batch := e.newBatch(conn, cond.Cond)
			e.activeBatches[rmId] = batch
			batchConds = append(batchConds, batch)
		}
	}
	if len(batchConds) > 0 {
		e.startBatch(batchConds)
	}
}

func (e *emigrator) startBatch(batch []*sendBatch) {
	it := &dbIterator{
		emigrator:     e,
		configuration: e.topology.Configuration,
		batch:         batch,
	}
	go it.iterate()
}

type dbIterator struct {
	*emigrator
	configuration *configuration.Configuration
	batch         []*sendBatch
}

func (it *dbIterator) iterate() {
	ran, err := it.db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		result, _ := rtxn.WithCursor(it.db.Vars, func(cursor *mdbs.Cursor) interface{} {
			vUUIdBytes, varBytes, err := cursor.Get(nil, nil, mdb.FIRST)
			for ; err == nil; vUUIdBytes, varBytes, err = cursor.Get(nil, nil, mdb.NEXT) {
				seg, _, err := capn.ReadFromMemoryZeroCopy(varBytes)
				if err != nil {
					cursor.Error(err)
					return true
				}
				varCap := msgs.ReadRootVar(seg)
				if bytes.Equal(varCap.Id(), configuration.TopologyVarUUId[:]) {
					continue
				}
				txnId := common.MakeTxnId(varCap.WriteTxnId())
				txnBytes := it.db.ReadTxnBytesFromDisk(cursor.RTxn, txnId)
				if txnBytes == nil {
					return true
				}
				txn := eng.TxnReaderFromData(txnBytes)
				// So, we only need to send based on the vars that we have
				// (in fact, we require the positions so we can only look
				// at the vars we have). However, the txn var allocations
				// only cover what's assigned to us at the time of txn
				// creation and that can change and we don't rewrite the
				// txn when it changes. So that all just means we must
				// ignore the allocations here, and just work through the
				// actions directly.
				actions := txn.Actions(true).Actions()
				varCaps, err := it.filterVars(cursor, vUUIdBytes, txnId[:], actions)
				if err != nil {
					return true
				} else if len(varCaps) == 0 {
					continue
				}
				for _, sb := range it.batch {
					matchingVarCaps, err := it.matchVarsAgainstCond(sb.cond, varCaps)
					if err != nil {
						cursor.Error(err)
						return true
					} else if len(matchingVarCaps) != 0 {
						sb.add(txn, matchingVarCaps)
					}
				}
			}
			if err == mdb.NotFound {
				return true
			} else {
				cursor.Error(err)
				return true
			}
		})
		return result
	}).ResultError()
	if err != nil {
		panic(fmt.Sprintf("Topology iterator error: %v", err))
	} else if ran != nil {
		for _, sb := range it.batch {
			sb.flush()
		}
		it.connectionManager.AddServerConnectionSubscriber(it)
	}
}

func (it *dbIterator) filterVars(cursor *mdbs.Cursor, vUUIdBytes []byte, txnIdBytes []byte, actions *msgs.Action_List) ([]*msgs.Var, error) {
	varCaps := make([]*msgs.Var, 0, actions.Len()>>1)
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if action.Which() == msgs.ACTION_READ {
			// no point looking up the var itself as there's no way it'll
			// point back to us.
			continue
		}
		actionVarUUIdBytes := action.VarId()
		varBytes, err := cursor.RTxn.Get(it.db.Vars, actionVarUUIdBytes)
		if err == mdb.NotFound {
			continue
		} else if err != nil {
			cursor.Error(err)
			return nil, err
		}

		seg, _, err := capn.ReadFromMemoryZeroCopy(varBytes)
		if err != nil {
			cursor.Error(err)
			return nil, err
		}
		varCap := msgs.ReadRootVar(seg)
		if !bytes.Equal(txnIdBytes, varCap.WriteTxnId()) {
			// this var has moved on to a different txn
			continue
		}
		if bytes.Compare(actionVarUUIdBytes, vUUIdBytes) < 0 {
			// We've found an action on a var that is 'before' the
			// current var (will match ordering in lmdb) and it's on the
			// same txn as the current var. Therefore we've already done
			// this txn so we can just skip now.
			return nil, nil
		}
		varCaps = append(varCaps, &varCap)
	}
	return varCaps, nil
}

func (it *dbIterator) matchVarsAgainstCond(cond configuration.Cond, varCaps []*msgs.Var) ([]*msgs.Var, error) {
	result := make([]*msgs.Var, 0, len(varCaps)>>1)
	for _, varCap := range varCaps {
		pos := varCap.Positions()
		server.DebugLog(it.logger, "debug", "Testing for condition.",
			"VarUUId", common.MakeVarUUId(varCap.Id()), "positions", (*common.Positions)(&pos),
			"condition", cond)
		if b, err := cond.SatisfiedBy(it.configuration, (*common.Positions)(&pos), it.logger); err == nil && b {
			result = append(result, varCap)
		} else if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (it *dbIterator) ConnectedRMs(conns map[common.RMId]paxos.Connection) {
	defer it.connectionManager.RemoveServerConnectionSubscriber(it)

	if atomic.LoadInt32(&it.stop) != 0 {
		return
	}

	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	mc := msgs.NewMigrationComplete(seg)
	mc.SetVersion(it.configuration.NextConfiguration.Version)
	msg.SetMigrationComplete(mc)
	bites := common.SegToBytes(seg)

	for _, sb := range it.batch {
		if conn, found := conns[sb.conn.RMId()]; found && sb.conn == conn {
			// The connection has not changed since we started sending to
			// it (because we cached it, you can discount the issue of
			// memory reuse here - phew). Therefore, it's safe to send
			// the completion msg. If it has changed, we rely on the
			// ConnectionLost being called in the emigrator to do any
			// necessary tidying up.
			server.DebugLog(it.logger, "debug", "Sending migration completion.", "recipient", conn.RMId())
			conn.Send(bites)
		}
	}
}
func (it *dbIterator) ConnectionLost(common.RMId, map[common.RMId]paxos.Connection) {}
func (it *dbIterator) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, servers map[common.RMId]paxos.Connection, done func()) {
	done()
}

type sendBatch struct {
	logger  log.Logger
	version uint32
	conn    paxos.Connection
	cond    configuration.Cond
	elems   []*migrationElem
}

type migrationElem struct {
	txn  *eng.TxnReader
	vars []*msgs.Var
}

func (e *emigrator) newBatch(conn paxos.Connection, cond configuration.Cond) *sendBatch {
	return &sendBatch{
		logger:  e.logger,
		version: e.topology.NextConfiguration.Version,
		conn:    conn,
		cond:    cond,
		elems:   make([]*migrationElem, 0, server.MigrationBatchElemCount),
	}
}

func (sb *sendBatch) flush() {
	if len(sb.elems) == 0 {
		return
	}
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	migration := msgs.NewMigration(seg)
	migration.SetVersion(sb.version)
	elems := msgs.NewMigrationElementList(seg, len(sb.elems))
	for idx, elem := range sb.elems {
		sb.elems[idx] = nil
		elemCap := msgs.NewMigrationElement(seg)
		elemCap.SetTxn(elem.txn.Data)
		vars := msgs.NewVarList(seg, len(elem.vars))
		for idy, varCap := range elem.vars {
			vars.Set(idy, *varCap)
		}
		elemCap.SetVars(vars)
		elems.Set(idx, elemCap)
	}
	migration.SetElems(elems)
	msg.SetMigration(migration)
	bites := common.SegToBytes(seg)
	server.DebugLog(sb.logger, "debug", "Migrating txns.", "count", len(sb.elems), "recipient", sb.conn.RMId())
	sb.conn.Send(bites)
	sb.elems = sb.elems[:0]
}

func (sb *sendBatch) add(txn *eng.TxnReader, varCaps []*msgs.Var) {
	elem := &migrationElem{
		txn:  txn,
		vars: varCaps,
	}
	sb.elems = append(sb.elems, elem)
	if len(sb.elems) == server.MigrationBatchElemCount {
		sb.flush()
	}
}
