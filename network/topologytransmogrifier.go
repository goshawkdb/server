package network

import (
	"bytes"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	cc "github.com/msackman/chancell"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

type TopologyTransmogrifier struct {
	db                   *db.Databases
	connectionManager    *ConnectionManager
	localConnection      *client.LocalConnection
	active               *configuration.Topology
	installedOnProposers *configuration.Topology
	hostToConnection     map[string]paxos.Connection
	activeConnections    map[common.RMId]paxos.Connection
	migrations           map[uint32]map[common.RMId]*int32
	task                 topologyTask
	cellTail             *cc.ChanCellTail
	enqueueQueryInner    func(topologyTransmogrifierMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan            <-chan topologyTransmogrifierMsg
	listenPort           uint16
	rng                  *rand.Rand
	localEstablished     chan struct{}
}

type topologyTransmogrifierMsg interface {
	witness() topologyTransmogrifierMsg
}

type topologyTransmogrifierMsgBasic struct{}

func (ttmb topologyTransmogrifierMsgBasic) witness() topologyTransmogrifierMsg { return ttmb }

type topologyTransmogrifierMsgShutdown struct{ topologyTransmogrifierMsgBasic }

func (tt *TopologyTransmogrifier) Shutdown() {
	if tt.enqueueQuery(topologyTransmogrifierMsgShutdown{}) {
		tt.cellTail.Wait()
	}
}

type topologyTransmogrifierMsgRequestConfigChange struct {
	topologyTransmogrifierMsgBasic
	config *configuration.Configuration
}

func (tt *TopologyTransmogrifier) RequestConfigurationChange(config *configuration.Configuration) {
	tt.enqueueQuery(topologyTransmogrifierMsgRequestConfigChange{config: config})
}

type topologyTransmogrifierMsgSetActiveConnections map[common.RMId]paxos.Connection

func (ttmsac topologyTransmogrifierMsgSetActiveConnections) witness() topologyTransmogrifierMsg {
	return ttmsac
}

type topologyTransmogrifierMsgTopologyObserved struct {
	topologyTransmogrifierMsgBasic
	topology *configuration.Topology
}

type topologyTransmogrifierMsgExe func() error

func (ttme topologyTransmogrifierMsgExe) witness() topologyTransmogrifierMsg { return ttme }

type topologyTransmogrifierMsgMigration struct {
	topologyTransmogrifierMsgBasic
	migration *msgs.Migration
	sender    common.RMId
}

func (tt *TopologyTransmogrifier) MigrationReceived(sender common.RMId, migration *msgs.Migration) {
	tt.enqueueQuery(topologyTransmogrifierMsgMigration{
		migration: migration,
		sender:    sender,
	})
}

type topologyTransmogrifierMsgMigrationComplete struct {
	topologyTransmogrifierMsgBasic
	complete *msgs.MigrationComplete
	sender   common.RMId
}

func (tt *TopologyTransmogrifier) MigrationCompleteReceived(sender common.RMId, migrationComplete *msgs.MigrationComplete) {
	tt.enqueueQuery(topologyTransmogrifierMsgMigrationComplete{
		complete: migrationComplete,
		sender:   sender,
	})
}

func (tt *TopologyTransmogrifier) enqueueQuery(msg topologyTransmogrifierMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return tt.enqueueQueryInner(msg, cell, f)
	}
	return tt.cellTail.WithCell(f)
}

func NewTopologyTransmogrifier(db *db.Databases, cm *ConnectionManager, lc *client.LocalConnection, listenPort uint16, config *configuration.Configuration) (*TopologyTransmogrifier, <-chan struct{}) {
	tt := &TopologyTransmogrifier{
		db:                db,
		connectionManager: cm,
		localConnection:   lc,
		migrations:        make(map[uint32]map[common.RMId]*int32),
		listenPort:        listenPort,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		localEstablished:  make(chan struct{}),
	}
	tt.task = &targetConfig{
		TopologyTransmogrifier: tt,
		config:                 &configuration.NextConfiguration{Configuration: config},
	}

	var head *cc.ChanCellHead
	head, tt.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan topologyTransmogrifierMsg, n)
			cell.Open = func() { tt.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			tt.enqueueQueryInner = func(msg topologyTransmogrifierMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
				if curCell == cell {
					select {
					case queryChan <- msg:
						return true, nil
					default:
						return false, nil
					}
				} else {
					return false, cont
				}
			}
		})

	cm.AddServerConnectionSubscriber(tt)

	go tt.actorLoop(head, config)
	return tt, tt.localEstablished
}

func (tt *TopologyTransmogrifier) ConnectedRMs(conns map[common.RMId]paxos.Connection) {
	tt.enqueueQuery(topologyTransmogrifierMsgSetActiveConnections(conns))
}

func (tt *TopologyTransmogrifier) ConnectionLost(rmId common.RMId, conns map[common.RMId]paxos.Connection) {
	tt.enqueueQuery(topologyTransmogrifierMsgSetActiveConnections(conns))
}

func (tt *TopologyTransmogrifier) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, conns map[common.RMId]paxos.Connection) {
	tt.enqueueQuery(topologyTransmogrifierMsgSetActiveConnections(conns))
}

func (tt *TopologyTransmogrifier) actorLoop(head *cc.ChanCellHead, config *configuration.Configuration) {
	subscriberInstalled := make(chan struct{})
	tt.connectionManager.Dispatchers.VarDispatcher.ApplyToVar(func(v *eng.Var, err error) {
		if err != nil {
			panic(fmt.Errorf("Error trying to subscribe to topology: %v", err))
		}
		v.AddWriteSubscriber(configuration.VersionOne,
			&eng.VarWriteSubscriber{
				Observe: func(v *eng.Var, value []byte, refs *msgs.VarIdPos_List, txn *eng.Txn) {
					var rootVarPosPtr *msgs.VarIdPos
					if refs.Len() > 0 {
						root := refs.At(0)
						rootVarPosPtr = &root
					}
					topology, err := configuration.TopologyFromCap(txn.Id, rootVarPosPtr, value)
					if err != nil {
						panic(fmt.Errorf("Unable to deserialize new topology: %v", err))
					}
					tt.enqueueQuery(topologyTransmogrifierMsgTopologyObserved{topology: topology})
				},
				Cancel: func(v *eng.Var) {
					panic("Subscriber on topology var has been cancelled!")
				},
			})
		close(subscriberInstalled)
	}, true, configuration.TopologyVarUUId)
	<-subscriberInstalled

	var (
		err       error
		queryChan <-chan topologyTransmogrifierMsg
		queryCell *cc.ChanCell
		oldTask   topologyTask
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = tt.queryChan, cell }
	head.WithCell(chanFun)

	terminate := err != nil
	for !terminate {
		if oldTask != tt.task {
			oldTask = tt.task
			if oldTask != nil {
				err = oldTask.tick()
				terminate = err != nil
			}
		} else if msg, ok := <-queryChan; ok {
			switch msgT := msg.(type) {
			case topologyTransmogrifierMsgShutdown:
				terminate = true
			case topologyTransmogrifierMsgSetActiveConnections:
				err = tt.activeConnectionsChange(msgT)
			case topologyTransmogrifierMsgTopologyObserved:
				server.Log("Topology: New topology observed:", msgT.topology)
				err = tt.setActive(msgT.topology)
			case topologyTransmogrifierMsgRequestConfigChange:
				server.Log("Topology: Topology change request:", msgT.config)
				tt.selectGoal(&configuration.NextConfiguration{Configuration: msgT.config})
			case topologyTransmogrifierMsgMigration:
				err = tt.migrationReceived(msgT)
			case topologyTransmogrifierMsgMigrationComplete:
				err = tt.migrationCompleteReceived(msgT)
			case topologyTransmogrifierMsgExe:
				err = msgT()
			default:
				err = fmt.Errorf("Fatal to TopologyTransmogrifier: Received unexpected message: %#v", msgT)
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		log.Println("TopologyTransmogrifier error:", err)
	}
	tt.connectionManager.RemoveServerConnectionSubscriber(tt)
	tt.cellTail.Terminate()
}

func (tt *TopologyTransmogrifier) activeConnectionsChange(conns map[common.RMId]paxos.Connection) error {
	tt.activeConnections = conns

	tt.hostToConnection = make(map[string]paxos.Connection, len(conns))
	for _, cd := range conns {
		tt.hostToConnection[cd.Host()] = cd
	}

	if tt.task != nil {
		return tt.task.tick()
	}
	return nil
}

func (tt *TopologyTransmogrifier) setActive(topology *configuration.Topology) error {
	if tt.active != nil {
		switch {
		case tt.active.ClusterId != topology.ClusterId:
			return fmt.Errorf("Topology: Fatal: config with ClusterId change from '%s' to '%s'.",
				tt.active.ClusterId, topology.ClusterId)

		case topology.Version < tt.active.Version:
			log.Printf("Topology: Ignoring config with version %v as newer version already active (%v).",
				topology.Version, tt.active.Version)
			return nil

		case tt.active.Configuration.Equal(topology.Configuration):
			// silently ignore it
			return nil
		}
	}

	if _, found := topology.RMsRemoved()[tt.connectionManager.RMId]; found {
		return errors.New("We have been removed from the cluster. Shutting down.")
	}
	tt.active = topology

	if tt.task != nil {
		if err := tt.task.tick(); err != nil {
			return err
		}
	}

	if tt.task == nil {
		if next := topology.Next(); next == nil {
			tt.installTopology(topology, nil)
			localHost, remoteHosts, err := tt.active.LocalRemoteHosts(tt.listenPort)
			if err != nil {
				return err
			}
			log.Printf(">==> We are %v (%v) <==<\n", localHost, tt.connectionManager.RMId)

			future := tt.db.WithEnv(func(env *mdb.Env) (interface{}, error) {
				return nil, env.SetFlags(mdb.NOSYNC, topology.NoSync)
			})
			tt.connectionManager.SetDesiredServers(localHost, remoteHosts)
			for version := range tt.migrations {
				if version <= topology.Version {
					delete(tt.migrations, version)
				}
			}

			_, err = future.ResultError()
			if err != nil {
				return err
			}

		} else {
			tt.selectGoal(next)
		}
	}
	return nil
}

func (tt *TopologyTransmogrifier) installTopology(topology *configuration.Topology, callbacks map[eng.TopologyChangeSubscriberType]func() error) {
	server.Log("Topology: Installing topology to connection manager, et al:", topology)
	if tt.localEstablished != nil {
		if callbacks == nil {
			callbacks = make(map[eng.TopologyChangeSubscriberType]func() error)
		}
		origFun := callbacks[eng.ConnectionManagerSubscriber]
		callbacks[eng.ConnectionManagerSubscriber] = func() error {
			if tt.localEstablished != nil {
				close(tt.localEstablished)
				tt.localEstablished = nil
			}
			if origFun == nil {
				return nil
			} else {
				return origFun()
			}
		}
	}
	wrapped := make(map[eng.TopologyChangeSubscriberType]func(), len(callbacks))
	for subType, cb := range callbacks {
		cbCopy := cb
		wrapped[subType] = func() { tt.enqueueQuery(topologyTransmogrifierMsgExe(cbCopy)) }
	}
	tt.connectionManager.SetTopology(topology, wrapped)
}

func (tt *TopologyTransmogrifier) selectGoal(goal *configuration.NextConfiguration) {
	if tt.active != nil {
		switch {
		case goal.Version == 0:
			return // done.

		case goal.ClusterId != tt.active.ClusterId:
			log.Printf("Topology: Illegal config: ClusterId should be '%s' instead of '%s'.",
				tt.active.ClusterId, goal.ClusterId)
			return

		case goal.MaxRMCount != tt.active.MaxRMCount && tt.active.Version != 0:
			log.Printf("Topology: Illegal config change: Currently changes to MaxRMCount are not supported, sorry.")
			return

		case goal.Version < tt.active.Version:
			log.Printf("Topology: Ignoring config with version %v as newer version already active (%v).",
				goal.Version, tt.active.Version)
			return

		case goal.Version == tt.active.Version:
			log.Printf("Topology: Config transition to version %v completed.", goal.Version)
			return
		}
	}

	if tt.task != nil {
		existingGoal := tt.task.goal()
		switch {
		case goal.ClusterId != existingGoal.ClusterId:
			log.Printf("Topology: Illegal config: ClusterId should be '%s' instead of '%s'.",
				existingGoal.ClusterId, goal.ClusterId)
			return

		case goal.Version < existingGoal.Version:
			log.Printf("Topology: Ignoring config with version %v as newer version already targetted (%v).",
				goal.Version, existingGoal.Version)
			return

		case goal.Version == existingGoal.Version:
			log.Printf("Topology: Config transition to version %v already in progress.", goal.Version)
			return // goal already in progress

		default:
			server.Log("Topology: Abandoning old task")
			tt.task.abandon()
			tt.task = nil
		}
	}

	if tt.task == nil {
		server.Log("Topology: Creating new task")
		tt.task = &targetConfig{
			TopologyTransmogrifier: tt,
			config:                 goal,
		}
	}
}

func (tt *TopologyTransmogrifier) enqueueTick(task topologyTask) {
	sleep := time.Duration(tt.rng.Intn(int(server.SubmissionMaxSubmitDelay)))
	go func() {
		time.Sleep(sleep)
		tt.enqueueQuery(topologyTransmogrifierMsgExe(func() error {
			if tt.task == task {
				return tt.task.tick()
			}
			return nil
		}))
	}()
}

func (tt *TopologyTransmogrifier) migrationReceived(migration topologyTransmogrifierMsgMigration) error {
	version := migration.migration.Version()
	if version <= tt.active.Version {
		// This topology change has been completed. Ignore this migration.
		return nil
	} else if next := tt.active.Next(); next != nil {
		if version < next.Version {
			// Whatever change that was for, it isn't happening any
			// more. Ignore.
			return nil
		} else if _, found := next.Pending[tt.connectionManager.RMId]; version == next.Version && !found {
			// Migration is for the current topology change, but we've
			// declared ourselves done, so Ignore.
			return nil
		}
	}

	senders, found := tt.migrations[version]
	if !found {
		senders = make(map[common.RMId]*int32)
		tt.migrations[version] = senders
	}
	sender := migration.sender
	inprogressPtr, found := senders[sender]
	if found {
		atomic.AddInt32(inprogressPtr, 1)
	} else {
		inprogress := int32(2)
		inprogressPtr = &inprogress
		senders[sender] = inprogressPtr
	}
	txnCount := int32(migration.migration.Elems().Len())
	lsc := tt.newTxnLSC(txnCount, inprogressPtr)
	tt.connectionManager.Dispatchers.ProposerDispatcher.ImmigrationReceived(migration.migration, lsc)
	return nil
}

func (tt *TopologyTransmogrifier) migrationCompleteReceived(migrationComplete topologyTransmogrifierMsgMigrationComplete) error {
	version := migrationComplete.complete.Version()
	sender := migrationComplete.sender
	server.Log("Topology: MCR from", sender, "v", version)
	senders, found := tt.migrations[version]
	if !found {
		if version > tt.active.Version {
			senders = make(map[common.RMId]*int32)
			tt.migrations[version] = senders
		} else {
			return nil
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
	if tt.task != nil && inprogress == 0 {
		return tt.task.tick()
	}
	return nil
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

func (mtlsc *migrationTxnLocalStateChange) TxnBallotsComplete(*eng.Txn, ...*eng.Ballot) {
	panic("TxnBallotsComplete called on migrating txn.")
}

// Careful: we're in the proposer dispatcher go routine here!
func (mtlsc *migrationTxnLocalStateChange) TxnLocallyComplete(txn *eng.Txn) {
	txn.CompletionReceived()
	if atomic.AddInt32(&mtlsc.pendingLocallyComplete, -1) == 0 &&
		atomic.AddInt32(mtlsc.inprogressPtr, -1) == 0 {
		mtlsc.enqueueQuery(topologyTransmogrifierMsgExe(func() error {
			if mtlsc.task != nil {
				return mtlsc.task.tick()
			}
			return nil
		}))
	}
}

func (mtlsc *migrationTxnLocalStateChange) TxnFinished(*eng.Txn) {}

// topologyTask

type topologyTask interface {
	tick() error
	abandon()
	goal() *configuration.NextConfiguration
	witness() topologyTask
}

// targetConfig

type targetConfig struct {
	*TopologyTransmogrifier
	config *configuration.NextConfiguration
	sender paxos.ServerConnectionSubscriber
}

func (task *targetConfig) tick() error {
	switch {
	case task.active == nil:
		log.Println("Topology: Ensuring local topology.")
		task.task = &ensureLocalTopology{task}

	case task.active.Version == 0:
		log.Printf("Topology: Attempting to join cluster with configuration: %v", task.config)
		task.task = &joinCluster{targetConfig: task}

	case task.active.Next() == nil || task.active.Next().Version < task.config.Version:
		log.Printf("Topology: Attempting to install topology change target: %v", task.config)
		task.task = &installTargetOld{targetConfig: task}

	case task.active.Next() != nil && task.active.Next().Version == task.config.Version:
		if !task.active.Next().InstalledOnNew {
			log.Printf("Topology: Attempting to install topology change to new cluster: %v", task.config)
			task.task = &installTargetNew{targetConfig: task}

		} else if !task.active.NextBarrierReached1(task.connectionManager.RMId) {
			log.Printf("Topology: Requesting vars go quiet (barrier 1): %v", task.config)
			task.task = &awaitBarrier1{targetConfig: task}

		} else if !task.active.NextBarrierReached2(task.connectionManager.RMId) {
			log.Printf("Topology: Awaiting quiet vars (barrier 2): %v", task.config)
			task.task = &awaitBarrier2{targetConfig: task}

		} else if len(task.active.Next().Pending) > 0 {
			log.Printf("Topology: Attempting to perform object migration for topology target: %v", task.config)
			task.task = &migrate{targetConfig: task}

		} else {
			log.Printf("Topology: Object migration completed, switching to new topology: %v", task.config)
			task.task = &installCompletion{targetConfig: task}
		}

	default:
		return fmt.Errorf("Topology: Confused about what to do. Active topology is: %v; goal is %v",
			task.active, task.config)
	}
	return nil
}

func (task *targetConfig) shareGoalWithAll() {
	if task.sender != nil {
		return
	}
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	msg.SetTopologyChangeRequest(task.config.AddToSegAutoRoot(seg))
	task.sender = paxos.NewRepeatingAllSender(server.SegToBytes(seg))
	task.connectionManager.AddServerConnectionSubscriber(task.sender)
}

func (task *targetConfig) ensureRemoveTaskSender() {
	if task.sender != nil {
		task.connectionManager.RemoveServerConnectionSubscriber(task.sender)
		task.sender = nil
	}
}

func (task *targetConfig) abandon()                               { task.ensureRemoveTaskSender() }
func (task *targetConfig) goal() *configuration.NextConfiguration { return task.config }
func (task *targetConfig) witness() topologyTask                  { return task }

func (task *targetConfig) fatal(err error) error {
	task.ensureRemoveTaskSender()
	task.task = nil
	log.Printf("Topology: fatal error: %v", err)
	return err
}

func (task *targetConfig) error(err error) error {
	task.ensureRemoveTaskSender()
	task.task = nil
	log.Printf("Topology: error: %v", err)
	return nil
}

func (task *targetConfig) completed() error {
	task.ensureRemoveTaskSender()
	log.Printf("Topology: task completed.")
	task.task = nil
	return nil
}

func (task *targetConfig) activeChange(active *configuration.Topology) error {
	return task.setActive(active)
}

// NB filters out empty RMIds so no need to pre-filter.
func (task *targetConfig) partitionByActiveConnection(rmIdLists ...common.RMIds) (active, passive common.RMIds) {
	active, passive = []common.RMId{}, []common.RMId{}
	for _, rmIds := range rmIdLists {
		for _, rmId := range rmIds {
			if rmId == common.RMIdEmpty {
				continue
			} else if _, found := task.activeConnections[rmId]; found {
				active = append(active, rmId)
			} else {
				passive = append(passive, rmId)
			}
		}
	}
	return active, passive
}

func (task *targetConfig) verifyRoots(rootId *common.VarUUId, remoteHosts []string) (bool, error) {
	for _, host := range remoteHosts {
		if cd, found := task.hostToConnection[host]; found {
			switch remoteRootId := cd.RootId(); {
			case remoteRootId == nil:
				// they're joining
			case rootId.Compare(remoteRootId) == common.EQ:
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
		config = config.Next().Configuration
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

// ensureLocalTopology

type ensureLocalTopology struct {
	*targetConfig
}

func (task *ensureLocalTopology) tick() error {
	if task.active != nil {
		// the fact we're here means we're done - there is a topology
		// discovered one way or another.
		if err := task.completed(); err != nil {
			return err
		}
		// However, just because we have a local config doesn't mean it
		// actually satisfies the goal. Essentially, we're pretending
		// that the goal is in Next().
		task.installTopology(task.active, map[eng.TopologyChangeSubscriberType]func() error{
			eng.ConnectionManagerSubscriber: func() error {
				if task.task != nil {
					return task.task.tick()
				}
				return nil
			}})
		task.selectGoal(task.config)
		return nil
	}

	if _, found := task.activeConnections[task.connectionManager.RMId]; !found {
		return nil
	}

	topology, err := task.getTopologyFromLocalDatabase()
	if err != nil {
		return task.fatal(err)
	}

	if topology == nil && task.config.ClusterId == "" {
		return task.fatal(errors.New("No configuration supplied and no configuration found in local store. Cannot continue."))

	} else if topology == nil {
		_, err = task.createTopologyZero(task.config)
		if err != nil {
			return task.fatal(err)
		}
		// if err == nil, the create succeeded, so wait for observation
		return nil
	} else {
		// It's already on disk, we're not going to see it through the subscriber.
		return task.activeChange(topology)
	}
}

// joinCluster

type joinCluster struct {
	*targetConfig
}

func (task *joinCluster) tick() error {
	if task.active.Version != 0 {
		return task.completed()
	}

	localHost, remoteHosts, err := task.config.LocalRemoteHosts(task.listenPort)
	if err != nil {
		// For joining, it's fatal if we can't find ourself in the
		// target.
		return task.fatal(err)
	}

	// must install to connectionManager before launching any connections
	task.installTopology(task.active, nil)
	// we may not have the youngest topology and there could be other
	// hosts who have connected to us who are trying to send us a more
	// up to date topology. So we shouldn't kill off those connections.
	task.connectionManager.SetDesiredServers(localHost, remoteHosts)

	// It's possible that different members of our goal are trying to
	// achieve different goals, so in all cases, we should share our
	// goal with them. This is essential if it turns out that we're
	// trying to join into an existing cluster - we can't possibly know
	// that's what's happening at this stage.
	task.shareGoalWithAll()

	rmIds := make([]common.RMId, 0, len(task.config.Hosts))
	var rootId *common.VarUUId
	for _, host := range task.config.Hosts {
		cd, found := task.hostToConnection[host]
		if !found {
			// We can only continue at this point if we really are
			// connected to everyone mentioned in the config.
			return nil
		}
		rmIds = append(rmIds, cd.RMId())
		switch theirRootId := cd.RootId(); {
		case theirRootId == nil:
			// they're joining too
		case rootId == nil:
			rootId = theirRootId
		case rootId.Compare(theirRootId) == common.EQ:
			// all good
		default:
			return task.fatal(
				errors.New("Attempt made to merge different logical clusters together, which is illegal. Aborting."))
		}
	}

	if allJoining := rootId == nil; allJoining {
		// Note that the order of RMIds here matches the order of hosts.
		return task.allJoining(rmIds)

	} else {
		// If we're not allJoining then we need the previous config
		// because we need to make sure that everyone in the old config
		// learns of the change. The shareGoalWithAll() call above will
		// ensure this happens.

		log.Println("Topology: Requesting help from existing cluster members for topology change.")
		return nil
	}
}

func (task *joinCluster) allJoining(allRMIds common.RMIds) error {
	targetTopology := configuration.NewTopology(task.active.DBVersion, nil, task.config.Configuration)
	targetTopology.SetRMs(allRMIds)

	// NB: activeWithNext never gets installed to the DB itself.
	activeWithNext := task.active.Clone()
	activeWithNext.SetNext(&configuration.NextConfiguration{
		Configuration: targetTopology.Configuration,
		AllHosts:      activeWithNext.Hosts,
		NewRMIds:      allRMIds,
	})

	// We're about to create and run a txn, so we must make sure that
	// txn's topology version is acceptable to our proposers.
	task.installTopology(activeWithNext, nil)

	switch resubmit, err := task.attemptCreateRoot(targetTopology); {
	case err != nil:
		return task.fatal(err)
	case resubmit:
		server.Log("Topology: Root creation needs resubmit")
		task.enqueueTick(task)
		return nil
	case targetTopology.Root.VarUUId == nil:
		// We failed; likely we need to wait for connections to change
		server.Log("Topology: Root creation failed")
		return nil
	}

	// Finally we need to rewrite the topology. For allJoining, we
	// must use everyone as active. This is because we could have
	// seen one of our peers when it had no RootId, but we've since
	// lost that connection and in fact that peer has gone off and
	// joined another cluster. So the only way to be instantaneously
	// sure that all peers are empty and moving to the same topology
	// is to have all peers as active.

	// If we got this far then attemptCreateRoot will have modified
	// targetTopology to include the updated root. We should install
	// this to the connectionManager.
	activeWithNext.Root = targetTopology.Root
	task.installTopology(activeWithNext, nil)

	result, resubmit, err := task.rewriteTopology(task.active, targetTopology, allRMIds, nil)
	if err != nil {
		return task.fatal(err)
	}
	if resubmit {
		server.Log("Topology: Topology rewrite needs resubmit", allRMIds, result)
		task.enqueueTick(task)
		return nil
	}
	// !resubmit, so MUST be a BadRead, or success. By definition,
	// if allJoining, everyone is active. So even if we weren't
	// successful rewriting ourself, we're guaranteed to be sent
	// someone else's write through the subscriber.
	return nil
}

// installTargetOld
// Purpose is to do a txn using the current topology in which we set
// topology.Next to be the target topology. We calculate and store the
// migration strategy at this point too.

type installTargetOld struct {
	*targetConfig
}

func (task *installTargetOld) tick() error {
	if next := task.active.Next(); !(next == nil || next.Version < task.config.Version) {
		return task.completed()
	}

	task.shareGoalWithAll()

	if !task.isInRMs(task.active.RMs()) {
		log.Printf("Topology: Awaiting existing cluster members.")
		// this step must be performed by the existing RMs
		return nil
	}

	targetTopology, err := task.calculateTargetTopology()
	if err != nil || targetTopology == nil {
		return err
	}

	// Here, we just want to use the RMs in the old topology only.
	active, passive := task.partitionByActiveConnection(task.active.RMs())
	if len(active) <= len(passive) {
		log.Printf("Topology: Can not make progress at this time due to too many failures (failures: %v)",
			passive)
		return nil
	}
	fInc := ((len(active) + len(passive)) >> 1) + 1
	active, passive = active[:fInc], append(active[fInc:], passive...)
	// add on all new (if there are any) as passives
	passive = append(passive, targetTopology.Next().NewRMIds...)

	log.Printf("Topology: Calculated target topology: %v (active: %v, passive: %v)", targetTopology.Next(), active, passive)

	_, resubmit, err := task.rewriteTopology(task.active, targetTopology, active, passive)
	if err != nil {
		return task.fatal(err)
	}
	if resubmit {
		task.enqueueTick(task)
		return nil
	}
	// Must be badread, which means again we should receive the
	// updated topology through the subscriber.
	return nil
}

func (task *installTargetOld) calculateTargetTopology() (*configuration.Topology, error) {
	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		return nil, task.fatal(err)
	}

	hostsSurvived, hostsRemoved, hostsAdded :=
		make(map[string]common.RMId),
		make(map[string]common.RMId),
		make(map[string]paxos.Connection)

	allRemoteHosts := make([]string, 0, len(task.active.Hosts)+len(task.config.Hosts))

	// 1. Start by assuming all old hosts have been removed
	rmIdsOld := task.active.RMs().NonEmpty()
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

	task.installTopology(task.active, nil)
	task.connectionManager.SetDesiredServers(localHost, allRemoteHosts)

	// the -1 is because allRemoteHosts will not include localHost
	hostsAddedList := allRemoteHosts[len(hostsOld)-1:]
	allAddedFound, err := task.verifyRoots(task.active.Root.VarUUId, hostsAddedList)
	if err != nil {
		return nil, task.error(err)
	} else if !allAddedFound {
		return nil, nil
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
			return nil, nil
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
	for _, rmIdOld := range task.active.RMs() { // need the gaps!
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
	next.SetRMs(rmIdsNew)
	next.Hosts = hostsNew

	// Pointer semantics, so we need to copy into our new set
	removed := make(map[common.RMId]server.EmptyStruct)
	alreadyRemoved := targetTopology.RMsRemoved()
	for rmId := range alreadyRemoved {
		removed[rmId] = server.EmptyStructVal
	}
	for _, rmId := range rmIdsLost {
		removed[rmId] = server.EmptyStructVal
	}
	next.SetRMsRemoved(removed)

	rmIdsAdded := make([]common.RMId, len(connsAdded))
	for idx, cd := range connsAdded {
		rmIdsAdded[idx] = cd.RMId()
	}
	conds := calculateMigrationConditions(rmIdsAdded, rmIdsLost, rmIdsSurvived, task.active.Configuration, next)

	targetTopology.SetNext(&configuration.NextConfiguration{
		Configuration:  next,
		AllHosts:       append(allRemoteHosts, localHost),
		NewRMIds:       rmIdsAdded,
		SurvivingRMIds: rmIdsSurvived,
		LostRMIds:      rmIdsLost,
		InstalledOnNew: len(rmIdsAdded) == 0,
		Pending:        conds,
	})

	return targetTopology, nil
}

func calculateMigrationConditions(added, lost, survived []common.RMId, from, to *configuration.Configuration) configuration.Conds {
	conditions := configuration.Conds(make(map[common.RMId]*configuration.CondSuppliers))
	twoFIncOld := (uint16(from.F) * 2) + 1

	for _, rmIdNew := range added {
		conditions.DisjoinWith(rmIdNew, &configuration.Generator{
			RMId:     rmIdNew,
			UseNext:  true,
			Includes: true,
		})
	}

	if int(twoFIncOld) < from.RMs().NonEmptyLen() {
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

func (task *installTargetNew) tick() error {
	next := task.active.Next()
	if !(next != nil && next.Version == task.config.Version && !next.InstalledOnNew) {
		return task.completed()
	}

	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	task.installTopology(task.active, nil)
	remoteHosts := task.allHostsBarLocalHost(localHost, next)
	task.connectionManager.SetDesiredServers(localHost, remoteHosts)
	task.shareGoalWithAll()

	if !task.isInRMs(next.NewRMIds) {
		log.Printf("Topology: Awaiting new cluster members.")
		// this step must be performed by the new RMs
		return nil
	}

	// Similar to allJoining, we use all of the new nodes as active,
	// but we must also have F+1 of the old nodes as actives too to
	// make sure the old nodes don't diverge and then get confused by
	// learning from the new nodes. If any of the new nodes are down,
	// we can't make progress, but that seems sane because we're going
	// to have to do migrations to the new nodes anyway.

	active, passive := task.partitionByActiveConnection(task.active.RMs())
	if len(active) <= len(passive) {
		log.Printf("Topology: Can not make progress at this time due to too many failures (failures: %v)",
			passive)
		return nil
	}
	fInc := ((len(active) + len(passive)) >> 1) + 1
	active, passive = active[:fInc], append(active[fInc:], passive...)

	newActive := task.active.Next().NewRMIds
	for _, rmId := range newActive {
		if _, found := task.activeConnections[rmId]; !found {
			log.Printf("Topology: awaiting connections to new cluster members.")
			return nil
		}
	}
	active = append(newActive, active...)

	log.Printf("Topology: Installing on new cluster members. Active: %v, Passive: %v", active, passive)

	topology := task.active.Clone()
	topology.Next().InstalledOnNew = true

	_, resubmit, err := task.rewriteTopology(task.active, topology, active, passive)
	if err != nil {
		return task.fatal(err)
	}
	if resubmit {
		server.Log("Topology: Topology extension requires resubmit.")
		task.enqueueTick(task)
	}
	return nil
}

// awaitBarrier1

type awaitBarrier1 struct {
	*targetConfig
	varBarrierReached               *configuration.Configuration
	proposerBarrierReached          *configuration.Configuration
	connectionBarrierReached        *configuration.Configuration
	connectionManagerBarrierReached *configuration.Configuration
	installing                      *configuration.Configuration
}

func (task *awaitBarrier1) witness() topologyTask { return task }

func (task *awaitBarrier1) tick() error {
	next := task.active.Next()
	if !(next != nil && next.Version == task.config.Version && !task.active.NextBarrierReached1(task.connectionManager.RMId)) {
		return task.completed()
	}

	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	activeNextConfig := next.Configuration
	if activeNextConfig == task.varBarrierReached &&
		activeNextConfig == task.proposerBarrierReached &&
		activeNextConfig == task.connectionBarrierReached &&
		activeNextConfig == task.connectionManagerBarrierReached {

		// again, we use all new RMs as actives, and F+1 surviving as actives
		active, passive := task.partitionByActiveConnection(task.active.RMs())
		if len(active) <= len(passive) {
			log.Printf("Topology: Can not make progress at this time due to too many failures (failures: %v)",
				passive)
			return nil
		}
		fInc := ((len(active) + len(passive)) >> 1) + 1
		active, passive = active[:fInc], append(active[fInc:], passive...)

		newActive := next.NewRMIds
		for _, rmId := range newActive {
			if _, found := task.activeConnections[rmId]; !found {
				log.Printf("Topology: awaiting connections to new cluster members.")
				return nil
			}
		}
		active = append(newActive, active...)

		log.Printf("Topology: Barrier1 reached. Active: %v, Passive: %v", active, passive)

		topology := task.active.Clone()
		next = topology.Next()
		next.BarrierReached1 = append(next.BarrierReached1, task.connectionManager.RMId)

		_, resubmit, err := task.rewriteTopology(task.active, topology, active, passive)
		if err != nil {
			return task.fatal(err)
		}
		if resubmit {
			server.Log("Topology: Barrier1 reached. Requires resubmit.")
			task.enqueueTick(task)
		}

	} else if activeNextConfig != task.installing {
		task.installing = activeNextConfig
		task.varBarrierReached = nil
		task.proposerBarrierReached = nil
		task.connectionBarrierReached = nil
		task.connectionManagerBarrierReached = nil
		task.installTopology(task.active, map[eng.TopologyChangeSubscriberType]func() error{
			eng.VarSubscriber: func() error {
				if activeNextConfig == task.installing {
					task.varBarrierReached = activeNextConfig
				}
				if task.task != nil {
					return task.task.tick()
				}
				return nil
			},
			eng.ProposerSubscriber: func() error {
				if activeNextConfig == task.installing {
					task.proposerBarrierReached = activeNextConfig
				}
				if task.task != nil {
					return task.task.tick()
				}
				return nil
			},
			eng.ConnectionSubscriber: func() error {
				if activeNextConfig == task.installing {
					task.connectionBarrierReached = activeNextConfig
				}
				if task.task != nil {
					return task.task.tick()
				}
				return nil
			},
			eng.ConnectionManagerSubscriber: func() error {
				if activeNextConfig == task.installing {
					task.connectionManagerBarrierReached = activeNextConfig
				}
				if task.task != nil {
					return task.task.tick()
				}
				return nil
			},
		})
		remoteHosts := task.allHostsBarLocalHost(localHost, next)
		task.connectionManager.SetDesiredServers(localHost, remoteHosts)
	}

	task.shareGoalWithAll()
	return nil
}

// awaitBarrier2

type awaitBarrier2 struct {
	*targetConfig
	varBarrierReached *configuration.Configuration
	installing        *configuration.Configuration
}

func (task *awaitBarrier2) witness() topologyTask { return task }

func (task *awaitBarrier2) tick() error {
	next := task.active.Next()
	if !(next != nil && next.Version == task.config.Version && !task.active.NextBarrierReached2(task.connectionManager.RMId)) {
		return task.completed()
	}

	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	activeNextConfig := next.Configuration
	if activeNextConfig == task.varBarrierReached {
		// again, we use all new RMs as actives, and F+1 surviving as actives
		active, passive := task.partitionByActiveConnection(task.active.RMs())
		if len(active) <= len(passive) {
			log.Printf("Topology: Can not make progress at this time due to too many failures (failures: %v)",
				passive)
			return nil
		}
		fInc := ((len(active) + len(passive)) >> 1) + 1
		active, passive = active[:fInc], append(active[fInc:], passive...)

		newActive := next.NewRMIds
		for _, rmId := range newActive {
			if _, found := task.activeConnections[rmId]; !found {
				log.Printf("Topology: awaiting connections to new cluster members.")
				return nil
			}
		}
		active = append(newActive, active...)

		log.Printf("Topology: Barrier2 reached. Active: %v, Passive: %v", active, passive)

		topology := task.active.Clone()
		next = topology.Next()
		next.BarrierReached2 = append(next.BarrierReached2, task.connectionManager.RMId)

		_, resubmit, err := task.rewriteTopology(task.active, topology, active, passive)
		if err != nil {
			return task.fatal(err)
		}
		if resubmit {
			server.Log("Topology: Barrier2 reached. Requires resubmit.")
			task.enqueueTick(task)
		}

	} else if activeNextConfig != task.installing {
		task.installing = activeNextConfig
		task.varBarrierReached = nil
		task.installTopology(task.active, map[eng.TopologyChangeSubscriberType]func() error{
			eng.VarSubscriber: func() error {
				if activeNextConfig == task.installing {
					task.varBarrierReached = activeNextConfig
				}
				if task.task != nil {
					return task.task.tick()
				}
				return nil
			},
		})
		remoteHosts := task.allHostsBarLocalHost(localHost, next)
		task.connectionManager.SetDesiredServers(localHost, remoteHosts)
	}

	task.shareGoalWithAll()
	return nil
}

// migrate

type migrate struct {
	*targetConfig
	emigrator *emigrator
}

func (task *migrate) witness() topologyTask { return task.targetConfig.witness() }

func (task *migrate) tick() error {
	next := task.active.Next()
	if !(next != nil && next.Version == task.config.Version && len(next.Pending) > 0) {
		return task.completed()
	}

	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	remoteHosts := task.allHostsBarLocalHost(localHost, next)
	task.installTopology(task.active, nil)
	task.connectionManager.SetDesiredServers(localHost, remoteHosts)
	task.shareGoalWithAll()

	if task.isInRMs(task.active.RMs()) {
		// don't attempt any emigration unless we were in the old
		// topology
		task.ensureEmigrator()
	}

	if _, found := next.Pending[task.connectionManager.RMId]; !found {
		log.Println("Topology: All migration into all this RM completed. Awaiting others.")
		return nil
	}

	senders, found := task.migrations[next.Version]
	if !found {
		return nil
	}
	maxSuppliers := task.active.RMs().NonEmptyLen() - int(task.active.F)
	if task.isInRMs(task.active.RMs()) {
		// We were part of the old topology, so we have already supplied ourselves!
		maxSuppliers--
	}
	topology := task.active.Clone()
	next = topology.Next()
	changed := false
	for sender, inprogressPtr := range senders {
		if atomic.LoadInt32(inprogressPtr) == 0 {
			// Because we wait for locallyComplete, we know they've gone to disk.
			changed = next.Pending.SuppliedBy(task.connectionManager.RMId, sender, maxSuppliers) || changed
		}
	}
	if !changed {
		return nil
	}

	// By this point we only need the next RMs to form our
	// 2F+1. LostRMs are always passive now
	active, passive := task.partitionByActiveConnection(next.RMs())
	if len(active) <= len(passive) {
		// too many failures right now
		return nil
	}
	fInc := ((len(active) + len(passive)) >> 1) + 1
	active, passive = active[:fInc], append(active[fInc:], passive...)
	passive = append(passive, next.LostRMIds...)

	log.Printf("Topology: Marking local immigration completed. Active: %v, Passive: %v", active, passive)

	_, resubmit, err := task.rewriteTopology(task.active, topology, active, passive)
	if err != nil {
		return task.fatal(err)
	}
	if resubmit {
		task.enqueueTick(task)
		return nil
	}
	// Must be badread, which means again we should receive the
	// updated topology through the subscriber.
	return nil
}

func (task *migrate) abandon() {
	task.ensureStopEmigrator()
	task.targetConfig.abandon()
}

func (task *migrate) completed() error {
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

func (task *installCompletion) tick() error {
	next := task.active.Next()
	if next == nil {
		log.Println("Topology: completion installed")
		return task.completed()
	}

	if _, found := next.RMsRemoved()[task.connectionManager.RMId]; found {
		log.Println("Topology: we've been removed from cluster. Taking no further part.")
		return nil
	}

	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	task.installTopology(task.active, nil)
	remoteHosts := task.allHostsBarLocalHost(localHost, next)
	task.connectionManager.SetDesiredServers(localHost, remoteHosts)
	task.shareGoalWithAll()

	// As before, we use the new topology now and we only need to
	// include the lostRMIds as passives.
	active, passive := task.partitionByActiveConnection(next.RMs())
	if len(active) <= len(passive) {
		// too many failures right now
		return nil
	}
	fInc := ((len(active) + len(passive)) >> 1) + 1
	active, passive = active[:fInc], append(active[fInc:], passive...)
	passive = append(passive, next.LostRMIds...)

	topology := task.active.Clone()
	topology.SetConfiguration(next.Configuration)

	_, resubmit, err := task.rewriteTopology(task.active, topology, active, passive)
	if err != nil {
		return task.fatal(err)
	}
	if resubmit {
		task.enqueueTick(task)
		return nil
	}
	// Must be badread, which means again we should receive the
	// updated topology through the subscriber.
	return nil
}

// utils

func (task *targetConfig) createTopologyTransaction(read, write *configuration.Topology, active, passive common.RMIds) *msgs.Txn {
	if write == nil && read != nil {
		panic("Topology transaction with nil write and non-nil read not supported")
	}

	seg := capn.NewBuffer(nil)
	txn := msgs.NewTxn(seg)
	txn.SetSubmitter(uint32(task.connectionManager.RMId))
	txn.SetSubmitterBootCount(task.connectionManager.BootCount)

	actions := msgs.NewActionList(seg, 1)
	txn.SetActions(actions)
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
		refs := msgs.NewVarIdPosList(seg, 1)
		rw.SetReferences(refs)
		varIdPos := refs.At(0)
		varIdPos.SetId(write.Root.VarUUId[:])
		varIdPos.SetPositions((capn.UInt8List)(*write.Root.Positions))
	}

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

	if read == nil {
		txn.SetFInc(uint8(len(active)))
		txn.SetTopologyVersion(0)
	} else {
		txn.SetFInc(read.FInc)
		if next := read.Next(); next != nil {
			txn.SetTopologyVersion(next.Version)
		} else {
			txn.SetTopologyVersion(read.Version)
		}
	}

	return &txn
}

func (task *targetConfig) getTopologyFromLocalDatabase() (*configuration.Topology, error) {
	empty, err := task.connectionManager.Dispatchers.IsDatabaseEmpty()
	if empty || err != nil {
		return nil, err
	}

	for {
		txn := task.createTopologyTransaction(nil, nil, []common.RMId{task.connectionManager.RMId}, nil)

		result, err := task.localConnection.RunTransaction(txn, true, task.connectionManager.RMId)
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
			continue
		}
		abortUpdates := abort.Rerun()
		if abortUpdates.Len() != 1 {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave multiple updates")
		}
		update := abortUpdates.At(0)
		dbversion := common.MakeTxnId(update.TxnId())
		updateActions := update.Actions()
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
		var rootPtr *msgs.VarIdPos
		if refs := write.References(); refs.Len() == 1 {
			root := refs.At(0)
			rootPtr = &root
		}
		return configuration.TopologyFromCap(dbversion, rootPtr, write.Value())
	}
}

func (task *targetConfig) createTopologyZero(config *configuration.NextConfiguration) (*configuration.Topology, error) {
	topology := configuration.BlankTopology(config.ClusterId)
	topology.SetNext(config)
	txn := task.createTopologyTransaction(nil, topology, []common.RMId{task.connectionManager.RMId}, nil)
	txnId := topology.DBVersion
	txn.SetId(txnId[:])
	result, err := task.localConnection.RunTransaction(txn, false, task.connectionManager.RMId)
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

func (task *targetConfig) rewriteTopology(read, write *configuration.Topology, active, passive common.RMIds) (*configuration.Topology, bool, error) {
	txn := task.createTopologyTransaction(read, write, active, passive)

	result, err := task.localConnection.RunTransaction(txn, true, active...)
	if result == nil || err != nil {
		return nil, false, err
	}
	txnId := common.MakeTxnId(result.Txn().Id())
	if result.Which() == msgs.OUTCOME_COMMIT {
		topology := write.Clone()
		topology.DBVersion = txnId
		server.Log("Topology Txn Committed ok with txnId", topology.DBVersion)
		return topology, false, nil
	}
	abort := result.Abort()
	server.Log("Topology Txn Aborted", txnId)
	if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
		return nil, true, nil
	}
	abortUpdates := abort.Rerun()
	if abortUpdates.Len() != 1 {
		return nil, false,
			fmt.Errorf("Internal error: readwrite of topology gave %v updates (1 expected)",
				abortUpdates.Len())
	}
	update := abortUpdates.At(0)
	dbversion := common.MakeTxnId(update.TxnId())

	updateActions := update.Actions()
	if updateActions.Len() != 1 {
		return nil, false,
			fmt.Errorf("Internal error: readwrite of topology gave update with %v actions instead of 1!",
				updateActions.Len())
	}
	updateAction := updateActions.At(0)
	if !bytes.Equal(updateAction.VarId(), configuration.TopologyVarUUId[:]) {
		return nil, false,
			fmt.Errorf("Internal error: update action from readwrite of topology is not for topology! %v",
				common.MakeVarUUId(updateAction.VarId()))
	}
	if updateAction.Which() != msgs.ACTION_WRITE {
		return nil, false,
			fmt.Errorf("Internal error: update action from readwrite of topology gave non-write action!")
	}
	writeAction := updateAction.Write()
	var rootVarPos *msgs.VarIdPos
	if refs := writeAction.References(); refs.Len() == 1 {
		root := refs.At(0)
		rootVarPos = &root
	} else if refs.Len() > 1 {
		return nil, false,
			fmt.Errorf("Internal error: update action from readwrite of topology has %v references instead of 1!",
				refs.Len())
	}
	topology, err := configuration.TopologyFromCap(dbversion, rootVarPos, writeAction.Value())
	if err != nil {
		return nil, false, err
	}
	return topology, false, nil
}

func (task *targetConfig) attemptCreateRoot(topology *configuration.Topology) (bool, error) {
	twoFInc, fInc, f := int(topology.TwoFInc), int(topology.FInc), int(topology.F)
	active := make([]common.RMId, fInc)
	passive := make([]common.RMId, f)
	// this is valid only because root's positions are hardcoded
	nonEmpties := topology.RMs().NonEmpty()
	for _, rmId := range nonEmpties {
		if _, found := task.activeConnections[rmId]; !found {
			return false, nil
		}
	}
	copy(active, nonEmpties[:fInc])
	nonEmpties = nonEmpties[fInc:]
	copy(passive, nonEmpties[:f])

	server.Log("Topology: Creating Root. Actives:", active, "; Passives:", passive)

	seg := capn.NewBuffer(nil)
	txn := msgs.NewTxn(seg)
	txn.SetSubmitter(uint32(task.connectionManager.RMId))
	txn.SetSubmitterBootCount(task.connectionManager.BootCount)
	actions := msgs.NewActionList(seg, 1)
	txn.SetActions(actions)
	action := actions.At(0)
	vUUId := task.localConnection.NextVarUUId()
	action.SetVarId(vUUId[:])
	action.SetCreate()
	create := action.Create()
	positions := seg.NewUInt8List(int(topology.MaxRMCount))
	create.SetPositions(positions)
	for idx, l := 0, positions.Len(); idx < l; idx++ {
		positions.Set(idx, uint8(idx))
	}
	create.SetValue([]byte{})
	create.SetReferences(msgs.NewVarIdPosList(seg, 0))
	allocs := msgs.NewAllocationList(seg, twoFInc)
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
	txn.SetFInc(topology.FInc)
	txn.SetTopologyVersion(topology.Version)
	result, err := task.localConnection.RunTransaction(&txn, true, active...)
	if err != nil {
		return false, err
	}
	if result == nil { // shutdown
		return false, nil
	}
	if result.Which() == msgs.OUTCOME_COMMIT {
		server.Log("Topology: Root created in", vUUId)
		topology.Root.VarUUId = vUUId
		topology.Root.Positions = (*common.Positions)(&positions)
		return false, nil
	}
	abort := result.Abort()
	if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
		return true, nil
	}
	return false, fmt.Errorf("Internal error: creation of root gave rerun outcome")
}

// emigrator

type emigrator struct {
	stop              int32
	db                *db.Databases
	connectionManager *ConnectionManager
	activeBatches     map[common.RMId]*sendBatch
	topology          *configuration.Topology
	conns             map[common.RMId]paxos.Connection
}

func newEmigrator(task *migrate) *emigrator {
	e := &emigrator{
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

func (e *emigrator) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, conns map[common.RMId]paxos.Connection) {
	if rmId == e.connectionManager.RMId {
		return
	}
	e.conns = conns
	e.startBatches()
}

func (e *emigrator) startBatches() {
	pending := e.topology.Next().Pending
	batchConds := make([]*sendBatch, 0, len(pending))
	for rmId, cond := range pending {
		if rmId == e.connectionManager.RMId {
			continue
		}
		if _, found := e.activeBatches[rmId]; found {
			continue
		}
		if conn, found := e.conns[rmId]; found && e.topology.NextBarrierReached2(rmId) {
			log.Println("starting emigration batch for", rmId)
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
		emigrator: e,
		topology:  e.topology,
		batch:     batch,
	}
	go it.iterate()
}

type dbIterator struct {
	*emigrator
	topology *configuration.Topology
	batch    []*sendBatch
}

func (it *dbIterator) iterate() {
	_, err := it.db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		result, err := rtxn.WithCursor(it.db.Vars, func(cursor *mdbs.Cursor) interface{} {
			vUUIdBytes, varBytes, err := cursor.Get(nil, nil, mdb.FIRST)
			for ; err == nil; vUUIdBytes, varBytes, err = cursor.Get(nil, nil, mdb.NEXT) {
				seg, _, err := capn.ReadFromMemoryZeroCopy(varBytes)
				if err != nil {
					cursor.Error(err)
					return nil
				}
				varCap := msgs.ReadRootVar(seg)
				if bytes.Equal(varCap.Id(), configuration.TopologyVarUUId[:]) {
					continue
				}
				txnId := common.MakeTxnId(varCap.WriteTxnId())
				txnBytes := it.db.ReadTxnBytesFromDisk(cursor.RTxn, txnId)
				if txnBytes == nil {
					return nil
				}
				seg, _, err = capn.ReadFromMemoryZeroCopy(txnBytes)
				if err != nil {
					cursor.Error(err)
					return nil
				}
				txnCap := msgs.ReadRootTxn(seg)
				// So, we only need to send based on the vars that we have
				// (in fact, we require the positions so we can only look
				// at the vars we have). However, the txn var allocations
				// only cover what's assigned to us at the time of txn
				// creation and that can change and we don't rewrite the
				// txn when it changes. So that all just means we must
				// ignore the allocations here, and just work through the
				// actions directly.
				actions := txnCap.Actions()
				varCaps, err := it.filterVars(cursor, vUUIdBytes, txnId[:], &actions)
				if err != nil {
					return nil
				} else if len(varCaps) == 0 {
					continue
				}
				for _, sb := range it.batch {
					matchingVarCaps, err := it.matchVarsAgainstCond(sb.cond, varCaps)
					if err != nil {
						cursor.Error(err)
						return nil
					} else if len(matchingVarCaps) != 0 {
						sb.add(&txnCap, matchingVarCaps)
					}
				}
			}
			if err == mdb.NotFound {
				return nil
			} else {
				return err
			}
		})
		if err == nil {
			return result
		}
		return nil
	}).ResultError()
	if err != nil {
		log.Println("Topology iterator:", err)
	}
	for _, sb := range it.batch {
		sb.flush()
	}
	it.connectionManager.AddServerConnectionSubscriber(it)
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
		server.Log("Topology: Testing", common.MakeVarUUId(varCap.Id()), (*common.Positions)(&pos), "against condition", cond)
		if b, err := cond.SatisfiedBy(it.topology, (*common.Positions)(&pos)); err == nil && b {
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
	mc.SetVersion(it.topology.Next().Version)
	msg.SetMigrationComplete(mc)
	bites := server.SegToBytes(seg)

	for _, sb := range it.batch {
		if conn, found := conns[sb.conn.RMId()]; found && sb.conn == conn {
			// The connection has not changed since we started sending to
			// it (because we cached it, you can discount the issue of
			// memory reuse here - phew). Therefore, it's safe to send
			// the completion msg. If it has changed, we rely on the
			// ConnectionLost being called in the emigrator to do any
			// necessary tidying up.
			server.Log("Topology: Sending migration completion to", conn.RMId())
			conn.Send(bites)
		}
	}
}
func (it *dbIterator) ConnectionLost(common.RMId, map[common.RMId]paxos.Connection) {}
func (it *dbIterator) ConnectionEstablished(common.RMId, paxos.Connection, map[common.RMId]paxos.Connection) {
}

type sendBatch struct {
	version uint32
	conn    paxos.Connection
	cond    configuration.Cond
	elems   []*migrationElem
}

type migrationElem struct {
	txn  *msgs.Txn
	vars []*msgs.Var
}

func (e *emigrator) newBatch(conn paxos.Connection, cond configuration.Cond) *sendBatch {
	return &sendBatch{
		version: e.topology.Next().Version,
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
		elemCap := msgs.NewMigrationElement(seg)
		elemCap.SetTxn(*elem.txn)
		vars := msgs.NewVarList(seg, len(elem.vars))
		for idy, varCap := range elem.vars {
			vars.Set(idy, *varCap)
		}
		elemCap.SetVars(vars)
		elems.Set(idx, elemCap)
	}
	migration.SetElems(elems)
	msg.SetMigration(migration)
	bites := server.SegToBytes(seg)
	server.Log("Topology: Migrating", len(sb.elems), "txns to", sb.conn.RMId())
	sb.conn.Send(bites)
	sb.elems = sb.elems[:0]
}

func (sb *sendBatch) add(txnCap *msgs.Txn, varCaps []*msgs.Var) {
	elem := &migrationElem{
		txn:  txnCap,
		vars: varCaps,
	}
	sb.elems = append(sb.elems, elem)
	if len(sb.elems) == server.MigrationBatchElemCount {
		sb.flush()
	}
}
