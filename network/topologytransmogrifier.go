package network

import (
	"bytes"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	cc "github.com/msackman/chancell"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"log"
	"math/rand"
	"time"
)

type TopologyTransmogrifier struct {
	connectionManager *ConnectionManager
	localConnection   *client.LocalConnection
	active            *configuration.Topology
	proposersDrained  *configuration.Topology
	hostToConnection  map[string]paxos.Connection
	activeConnections map[common.RMId]paxos.Connection
	task              topologyTask
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(topologyTransmogrifierMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan topologyTransmogrifierMsg
	listenPort        uint16
	rng               *rand.Rand
	localEstablished  chan struct{}
}

type topologyTransmogrifierMsg interface {
	topologyTransmogrifierMsgWitness()
}

type topologyTransmogrifierMsgShutdown struct{}

func (ttms *topologyTransmogrifierMsgShutdown) topologyTransmogrifierMsgWitness() {}

var topologyTransmogrifierMsgShutdownInst = &topologyTransmogrifierMsgShutdown{}

func (tt *TopologyTransmogrifier) Shutdown() {
	if tt.enqueueQuery(topologyTransmogrifierMsgShutdownInst) {
		tt.cellTail.Wait()
	}
}

type topologyTransmogrifierMsgRequestConfigChange configuration.NextConfiguration

func (tt *TopologyTransmogrifier) RequestConfigurationChange(config *configuration.Configuration) {
	tt.enqueueQuery((*topologyTransmogrifierMsgRequestConfigChange)(
		&configuration.NextConfiguration{Configuration: config}))
}

func (ttmrcc *topologyTransmogrifierMsgRequestConfigChange) topologyTransmogrifierMsgWitness() {}

type topologyTransmogrifierMsgSetActiveConnections map[common.RMId]paxos.Connection

func (ttmsac topologyTransmogrifierMsgSetActiveConnections) topologyTransmogrifierMsgWitness() {}

type topologyTransmogrifierMsgTopologyObserved configuration.Topology

func (ttmvc *topologyTransmogrifierMsgTopologyObserved) topologyTransmogrifierMsgWitness() {}

type topologyTransmogrifierMsgProposersDrained configuration.Topology

func (ttmvc *topologyTransmogrifierMsgProposersDrained) topologyTransmogrifierMsgWitness() {}

type topologyTransmogrifierMsgExe func() error

func (ttme topologyTransmogrifierMsgExe) topologyTransmogrifierMsgWitness() {}

func (tt *TopologyTransmogrifier) enqueueQuery(msg topologyTransmogrifierMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return tt.enqueueQueryInner(msg, cell, f)
	}
	return tt.cellTail.WithCell(f)
}

func NewTopologyTransmogrifier(cm *ConnectionManager, lc *client.LocalConnection, listenPort uint16, config *configuration.Configuration) (*TopologyTransmogrifier, <-chan struct{}) {
	tt := &TopologyTransmogrifier{
		connectionManager: cm,
		localConnection:   lc,
		hostToConnection:  make(map[string]paxos.Connection),
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

	subscriberInstalled := make(chan struct{})
	cm.Dispatchers.VarDispatcher.ApplyToVar(func(v *eng.Var, err error) {
		if err != nil {
			panic(fmt.Errorf("Error trying to subscribe to topology: %v", err))
		}
		v.AddWriteSubscriber(configuration.VersionOne,
			func(v *eng.Var, value []byte, refs *msgs.VarIdPos_List, txn *eng.Txn) {
				var rootVarPosPtr *msgs.VarIdPos
				if refs.Len() > 0 {
					root := refs.At(0)
					rootVarPosPtr = &root
				}
				topology, err := configuration.TopologyFromCap(txn.Id, rootVarPosPtr, value)
				if err != nil {
					panic(fmt.Errorf("Unable to deserialize new topology: %v", err))
				}
				tt.enqueueQuery((*topologyTransmogrifierMsgTopologyObserved)(topology))
			})
		close(subscriberInstalled)
	}, true, configuration.TopologyVarUUId)
	<-subscriberInstalled

	cm.AddSender(tt)

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
			case *topologyTransmogrifierMsgShutdown:
				terminate = true
			case topologyTransmogrifierMsgSetActiveConnections:
				err = tt.activeConnectionsChange(msgT)
			case *topologyTransmogrifierMsgTopologyObserved:
				err = tt.setActive((*configuration.Topology)(msgT))
			case *topologyTransmogrifierMsgRequestConfigChange:
				tt.selectGoal((*configuration.NextConfiguration)(msgT))
			case topologyTransmogrifierMsgExe:
				err = msgT()
			case *topologyTransmogrifierMsgProposersDrained:
				tt.proposersDrained = (*configuration.Topology)(msgT)
				if tt.task != nil {
					err = tt.task.tick()
				}
			}
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	if err != nil {
		log.Println("TopologyTransmogrifier error:", err)
	}
	tt.connectionManager.RemoveSenderAsync(tt)
	tt.cellTail.Terminate()
}

func (tt *TopologyTransmogrifier) activeConnectionsChange(conns map[common.RMId]paxos.Connection) error {
	tt.activeConnections = conns

	for _, cd := range conns {
		host := cd.Host()
		tt.hostToConnection[host] = cd
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
			return fmt.Errorf("Fatal: config with ClusterId change from '%s' to '%s'.",
				tt.active.ClusterId, topology.ClusterId)

		case topology.Version < tt.active.Version:
			log.Printf("Ignoring config with version %v as newer version already active (%v).",
				topology.Version, tt.active.Version)
			return nil

		case tt.active.Configuration.Equal(topology.Configuration):
			// silently ignore it
			return nil
		}
	}

	var emptyFun func()
	if topology.Next() != nil {
		tt.proposersDrained = nil
		emptyFun = func() {
			tt.enqueueQuery((*topologyTransmogrifierMsgProposersDrained)(topology))
		}
	}
	tt.connectionManager.SetTopology(topology, emptyFun)
	if _, found := topology.RMsRemoved()[tt.connectionManager.RMId]; found {
		return errors.New("We have been removed from the cluster. Shutting down.")
	}
	if tt.active == nil && tt.localEstablished != nil {
		close(tt.localEstablished)
		tt.localEstablished = nil
	}
	tt.active = topology

	if tt.task != nil {
		if err := tt.task.tick(); err != nil {
			return err
		}
	}

	if tt.task == nil {
		if next := topology.Next(); next == nil {
			localHost, remoteHosts, err := tt.active.LocalRemoteHosts(tt.listenPort)
			if err != nil {
				return err
			}
			log.Printf(">==> We are %v (%v) <==<\n", localHost, tt.connectionManager.RMId)
			tt.connectionManager.SetDesiredServers(localHost, remoteHosts)

		} else {
			tt.selectGoal(next)
		}
	}
	return nil
}

func (tt *TopologyTransmogrifier) selectGoal(goal *configuration.NextConfiguration) {
	if tt.active != nil {
		switch {
		case goal.Version == 0:
			return // done.

		case goal.ClusterId != tt.active.ClusterId:
			log.Printf("Illegal config: ClusterId should be '%s' instead of '%s'.",
				tt.active.ClusterId, goal.ClusterId)
			return

		case goal.Version < tt.active.Version:
			log.Printf("Ignoring config with version %v as newer version already active (%v).",
				goal.Version, tt.active.Version)
			return

		case goal.Version == tt.active.Version:
			log.Printf("Config transition to version %v completed.", goal.Version)
			return
		}
	}

	if tt.task != nil {
		existingGoal := tt.task.goal()
		switch {
		case goal.ClusterId != existingGoal.ClusterId:
			log.Printf("Illegal config: ClusterId should be '%s' instead of '%s'.",
				existingGoal.ClusterId, goal.ClusterId)
			return

		case goal.Version < existingGoal.Version:
			log.Printf("Ignoring config with version %v as newer version already targetted (%v).",
				goal.Version, existingGoal.Version)
			return

		case goal.Version == existingGoal.Version:
			log.Printf("Config transition to version %v already in progress.", goal.Version)
			return // goal already in progress

		default:
			tt.task.abandon()
			tt.task = nil
		}
	}

	if tt.task == nil {
		tt.task = &targetConfig{
			TopologyTransmogrifier: tt,
			config:                 goal,
		}
	}
}

func (tt *TopologyTransmogrifier) enqueueTick(task topologyTask) {
	go func() {
		time.Sleep(time.Duration(tt.rng.Intn(int(server.SubmissionMaxSubmitDelay))))
		tt.enqueueQuery(topologyTransmogrifierMsgExe(func() error {
			if tt.task == task {
				return tt.task.tick()
			}
			return nil
		}))
	}()
}

// topologyTask

type topologyTask interface {
	tick() error
	abandon()
	goal() *configuration.NextConfiguration
}

// targetConfig

type targetConfig struct {
	*TopologyTransmogrifier
	config *configuration.NextConfiguration
}

func (task *targetConfig) tick() error {
	switch {
	case task.active == nil:
		log.Println("Ensuring local topology.")
		task.task = &ensureLocalTopology{task}
	case task.active.Version == 0:
		log.Printf("Attempting to join cluster with configuration: %v", task.config)
		task.task = &joinCluster{targetConfig: task}
	case task.active.Next() == nil || task.active.Next().Version < task.config.Version:
		log.Printf("Attempting to install topology change target: %v", task.config)
		task.task = &installTargetOld{targetConfig: task}
	case task.active.Next() != nil && task.active.Next().Version == task.config.Version && !task.active.Next().InstalledOnAll:
		log.Printf("Attempting to install topology change to new cluster: %v", task.config)
		task.task = &installTargetNew{targetConfig: task}
	case task.active.Next() != nil && task.active.Next().Version == task.config.Version:
		log.Printf("Attempting to perform object migration for topology target: %v", task.config)
		task.task = &migrate{targetConfig: task}
	default:
		return fmt.Errorf("Confused about what to do. Active topology is: %v; goal is %v",
			task.active, task.config)
	}
	return nil
}

func (task *targetConfig) abandon() {}

func (task *targetConfig) goal() *configuration.NextConfiguration {
	return task.config
}

func (task *targetConfig) fatal(err error) error {
	task.task = nil
	return err
}

func (task *targetConfig) error(err error) error {
	task.task = nil
	log.Println(err)
	return nil
}

func (task *targetConfig) completed() error {
	task.task = nil
	return nil
}

func (task *targetConfig) activeChange(active *configuration.Topology) error {
	return task.setActive(active)
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
		// It's already on disk, we're not going to see it through the observer.
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
		// for joining, it's fatal if we can't find ourself
		return task.fatal(err)
	}
	task.connectionManager.SetDesiredServers(localHost, remoteHosts)

	for _, host := range task.config.Hosts {
		if _, found := task.hostToConnection[host]; !found {
			return nil
		}
	}

	// Ok, we know who everyone is. Are we connected to them though?
	rmIds := make([]common.RMId, 0, len(task.config.Hosts))
	var rootId *common.VarUUId
	for _, host := range task.config.Hosts {
		if host == localHost {
			continue
		}
		cd, _ := task.hostToConnection[host]
		rmIds = append(rmIds, cd.RMId())
		switch remoteRootId := cd.RootId(); {
		case remoteRootId == nil:
			// they're joining too
		case rootId == nil:
			rootId = remoteRootId
		case rootId.Compare(remoteRootId) == common.EQ:
			// all good
		default:
			return task.fatal(
				errors.New("Attempt made to merge different logical clusters together, which is illegal. Aborting."))
		}
	}

	if allJoining := rootId == nil; allJoining {
		return task.allJoining(append(rmIds, task.connectionManager.RMId))

	} else {
		// If we're not allJoining then we need the previous config
		// because we need to make sure that everyone in the old config
		// learns of the change. Consider nodes that are being removed
		// in this new config. We have no idea who they are. If we
		// attempted to do discovery and run a txn that reads the
		// topology from the nodes in common between old and new, we run
		// the risk that we are asking < F+1 nodes for their opinion on
		// the old topology so we may get it wrong, and divergency could
		// happen. Therefore the _only_ safe thing to do is to punt the
		// topology change off to the nodes in common and allow them to
		// drive the change. We must wait until we're connected to one
		// of the oldies and then we ask them to do the config change
		// for us.

		seg := capn.NewBuffer(nil)
		msg := msgs.NewRootMessage(seg)
		msg.SetTopologyChangeRequest(task.config.AddToSegAutoRoot(seg))
		sender := paxos.NewRepeatingSender(server.SegToBytes(seg), rmIds...)
		task.connectionManager.AddSender(sender)

		log.Println("Requesting help from existing cluster members for topology change.")
		return task.completed()
	}
}

func (task *joinCluster) allJoining(allRMIds common.RMIds) error {
	targetTopology := configuration.NewTopology(task.active.DBVersion, nil, task.config.Configuration)
	targetTopology.SetRMs(allRMIds)

	switch resubmit, err := task.attemptCreateRoot(targetTopology); {
	case err != nil:
		return task.fatal(err)
	case resubmit:
		task.enqueueTick(task)
		return nil
	case targetTopology.Root.VarUUId == nil:
		// We failed; likely we need to wait for connections to change
		return nil
	}

	// Finally we need to rewrite the topology. For allJoining, we
	// must use everyone as active. This is because we could have
	// seen one of our peers when it had no RootId, but we've since
	// lost that connection and in fact that peer has gone off and
	// joined another cluster. So the only way to be instantaneously
	// sure that all peers are empty and moving to the same topology
	// is to have all peers as active.

	_, resubmit, err := task.rewriteTopology(task.active, targetTopology, allRMIds, nil)
	if err != nil {
		return task.fatal(err)
	}
	if resubmit {
		task.enqueueTick(task)
		return nil
	}
	// !resubmit, so MUST be a BadRead, or success. By definition,
	// if allJoining, everyone is active. So even if we weren't
	// successful rewriting ourself, we're guaranteed to be sent
	// someone else's write through the observer.
	return nil
}

// installTargetOld
// Purpose is to do a txn using the current topology in which we set
// topology.Next to be the target topology.

type installTargetOld struct {
	*targetConfig
}

func (task *installTargetOld) tick() error {
	if next := task.active.Next(); !(next == nil || next.Version < task.config.Version) {
		return task.completed()
	}

	localHost, targetTopology, err := task.calculateTargetTopology()
	if err != nil || targetTopology == nil {
		return err
	}
	log.Printf("Calculated target topology: %v", targetTopology.Next())

	active, passive := task.chooseRMIdsForTopology(localHost, task.active)
	if active == nil {
		return nil
	}

	_, resubmit, err := task.rewriteTopology(task.active, targetTopology, active, passive)
	if err != nil {
		return task.fatal(err)
	}
	if resubmit {
		task.enqueueTick(task)
		return nil
	}
	// Must be badread, which means again we should receive the
	// updated topology through the observer.
	return nil
}

func (task *installTargetOld) calculateTargetTopology() (string, *configuration.Topology, error) {
	localHost, _, err := task.active.LocalRemoteHosts(task.listenPort)
	if err != nil {
		// this calc is being done off the active. So it is fatal if we
		// can't find ourself.
		return "", nil, task.fatal(err)
	}

	hostsSurvived, hostsRemoved, hostsAdded :=
		make(map[string]server.EmptyStruct),
		make(map[string]server.EmptyStruct),
		make(map[string]server.EmptyStruct)

	allRemoteHosts := make([]string, 0, len(task.active.Hosts)+len(task.config.Hosts))

	for _, host := range task.active.Hosts {
		hostsRemoved[host] = server.EmptyStructVal
		if host != localHost {
			allRemoteHosts = append(allRemoteHosts, host)
		}
	}

	for _, host := range task.config.Hosts {
		if _, found := hostsRemoved[host]; found {
			hostsSurvived[host] = server.EmptyStructVal
			delete(hostsRemoved, host)
		} else {
			hostsAdded[host] = server.EmptyStructVal
			if host != localHost {
				allRemoteHosts = append(allRemoteHosts, host)
			}
		}
	}
	task.connectionManager.SetDesiredServers(localHost, allRemoteHosts)
	for _, host := range allRemoteHosts {
		if _, found := task.hostToConnection[host]; !found {
			return "", nil, nil
		}
	}

	rmIdsSurvived, rmIdsAdded, rmIdsRemoved :=
		make(map[common.RMId]server.EmptyStruct),
		make(map[common.RMId]server.EmptyStruct),
		make(map[common.RMId]server.EmptyStruct)

	// ok, we've figured out changes to hosts, but we also need to xref
	// with RMIds because we could have a host that has changed RMId
	// (i.e. it's been wiped and re-added).
	for _, rmId := range task.active.RMs() {
		rmIdsRemoved[rmId] = server.EmptyStructVal
	}
	var rmId common.RMId
	for host := range hostsSurvived {
		rmId = task.hostToConnection[host].RMId()
		if _, found := rmIdsRemoved[rmId]; found {
			// the rmId of host hasn't changed
			rmIdsSurvived[rmId] = server.EmptyStructVal
			delete(rmIdsRemoved, rmId)
		} else {
			// the rmId of host must have changed
			delete(hostsSurvived, host)
			hostsAdded[host] = server.EmptyStructVal
			hostsRemoved[host] = server.EmptyStructVal
			rmIdsAdded[rmId] = server.EmptyStructVal
		}
	}
	// removed and added are much simpler
	for host := range hostsRemoved {
		rmId = task.hostToConnection[host].RMId()
		rmIdsRemoved[rmId] = server.EmptyStructVal
	}
	for host := range hostsAdded {
		rmId = task.hostToConnection[host].RMId()
		rmIdsAdded[rmId] = server.EmptyStructVal
	}

	rmIdsNew := make([]common.RMId, 0, len(allRemoteHosts)+1)
	for _, rmId := range task.active.RMs() {
		if _, found := rmIdsSurvived[rmId]; found {
			rmIdsNew = append(rmIdsNew, rmId)
		} else {
			rmIdsNew = append(rmIdsNew, common.RMIdEmpty)
		}
	}
	idx := 0
	for rmIdAdded := range rmIdsAdded {
		for ; idx < len(rmIdsNew) && rmIdsNew[idx] != common.RMIdEmpty; idx++ {
		}
		if idx == len(rmIdsNew) {
			rmIdsNew = append(rmIdsNew, rmIdAdded)
		} else {
			rmIdsNew[idx] = rmIdAdded
		}
	}
	targetTopology := task.active.Clone()
	next := task.config.Clone()
	next.SetRMs(rmIdsNew)
	// pointer semantics, so we need to copy into our new set
	alreadyRemoved := targetTopology.RMsRemoved()
	for rmId := range alreadyRemoved {
		rmIdsRemoved[rmId] = server.EmptyStructVal
	}
	next.SetRMsRemoved(rmIdsRemoved)
	targetTopology.SetNext(&configuration.NextConfiguration{Configuration: next})

	return localHost, targetTopology, nil
}

// installTargetNew
// Now that everyone in the old/current topology knows about the Next
// topology, we need to do a further txn to ensure everyone new who's
// joining the cluster gets told.

type installTargetNew struct {
	*targetConfig
}

func (task *installTargetNew) tick() error {
	if next := task.active.Next(); !(next != nil && next.Version == task.config.Version && !next.InstalledOnAll) {
		return task.completed()
	}

	localHost, remoteHosts, err := task.active.LocalRemoteHosts(task.listenPort)
	if err != nil {
		// again, fatal if we can't find ourself.
		return task.fatal(err)
	}
	remoteHostsMap := make(map[string]server.EmptyStruct, len(remoteHosts))
	for _, host := range remoteHosts {
		remoteHostsMap[host] = server.EmptyStructVal
	}
	localHostNew, _, _ := task.config.LocalRemoteHosts(task.listenPort)
	for _, host := range task.config.Hosts {
		if host == localHostNew {
			continue
		}
		if _, found := remoteHostsMap[host]; !found {
			remoteHosts = append(remoteHosts, host)
			remoteHostsMap[host] = server.EmptyStructVal
		}
	}
	task.connectionManager.SetDesiredServers(localHost, remoteHosts)
	for _, host := range remoteHosts {
		if _, found := task.hostToConnection[host]; !found {
			return nil
		}
	}

	twoFInc := len(remoteHosts) + 1
	fInc := (twoFInc >> 1) + 1
	f := twoFInc - fInc

	active := make([]common.RMId, 0, fInc)
	passive := make([]common.RMId, 0, f)
	idx := 0
	curRMs, nextRMs := task.active.RMs(), task.active.Next().RMs()
	for ; idx < len(curRMs) && idx < len(nextRMs); idx++ {
		cur, next := curRMs[idx], nextRMs[idx]
		switch {
		case next == common.RMIdEmpty && cur == common.RMIdEmpty:
			// do nothing
		case next == common.RMIdEmpty || cur == next:
			if _, found := task.activeConnections[cur]; found && len(active) < cap(active) {
				active = append(active, cur)
			} else if len(passive) < cap(passive) {
				passive = append(passive, cur)
			} else { // not found in activeConnections, and passive is full
				return nil
			}
		default:
			// cur has been removed, and next has been added; or cur is empty
			if len(passive) < cap(passive) {
				passive = append(passive, next)
			} else { // not found in activeConnections, and passive is full
				return nil
			}
		}
	}
	if idx < len(curRMs) {
		// next is shorter; need to still process the end of cur
		for _, rmId := range curRMs[idx:].NonEmpty() {
			if _, found := task.activeConnections[rmId]; found && len(active) < cap(active) {
				active = append(active, rmId)
			} else if len(passive) < cap(passive) {
				passive = append(passive, rmId)
			} else { // not found in activeConnections, and passive is full
				return nil
			}
		}
	} else if idx < len(nextRMs) {
		// next is longer; need to still process end of next
		for _, rmId := range nextRMs[idx:].NonEmpty() {
			if len(passive) < cap(passive) {
				passive = append(passive, rmId)
			} else { // not found in activeConnections, and passive is full
				return nil
			}
		}
	}

	if len(active) < cap(active) || len(passive) < cap(passive) {
		return nil
	}
	log.Printf("Extending topology. Actives: %v; Passives: %v", active, passive)
	topology := task.active.Clone()
	topology.Next().InstalledOnAll = true

	_, resubmit, err := task.rewriteTopology(task.active, topology, active, passive)
	if err != nil {
		return task.fatal(err)
	}
	if resubmit {
		task.enqueueTick(task)
	}
	return nil

}

type migrate struct {
	*targetConfig
}

func (task *migrate) tick() error {
	if !(task.active.Next() != nil && task.active.Next().Version == task.config.Version) {
		return task.completed()
	}
	if task.proposersDrained != nil && task.proposersDrained.Configuration.Equal(task.active.Configuration) {
		log.Println("Proporsers drained. Starting migration.")
	}
	return nil
}

// utils

func (tt *TopologyTransmogrifier) createTopologyTransaction(read, write *configuration.Topology, active, passive common.RMIds) *msgs.Txn {
	if write == nil && read != nil {
		panic("Topology transaction with nil write and non-nil read not supported")
	}

	seg := capn.NewBuffer(nil)
	txn := msgs.NewTxn(seg)
	txn.SetSubmitter(uint32(tt.connectionManager.RMId))
	txn.SetSubmitterBootCount(tt.connectionManager.BootCount)

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
				alloc.SetActive(tt.activeConnections[rmId].BootCount())
			} else {
				alloc.SetActive(0)
			}
			indices := seg.NewUInt16List(1)
			alloc.SetActionIndices(indices)
			indices.Set(0, 0)
		}
		offset += len(rmIds)
	}

	txn.SetFInc(uint8(len(active)))
	if read == nil {
		txn.SetTopologyVersion(0)
	} else if read.Next() == nil {
		txn.SetTopologyVersion(read.Version)
	} else {
		txn.SetTopologyVersion(read.Next().Version)
	}

	return &txn
}

func (tt *TopologyTransmogrifier) getTopologyFromLocalDatabase() (*configuration.Topology, error) {
	empty, err := tt.connectionManager.Dispatchers.IsDatabaseEmpty()
	if empty || err != nil {
		return nil, err
	}

	for {
		txn := tt.createTopologyTransaction(nil, nil, []common.RMId{tt.connectionManager.RMId}, nil)

		result, err := tt.localConnection.RunTransaction(txn, true, tt.connectionManager.RMId)
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

func (tt *TopologyTransmogrifier) createTopologyZero(config *configuration.NextConfiguration) (*configuration.Topology, error) {
	topology := configuration.BlankTopology(config.ClusterId)
	topology.SetNext(config)
	txn := tt.createTopologyTransaction(nil, topology, []common.RMId{tt.connectionManager.RMId}, nil)
	txnId := topology.DBVersion
	txn.SetId(txnId[:])
	result, err := tt.localConnection.RunTransaction(txn, false, tt.connectionManager.RMId)
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

func (tt *TopologyTransmogrifier) rewriteTopology(read, write *configuration.Topology, active, passive common.RMIds) (*configuration.Topology, bool, error) {
	txn := tt.createTopologyTransaction(read, write, active, passive)

	result, err := tt.localConnection.RunTransaction(txn, true, active...)
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

func (tt *TopologyTransmogrifier) chooseRMIdsForTopology(localHost string, topology *configuration.Topology) ([]common.RMId, []common.RMId) {
	twoFInc := len(topology.Hosts)
	fInc := (twoFInc >> 1) + 1
	f := twoFInc - fInc
	if len(tt.hostToConnection) < twoFInc {
		return nil, nil
	}
	active := make([]common.RMId, 0, fInc)
	passive := make([]common.RMId, 0, f)
	for _, rmId := range topology.RMs().NonEmpty() {
		if _, found := tt.activeConnections[rmId]; found && len(active) < cap(active) {
			active = append(active, rmId)
		} else if len(passive) < cap(passive) {
			passive = append(passive, rmId)
		} else { // not found in activeConnections, and passive is full
			return nil, nil
		}
	}
	if len(active) < cap(active) || len(passive) < cap(passive) {
		return nil, nil
	}
	return active, passive
}

func (tt *TopologyTransmogrifier) attemptCreateRoot(topology *configuration.Topology) (bool, error) {
	twoFInc, fInc, f := int(topology.TwoFInc), int(topology.FInc), int(topology.F)
	active := make([]common.RMId, fInc)
	passive := make([]common.RMId, f)
	// this is valid only because root's positions are hardcoded
	nonEmpties := topology.RMs().NonEmpty()
	for _, rmId := range nonEmpties {
		if _, found := tt.activeConnections[rmId]; !found {
			return false, nil
		}
	}
	copy(active, nonEmpties[:fInc])
	nonEmpties = nonEmpties[fInc:]
	copy(passive, nonEmpties[:f])

	server.Log("Creating Root. Actives:", active, "; Passives:", passive)

	seg := capn.NewBuffer(nil)
	txn := msgs.NewTxn(seg)
	txn.SetSubmitter(uint32(tt.connectionManager.RMId))
	txn.SetSubmitterBootCount(tt.connectionManager.BootCount)
	actions := msgs.NewActionList(seg, 1)
	txn.SetActions(actions)
	action := actions.At(0)
	vUUId := tt.localConnection.NextVarUUId()
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
				alloc.SetActive(tt.activeConnections[rmId].BootCount())
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
	result, err := tt.localConnection.RunTransaction(&txn, true, active...)
	if err != nil {
		return false, err
	}
	if result == nil { // shutdown
		return false, nil
	}
	if result.Which() == msgs.OUTCOME_COMMIT {
		server.Log("Root created in", vUUId)
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
