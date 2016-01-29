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
	activeTopology    *configuration.Topology
	hostRMIds         map[string]*rmIdAndRootId
	activeConnections map[common.RMId]paxos.Connection
	tasks             []topologyTask
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(topologyTransmogrifierMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan topologyTransmogrifierMsg
	listenPort        uint16
	rng               *rand.Rand
}

type rmIdAndRootId struct {
	rmId   common.RMId
	rootId *common.VarUUId
}

type topologyTransmogrifierMsg interface {
	topologyTransmogrifierMsgWitness()
}

type topologyTransmogrifierMsgShutdown struct{}

func (ttms *topologyTransmogrifierMsgShutdown) topologyTransmogrifierMsgWitness() {}

var topologyTransmogrifierMsgShutdownInst = &topologyTransmogrifierMsgShutdown{}

type topologyTransmogrifierMsgSetActiveConnections map[common.RMId]paxos.Connection

func (ttmsac topologyTransmogrifierMsgSetActiveConnections) topologyTransmogrifierMsgWitness() {}

type topologyTransmogrifierMsgVarChanged configuration.Topology

func (ttmvc *topologyTransmogrifierMsgVarChanged) topologyTransmogrifierMsgWitness() {}

type topologyTransmogrifierMsgRequestConfigChange struct {
	config  *configuration.Configuration
	errChan chan error
}

func (ttmrcc *topologyTransmogrifierMsgRequestConfigChange) topologyTransmogrifierMsgWitness() {}

func (tt *TopologyTransmogrifier) Shutdown() {
	if tt.enqueueQuery(topologyTransmogrifierMsgShutdownInst) {
		tt.cellTail.Wait()
	}
}

func (tt *TopologyTransmogrifier) RequestConfigurationChange(config *configuration.Configuration) chan error {
	errChan := make(chan error, 1)
	tt.enqueueQuery(&topologyTransmogrifierMsgRequestConfigChange{
		config:  config,
		errChan: errChan,
	})
	return errChan
}

func (tt *TopologyTransmogrifier) enqueueQuery(msg topologyTransmogrifierMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return tt.enqueueQueryInner(msg, cell, f)
	}
	return tt.cellTail.WithCell(f)
}

func NewTopologyTransmogrifier(cm *ConnectionManager, lc *client.LocalConnection, clusterId string, listenPort uint16) (*TopologyTransmogrifier, func() error) {
	tt := &TopologyTransmogrifier{
		connectionManager: cm,
		localConnection:   lc,
		hostRMIds:         make(map[string]*rmIdAndRootId),
		listenPort:        listenPort,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
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
				if refs.Len() == 1 {
					root := refs.At(0)
					rootVarPosPtr = &root
				}
				topology, err := configuration.TopologyFromCap(txn.Id, rootVarPosPtr, value)
				if err != nil {
					panic(fmt.Errorf("Unable to deserialize new topology: %v", err))
				}
				tt.enqueueQuery((*topologyTransmogrifierMsgVarChanged)(topology))
			})
		close(subscriberInstalled)
	}, true, configuration.TopologyVarUUId)

	<-subscriberInstalled

	established := make(chan error)
	tt.enqueueQuery(&ensureLocalTopology{
		TopologyTransmogrifier: tt,
		clusterId:              clusterId,
		established:            established,
	})

	cm.AddSender(tt)
	go tt.actorLoop(head)
	return tt, func() error {
		return <-established
	}
}

func (tt *TopologyTransmogrifier) actorLoop(head *cc.ChanCellHead) {
	var (
		err       error
		queryChan <-chan topologyTransmogrifierMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = tt.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		if msg, ok := <-queryChan; ok {
			switch msgT := msg.(type) {
			case *topologyTransmogrifierMsgShutdown:
				terminate = true
			case topologyTransmogrifierMsgSetActiveConnections:
				err = tt.activeConnectionsChange(msgT)
			case *topologyTransmogrifierMsgVarChanged:
				err = tt.topologyVarChanged((*configuration.Topology)(msgT))
			case *topologyTransmogrifierMsgRequestConfigChange:
				err = tt.initiateChange(msgT.config, msgT.errChan)
			case topologyTask:
				tt.tasks = append(tt.tasks, msgT)
				if len(tt.tasks) == 1 {
					err = tt.tasks[0].start()
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

	for rmId, conn := range conns {
		host := conn.Host()
		if rmIdRoot, found := tt.hostRMIds[host]; found {
			rmIdRoot.rmId = rmId
			rmIdRoot.rootId = conn.RootId()
		} else {
			tt.hostRMIds[host] = &rmIdAndRootId{
				rmId:   rmId,
				rootId: conn.RootId(),
			}
		}
	}

	if len(tt.tasks) == 0 {
		return nil
	} else {
		return tt.tasks[0].activeConnectionsChange(conns)
	}
}

// Observer from the db itself.
func (tt *TopologyTransmogrifier) topologyVarChanged(topology *configuration.Topology) error {
	if topology.Version > tt.activeTopology.Version {
		if len(tt.tasks) == 0 {
			log.Println("TODO: unhandled unexpected topology change")
			return nil
		} else {
			return tt.tasks[0].topologyVarChanged(topology)
		}
	}
	return nil
}

// from local command line
func (tt *TopologyTransmogrifier) initiateChange(config *configuration.Configuration, errChan chan error) error {
	for _, task := range tt.tasks {
		if accomodated, confErr, fatalErr := task.accomodateTarget(config, errChan); fatalErr != nil {
			errChan <- confErr
			close(errChan)
			return fatalErr
		} else if confErr != nil {
			log.Printf("Ignoring requested changed due to problem with configuration: %v", confErr)
			errChan <- confErr
			close(errChan)
			return nil
		} else if accomodated {
			return nil
		}
	}
	tt.tasks = append(tt.tasks, &topologyChange{TopologyTransmogrifier: tt, config: config, errChan: errChan})
	if len(tt.tasks) == 1 {
		return tt.tasks[0].start()
	}
	return nil
}

func (tt *TopologyTransmogrifier) taskCompleted(topology *configuration.Topology, setServers bool) error {
	if topology == nil {
		return errors.New("Task completed with nil topology.")
	}
	if topology != tt.activeTopology {
		if _, found := topology.RMsRemoved()[tt.connectionManager.RMId]; found {
			return errors.New("We have been removed from the topology. Shutting down.")
		}
		tt.activeTopology = topology
		tt.connectionManager.SetTopology(topology)
		if setServers {
			localHost, remoteHosts, err := topology.LocalRemoteHosts(tt.listenPort)
			if err != nil {
				return err
			}
			log.Printf(">==> We are %v (%v) <==<\n", localHost, tt.connectionManager.RMId)
			tt.connectionManager.SetDesiredServers(localHost, remoteHosts)
		}
	}

	if len(tt.tasks) == 1 {
		tt.tasks = nil
		return nil
	} else {
		tt.tasks = tt.tasks[1:]
		return tt.tasks[0].start()
	}
}

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
	} else {
		txn.SetTopologyVersion(read.Version)
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

func (tt *TopologyTransmogrifier) createTopologyZero(clusterId string) (*configuration.Topology, error) {
	topology := configuration.BlankTopology(clusterId)
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
		return nil, false, fmt.Errorf("Internal error: readwrite of topology gave %v updates (1 expected)",
			abortUpdates.Len())
	}
	update := abortUpdates.At(0)
	dbversion := common.MakeTxnId(update.TxnId())

	updateActions := update.Actions()
	if updateActions.Len() != 1 {
		return nil, false, fmt.Errorf("Internal error: readwrite of topology gave update with %v actions instead of 1!",
			updateActions.Len())
	}
	updateAction := updateActions.At(0)
	if !bytes.Equal(updateAction.VarId(), configuration.TopologyVarUUId[:]) {
		return nil, false, fmt.Errorf("Internal error: update action from readwrite of topology is not for topology! %v",
			common.MakeVarUUId(updateAction.VarId()))
	}
	if updateAction.Which() != msgs.ACTION_WRITE {
		return nil, false, fmt.Errorf("Internal error: update action from readwrite of topology gave non-write action!")
	}
	writeAction := updateAction.Write()
	var rootVarPos *msgs.VarIdPos
	if refs := writeAction.References(); refs.Len() == 1 {
		root := refs.At(0)
		rootVarPos = &root
	} else if refs.Len() > 1 {
		return nil, false, fmt.Errorf("Internal error: update action from readwrite of topology has %v references instead of 1!",
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
	if len(tt.hostRMIds) < twoFInc {
		return nil, nil
	}
	active := make([]common.RMId, 0, fInc)
	passive := make([]common.RMId, 0, f)
	for _, host := range topology.Hosts {
		if rmIdRoot, found := tt.hostRMIds[host]; found {
			rmId := rmIdRoot.rmId
			if _, found := tt.activeConnections[rmId]; found && len(active) < cap(active) {
				active = append(active, rmId)
			} else if len(passive) < cap(passive) {
				passive = append(passive, rmId)
			} else { // not found in activeConnections, and passive is full
				return nil, nil
			}
		} else if host == localHost {
			if len(active) < cap(active) {
				active = append(active, tt.connectionManager.RMId)
			} else {
				passive = append(passive, tt.connectionManager.RMId)
			}
		} else {
			return nil, nil
		}
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

func (tt *TopologyTransmogrifier) ConnectedRMs(conns map[common.RMId]paxos.Connection) {
	tt.enqueueQuery(topologyTransmogrifierMsgSetActiveConnections(conns))
}

func (tt *TopologyTransmogrifier) ConnectionLost(rmId common.RMId, conns map[common.RMId]paxos.Connection) {
	tt.enqueueQuery(topologyTransmogrifierMsgSetActiveConnections(conns))
}

func (tt *TopologyTransmogrifier) ConnectionEstablished(rmId common.RMId, conn paxos.Connection, conns map[common.RMId]paxos.Connection) {
	tt.enqueueQuery(topologyTransmogrifierMsgSetActiveConnections(conns))
}

// topologyTask

type topologyTask interface {
	start() error
	topologyVarChanged(topology *configuration.Topology) error
	activeConnectionsChange(conns map[common.RMId]paxos.Connection) error
	accomodateTarget(config *configuration.Configuration, errChan chan error) (bool, error, error)
	topologyTransmogrifierMsg
}

// ensureLocalTopology

type ensureLocalTopology struct {
	*TopologyTransmogrifier
	clusterId              string
	startOnLocalConnection bool
	established            chan error
}

func (task *ensureLocalTopology) start() error {
	if _, found := task.activeConnections[task.connectionManager.RMId]; !found {
		task.startOnLocalConnection = true
		return nil
	}

	topology, err := task.getTopologyFromLocalDatabase()
	if err != nil {
		task.established <- err
		return err
	}

	if (topology == nil || topology.Version == 0) && task.clusterId == "" {
		err := errors.New("No configuration supplied and no configuration found in local store. Cannot continue")
		task.established <- err
		return err

	} else if topology == nil {
		topology, err = task.createTopologyZero(task.clusterId)
		if err != nil {
			task.established <- err
			return err
		}
		close(task.established)
		return task.taskCompleted(topology, false)

	} else {
		close(task.established)
		return task.taskCompleted(topology, topology.Version > 0)
	}
}

func (task *ensureLocalTopology) topologyVarChanged(topology *configuration.Topology) error {
	// This should be impossible: we don't have any desired connections
	// and the listener shouldn't be active yet.
	return nil
}

func (task *ensureLocalTopology) activeConnectionsChange(conns map[common.RMId]paxos.Connection) error {
	if task.startOnLocalConnection {
		task.startOnLocalConnection = false
		return task.start()
	}
	return nil
}

func (task *ensureLocalTopology) accomodateTarget(config *configuration.Configuration, errChan chan error) (bool, error, error) {
	return false, nil, nil
}

func (task *ensureLocalTopology) topologyTransmogrifierMsgWitness() {}

// topologyChange

type topologyChange struct {
	*TopologyTransmogrifier
	config  *configuration.Configuration
	errChan chan error
}

func (task *topologyChange) error(err error) error {
	if task.errChan != nil {
		if err != nil {
			task.errChan <- err
		}
		close(task.errChan)
		task.errChan = nil
	}
	return err
}

func (task *topologyChange) start() error {
	if task.activeTopology == nil {
		return task.error(errors.New("Internal logic failure: config change started with nil activeTopology"))
	}
	if task.activeTopology.ClusterId != task.config.ClusterId {
		err := fmt.Errorf("Ignoring supplied config due to incorrect ClusterId: should be '%v'; is '%v'",
			task.activeTopology.ClusterId, task.config.ClusterId)
		task.error(err)
		return task.taskCompleted(task.activeTopology, true)
	}
	if task.activeTopology.Version >= task.config.Version {
		err := fmt.Errorf("Ignoring supplied config as version is not greater than current active version (%v)",
			task.activeTopology.Version)
		task.error(err)
		return task.taskCompleted(task.activeTopology, true)
	}
	log.Printf("Attempting to transition to %v", task.config)
	if task.activeTopology.Version == 0 {
		task.tasks[0] = &joinCluster{topologyChange: task}
	} else {
		task.tasks[0] = &modifyCluster{task}
	}
	return task.tasks[0].start()
}

func (task *topologyChange) topologyVarChanged(topology *configuration.Topology) error {
	// cannot be here because as soon as task starts, it will change to something else
	return nil
}

func (task *topologyChange) activeConnectionsChange(conns map[common.RMId]paxos.Connection) error {
	// cannot be here because as soon as task starts, it will change to something else
	return nil
}

func (task *topologyChange) accomodateTarget(config *configuration.Configuration, errChan chan error) (bool, error, error) {
	// The only way we're here is if we're queued up waiting to start.
	if task.activeTopology != nil {
		if task.activeTopology.ClusterId != config.ClusterId {
			return false, fmt.Errorf("Incorrect ClusterId: should be '%v'; is '%v'",
				task.activeTopology.ClusterId, config.ClusterId), nil
		}
		if task.activeTopology.Version >= config.Version {
			return false, fmt.Errorf("Version (%v) is not greater than current active version (%v)",
				config.Version, task.activeTopology.Version), nil
		}
	}
	if task.config.ClusterId != config.ClusterId {
		return false, fmt.Errorf("Incorrect ClusterId: should be '%v'; is '%v'",
			task.activeTopology.ClusterId, config.ClusterId), nil
	}
	if task.config.Version >= config.Version {
		return false, fmt.Errorf("Version (%v) is not greater than current scheduled version (%v)",
			config.Version, task.config.Version), nil
	}
	task.error(nil)
	task.config = config
	task.errChan = errChan
	return true, nil, nil
}

func (task *topologyChange) topologyTransmogrifierMsgWitness() {}

// joinCluster

type joinCluster struct {
	*topologyChange
	targetTopology               *configuration.Topology
	localHost                    string
	maybeBeginOnConnectionChange bool
}

func (task *joinCluster) start() error {
	localHost, remoteHosts, err := task.config.LocalRemoteHosts(task.listenPort)
	if err != nil {
		return task.error(err)
	}
	task.localHost = localHost
	task.connectionManager.SetDesiredServers(localHost, remoteHosts)

	return task.maybeBegin()
}

func (task *joinCluster) maybeBegin() error {
	for _, host := range task.config.Hosts {
		if host == task.localHost {
			continue
		}
		if _, found := task.hostRMIds[host]; !found {
			task.maybeBeginOnConnectionChange = true
			return nil
		}
	}

	// Ok, we know who everyone is. Are we connected to them though?
	rmIds := make([]common.RMId, 0, len(task.config.Hosts))
	var rootId *common.VarUUId
	for _, host := range task.config.Hosts {
		if host == task.localHost {
			continue
		}
		rmIdRoot, _ := task.hostRMIds[host]
		rmIds = append(rmIds, rmIdRoot.rmId)
		switch remoteRootId := rmIdRoot.rootId; {
		case remoteRootId == nil:
			// they're joining too
		case rootId == nil:
			rootId = remoteRootId
		case rootId.Equal(remoteRootId):
			// all good
		default:
			err := errors.New("Attempt made to merge different logical clusters together, which is illegal. Aborting.")
			return task.error(err)
		}
	}

	allJoining := rootId == nil

	if allJoining {
		task.allJoining(append(rmIds, task.connectionManager.RMId))

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

		fmt.Println("Hello 1")
	}

	return nil
}

func (task *joinCluster) allJoining(allRMIds common.RMIds) error {
	if task.targetTopology == nil {
		targetTopology := configuration.NewTopology(task.activeTopology.DBVersion, nil, task.config)
		targetTopology.SetRMs(allRMIds)
	Outer:
		for {
			switch resubmit, err := task.attemptCreateRoot(targetTopology); {
			case err != nil:
				return task.error(err)
			case resubmit:
				time.Sleep(time.Duration(task.rng.Intn(int(server.SubmissionMaxSubmitDelay))))
				continue
			case targetTopology.Root.VarUUId == nil:
				// We failed; likely we need to wait for connections to change
				task.maybeBeginOnConnectionChange = true
				return nil
			default:
				task.targetTopology = targetTopology
				break Outer
			}
		}
	}

	// Finally we need to rewrite the topology. For allJoining, we
	// must use everyone as active. This is because we could have
	// seen one of our peers when it had no RootId, but we've since
	// lost that connection and in fact that peer has gone off and
	// joined another cluster. So the only way to be instantaneously
	// sure that all peers are empty and moving to the same topology
	// is to have all peers as active.

	activeTopology := task.activeTopology
	for {
		topology, resubmit, err := task.rewriteTopology(activeTopology, task.targetTopology, allRMIds, nil)
		if err != nil {
			return task.error(err)
		}
		if resubmit {
			time.Sleep(time.Duration(task.rng.Intn(int(server.SubmissionMaxSubmitDelay))))
			continue
		}
		// It would be possible for all to be starting from empty,
		// but some to be being fed a bigger config than
		// others. Bizzare, but it is possible.
		if topology.Version < task.targetTopology.Version &&
			(topology.Next() == nil || topology.Next().Version < task.targetTopology.Version) {
			task.tasks = append(task.tasks, task.topologyChange)
		}
		task.error(nil)
		return task.taskCompleted(topology, true)
	}
}

func (task *joinCluster) topologyVarChanged(topology *configuration.Topology) error {
	// Ignore this because if we're joining then we'll discover directly this change.
	return nil
}

func (task *joinCluster) activeConnectionsChange(conns map[common.RMId]paxos.Connection) error {
	if task.maybeBeginOnConnectionChange {
		task.maybeBeginOnConnectionChange = false
		return task.maybeBegin()
	}
	return nil
}

func (task *joinCluster) accomodateTarget(config *configuration.Configuration, errChan chan error) (bool, error, error) {
	// If we're here then we must be waiting to start or
	// restart. Perfectly safe then to revert back to the
	// topologyChange and get that to make the decision. The fact that
	// we are a joinCluster means our parent topologyChange must have
	// been started, so we must be at task.tasks[0].
	accomodated, confErr, fatalErr := task.topologyChange.accomodateTarget(config, errChan)
	if !accomodated || confErr != nil || fatalErr != nil {
		return accomodated, confErr, fatalErr
	}
	task.tasks[0] = task.topologyChange
	return true, nil, task.tasks[0].start()
}

func (task *joinCluster) topologyTransmogrifierMsgWitness() {}

// modifyCluster

type modifyCluster struct{ *topologyChange }

func (task *modifyCluster) start() error {
	fmt.Println("Hello 2")
	return nil
}
func (task *modifyCluster) topologyVarChanged(topology *configuration.Topology) error {
	return nil
}
func (task *modifyCluster) activeConnectionsChange(conns map[common.RMId]paxos.Connection) error {
	return nil
}
func (task *modifyCluster) accomodateTarget(config *configuration.Configuration, errChan chan error) (bool, error, error) {
	return false, nil, nil
}
func (task *modifyCluster) topologyTransmogrifierMsgWitness() {}
