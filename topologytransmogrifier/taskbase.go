package topologytransmogrifier

import (
	"bytes"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdb "github.com/msackman/gomdb"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/binarybackoff"
	"goshawkdb.io/server/utils/senders"
	"goshawkdb.io/server/utils/txnreader"
	"time"
)

type Task interface {
	Tick() (bool, error)
	TargetConfig() *configuration.Configuration
	Abandon()
}

type stage interface {
	Task
	init(*transmogrificationTask)
	isValid() bool
	announce()
}

type transmogrificationTask struct {
	*TopologyTransmogrifier
	targetConfig *configuration.Configuration
	sender       sconn.ServerConnectionSubscriber
	runTxnMsg    actor.MsgExec

	ensureLocalTopology
	joinCluster
	installTargetOld
	// migrateTopology here?
	subscribe
	installTargetNew
	quiet
	migrate
	installCompletion
	stages []stage
}

func (tt *TopologyTransmogrifier) newTransmogrificationTask(targetConfig *configuration.Configuration) *transmogrificationTask {
	base := &transmogrificationTask{
		TopologyTransmogrifier: tt,
		targetConfig:           targetConfig,
	}
	base.stages = []stage{
		&base.ensureLocalTopology,
		&base.joinCluster,
		&base.installTargetOld,
		&base.subscribe,
		&base.installTargetNew,
		&base.quiet,
		&base.migrate,
		&base.installCompletion,
	}
	for _, s := range base.stages {
		s.init(base)
	}

	return base
}

func (tt *transmogrificationTask) selectStage() stage {
	for idx, s := range tt.stages {
		if s.isValid() {
			// make sure we can't go backwards
			tt.stages = tt.stages[idx:]
			return s
		}
	}
	return nil
}

func (tt *transmogrificationTask) TargetConfig() *configuration.Configuration {
	return tt.targetConfig
}

func (tt *transmogrificationTask) Tick() (bool, error) {
	s := tt.selectStage()
	tt.currentTask = s
	if s == nil {
		tt.inner.Logger.Log("msg", "Task completed.", "active", tt.activeTopology, "target", tt.targetConfig)
		activeTopology := tt.activeTopology
		if activeTopology != nil {
			if activeTopology.NextConfiguration == nil &&
				(tt.targetConfig == nil || activeTopology.Version == tt.targetConfig.Version) {
				localHost, remoteHosts, err := tt.activeTopology.LocalRemoteHosts(tt.listenPort)
				if err != nil {
					return false, err
				}
				tt.installTopology(tt.activeTopology, nil, localHost, remoteHosts)
				tt.inner.Logger.Log("msg", "Topology change complete.", "localhost", localHost, "RMId", tt.self)

				for version := range tt.migrations {
					if version <= tt.activeTopology.Version {
						delete(tt.migrations, version)
					}
				}

				_, err = tt.db.WithEnv(func(env *mdb.Env) (interface{}, error) {
					return nil, env.SetFlags(mdb.NOSYNC, activeTopology.NoSync)
				}).ResultError()

			} else if activeTopology.NextConfiguration != nil {
				// Our own targetConfig is either reached or can't be reached
				// given where activeTopology is. And activeTopology has its
				// own target (NextConfiguration). So we should adopt that as
				// a fresh new target.
				return false, tt.setTarget(activeTopology.NextConfiguration.Configuration)
			}
		}
		return false, nil
	} else {
		s.announce()
		return false, nil
	}
}

func (tt *transmogrificationTask) ensureShareGoalWithAll() {
	if tt.sender != nil {
		return
	}
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	msg.SetTopologyChangeRequest(tt.targetConfig.AddToSegAutoRoot(seg))
	tt.sender = senders.NewRepeatingAllSender(common.SegToBytes(seg))
	tt.connectionManager.AddServerConnectionSubscriber(tt.sender)
}

func (tt *transmogrificationTask) shutdown() {
	if tt.sender != nil {
		tt.connectionManager.RemoveServerConnectionSubscriber(tt.sender)
		tt.sender = nil
	}
	tt.currentTask = nil
	tt.runTxnMsg = nil
}

func (tt *transmogrificationTask) fatal(err error) (bool, error) {
	tt.shutdown()
	tt.inner.Logger.Log("msg", "Fatal error.", "error", err)
	return false, err
}

func (tt *transmogrificationTask) error(err error) (bool, error) {
	tt.shutdown()
	tt.inner.Logger.Log("msg", "Non-fatal error.", "error", err)
	return false, nil
}

func (tt *transmogrificationTask) completed() (bool, error) {
	tt.shutdown()
	tt.inner.Logger.Log("msg", "Stage completed.")
	// next Tick will force reevaluation as to how to get to this
	// targetConfig
	tt.currentTask = tt
	return false, nil
}

func (tt *transmogrificationTask) Abandon() {
	tt.shutdown()
}

func (tt *transmogrificationTask) runTopologyTransaction(txn *msgs.Txn, active, passive common.RMIds, target *configuration.Topology) {
	tt.runTxnMsg = &topologyTransmogrifierMsgRunTransaction{
		transmogrificationTask: tt,
		task:    tt.currentTask,
		backoff: binarybackoff.NewBinaryBackoffEngine(tt.rng, 2*time.Second, time.Duration(len(tt.targetConfig.Hosts)+1)*2*time.Second),
		txn:     txn,
		target:  target,
		active:  active,
		passive: passive,
	}
	tt.EnqueueMsg(tt.runTxnMsg)
}

func (tt *transmogrificationTask) verifyClusterUUIds(clusterUUId uint64, remoteHosts []string) (bool, uint64, error) {
	for _, host := range remoteHosts {
		if cd, found := tt.hostToConnection[host]; found {
			switch remoteClusterUUId := cd.ClusterUUId; {
			case remoteClusterUUId == 0:
				// they're joining
			case clusterUUId == 0:
				clusterUUId = remoteClusterUUId
			case clusterUUId == remoteClusterUUId:
				// all good
			default:
				return false, 0, errors.New("Attempt made to merge different logical clusters together, which is illegal. Aborting topology change.")
			}
		} else {
			return false, 0, nil
		}
	}
	return true, clusterUUId, nil
}

// NB filters out empty RMIds so no need to pre-filter.
func (tt *transmogrificationTask) formActivePassive(activeCandidates, extraPassives common.RMIds) (active, passive common.RMIds) {
	active, passive = []common.RMId{}, []common.RMId{}
	for _, rmId := range activeCandidates {
		if rmId == common.RMIdEmpty {
			continue
		} else if _, found := tt.activeConnections[rmId]; found {
			active = append(active, rmId)
		} else {
			passive = append(passive, rmId)
		}
	}

	// Be careful with this maths. The topology object is on every
	// node, so we must use a majority of nodes. So if we have 6 nodes,
	// then we must use 4 as actives. So we're essentially treating
	// this as if it's a cluster of 7 with one failure.
	fInc := ((len(active) + len(passive)) >> 1) + 1
	if len(active) < fInc {
		tt.inner.Logger.Log("msg", "Can not make progress at this time due to too many failures.",
			"failures", fmt.Sprint(passive))
		return nil, nil
	}
	active, passive = active[:fInc], append(active[fInc:], passive...)
	return active, append(passive, extraPassives...)
}

func (tt *transmogrificationTask) firstLocalHost(config *configuration.Configuration) (localHost string, err error) {
	for config != nil && err == nil {
		localHost, _, err = config.LocalRemoteHosts(tt.listenPort)
		if err == nil && len(localHost) > 0 {
			return localHost, err
		}
		if config.NextConfiguration == nil {
			break
		} else {
			err = nil
			config = config.NextConfiguration.Configuration
		}
	}
	return "", err
}

func (tt *transmogrificationTask) allHostsBarLocalHost(localHost string, next *configuration.NextConfiguration) []string {
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

func (tt *transmogrificationTask) isInRMs(rmIds common.RMIds) bool {
	for _, rmId := range rmIds {
		if rmId == tt.self {
			return true
		}
	}
	return false
}

func (tt *transmogrificationTask) createTopologyTransaction(read, write *configuration.Topology, twoFInc uint16, active, passive common.RMIds) *msgs.Txn {
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
	value := action.Value()

	switch {
	case write == nil && read == nil: // discovery
		value.SetExisting()
		existing := value.Existing()
		existing.SetRead(common.VersionZero[:])
		existing.Modify().SetNot()

	case read == nil: // creation
		// When we create, we're creating with the blank topology. Blank
		// topology has MaxRMCount = 0. But we never actually use
		// positions of the topology var anyway. So the following code
		// basically never does anything, and is just here for
		// completeness, but it's still all safe.
		positions := seg.NewUInt8List(int(write.MaxRMCount))
		for idx, l := 0, positions.Len(); idx < l; idx++ {
			positions.Set(idx, uint8(idx))
		}
		value.SetCreate()
		create := value.Create()
		create.SetPositions(positions)
		create.SetValue(write.Serialize())
		create.SetReferences(msgs.NewVarIdPosList(seg, 0))

	default: // modification
		value.SetExisting()
		existing := value.Existing()
		existing.SetRead(read.DBVersion[:])
		existing.Modify().SetWrite()
		modWrite := existing.Modify().Write()
		modWrite.SetValue(write.Serialize())
		roots := write.RootVarUUIds
		refs := msgs.NewVarIdPosList(seg, len(roots))
		for idx, root := range roots {
			varIdPos := refs.At(idx)
			varIdPos.SetId(root.VarUUId[:])
			varIdPos.SetPositions((capn.UInt8List)(*root.Positions))
			varIdPos.SetCapability(common.ReadWriteCapability.AsMsg())
		}
		modWrite.SetReferences(refs)
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
				alloc.SetActive(tt.activeConnections[rmId].BootCount)
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

func (tt *transmogrificationTask) getTopologyFromLocalDatabase() (*configuration.Topology, error) {
	empty, err := tt.router.IsDatabaseEmpty()
	if empty || err != nil {
		return nil, err
	}

	backoff := binarybackoff.NewBinaryBackoffEngine(tt.rng, 2*time.Second, 4*time.Second)
	for {
		txn := tt.createTopologyTransaction(nil, nil, 1, []common.RMId{tt.self}, nil)

		_, result, err := tt.localConnection.RunTransaction(txn, nil, nil, backoff, tt.self)
		if err != nil {
			return nil, err
		} else if result == nil {
			return nil, nil // shutting down
		} else if result.Which() == msgs.OUTCOME_COMMIT {
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
		updateActions := txnreader.TxnActionsFromData(update.Actions(), true).Actions()
		if updateActions.Len() != 1 {
			return nil, fmt.Errorf("Internal error: read of topology version 0 didn't give 1 update action: %v", updateActions.Len())
		}
		updateAction := updateActions.At(0)
		if !bytes.Equal(updateAction.VarId(), configuration.TopologyVarUUId[:]) {
			return nil, fmt.Errorf("Internal error: unable to find action for topology from read of topology version 0")
		}
		if !txnreader.IsWriteWithValue(&updateAction) {
			return nil, fmt.Errorf("Internal error: read of topology version 0 gave non-write action")
		}
		updateValue := updateAction.Value()
		write := updateValue.Existing().Modify().Write()
		value := write.Value()
		refs := write.References()
		return configuration.TopologyFromCap(dbversion, &refs, value)
	}
}

func (tt *transmogrificationTask) createTopologyZero() (*configuration.Topology, error) {
	topology := configuration.BlankTopology()
	txn := tt.createTopologyTransaction(nil, topology, 1, []common.RMId{tt.self}, nil)
	txnId := topology.DBVersion
	txn.SetId(txnId[:])
	// in general, we do backoff locally, so don't pass backoff through here
	_, result, err := tt.localConnection.RunTransaction(txn, txnId, nil, nil, tt.self)
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

func (tt *transmogrificationTask) submitTopologyTransaction(txn *msgs.Txn, subscriptionConsumer client.SubscriptionConsumer, active, passive common.RMIds) (*configuration.Topology, *common.TxnId, bool, error) {
	// in general, we do backoff locally, so don't pass backoff through here
	utils.DebugLog(tt.inner.Logger, "debug", "Running transaction.", "active", active, "passive", passive)
	txnReader, result, err := tt.localConnection.RunTransaction(txn, nil, subscriptionConsumer, nil, active...)
	if result == nil || err != nil {
		return nil, nil, false, err
	}
	txnId := txnReader.Id
	if result.Which() == msgs.OUTCOME_COMMIT {
		utils.DebugLog(tt.inner.Logger, "debug", "Txn Committed.", "TxnId", txnId)
		return nil, txnId, false, nil
	}
	abort := result.Abort()
	utils.DebugLog(tt.inner.Logger, "debug", "Txn Aborted.", "TxnId", txnId)
	if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
		return nil, txnId, true, nil
	}
	abortUpdates := abort.Rerun()
	if abortUpdates.Len() != 1 {
		panic(
			fmt.Sprintf("Internal error: readwrite of topology gave %v updates (1 expected)",
				abortUpdates.Len()))
	}
	update := abortUpdates.At(0)
	dbversion := common.MakeTxnId(update.TxnId())

	updateActions := txnreader.TxnActionsFromData(update.Actions(), true).Actions()
	if updateActions.Len() != 1 {
		panic(
			fmt.Sprintf("Internal error: readwrite of topology gave update with %v actions instead of 1!",
				updateActions.Len()))
	}
	updateAction := updateActions.At(0)
	if !bytes.Equal(updateAction.VarId(), configuration.TopologyVarUUId[:]) {
		panic(
			fmt.Sprintf("Internal error: update action from readwrite of topology is not for topology! %v",
				common.MakeVarUUId(updateAction.VarId())))
	}
	if !txnreader.IsWriteWithValue(&updateAction) {
		panic(
			fmt.Sprintf("Internal error: update action from readwrite of topology gave non-write action!"))
	}
	updateValue := updateAction.Value()
	write := updateValue.Existing().Modify().Write()
	value := write.Value()
	refs := write.References()
	topologyBadRead, err := configuration.TopologyFromCap(dbversion, &refs, value)
	return topologyBadRead, txnId, false, err
}

func (tt *transmogrificationTask) attemptCreateRoots(rootCount int) (bool, configuration.Roots, error) {
	utils.DebugLog(tt.inner.Logger, "debug", "Creating Roots.", "count", rootCount)

	seg := capn.NewBuffer(nil)
	ctxn := cmsgs.NewClientTxn(seg)
	roots := make([]configuration.Root, rootCount)
	actions := cmsgs.NewClientActionList(seg, rootCount)
	for idx := range roots {
		action := actions.At(idx)
		vUUId := tt.localConnection.NextVarUUId()
		action.SetVarId(vUUId[:])
		value := action.Value()
		value.SetCreate()
		create := value.Create()
		create.SetValue([]byte{})
		create.SetReferences(cmsgs.NewClientVarIdPosList(seg, 0))
		root := &roots[idx]
		root.VarUUId = vUUId
	}
	ctxn.SetActions(actions)
	txnReader, result, err := tt.localConnection.RunClientTransaction(&ctxn, false, nil, nil)
	utils.DebugLog(tt.inner.Logger, "debug", "Created root.", "result", result, "error", err)
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
			value := action.Value()
			if value.Which() != msgs.ACTIONVALUE_CREATE {
				return false, nil, fmt.Errorf("Internal error: actions changed type! At %v expecting create, found %v", idx, value.Which())
			}
			create := value.Create()
			positions := create.Positions()
			root.Positions = (*common.Positions)(&positions)
		}
		utils.DebugLog(tt.inner.Logger, "debug", "Roots created.", "roots", roots)
		return false, roots, nil
	}
	if result.Abort().Which() == msgs.OUTCOMEABORT_RESUBMIT {
		return true, nil, nil
	}
	return false, nil, fmt.Errorf("Internal error: creation of root gave rerun outcome")
}
