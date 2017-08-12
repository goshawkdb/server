package topologyTransmogrifier

import (
	"bytes"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"time"
)

type topologyTask interface {
	Tick() (bool, error)
	Abandon()
	Goal() *configuration.NextConfiguration
}

type targetConfigBase struct {
	*TopologyTransmogrifier
	targetConfig *configuration.NextConfiguration
	sender       paxos.ServerConnectionSubscriber
	backoff      *server.BinaryBackoffEngine
	tickEnqueued bool
}

func (tcb *targetConfigBase) Tick() (bool, error) {
	tcb.backoff = nil
	tcb.tickEnqueued = false

	switch {
	case tcb.activeTopology == nil:
		tcb.inner.Logger.Log("msg", "Ensuring local topology.")
		tcb.currentTask = &ensureLocalTopology{tcb}

	case len(tcb.activeTopology.ClusterId) == 0:
		tcb.inner.Logger.Log("msg", "Attempting to join cluster.", "configuration", tcb.targetConfig)
		tcb.currentTask = &joinCluster{targetConfigBase: tcb}

	case tcb.activeTopology.NextConfiguration == nil || tcb.activeTopology.NextConfiguration.Version < tcb.targetConfig.Version:
		tcb.inner.Logger.Log("msg", "Attempting to install topology change target.", "configuration", tcb.targetConfig)
		tcb.currentTask = &installTargetOld{targetConfigBase: tcb}

	case tcb.activeTopology.NextConfiguration != nil && tcb.activeTopology.NextConfiguration.Version == tcb.targetConfig.Version:
		switch {
		case !tcb.activeTopology.NextConfiguration.InstalledOnNew:
			tcb.inner.Logger.Log("msg", "Attempting to install topology change to new cluster.", "configuration", tcb.targetConfig)
			tcb.currentTask = &installTargetNew{targetConfigBase: tcb}

		case !tcb.activeTopology.NextConfiguration.QuietRMIds[tcb.connectionManager.RMId]:
			tcb.inner.Logger.Log("msg", "Waiting for quiet.", "configuration", tcb.targetConfig)
			tcb.currentTask = &quiet{targetConfigBase: tcb}

		case len(tcb.activeTopology.NextConfiguration.Pending) > 0:
			tcb.inner.Logger.Log("msg", "Attempting to perform object migration for topology target.", "configuration", tcb.targetConfig)
			tcb.currentTask = &migrate{targetConfigBase: tcb}

		default:
			tcb.inner.Logger.Log("msg", "Object migration completed, switching to new topology.", "configuration", tcb.targetConfig)
			tcb.currentTask = &installCompletion{targetConfigBase: tcb}
		}

	default:
		return false, fmt.Errorf("Topology: Confused about what to do. Active topology is: %v; target config is %v",
			tcb.activeTopology, tcb.targetConfig)
	}
	return false, nil
}

func (tcb *targetConfigBase) ensureShareGoalWithAll() {
	if tcb.sender != nil {
		return
	}
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	msg.SetTopologyChangeRequest(tcb.targetConfig.AddToSegAutoRoot(seg))
	tcb.sender = paxos.NewRepeatingAllSender(common.SegToBytes(seg))
	tcb.connectionManager.AddServerConnectionSubscriber(tcb.sender)
}

func (tcb *targetConfigBase) ensureRemoveTaskSender() {
	if tcb.sender != nil {
		tcb.connectionManager.RemoveServerConnectionSubscriber(tcb.sender)
		tcb.sender = nil
	}
}

// todo - should we use shutdown?
// is there any reason to avoid the re-eval that's now on completed?
func (tcb *targetConfigBase) Abandon() {
	tcb.ensureRemoveTaskSender()
}

func (tcb *targetConfigBase) Goal() *configuration.NextConfiguration {
	return tcb.targetConfig
}

func (tcb *targetConfigBase) shutdown() {
	tcb.ensureRemoveTaskSender()
	tcb.currentTask = nil
}

func (tcb *targetConfigBase) fatal(err error) (bool, error) {
	tcb.shutdown()
	tcb.inner.Logger.Log("msg", "Fatal error.", "error", err)
	return false, err
}

func (tcb *targetConfigBase) error(err error) (bool, error) {
	tcb.shutdown()
	tcb.inner.Logger.Log("msg", "Non-fatal error.", "error", err)
	return false, nil
}

func (tcb *targetConfigBase) completed() (bool, error) {
	tcb.shutdown()
	tcb.inner.Logger.Log("msg", "Task completed.")
	// force reevaluation as to how to get to this targetConfig
	return false, tcb.setTarget(tcb.targetConfig)
}

// NB filters out empty RMIds so no need to pre-filter.
func (tcb *targetConfigBase) formActivePassive(activeCandidates, extraPassives common.RMIds) (active, passive common.RMIds) {
	active, passive = []common.RMId{}, []common.RMId{}
	for _, rmId := range activeCandidates {
		if rmId == common.RMIdEmpty {
			continue
		} else if _, found := tcb.activeConnections[rmId]; found {
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
		tcb.inner.Logger.Log("msg", "Can not make progress at this time due to too many failures.",
			"failures", fmt.Sprint(passive))
		return nil, nil
	}
	active, passive = active[:fInc], append(active[fInc:], passive...)
	return active, append(passive, extraPassives...)
}

func (tcb *targetConfigBase) firstLocalHost(config *configuration.Configuration) (localHost string, err error) {
	for config != nil {
		localHost, _, err = config.LocalRemoteHosts(tcb.listenPort)
		if err == nil {
			return localHost, err
		}
		config = config.NextConfiguration.Configuration
	}
	return "", err
}

func (tcb *targetConfigBase) allHostsBarLocalHost(localHost string, next *configuration.NextConfiguration) []string {
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

func (tcb *targetConfigBase) isInRMs(rmIds common.RMIds) bool {
	for _, rmId := range rmIds {
		if rmId == tcb.connectionManager.RMId {
			return true
		}
	}
	return false
}

func (tcb *targetConfigBase) createOrAdvanceBackoff() {
	if tcb.backoff == nil {
		tcb.backoff = server.NewBinaryBackoffEngine(tcb.rng, server.SubmissionMinSubmitDelay, time.Duration(len(tcb.targetConfig.Hosts))*server.SubmissionMaxSubmitDelay)
	} else {
		tcb.backoff.Advance()
	}
}

func (tcb *targetConfigBase) runTopologyTransaction(tcb topologyTask, txn *msgs.Txn, active, passive common.RMIds) {
	closer := tcb.maybeTick2(tcb, tcb) // todo - wtf
	_, resubmit, err := tcb.rewriteTopology(txn, active, passive)
	if !closer() {
		return
	}
	tcb.EnqueueFuncAsync(func() (bool, error) {
		switch {
		case tcb.currentTask != tcb:
			return false, nil

		case err != nil:
			return tcb.fatal(err)

		case resubmit:
			tcb.enqueueTick(tcb, tcb) // todo similarly - wtf
			return false, nil

		default:
			// Must be commit, or badread, which means again we should
			// receive the updated topology through the subscriber.
			return false, nil
		}
	})
}

func (tcb *targetConfigBase) createTopologyTransaction(read, write *configuration.Topology, twoFInc uint16, active, passive common.RMIds) *msgs.Txn {
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
				alloc.SetActive(tcb.activeConnections[rmId].BootCount())
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

func (tcb *targetConfigBase) getTopologyFromLocalDatabase() (*configuration.Topology, error) {
	empty, err := tcb.connectionManager.Dispatchers.IsDatabaseEmpty()
	if empty || err != nil {
		return nil, err
	}

	backoff := server.NewBinaryBackoffEngine(tcb.rng, server.SubmissionMinSubmitDelay, server.SubmissionMaxSubmitDelay)
	for {
		txn := tcb.createTopologyTransaction(nil, nil, 1, []common.RMId{tcb.connectionManager.RMId}, nil)

		_, result, err := tcb.localConnection.RunTransaction(txn, nil, backoff, tcb.connectionManager.RMId)
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

func (tcb *targetConfigBase) createTopologyZero(config *configuration.NextConfiguration) (*configuration.Topology, error) {
	topology := configuration.BlankTopology()
	topology.NextConfiguration = config
	txn := tcb.createTopologyTransaction(nil, topology, 1, []common.RMId{tcb.connectionManager.RMId}, nil)
	txnId := topology.DBVersion
	txn.SetId(txnId[:])
	// in general, we do backoff locally, so don't pass backoff through here
	_, result, err := tcb.localConnection.RunTransaction(txn, txnId, nil, tcb.connectionManager.RMId)
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

func (tcb *targetConfigBase) rewriteTopology(txn *msgs.Txn, active, passive common.RMIds) (bool, bool, error) {
	// in general, we do backoff locally, so don't pass backoff through here
	server.DebugLog(tcb.inner.Logger, "debug", "Running transaction.", "active", active, "passive", passive)
	txnReader, result, err := tcb.localConnection.RunTransaction(txn, nil, nil, active...)
	if result == nil || err != nil {
		return false, false, err
	}
	txnId := txnReader.Id
	if result.Which() == msgs.OUTCOME_COMMIT {
		server.DebugLog(tcb.inner.Logger, "debug", "Txn Committed.", "TxnId", txnId)
		return true, false, nil
	}
	abort := result.Abort()
	server.DebugLog(tcb.inner.Logger, "debug", "Txn Aborted.", "TxnId", txnId)
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

func (tcb *targetConfigBase) attemptCreateRoots(rootCount int) (bool, configuration.Roots, error) {
	server.DebugLog(tcb.inner.Logger, "debug", "Creating Roots.", "count", rootCount)

	seg := capn.NewBuffer(nil)
	ctxn := cmsgs.NewClientTxn(seg)
	ctxn.SetRetry(false)
	roots := make([]configuration.Root, rootCount)
	actions := cmsgs.NewClientActionList(seg, rootCount)
	for idx := range roots {
		action := actions.At(idx)
		vUUId := tcb.localConnection.NextVarUUId()
		action.SetVarId(vUUId[:])
		action.SetCreate()
		create := action.Create()
		create.SetValue([]byte{})
		create.SetReferences(cmsgs.NewClientVarIdPosList(seg, 0))
		root := &roots[idx]
		root.VarUUId = vUUId
	}
	ctxn.SetActions(actions)
	txnReader, result, err := tcb.localConnection.RunClientTransaction(&ctxn, false, nil, nil)
	server.DebugLog(tcb.inner.Logger, "debug", "Created root.", "result", result, "error", err)
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
		server.DebugLog(tcb.inner.Logger, "debug", "Roots created.", "roots", roots)
		return false, roots, nil
	}
	if result.Abort().Which() == msgs.OUTCOMEABORT_RESUBMIT {
		return true, nil, nil
	}
	return false, nil, fmt.Errorf("Internal error: creation of root gave rerun outcome")
}
