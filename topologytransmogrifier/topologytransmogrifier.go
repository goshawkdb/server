package topologytransmogrifier

import (
	"bytes"
	"errors"
	"fmt"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/types"
	topo "goshawkdb.io/server/types/topology"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/txnreader"
	"goshawkdb.io/server/utils/vectorclock"
)

func (tt *TopologyTransmogrifier) maybeTick() (bool, error) {
	if tt.currentTask == nil {
		return false, nil
	} else {
		return tt.currentTask.Tick()
	}
}

func (tt *TopologyTransmogrifier) setActiveTopology(topology *configuration.Topology) (bool, error) {
	//utils.DebugLog(tt.inner.Logger, "debug", "SetActiveTopology.", "topology", topology)
	tt.inner.Logger.Log("debug", "SetActiveTopology.", "activeTopology", tt.activeTopology, "topology", topology)
	if tt.activeTopology != nil {
		if !tt.activeTopology.VerClock.Before(topology.VerClock) {
			// topology is either eq or before activeTopology. So ignore it.
			tt.inner.Logger.Log("debug", "SetActiveTopology ignored: too old.")
			return false, nil
		}
		switch {
		case tt.activeTopology.ClusterId != topology.ClusterId && len(tt.activeTopology.ClusterId) > 0:
			return false, fmt.Errorf("Topology: Fatal: config with ClusterId change from '%s' to '%s'.",
				tt.activeTopology.ClusterId, topology.ClusterId)

		case topology.Version < tt.activeTopology.Version:
			tt.inner.Logger.Log("msg", "Ignoring config with version less than active version.",
				"goalVersion", topology.Version, "activeVersion", tt.activeTopology.Version)
			return false, nil
		}
	}

	if _, found := topology.RMsRemoved[tt.self]; found {
		return false, errors.New("We have been removed from the cluster. Shutting down.")
	}
	tt.activeTopology = topology
	tt.subscriber.Subscribe()

	if tt.currentTask == nil {
		if next := topology.NextConfiguration; next == nil {
			return false, nil
		} else {
			return false, tt.setTarget(next.Configuration)
		}
	} else {
		return tt.currentTask.Tick()
	}
}

func (tt *TopologyTransmogrifier) installTopology(topology *configuration.Topology, callbacks map[topo.TopologyChangeSubscriberType]func() (bool, error), localHost string, remoteHosts []string) {
	utils.DebugLog(tt.inner.Logger, "debug", "Installing topology to connection manager, et al.", "topology", topology)
	if tt.localEstablished != nil {
		if callbacks == nil {
			callbacks = make(map[topo.TopologyChangeSubscriberType]func() (bool, error))
		}
		origFun := callbacks[topo.ConnectionManagerSubscriber]
		callbacks[topo.ConnectionManagerSubscriber] = func() (bool, error) {
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
	wrapped := make(map[topo.TopologyChangeSubscriberType]func(), len(callbacks))
	for subType, cb := range callbacks {
		cbCopy := cb
		wrapped[subType] = func() { tt.EnqueueFuncAsync(cbCopy) }
	}
	tt.connectionManager.SetTopology(topology, wrapped, localHost, remoteHosts)
}

func (tt *TopologyTransmogrifier) setTarget(targetConfig *configuration.Configuration) error {
	// This can be called both via a msg (eg cmdline and SIGHUP), or
	// when there is no current task and we have to think about
	// creating one. If there is a currentTask, then we compare
	// targetConfig with that. Otherwise we compare with the
	// activeTopology.

	var versusConfig *configuration.Configuration
	if tt.currentTask == nil {
		if tt.activeTopology != nil {
			versusConfig = tt.activeTopology.Configuration
		}
	} else {
		versusConfig = tt.currentTask.TargetConfig()
	}

	if versusConfig != nil {
		versusClusterUUId, targetClusterUUId := versusConfig.ClusterUUId, targetConfig.ClusterUUId
		switch {
		case targetConfig.ClusterId != versusConfig.ClusterId && len(versusConfig.ClusterId) > 0:
			return fmt.Errorf("Illegal config change: ClusterId should be '%s' instead of '%s'.",
				versusConfig.ClusterId, targetConfig.ClusterId)

		case targetClusterUUId != 0 && versusClusterUUId != 0 && targetClusterUUId != versusClusterUUId:
			return fmt.Errorf("Illegal config change: ClusterUUId should be '%v' instead of '%v'.",
				versusClusterUUId, targetClusterUUId)

		case targetConfig.MaxRMCount != versusConfig.MaxRMCount && versusConfig.Version != 0:
			return fmt.Errorf("Illegal config change: Currently changes to MaxRMCount are not supported, sorry.")

		case targetConfig.EqualExternally(versusConfig):
			if tt.currentTask == nil {
				tt.inner.Logger.Log("msg", "Config already reached.", "version", versusConfig.Version)
			} else {
				tt.inner.Logger.Log("msg", "Config already being targetted.", "version", versusConfig.Version)
			}
			return nil

		case targetConfig.Version == versusConfig.Version:
			return fmt.Errorf("Illegal config change: Config has changed but Version has not been increased (%v). Ignoring.", targetConfig.Version)

		case targetConfig.Version < versusConfig.Version:
			return fmt.Errorf("Illegal config change: Ignoring config with version %v as newer version already seen (%v).",
				targetConfig.Version, versusConfig.Version)
		}
	}

	// if we're here and there is a currentTask then we know
	// currentTask is insufficient
	if tt.currentTask != nil {
		tt.currentTask.Abandon()
	}

	utils.DebugLog(tt.inner.Logger, "debug", "Creating new task.")
	tt.currentTask = tt.newTransmogrificationTask(targetConfig)
	return nil
}

// NB filters out empty RMIds so no need to pre-filter.
func (tt *TopologyTransmogrifier) formActivePassive(activeCandidates, extraPassives common.RMIds) (active, passive common.RMIds) {
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

func (tt *TopologyTransmogrifier) firstLocalHost(config *configuration.Configuration) (localHost string, err error) {
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

func (tt *TopologyTransmogrifier) allHostsBarLocalHost(localHost string, next *configuration.NextConfiguration) []string {
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

func (tt *TopologyTransmogrifier) isInRMs(rmIds common.RMIds) bool {
	for _, rmId := range rmIds {
		if rmId == tt.self {
			return true
		}
	}
	return false
}

func (tt *TopologyTransmogrifier) submitTopologyTransaction(txn *msgs.Txn, subscriptionConsumer client.SubscriptionConsumer, active, passive common.RMIds) (*configuration.Topology, *types.VerClock, bool, error) {
	// in general, we do backoff locally, so don't pass backoff through here
	utils.DebugLog(tt.inner.Logger, "debug", "Running transaction.", "active", active, "passive", passive)
	txnReader, result, err := tt.localConnection.RunTransaction(txn, nil, subscriptionConsumer, nil, active...)
	if result == nil || err != nil {
		return nil, nil, false, err
	}
	txnId := txnReader.Id
	if result.Which() == msgs.OUTCOME_COMMIT {
		clockElem := vectorclock.VectorClockFromData(result.Commit(), true).At(configuration.TopologyVarUUId)
		utils.DebugLog(tt.inner.Logger, "debug", "Txn Committed.", "TxnId", txnId)
		return nil, &types.VerClock{ClockElem: clockElem, Version: txnId}, false, nil
	}
	abort := result.Abort()
	utils.DebugLog(tt.inner.Logger, "debug", "Txn Aborted.", "TxnId", txnId)
	if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
		return nil, nil, true, nil
	}
	abortUpdates := abort.Rerun()
	if abortUpdates.Len() != 1 {
		panic(
			fmt.Sprintf("Internal error: readwrite of topology gave %v updates (1 expected)",
				abortUpdates.Len()))
	}
	update := abortUpdates.At(0)
	version := common.MakeTxnId(update.TxnId())
	clockElem := vectorclock.VectorClockFromData(update.Clock(), true).At(configuration.TopologyVarUUId)
	vc := types.VerClock{ClockElem: clockElem, Version: version}

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
		panic("Internal error: update action from readwrite of topology gave non-write action!")
	}
	updateValue := updateAction.Value()
	write := updateValue.Existing().Modify().Write()
	value := write.Value()
	refs := write.References()
	topologyBadRead, err := configuration.TopologyFromCap(vc, &refs, value)
	utils.DebugLog(tt.inner.Logger, "debug", "submitTopologyTransaction, badread.", "vc", vc, "value", value, "topology", topologyBadRead)
	return topologyBadRead, nil, false, err
}
