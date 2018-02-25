package topologytransmogrifier

import (
	"goshawkdb.io/common"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/types"
	sconn "goshawkdb.io/server/types/connections/server"
	topo "goshawkdb.io/server/types/topology"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/binarybackoff"
	"time"
)

// installTargetOld
// Purpose is to do a txn using the current topology in which we set
// topology.Next to be the target topology. We calculate and store the
// migration strategy at this point too.

type installTargetOld struct {
	*transmogrificationTask
	onTopologyInstalled func() (bool, error)
}

func (task *installTargetOld) init(base *transmogrificationTask) {
	task.transmogrificationTask = base
	task.onTopologyInstalled = nil
}

func (task *installTargetOld) isValid() bool {
	base := task.joinCluster.baseTopology()
	active := task.activeTopology
	return base != nil && len(base.ClusterId) > 0 &&
		task.targetConfig != nil &&
		base.Version < task.targetConfig.Version &&
		(base.NextConfiguration == nil || base.NextConfiguration.Version < task.targetConfig.Version) &&
		active != nil && active.Version < task.targetConfig.Version &&
		(active.NextConfiguration == nil || active.NextConfiguration.Version < task.targetConfig.Version)
}

func (task *installTargetOld) announce() {
	task.inner.Logger.Log("stage", "Old", "msg", "Attempting to install topology change target.", "configuration", task.targetConfig)
}

func (task *installTargetOld) Tick() (bool, error) {
	if task.selectStage() != task {
		return task.completed()
	}

	base := task.joinCluster.baseTopology()

	if !task.isInRMs(base.RMs) {
		task.ensureShareGoalWithAll()
		task.inner.Logger.Log("msg", "Awaiting existing cluster members.")
		// this step must be performed by the existing RMs
		return false, nil
	}

	targetTopology, rootsRequired, terminate, err := task.calculateTargetTopology()
	if terminate || err != nil || targetTopology == nil {
		return terminate, err
	}

	task.inner.Logger.Log("msg", "Calculated target topology.", "configuration", targetTopology.NextConfiguration,
		"newRoots", rootsRequired)

	if rootsRequired != 0 {
		task.onTopologyInstalled = func() (bool, error) {
			task.runTxnMsg = &topologyTransmogrifierMsgCreateRoots{
				transmogrificationTask: task.transmogrificationTask,
				task:          task,
				backoff:       binarybackoff.NewBinaryBackoffEngine(task.rng, 2*time.Second, time.Duration(len(task.targetConfig.Hosts)+1)*2*time.Second),
				rootsRequired: rootsRequired,
				target:        targetTopology,
			}
			return task.runTxnMsg.Exec()
		}
	} else {
		task.onTopologyInstalled = func() (bool, error) {
			return task.installTargetOld(targetTopology)
		}
	}
	return false, nil
}

func (task *installTargetOld) installTargetOld(targetTopology *configuration.Topology) (bool, error) {
	// We use all the nodes in the old cluster as potential
	// acceptors. We will require a majority of them are alive. And add
	// on all new (if there are any) as passives
	base := task.joinCluster.baseTopology()
	active, passive := task.formActivePassive(base.RMs, targetTopology.NextConfiguration.NewRMIds)
	if active == nil {
		return false, nil
	}

	twoFInc := uint16(base.RMs.NonEmptyLen())
	txn := task.createTopologyTransaction(task.activeTopology, targetTopology, twoFInc, active, passive)
	task.runTopologyTransaction(txn, active, passive, targetTopology)
	return false, nil
}

func (task *installTargetOld) calculateTargetTopology() (*configuration.Topology, int, bool, error) {
	base := task.joinCluster.baseTopology()
	localHost, err := task.firstLocalHost(base.Configuration)
	if err != nil {
		terminate, err := task.fatal(err)
		return nil, 0, terminate, err
	}
	if len(localHost) == 0 { // we must be joining
		localHost, err = task.firstLocalHost(task.targetConfig)
		if err != nil {
			terminate, err := task.fatal(err)
			return nil, 0, terminate, err
		}
	}

	hostsSurvived, hostsRemoved, hostsAdded :=
		make(map[string]common.RMId),
		make(map[string]common.RMId),
		make(map[string]*sconn.ServerConnection)

	allRemoteHosts := make([]string, 0, len(base.Hosts)+len(task.targetConfig.Hosts))

	// 1. Start by assuming all old hosts have been removed
	rmIdsOld := base.RMs.NonEmpty()
	// rely on hosts and rms being in the same order.
	hostsOld := base.Hosts
	for idx, host := range hostsOld {
		hostsRemoved[host] = rmIdsOld[idx]
		if host != localHost {
			allRemoteHosts = append(allRemoteHosts, host)
		}
	}

	// 2. For each new host, if it is in the removed set, it's
	// "survived". Else it's new. Don't care about correcting
	// hostsRemoved (it's not used again).
	for _, host := range task.targetConfig.Hosts {
		if rmId, found := hostsRemoved[host]; found {
			hostsSurvived[host] = rmId
		} else {
			hostsAdded[host] = nil
			if host != localHost {
				allRemoteHosts = append(allRemoteHosts, host)
			}
		}
	}

	// this will now start connections to allRemoteHosts. CreateRoots
	// uses a client transaction which means we have to make sure a
	// non-blank topology has made it into localConnection before we
	// try to create roots. Hence using the callback mechanism here.
	task.installTopology(base, map[topo.TopologyChangeSubscriberType]func() (bool, error){
		topo.ConnectionSubscriber: func() (bool, error) {
			if task.currentTask == task && task.onTopologyInstalled != nil {
				return task.onTopologyInstalled()
			}
			return false, nil
		},
	}, localHost, allRemoteHosts)
	task.ensureShareGoalWithAll()

	// the -1 is because allRemoteHosts will not include localHost
	hostsAddedList := allRemoteHosts[len(hostsRemoved)-1:]
	allAddedFound, clusterUUId, err := task.verifyClusterUUIds(base.ClusterUUId, hostsAddedList)
	if err != nil {
		terminate, err := task.error(err)
		return nil, 0, terminate, err
	} else if !allAddedFound {
		return nil, 0, false, nil
	}

	// map(old -> new)
	rmIdsTranslation := make(map[common.RMId]common.RMId)
	connsAdded := make([]*sconn.ServerConnection, 0, len(hostsAdded))

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
	rmIdsSurvived := make([]common.RMId, 0, len(hostsSurvived))
	for host, rmIdOld := range hostsSurvived {
		cd, found := task.hostToConnection[host]
		if found && rmIdOld != cd.RMId {
			// We have evidence the RMId has changed!
			rmIdNew := cd.RMId
			rmIdsTranslation[rmIdOld] = rmIdNew
			hostsAdded[host] = cd
		} else {
			// No evidence it's changed RMId, so it maps to itself.
			rmIdsTranslation[rmIdOld] = rmIdOld
			rmIdsSurvived = append(rmIdsSurvived, rmIdOld)
		}
	}

	connsAddedCopy := connsAdded
	hostsOldCopy := hostsOld

	// Now construct the new RMId list.
	hostsNew := make([]string, 0, len(allRemoteHosts)+1)
	rmIdsNew := make([]common.RMId, 0, len(allRemoteHosts)+1)
	rmIdsLost := make([]common.RMId, 0, len(hostsRemoved))
	for _, rmIdOld := range base.RMs { // need the gaps!
		rmIdNew := rmIdsTranslation[rmIdOld]
		switch {
		case rmIdNew == common.RMIdEmpty && len(connsAddedCopy) > 0: // removal of old, and we have conn added
			cd := connsAddedCopy[0]
			connsAddedCopy = connsAddedCopy[1:]
			rmIdNew = cd.RMId
			rmIdsNew = append(rmIdsNew, rmIdNew)
			hostsNew = append(hostsNew, cd.Host)
			if rmIdOld != common.RMIdEmpty { // old wasn't a gap (so conn added is replacing old)
				hostsOldCopy = hostsOldCopy[1:]
				rmIdsLost = append(rmIdsLost, rmIdOld)
				rmIdsTranslation[rmIdOld] = rmIdNew
			}
		case rmIdNew == common.RMIdEmpty: // removal of old, but no conn added
			rmIdsNew = append(rmIdsNew, rmIdNew)
			if rmIdOld != common.RMIdEmpty { // old wasn't a gap
				hostsOldCopy = hostsOldCopy[1:]
				rmIdsLost = append(rmIdsLost, rmIdOld)
			}
		case rmIdNew != rmIdOld: // via rmIdsTranslation, must be a reset RM (we keep these in the same position)
			rmIdsNew = append(rmIdsNew, rmIdNew)
			host := hostsOldCopy[0]
			hostsOldCopy = hostsOldCopy[1:]
			hostsNew = append(hostsNew, host)
			rmIdsLost = append(rmIdsLost, rmIdOld)
			connsAdded = append(connsAdded, hostsAdded[host]) // will not affect connsAddedCopy
		default: // no change
			rmIdsNew = append(rmIdsNew, rmIdNew)
			hostsNew = append(hostsNew, hostsOldCopy[0])
			hostsOldCopy = hostsOldCopy[1:]
		}
	}
	// Finally, we may still have some new RMIds we never found space
	// for.
	for _, cd := range connsAddedCopy {
		rmIdsNew = append(rmIdsNew, cd.RMId)
		hostsNew = append(hostsNew, cd.Host)
	}

	nextConfig := task.targetConfig.Clone()
	nextConfig.RMs = rmIdsNew
	// Note this means that the Hosts in the db config can differ from
	// the hosts in the cmdline config.
	nextConfig.Hosts = hostsNew

	baseCloned := base.Clone()
	// Pointer semantics, so we need to copy into our new set
	removed := make(map[common.RMId]types.EmptyStruct)
	for rmId := range baseCloned.RMsRemoved {
		removed[rmId] = types.EmptyStructVal
	}
	for _, rmId := range rmIdsLost {
		removed[rmId] = types.EmptyStructVal
	}
	nextConfig.RMsRemoved = removed

	rmIdsAdded := make([]common.RMId, len(connsAdded))
	for idx, cd := range connsAdded {
		rmIdsAdded[idx] = cd.RMId
	}
	conds := calculateMigrationConditions(rmIdsAdded, rmIdsLost, rmIdsSurvived, base.Configuration, nextConfig)

	// now figure out which roots have survived and how many new ones
	// we need to create.
	oldNamesList := base.Roots
	oldNamesCount := len(oldNamesList)
	oldNames := make(map[string]uint32, oldNamesCount)
	for idx, name := range oldNamesList {
		oldNames[name] = uint32(idx)
	}
	newNames := nextConfig.Roots
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

	baseCloned.RootVarUUIds = baseCloned.RootVarUUIds[:oldNamesCount]

	baseCloned.NextConfiguration = &configuration.NextConfiguration{
		Configuration:  nextConfig,
		AllHosts:       append(allRemoteHosts, localHost),
		NewRMIds:       rmIdsAdded,
		SurvivingRMIds: rmIdsSurvived,
		LostRMIds:      rmIdsLost,
		RootIndices:    rootIndices,
		InstalledOnNew: len(rmIdsAdded) == 0,
		Pending:        conds,
	}
	// This is the only time that we go from a config with a nil next,
	// to one with a non-nil next. Therefore, we must ensure the
	// ClusterUUId is non-0 and consistent down the chain. Of course,
	// the txn will ensure only one such rewrite will win.
	baseCloned.EnsureClusterUUId(clusterUUId)
	utils.DebugLog(task.inner.Logger, "debug", "Set cluster uuid.", "uuid", baseCloned.ClusterUUId)

	return baseCloned, rootsRequired, false, nil
}

func calculateMigrationConditions(added, lost, survived []common.RMId, from, to *configuration.Configuration) configuration.Conds {
	conditions := make(configuration.Conds)
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
