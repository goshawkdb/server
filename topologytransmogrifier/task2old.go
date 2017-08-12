package topologyTransmogrifier

import (
	"fmt"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
)

// installTargetOld
// Purpose is to do a txn using the current topology in which we set
// topology.Next to be the target topology. We calculate and store the
// migration strategy at this point too.

type installTargetOld struct {
	*targetConfigBase
}

func (task *installTargetOld) Tick() (bool, error) {
	if next := task.activeTopology.NextConfiguration; !(next == nil || next.Version < task.targetConfig.Version) {
		return task.completed()
	}

	if !task.isInRMs(task.activeTopology.RMs) {
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
	active, passive := task.formActivePassive(task.activeTopology.RMs, targetTopology.NextConfiguration.NewRMIds)
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
	twoFInc := uint16(task.activeTopology.RMs.NonEmptyLen())
	txn := task.createTopologyTransaction(task.activeTopology, targetTopology, twoFInc, active, passive)
	go task.runTopologyTransaction(task, txn, active, passive)
	return false, nil
}

func (task *installTargetOld) calculateTargetTopology() (*configuration.Topology, int, bool, error) {
	active := task.activeTopology
	localHost, err := task.firstLocalHost(active.Configuration)
	if err != nil {
		terminate, err := task.fatal(err)
		return nil, 0, terminate, err
	}

	hostsSurvived, hostsRemoved, hostsAdded :=
		make(map[string]common.RMId),
		make(map[string]common.RMId),
		make(map[string]paxos.Connection)

	allRemoteHosts := make([]string, 0, len(active.Hosts)+len(task.targetConfig.Hosts))

	// 1. Start by assuming all old hosts have been removed
	rmIdsOld := active.RMs.NonEmpty()
	// rely on hosts and rms being in the same order.
	hostsOld := active.Hosts
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

	// this will now start connections to allRemoteHosts
	task.installTopology(active, nil, localHost, allRemoteHosts)
	task.ensureShareGoalWithAll()

	// the -1 is because allRemoteHosts will not include localHost
	hostsAddedList := allRemoteHosts[len(hostsRemoved)-1:]
	allAddedFound, err := task.verifyClusterUUIds(active.ClusterUUId, hostsAddedList)
	if err != nil {
		terminate, err := task.error(err)
		return nil, 0, terminate, err
	} else if !allAddedFound {
		return nil, 0, false, nil
	}

	// map(old -> new)
	rmIdsTranslation := make(map[common.RMId]common.RMId)
	connsAdded := make([]paxos.Connection, 0, len(hostsAdded))

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
	hostsOldCopy := hostsOld

	// Now construct the new RMId list.
	hostsNew := make([]string, 0, len(allRemoteHosts)+1)
	rmIdsNew := make([]common.RMId, 0, len(allRemoteHosts)+1)
	rmIdsLost := make([]common.RMId, 0, len(hostsRemoved))
	for _, rmIdOld := range active.RMs { // need the gaps!
		rmIdNew := rmIdsTranslation[rmIdOld]
		switch {
		case rmIdNew == common.RMIdEmpty && len(connsAddedCopy) > 0: // removal of old, and we have conn added
			cd := connsAddedCopy[0]
			connsAddedCopy = connsAddedCopy[1:]
			rmIdNew = cd.RMId()
			rmIdsNew = append(rmIdsNew, rmIdNew)
			hostsNew = append(hostsNew, cd.Host())
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
		rmIdsNew = append(rmIdsNew, cd.RMId())
		hostsNew = append(hostsNew, cd.Host())
	}

	nextConfig := task.targetConfig.Clone()
	nextConfig.RMs = rmIdsNew
	// Note this means that the Hosts in the db config can differ from
	// the hosts in the cmdline config.
	nextConfig.Hosts = hostsNew

	activeCloned := active.Clone()
	// Pointer semantics, so we need to copy into our new set
	removed := make(map[common.RMId]server.EmptyStruct)
	for rmId := range activeCloned.RMsRemoved {
		removed[rmId] = server.EmptyStructVal
	}
	for _, rmId := range rmIdsLost {
		removed[rmId] = server.EmptyStructVal
	}
	nextConfig.RMsRemoved = removed

	rmIdsAdded := make([]common.RMId, len(connsAdded))
	for idx, cd := range connsAdded {
		rmIdsAdded[idx] = cd.RMId()
	}
	conds := calculateMigrationConditions(rmIdsAdded, rmIdsLost, rmIdsSurvived, task.activeTopology.Configuration, nextConfig)

	// now figure out which roots have survived and how many new ones
	// we need to create.
	oldNamesList := activeCloned.Roots
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
	activeCloned.RootVarUUIds = activeCloned.RootVarUUIds[:oldNamesCount]

	activeCloned.NextConfiguration = &configuration.NextConfiguration{
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
	activeCloned.EnsureClusterUUId(0)
	server.DebugLog(task.inner.Logger, "debug", "Set cluster uuid.", "uuid", activeCloned.ClusterUUId)

	return activeCloned, rootsRequired, false, nil
}

func (task *installTargetOld) verifyClusterUUIds(clusterUUId uint64, remoteHosts []string) (bool, error) {
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
