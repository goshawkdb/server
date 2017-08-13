package topologyTransmogrifier

import (
	"fmt"
	"goshawkdb.io/server/configuration"
)

type installCompletion struct {
	*targetConfigBase
}

func (task *installCompletion) init(base *targetConfigBase) {
	task.targetConfigBase = base
}

func (task *installCompletion) IsValidTask() bool {
	active := task.activeTopology
	return active != nil && len(active.ClusterId) > 0 &&
		active.NextConfiguration != nil && active.NextConfiguration.Version == task.targetConfig.Version &&
		active.NextConfiguration.InstalledOnNew &&
		active.NextConfiguration.QuietRMIds[task.connectionManager.RMId] &&
		len(next.Pending) == 0
}

func (task *installCompletion) Tick() (bool, error) {
	if !task.IsValidTask() {
		task.inner.Logger.Log("msg", "Completion installed.")
		return task.completed()
	}

	next := task.activeTopology.NextConfiguration
	if _, found := next.RMsRemoved[task.connectionManager.RMId]; found {
		task.inner.Logger.Log("msg", "We've been removed from cluster. Taking no further part.")
		return false, nil
	}

	noisyCount := 0
	for _, rmId := range task.activeTopology.RMs {
		if _, found := next.QuietRMIds[rmId]; !found {
			noisyCount++
			if noisyCount > int(task.activeTopology.F) {
				task.inner.Logger.Log("msg", "Awaiting more original RMIds to become quiet.",
					"originals", fmt.Sprint(task.activeTopology.RMs))
				return false, nil
			}
		}
	}

	localHost, err := task.firstLocalHost(task.activeTopology.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	remoteHosts := task.allHostsBarLocalHost(localHost, next)
	task.installTopology(task.activeTopology, nil, localHost, remoteHosts)
	task.shareGoalWithAll()

	active, passive := task.formActivePassive(next.RMs, next.LostRMIds)
	if active == nil {
		return false, nil
	}

	twoFInc := uint16(next.RMs.NonEmptyLen())

	topology := task.activeTopology.Clone()
	topology.SetConfiguration(next.Configuration)

	oldRoots := task.activeTopology.RootVarUUIds
	newRoots := make([]configuration.Root, len(next.RootIndices))
	for idx, index := range next.RootIndices {
		newRoots[idx] = oldRoots[index]
	}
	topology.RootVarUUIds = newRoots

	txn := task.createTopologyTransaction(task.activeTopology, topology, twoFInc, active, passive)
	go task.runTopologyTransaction(task, txn, active, passive)
	return false, nil
}
