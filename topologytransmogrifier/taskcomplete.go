package topologytransmogrifier

import (
	"fmt"
	"goshawkdb.io/server/configuration"
)

type installCompletion struct {
	*transmogrificationTask
}

func (task *installCompletion) init(base *transmogrificationTask) {
	task.transmogrificationTask = base
}

func (task *installCompletion) isValid() bool {
	active := task.activeTopology
	return active != nil && len(active.ClusterId) > 0 &&
		task.targetConfig != nil &&
		active.NextConfiguration != nil &&
		active.NextConfiguration.Version == task.targetConfig.Version &&
		task.subscribed && active.NextConfiguration.InstalledOnNew &&
		active.NextConfiguration.QuietRMIds[task.self] &&
		len(active.NextConfiguration.Pending) == 0
}

func (task *installCompletion) announce() {
	task.inner.Logger.Log("stage", "Complete", "msg", "Object migration completed, switching to new topology.", "configuration", task.targetConfig)
}

func (task *installCompletion) Tick() (bool, error) {
	if task.selectStage() != task {
		return task.completed()
	}

	next := task.activeTopology.NextConfiguration
	if _, found := next.RMsRemoved[task.self]; found {
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
	task.ensureShareGoalWithAll()

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
	task.runTopologyTransaction(txn, active, passive, topology)
	return false, nil
}
