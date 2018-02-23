package topologytransmogrifier

import (
	"fmt"
)

// installTargetNew
// Now that everyone in the old/current topology knows about the Next
// topology, we need to do a further txn to ensure everyone new who's
// joining the cluster gets told.

type installTargetNew struct {
	*transmogrificationTask
}

func (task *installTargetNew) init(base *transmogrificationTask) {
	task.transmogrificationTask = base
}

func (task *installTargetNew) isValid() bool {
	active := task.activeTopology
	return active != nil && len(active.ClusterId) > 0 &&
		task.targetConfig != nil &&
		active.Version < task.targetConfig.Version &&
		active.NextConfiguration != nil &&
		active.NextConfiguration.Version == task.targetConfig.Version &&
		!active.NextConfiguration.InstalledOnNew
}

func (task *installTargetNew) announce() {
	task.inner.Logger.Log("stage", "New", "msg", "Attempting to install topology change to new cluster.", "configuration", task.targetConfig)
}

func (task *installTargetNew) Tick() (bool, error) {
	if task.selectStage() != task {
		return task.completed()
	}

	next := task.activeTopology.NextConfiguration
	localHost, err := task.firstLocalHost(task.activeTopology.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	remoteHosts := task.allHostsBarLocalHost(localHost, next)
	task.installTopology(task.activeTopology, nil, localHost, remoteHosts)
	task.ensureShareGoalWithAll()

	if !task.isInRMs(next.NewRMIds) {
		task.inner.Logger.Log("msg", "Awaiting new cluster members.")
		// this step must be performed by the new RMs
		return false, nil
	}

	active, passive := task.formActivePassive(next.RMs, next.LostRMIds)
	if active == nil {
		return false, nil
	}

	task.inner.Logger.Log("msg", "Installing on new cluster members.",
		"active", fmt.Sprint(active), "passive", fmt.Sprint(passive))

	topology := task.activeTopology.Clone()
	topology.NextConfiguration.InstalledOnNew = true

	twoFInc := uint16(next.RMs.NonEmptyLen())
	txn := task.createTopologyTransaction(task.activeTopology, topology, twoFInc, active, passive)
	task.runTopologyTransaction(txn, active, passive, topology)
	return false, nil
}
