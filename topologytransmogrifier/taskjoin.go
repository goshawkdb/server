package topologytransmogrifier

import (
	"goshawkdb.io/common"
)

// joinCluster
// This task gets the local topology into a state where it can proceed
// to join and take part in transactions with other members of the
// cluster.

type joinCluster struct {
	*transmogrificationTask
}

func (task *joinCluster) init(base *transmogrificationTask) {
	task.transmogrificationTask = base
}

func (task *joinCluster) isValid() bool {
	active := task.activeTopology
	return active != nil && len(active.ClusterId) == 0 &&
		task.targetConfig != nil && task.targetConfig.Configuration != nil
}

func (task *joinCluster) announce() {
	task.inner.Logger.Log("stage", "Preparing to join cluster.", "configuration", task.targetConfig)
}

func (task *joinCluster) Tick() (bool, error) {
	if task.selectStage() != task {
		return task.completed()
	}

	targetClone := task.targetConfig.Configuration.Clone()
	localHost, remoteHosts, err := targetClone.LocalRemoteHosts(task.listenPort)
	if err != nil || len(localHost) == 0 {
		// For joining, it's fatal if we can't find ourself in the
		// target.
		return task.fatal(err)
	}

	// Set up the ClusterId so that we can actually create some connections.
	activeClone := task.activeTopology.Clone()
	activeClone.ClusterId = targetClone.ClusterId

	// Must install to connectionManager before launching any connections.
	task.installTopology(activeClone, nil, localHost, remoteHosts)

	// It's possible that different members of our goal are trying to
	// achieve different goals, so in all cases, we should share our
	// goal with them. This is essential if it turns out that we're
	// trying to join into an existing cluster - we can't possibly know
	// that's what's happening at this stage.
	task.ensureShareGoalWithAll()

	allHosts := append(remoteHosts, localHost)
	allConnected, clusterUUId, err := task.verifyClusterUUIds(activeClone.ClusterUUId, allHosts)
	// for joining, we must be connected to everyone
	if err != nil {
		return false, err
	} else if !allConnected {
		return false, nil
	}

	if allJoining := clusterUUId == 0; allJoining {
		allRMIds := make(common.RMIds, len(targetClone.Hosts))
		// make sure rmIds are in the same order as hosts
		for idx, host := range targetClone.Hosts {
			cd := task.hostToConnection[host]
			allRMIds[idx] = cd.RMId
		}

		activeClone = task.activeTopology.Clone()
		activeClone.ClusterId = targetClone.ClusterId
		activeClone.Hosts = targetClone.Hosts
		activeClone.F = targetClone.F
		activeClone.MaxRMCount = targetClone.MaxRMCount
		activeClone.RMs = allRMIds
		// we have to do this to correct the value of TwoFInc which is
		// used e.g. by localConnection.
		activeClone.SetConfiguration(activeClone.Configuration)

		return task.setActiveTopology(activeClone)

	} else {
		// If we're not allJoining then we need the previous config
		// because we need to make sure that everyone in the old config
		// learns of the change. The shareGoalWithAll() call above will
		// ensure this happens.

		task.inner.Logger.Log("msg", "Requesting help from existing cluster members for topology change.")
		return false, nil
	}
}
