package topologytransmogrifier

import (
	"errors"
	"goshawkdb.io/common"
)

type joinCluster struct {
	*transmogrificationTask
}

func (task *joinCluster) init(base *transmogrificationTask) {
	task.transmogrificationTask = base
}

func (task *joinCluster) isValid() bool {
	return len(task.activeTopology.ClusterId) == 0
}

func (task *joinCluster) announce() {
	task.inner.Logger.Log("msg", "Attempting to join cluster.", "configuration", task.targetConfig)
}

func (task *joinCluster) Tick() (bool, error) {
	if task.selectStage() != task {
		return task.completed()
	}

	localHost, remoteHosts, err := task.targetConfig.LocalRemoteHosts(task.listenPort)
	if err != nil {
		// For joining, it's fatal if we can't find ourself in the
		// target.
		return task.fatal(err)
	}

	// Set up the ClusterId so that we can actually create some connections.
	active := task.activeTopology.Clone()
	active.ClusterId = task.targetConfig.ClusterId

	// Must install to connectionManager before launching any
	// connections so that we get the ClusterId verification correct.
	task.installTopology(active, nil, localHost, remoteHosts)

	// It's possible that different members of our goal are trying to
	// achieve different goals, so in all cases, we should share our
	// goal with them. This is essential if it turns out that we're
	// trying to join into an existing cluster - we can't possibly know
	// that's what's happening at this stage.
	task.ensureShareGoalWithAll()

	rmIds := make([]common.RMId, 0, len(task.targetConfig.Hosts))
	clusterUUId := uint64(0)
	for _, host := range task.targetConfig.Hosts {
		cd, found := task.hostToConnection[host]
		if !found {
			// We can only continue at this point if we really are
			// connected to everyone mentioned in the config.
			return false, nil
		}
		rmIds = append(rmIds, cd.RMId)
		switch theirClusterUUId := cd.ClusterUUId; {
		case theirClusterUUId == 0:
			// they're joining too
		case clusterUUId == 0:
			clusterUUId = theirClusterUUId
		case clusterUUId == theirClusterUUId:
			// all good
		default:
			return task.fatal(
				errors.New("Attempt made to merge different logical clusters together, which is illegal. Aborting."))
		}
	}

	if allJoining := clusterUUId == 0; allJoining {
		// Note that the order of RMIds here matches the order of hosts.
		return task.allJoining(rmIds)

	} else {
		// If we're not allJoining then we need the previous config
		// because we need to make sure that everyone in the old config
		// learns of the change. The ensureShareGoalWithAll() call above
		// will ensure this happens.

		task.inner.Logger.Log("msg", "Requesting help from existing cluster members for topology change.")
		return false, nil
	}
}

func (task *joinCluster) allJoining(allRMIds common.RMIds) (bool, error) {
	// NB: active never gets installed to the DB itself.
	active := task.activeTopology.Clone()
	active.ClusterId = task.targetConfig.ClusterId
	active.RMs = common.RMIds{task.self}

	return task.setActiveTopology(active)
}
