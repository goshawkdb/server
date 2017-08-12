package topologyTransmogrifier

import (
	"errors"
)

// ensureLocalTopology

type ensureLocalTopology struct {
	*targetConfigBase
}

func (task *ensureLocalTopology) Tick() (bool, error) {
	if task.activeTopology != nil {
		if task.targetConfig.Configuration == nil {
			// There was no config supplied on the command line, so just
			// pop what we've read in here.
			task.targetConfig.Configuration = task.activeTopology.Configuration
		}
		// The fact we're here means we're done - there is a topology
		// discovered one way or another.
		return task.completed()
	}

	if _, found := task.activeConnections[task.connectionManager.RMId]; !found {
		return false, nil
	}

	topology, err := task.getTopologyFromLocalDatabase()
	if err != nil {
		return task.fatal(err)
	}

	if topology == nil {
		if task.targetConfig == nil || task.targetConfig.Configuration == nil || len(task.targetConfig.Configuration.ClusterId) == 0 {
			return task.fatal(errors.New("No configuration supplied and no configuration found in local store. Cannot continue."))
		}

		_, err = task.createTopologyZero(task.targetConfig)
		if err != nil {
			return task.fatal(err)
		}
		// if err == nil, the create succeeded, so wait for observation
		return false, nil
	} else {
		// It's already on disk, we're not going to see it through the subscriber.
		return task.setActive(topology)
	}
}
