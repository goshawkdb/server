package topologyTransmogrifier

import (
	"errors"
)

// ensureLocalTopology

type ensureLocalTopology struct {
	*transmogrificationTask
}

func (task *ensureLocalTopology) init(base *transmogrificationTask) {
	task.transmogrificationTask = base
}

func (task *ensureLocalTopology) isValid() bool {
	return task.activeTopology == nil
}

func (task *ensureLocalTopology) announce() {
	task.inner.Logger.Log("msg", "Ensuring local topology.")
}

func (task *ensureLocalTopology) Tick() (bool, error) {
	if task.selectStage() != task {
		return task.completed()
	}

	if _, found := task.activeConnections[task.self]; !found {
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
		return task.setActiveTopology(topology)
	}
}
