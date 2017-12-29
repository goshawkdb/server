package topologytransmogrifier

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
	task.inner.Logger.Log("stage", "Ensure", "msg", "Ensuring local topology.", "configuration", task.targetConfig)
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
		if task.targetConfig == nil || len(task.targetConfig.ClusterId) == 0 {
			return task.fatal(errors.New("No configuration supplied and no configuration found in local store. Cannot continue."))
		}

		topology, err = task.createTopologyZero()
		if err != nil {
			return task.fatal(err)
		}
	}
	// We don't have a working subscriber added yet, so we need to set
	// this one manually. Plus this also copes with when we already
	// have a topology on disk.
	return task.setActiveTopology(topology)
}
