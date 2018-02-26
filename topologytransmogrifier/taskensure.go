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
		// We can't subscribe unless the topology exists. Now that we
		// know it exists, we must set it active so the subscriber can
		// do its work.
		return task.setActiveTopology(topology)

	} else {
		// Similarly, the subscriber will only start trying to subscribe
		// if we tell it there's a topology on disk that can be
		// subscribed to!

		return task.setActiveTopology(topology)
	}
}
