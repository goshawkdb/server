package topologytransmogrifier

type subscribe struct {
	*transmogrificationTask
	subscribed bool
}

func (task *subscribe) init(base *transmogrificationTask) {
	task.transmogrificationTask = base
	task.subscribed = false
}

func (task *subscribe) isValid() bool {
	active := task.activeTopology
	return active != nil && len(active.ClusterId) > 0 &&
		task.targetConfig != nil && !task.subscribed
}

func (task *subscribe) announce() {
	task.inner.Logger.Log("stage", "Subscribe", "msg", "Subscribing to topology.", "configuration", task.targetConfig)
}

func (task *subscribe) Tick() (bool, error) {
	if task.selectStage() != task {
		return task.completed()
	}

	return false, nil
}
