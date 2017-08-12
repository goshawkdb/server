package topologyTransmogrifier

import (
	"fmt"
	"sync/atomic"
)

type migrate struct {
	*targetConfigBase
	emigrator *emigrator
}

func (task *migrate) Tick() (bool, error) {
	next := task.activeTopology.NextConfiguration
	if !(next != nil && next.Version == task.targetConfig.Version && len(next.Pending) > 0) {
		return task.completed()
	}

	task.inner.Logger.Log("msg", "Migration: all quiet, ready to attempt migration.")

	// By this point, we know that our vars can be safely
	// migrated. They can still learn from other txns going on, but,
	// because any RM receiving immigration will get F+1 copies, we
	// guarantee that they will get at least one most-up-to-date copy
	// of each relevant var, so it does not cause any problems for us
	// if we receive learnt outcomes during emigration.
	if task.isInRMs(task.activeTopology.RMs) {
		// don't attempt any emigration unless we were in the old
		// topology
		task.ensureEmigrator()
	}

	if _, found := next.Pending[task.connectionManager.RMId]; !found {
		task.inner.Logger.Log("msg", "All migration into all this RM completed. Awaiting others.")
		return false, nil
	}

	senders, found := task.migrations[next.Version]
	if !found {
		return false, nil
	}
	maxSuppliers := task.activeTopology.RMs.NonEmptyLen() - int(task.activeTopology.F)
	if task.isInRMs(task.activeTopology.RMs) {
		// We were part of the old topology, so we have already supplied ourselves!
		maxSuppliers--
	}
	topology := task.activeTopology.Clone()
	next = topology.NextConfiguration
	changed := false
	for sender, inprogressPtr := range senders {
		if atomic.LoadInt32(inprogressPtr) == 0 {
			// Because we wait for locallyComplete, we know they've gone to disk.
			changed = next.Pending.SuppliedBy(task.connectionManager.RMId, sender, maxSuppliers) || changed
		}
	}
	// We track progress by updating the topology to remove RMs who
	// have completed sending to us.
	if !changed {
		return false, nil
	}

	active, passive := task.formActivePassive(next.RMs, next.LostRMIds)
	if active == nil {
		return false, nil
	}

	twoFInc := uint16(next.RMs.NonEmptyLen())

	task.inner.Logger.Log("msg", "Recording local immigration progress.", "pending", next.Pending,
		"active", fmt.Sprint(active), "passive", fmt.Sprint(passive))

	txn := task.createTopologyTransaction(task.activeTopology, topology, twoFInc, active, passive)
	go task.runTopologyTransaction(task, txn, active, passive)

	task.shareGoalWithAll()
	return false, nil
}

func (task *migrate) Abandon() {
	task.ensureStopEmigrator()
	task.targetConfig.Abandon()
}

func (task *migrate) completed() (bool, error) {
	task.ensureStopEmigrator()
	return task.targetConfig.completed()
}

func (task *migrate) ensureEmigrator() {
	if task.emigrator == nil {
		task.emigrator = newEmigrator(task)
	}
}

func (task *migrate) ensureStopEmigrator() {
	if task.emigrator != nil {
		task.emigrator.stopAsync()
		task.emigrator = nil
	}
}