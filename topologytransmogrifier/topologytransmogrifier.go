package topologyTransmogrifier

import (
	"bytes"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/client"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"math/rand"
	"sync/atomic"
	"time"
)

func (tt *TopologyTransmogrifier) maybeTick() (bool, error) {
	if tt.currentTask == nil {
		return false, nil
	} else {
		return tt.currentTask.Tick()
	}
}

func (tt *TopologyTransmogrifier) setActiveTopology(topology *configuration.Topology) (bool, error) {
	server.DebugLog(tt.inner.Logger, "debug", "SetActiveTopology.", "topology", topology)
	if tt.active != nil {
		switch {
		case tt.active.ClusterId != topology.ClusterId && tt.active.ClusterId != "":
			return false, fmt.Errorf("Topology: Fatal: config with ClusterId change from '%s' to '%s'.",
				tt.active.ClusterId, topology.ClusterId)

		case topology.Version < tt.active.Version:
			tt.inner.Logger.Log("msg", "Ignoring config with version less than active version.",
				"goalVersion", topology.Version, "activeVersion", tt.active.Version)
			return false, nil

		case tt.active.Configuration.Equal(topology.Configuration):
			// silently ignore it
			return false, nil
		}
	}

	if _, found := topology.RMsRemoved[tt.connectionManager.RMId]; found {
		return false, errors.New("We have been removed from the cluster. Shutting down.")
	}
	tt.active = topology

	if tt.currentTask != nil {
		if terminate, err := tt.currentTask.Tick(); terminate || err != nil {
			return terminate, err
		}
	}

	if tt.currentTask == nil {
		if next := topology.NextConfiguration; next == nil {
			localHost, remoteHosts, err := tt.active.LocalRemoteHosts(tt.listenPort)
			if err != nil {
				return false, err
			}
			tt.installTopology(topology, nil, localHost, remoteHosts)
			tt.inner.Logger.Log("msg", "Topology change complete.", "localhost", localHost, "RMId", tt.connectionManager.RMId)

			future := tt.db.WithEnv(func(env *mdb.Env) (interface{}, error) {
				return nil, env.SetFlags(mdb.NOSYNC, topology.NoSync)
			})
			for version := range tt.migrations {
				if version <= topology.Version {
					delete(tt.migrations, version)
				}
			}

			_, err = future.ResultError()
			if err != nil {
				return false, err
			}

		} else {
			return false, tt.selectGoal(next)
		}
	}
	return false, nil
}

func (tt *TopologyTransmogrifier) installTopology(topology *configuration.Topology, callbacks map[eng.TopologyChangeSubscriberType]func() (bool, error), localHost string, remoteHosts []string) {
	server.DebugLog(tt.inner.Logger, "debug", "Installing topology to connection manager, et al.", "topology", topology)
	if tt.localEstablished != nil {
		if callbacks == nil {
			callbacks = make(map[eng.TopologyChangeSubscriberType]func() (bool, error))
		}
		origFun := callbacks[eng.ConnectionManagerSubscriber]
		callbacks[eng.ConnectionManagerSubscriber] = func() (bool, error) {
			if tt.localEstablished != nil {
				close(tt.localEstablished)
				tt.localEstablished = nil
			}
			if origFun == nil {
				return false, nil
			} else {
				return origFun()
			}
		}
	}
	wrapped := make(map[eng.TopologyChangeSubscriberType]func(), len(callbacks))
	for subType, cb := range callbacks {
		cbCopy := cb
		wrapped[subType] = func() { tt.EnqueueFuncAsync(cbCopy) }
	}
	tt.connectionManager.SetTopology(topology, wrapped, localHost, remoteHosts)
}

func (tt *TopologyTransmogrifier) selectGoal(goal *configuration.NextConfiguration) error {
	if tt.active != nil {
		activeClusterUUId, goalClusterUUId := tt.active.ClusterUUId, goal.ClusterUUId
		switch {
		case goal.Version == 0:
			return nil // done installing version0.

		case goal.ClusterId != tt.active.ClusterId && tt.active.ClusterId != "":
			return fmt.Errorf("Illegal config change: ClusterId should be '%s' instead of '%s'.",
				tt.active.ClusterId, goal.ClusterId)

		case goalClusterUUId != 0 && activeClusterUUId != 0 && goalClusterUUId != activeClusterUUId:
			return fmt.Errorf("Illegal config change: ClusterUUId should be '%v' instead of '%v'.",
				activeClusterUUId, goalClusterUUId)

		case goal.MaxRMCount != tt.active.MaxRMCount && tt.active.Version != 0:
			return fmt.Errorf("Illegal config change: Currently changes to MaxRMCount are not supported, sorry.")

		case goal.Version < tt.active.Version:
			return fmt.Errorf("Illegal config change: Ignoring config with version %v as newer version already active (%v).",
				goal.Version, tt.active.Version)

		case goal.Configuration.EqualExternally(tt.active.Configuration):
			tt.inner.Logger.Log("msg", "Config transition completed.", "activeVersion", goal.Version)
			return nil

		case goal.Version == tt.active.Version:
			return fmt.Errorf("Illegal config change: Config has changed but Version has not been increased (%v). Ignoring.", goal.Version)
		}
	}

	if tt.currentTask != nil {
		existingGoal := tt.currentTask.Goal()
		switch {
		case goal.ClusterId != existingGoal.ClusterId:
			return fmt.Errorf("Illegal config change: ClusterId should be '%s' instead of '%s'.",
				existingGoal.ClusterId, goal.ClusterId)

		case goal.Version < existingGoal.Version:
			return fmt.Errorf("Illegal config change: Ignoring config with version %v as newer version already targetted (%v).",
				goal.Version, existingGoal.Version)

		case goal.Configuration.EqualExternally(existingGoal.Configuration):
			tt.inner.Logger.Log("msg", "Config transition already in progress.", "goalVersion", goal.Version)
			return nil

		case goal.Version == existingGoal.Version:
			return fmt.Errorf("Illegal config change: Config has changed but Version has not been increased (%v). Ignoring.", goal.Version)

		default:
			server.DebugLog(tt.inner.Logger, "debug", "Abandoning old task.")
			tt.currentTask.Abandon()
			tt.currentTask = nil
		}
	}

	if tt.currentTask == nil {
		server.DebugLog(tt.inner.Logger, "debug", "Creating new task.")
		tt.currentTask = &targetConfig{
			TopologyTransmogrifier: tt,
			config:                 goal,
		}
	}
	return nil
}

func (tt *TopologyTransmogrifier) enqueueTick(task topologyTask, tc *targetConfig) {
	if !tc.tickEnqueued {
		tc.tickEnqueued = true
		tc.createOrAdvanceBackoff()
		tc.backoff.After(func() {
			tt.EnqueueFuncAsync(func() (bool, error) {
				tc.tickEnqueued = false
				if tt.currentTask == task {
					return tt.currentTask.Tick()
				}
				return false, nil
			})
		})
	}
}

func (tt *TopologyTransmogrifier) maybeTick2(task topologyTask, tc *targetConfig) func() bool {
	var i uint32 = 0
	closer := func() bool {
		return atomic.CompareAndSwapUint32(&i, 0, 1)
	}
	time.AfterFunc(2*time.Second, func() {
		if !closer() {
			return
		}
		tt.EnqueueFuncAsync(func() (bool, error) {
			tt.enqueueTick(task, tc)
			return false, nil
		})
	})
	return closer
}

// migrate

type migrate struct {
	*targetConfig
	emigrator *emigrator
}

func (task *migrate) Tick() (bool, error) {
	next := task.active.NextConfiguration
	if !(next != nil && next.Version == task.config.Version && len(next.Pending) > 0) {
		return task.completed()
	}

	task.inner.Logger.Log("msg", "Migration: all quiet, ready to attempt migration.")

	// By this point, we know that our vars can be safely
	// migrated. They can still learn from other txns going on, but,
	// because any RM receiving immigration will get F+1 copies, we
	// guarantee that they will get at least one most-up-to-date copy
	// of each relevant var, so it does not cause any problems for us
	// if we receive learnt outcomes during emigration.
	if task.isInRMs(task.active.RMs) {
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
	maxSuppliers := task.active.RMs.NonEmptyLen() - int(task.active.F)
	if task.isInRMs(task.active.RMs) {
		// We were part of the old topology, so we have already supplied ourselves!
		maxSuppliers--
	}
	topology := task.active.Clone()
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

	txn := task.createTopologyTransaction(task.active, topology, twoFInc, active, passive)
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

// install Completion

type installCompletion struct {
	*targetConfig
}

func (task *installCompletion) Tick() (bool, error) {
	next := task.active.NextConfiguration
	if next == nil {
		task.inner.Logger.Log("msg", "Completion installed.")
		return task.completed()
	}

	if _, found := next.RMsRemoved[task.connectionManager.RMId]; found {
		task.inner.Logger.Log("msg", "We've been removed from cluster. Taking no further part.")
		return false, nil
	}

	noisyCount := 0
	for _, rmId := range task.active.RMs {
		if _, found := next.QuietRMIds[rmId]; !found {
			noisyCount++
			if noisyCount > int(task.active.F) {
				task.inner.Logger.Log("msg", "Awaiting more original RMIds to become quiet.",
					"originals", fmt.Sprint(task.active.RMs))
				return false, nil
			}
		}
	}

	localHost, err := task.firstLocalHost(task.active.Configuration)
	if err != nil {
		return task.fatal(err)
	}

	remoteHosts := task.allHostsBarLocalHost(localHost, next)
	task.installTopology(task.active, nil, localHost, remoteHosts)
	task.shareGoalWithAll()

	active, passive := task.formActivePassive(next.RMs, next.LostRMIds)
	if active == nil {
		return false, nil
	}

	twoFInc := uint16(next.RMs.NonEmptyLen())

	topology := task.active.Clone()
	topology.SetConfiguration(next.Configuration)

	oldRoots := task.active.RootVarUUIds
	newRoots := make([]configuration.Root, len(next.RootIndices))
	for idx, index := range next.RootIndices {
		newRoots[idx] = oldRoots[index]
	}
	topology.RootVarUUIds = newRoots

	txn := task.createTopologyTransaction(task.active, topology, twoFInc, active, passive)
	go task.runTopologyTransaction(task, txn, active, passive)
	return false, nil
}
