package topologyTransmogrifier

import (
	"errors"
	"fmt"
	mdb "github.com/msackman/gomdb"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	eng "goshawkdb.io/server/txnengine"
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
	if tt.activeTopology != nil {
		switch {
		case tt.activeTopology.ClusterId != topology.ClusterId && len(tt.activeTopology.ClusterId) > 0:
			return false, fmt.Errorf("Topology: Fatal: config with ClusterId change from '%s' to '%s'.",
				tt.activeTopology.ClusterId, topology.ClusterId)

		case topology.Version < tt.activeTopology.Version:
			tt.inner.Logger.Log("msg", "Ignoring config with version less than active version.",
				"goalVersion", topology.Version, "activeVersion", tt.activeTopology.Version)
			return false, nil

		case tt.activeTopology.Configuration.Equal(topology.Configuration):
			// silently ignore it
			return false, nil
		}
	}

	if _, found := topology.RMsRemoved[tt.connectionManager.RMId]; found {
		return false, errors.New("We have been removed from the cluster. Shutting down.")
	}
	tt.activeTopology = topology

	if tt.currentTask == nil {
		if next := topology.NextConfiguration; next == nil {
			localHost, remoteHosts, err := tt.activeTopology.LocalRemoteHosts(tt.listenPort)
			if err != nil {
				return false, err
			}
			tt.installTopology(topology, nil, localHost, remoteHosts)
			tt.inner.Logger.Log("msg", "Topology change complete.", "localhost", localHost, "RMId", tt.connectionManager.RMId)

			for version := range tt.migrations {
				if version <= topology.Version {
					delete(tt.migrations, version)
				}
			}

			_, err = tt.db.WithEnv(func(env *mdb.Env) (interface{}, error) {
				return nil, env.SetFlags(mdb.NOSYNC, topology.NoSync)
			}).ResultError()
			return false, err

		} else {
			return false, tt.setTarget(next)
		}
	} else {
		return tt.currentTask.Tick()
	}
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

func (tt *TopologyTransmogrifier) setTarget(targetConfig *configuration.NextConfiguration) error {
	if tt.activeTopology != nil {
		activeConfig := tt.activeTopology.Configuration
		activeClusterUUId, targetClusterUUId := activeConfig.ClusterUUId, targetConfig.ClusterUUId
		switch {
		case targetConfig.ClusterId != activeConfig.ClusterId && len(activeConfig.ClusterId) > 0:
			return fmt.Errorf("Illegal config change: ClusterId should be '%s' instead of '%s'.",
				activeConfig.ClusterId, targetConfig.ClusterId)

		case targetClusterUUId != 0 && activeClusterUUId != 0 && targetClusterUUId != activeClusterUUId:
			return fmt.Errorf("Illegal config change: ClusterUUId should be '%v' instead of '%v'.",
				activeClusterUUId, targetClusterUUId)

		case targetConfig.MaxRMCount != activeConfig.MaxRMCount && activeConfig.Version != 0:
			return fmt.Errorf("Illegal config change: Currently changes to MaxRMCount are not supported, sorry.")

		case targetConfig.Configuration.EqualExternally(activeConfig):
			tt.inner.Logger.Log("msg", "Config transition completed.", "activeVersion", activeConfig.Version)
			return nil

		case targetConfig.Version == activeConfig.Version:
			return fmt.Errorf("Illegal config change: Config has changed but Version has not been increased (%v). Ignoring.", targetConfig.Version)

		case targetConfig.Version < activeConfig.Version:
			return fmt.Errorf("Illegal config change: Ignoring config with version %v as newer version already active (%v).",
				targetConfig.Version, activeConfig.Version)
		}
	}

	if tt.currentTask != nil {
		panic("setTarget called with non nil currentTask")
	}

	server.DebugLog(tt.inner.Logger, "debug", "Creating new task.")
	tt.currentTask = &targetConfig{
		TopologyTransmogrifier: tt,
		targetConfig:           targetConfig,
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
