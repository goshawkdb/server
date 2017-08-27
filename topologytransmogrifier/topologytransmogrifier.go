package topologytransmogrifier

import (
	"errors"
	"fmt"
	"goshawkdb.io/server/configuration"
	topo "goshawkdb.io/server/types/topology"
	"goshawkdb.io/server/utils"
)

func (tt *TopologyTransmogrifier) maybeTick() (bool, error) {
	if tt.currentTask == nil {
		return false, nil
	} else {
		return tt.currentTask.Tick()
	}
}

func (tt *TopologyTransmogrifier) setActiveTopology(topology *configuration.Topology) (bool, error) {
	utils.DebugLog(tt.inner.Logger, "debug", "SetActiveTopology.", "topology", topology)
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

	if _, found := topology.RMsRemoved[tt.self]; found {
		return false, errors.New("We have been removed from the cluster. Shutting down.")
	}
	tt.activeTopology = topology

	if tt.currentTask == nil {
		if next := topology.NextConfiguration; next == nil {
			return false, nil
		} else {
			return false, tt.setTarget(next.Configuration)
		}
	} else {
		return tt.currentTask.Tick()
	}
}

func (tt *TopologyTransmogrifier) installTopology(topology *configuration.Topology, callbacks map[topo.TopologyChangeSubscriberType]func() (bool, error), localHost string, remoteHosts []string) {
	utils.DebugLog(tt.inner.Logger, "debug", "Installing topology to connection manager, et al.", "topology", topology)
	if tt.localEstablished != nil {
		if callbacks == nil {
			callbacks = make(map[topo.TopologyChangeSubscriberType]func() (bool, error))
		}
		origFun := callbacks[topo.ConnectionManagerSubscriber]
		callbacks[topo.ConnectionManagerSubscriber] = func() (bool, error) {
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
	wrapped := make(map[topo.TopologyChangeSubscriberType]func(), len(callbacks))
	for subType, cb := range callbacks {
		cbCopy := cb
		wrapped[subType] = func() { tt.EnqueueFuncAsync(cbCopy) }
	}
	tt.connectionManager.SetTopology(topology, wrapped, localHost, remoteHosts)
}

func (tt *TopologyTransmogrifier) setTarget(targetConfig *configuration.Configuration) error {
	// This can be called both via a msg (eg cmdline and SIGHUP), or
	// when there is no current task and we have to think about
	// creating one. If there is a currentTask, then we compare
	// targetConfig with that. Otherwise we compare with the
	// activeTopology.

	var versusConfig *configuration.Configuration
	if tt.currentTask == nil {
		if tt.activeTopology != nil {
			versusConfig = tt.activeTopology.Configuration
		}
	} else {
		versusConfig = tt.currentTask.TargetConfig()
	}

	if versusConfig != nil {
		versusClusterUUId, targetClusterUUId := versusConfig.ClusterUUId, targetConfig.ClusterUUId
		switch {
		case targetConfig.ClusterId != versusConfig.ClusterId && len(versusConfig.ClusterId) > 0:
			return fmt.Errorf("Illegal config change: ClusterId should be '%s' instead of '%s'.",
				versusConfig.ClusterId, targetConfig.ClusterId)

		case targetClusterUUId != 0 && versusClusterUUId != 0 && targetClusterUUId != versusClusterUUId:
			return fmt.Errorf("Illegal config change: ClusterUUId should be '%v' instead of '%v'.",
				versusClusterUUId, targetClusterUUId)

		case targetConfig.MaxRMCount != versusConfig.MaxRMCount && versusConfig.Version != 0:
			return fmt.Errorf("Illegal config change: Currently changes to MaxRMCount are not supported, sorry.")

		case targetConfig.EqualExternally(versusConfig):
			if tt.currentTask == nil {
				tt.inner.Logger.Log("msg", "Config already reached.", "version", versusConfig.Version)
			} else {
				tt.inner.Logger.Log("msg", "Config already being targetted.", "version", versusConfig.Version)
			}
			return nil

		case targetConfig.Version == versusConfig.Version:
			return fmt.Errorf("Illegal config change: Config has changed but Version has not been increased (%v). Ignoring.", targetConfig.Version)

		case targetConfig.Version < versusConfig.Version:
			return fmt.Errorf("Illegal config change: Ignoring config with version %v as newer version already seen (%v).",
				targetConfig.Version, versusConfig.Version)
		}
	}

	// if we're here and there is a currentTask then we know
	// currentTask is insufficient
	if tt.currentTask != nil {
		tt.currentTask.Abandon()
	}

	utils.DebugLog(tt.inner.Logger, "debug", "Creating new task.")
	tt.currentTask = tt.newTransmogrificationTask(targetConfig)
	return nil
}
