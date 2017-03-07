package txnengine

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
)

type TopologyPublisher interface {
	AddTopologySubscriber(TopologyChangeSubscriberType, TopologySubscriber) *configuration.Topology
	RemoveTopologySubscriberAsync(TopologyChangeSubscriberType, TopologySubscriber)
}

type TopologySubscriber interface {
	TopologyChanged(*configuration.Topology, func(bool))
}

type TopologyChangeSubscriberType uint8

const (
	VarSubscriber                     TopologyChangeSubscriberType = iota
	ProposerSubscriber                TopologyChangeSubscriberType = iota
	AcceptorSubscriber                TopologyChangeSubscriberType = iota
	ConnectionSubscriber              TopologyChangeSubscriberType = iota
	ConnectionManagerSubscriber       TopologyChangeSubscriberType = iota
	EmigratorSubscriber               TopologyChangeSubscriberType = iota
	MiscSubscriber                    TopologyChangeSubscriberType = iota
	TopologyChangeSubscriberTypeLimit int                          = iota
)

type VarDispatcher struct {
	dispatcher.Dispatcher
	varmanagers []*VarManager
}

func NewVarDispatcher(count uint8, rmId common.RMId, cm TopologyPublisher, db *db.Databases, lc LocalConnection, logger log.Logger) *VarDispatcher {
	vd := &VarDispatcher{
		varmanagers: make([]*VarManager, count),
	}
	logger = log.With(logger, "subsystem", "varManager")
	vd.Dispatcher.Init(count, logger)
	for idx, exe := range vd.Executors {
		vd.varmanagers[idx] = NewVarManager(exe, rmId, cm, db, lc,
			log.With(logger, "instance", idx))
	}
	return vd
}

func (vd *VarDispatcher) ApplyToVar(fun func(*Var), createIfMissing bool, vUUId *common.VarUUId) {
	vd.withVarManager(vUUId, func(vm *VarManager) { vm.ApplyToVar(fun, createIfMissing, vUUId) })
}

func (vd *VarDispatcher) Status(sc *server.StatusConsumer) {
	sc.Emit("Vars")
	for idx, executor := range vd.Executors {
		s := sc.Fork()
		s.Emit(fmt.Sprintf("Var Manager %v", idx))
		manager := vd.varmanagers[idx]
		executor.Enqueue(func() { manager.Status(s) })
	}
	sc.Join()
}

func (vd *VarDispatcher) withVarManager(vUUId *common.VarUUId, fun func(*VarManager)) bool {
	idx := uint8(vUUId[server.MostRandomByteIndex]) % vd.ExecutorCount
	executor := vd.Executors[idx]
	manager := vd.varmanagers[idx]
	return executor.Enqueue(func() { fun(manager) })
}

type TranslationCallback func(*cmsgs.ClientAction, *msgs.Action, []common.RMId, map[common.RMId]bool) error
type LocalConnection interface {
	RunClientTransaction(*cmsgs.ClientTxn, map[common.VarUUId]*common.Positions, TranslationCallback) (*TxnReader, *msgs.Outcome, error)
	Status(*server.StatusConsumer)
}
