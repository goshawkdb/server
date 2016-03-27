package txnengine

import (
	"fmt"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	"sync/atomic"
)

type VarDispatcher struct {
	dispatcher.Dispatcher
	varmanagers []*VarManager
}

func NewVarDispatcher(count uint8, db *db.Databases, lc LocalConnection) *VarDispatcher {
	vd := &VarDispatcher{
		varmanagers: make([]*VarManager, count),
	}
	vd.Dispatcher.Init(count)
	for idx, exe := range vd.Executors {
		vd.varmanagers[idx] = NewVarManager(exe, db, lc)
	}
	return vd
}

func (vd *VarDispatcher) ApplyToVar(fun func(*Var, error), createIfMissing bool, vUUId *common.VarUUId) {
	vd.withVarManager(vUUId, func(vm *VarManager) { vm.ApplyToVar(fun, createIfMissing, vUUId) })
}

func (vd *VarDispatcher) ForceToIdle(onIdleOrig func()) {
	count := int32(vd.ExecutorCount)
	onIdle := func() {
		if atomic.AddInt32(&count, -1) == 0 {
			onIdleOrig()
		}
	}
	for idx, exe := range vd.Executors {
		mgr := vd.varmanagers[idx]
		exe.Enqueue(func() { mgr.ForceToIdle(onIdle) })
	}
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

type LocalConnection interface {
	RunClientTransaction(txn *cmsgs.ClientTxn, varPosMap map[common.VarUUId]*common.Positions, assignTxnId bool) (*msgs.Outcome, error)
	Status(*server.StatusConsumer)
}
