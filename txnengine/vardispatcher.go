package txnengine

import (
	"fmt"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/dispatcher"
	"sync/atomic"
)

type VarDispatcher struct {
	dispatcher.Dispatcher
	varmanagers []*VarManager
}

func NewVarDispatcher(count uint8, server *mdbs.MDBServer, lc LocalConnection) *VarDispatcher {
	vd := &VarDispatcher{
		varmanagers: make([]*VarManager, count),
	}
	vd.Dispatcher.Init(count)
	for idx, exe := range vd.Executors {
		vd.varmanagers[idx] = NewVarManager(exe, server, lc)
	}
	return vd
}

func (vd *VarDispatcher) ApplyToVar(fun func(*Var, error), createIfMissing bool, vUUId *common.VarUUId) {
	vd.withVarManager(vUUId, func(vm *VarManager) { vm.ApplyToVar(fun, createIfMissing, vUUId) })
}

func (vd *VarDispatcher) OnAllCommitted(f func()) {
	count := int32(vd.ExecutorCount)
	g := func() {
		if atomic.AddInt32(&count, -1) == 0 {
			f()
		}
	}
	for idx, exe := range vd.Executors {
		mgr := vd.varmanagers[idx]
		exe.Enqueue(func() { mgr.OnAllCommitted(g) })
	}
}

func (vd *VarDispatcher) Immigrate(migration *msgs.Migration, cont func(error)) {
	// Technically, the way that the message is built means that we
	// don't have to create this map because every use of each txn will
	// occur in a block. However, we're not going to make that
	// guarantee, and this is more obvious code anyway.
	txnsList := migration.Txns()
	txnsCount := txnsList.Len()
	txns := make(map[common.TxnId]*msgs.Txn, txnsCount)
	for idx := 0; idx < txnsCount; idx++ {
		txnCap := txnsList.At(idx)
		txnId := common.MakeTxnId(txnCap.Id())
		txns[*txnId] = &txnCap
	}

	varsList := migration.Vars()
	for idx, l := 0, varsList.Len(); idx < l; idx++ {
		varCap := varsList.At(idx)
		vUUId := common.MakeVarUUId(varCap.Id())
		txnId := common.MakeTxnId(varCap.WriteTxnId())
		txnCap, found := txns[*txnId]
		if !found {
			panic(fmt.Sprintf("Migration contained %v which has write txn %v which is not found in message! %v",
				vUUId, txnId, txns))
		}
		vd.withVarManager(vUUId, func(vm *VarManager) { vm.Immigrate(vUUId, &varCap, txnId, txnCap, cont) })
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
