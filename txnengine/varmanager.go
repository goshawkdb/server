package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	"math/rand"
	"time"
)

type VarManager struct {
	LocalConnection
	disk       *mdbs.MDBServer
	active     map[common.VarUUId]*Var
	onIdle     func()
	onVarIdle  map[common.VarUUId][]func()
	lc         LocalConnection
	callbacks  []func()
	beaterLive bool
	exe        *dispatcher.Executor
}

func init() {
	db.DB.Vars = &mdbs.DBISettings{Flags: mdb.CREATE}
}

func NewVarManager(exe *dispatcher.Executor, server *mdbs.MDBServer, lc LocalConnection) *VarManager {
	return &VarManager{
		LocalConnection: lc,
		disk:            server,
		active:          make(map[common.VarUUId]*Var),
		onVarIdle:       make(map[common.VarUUId][]func()),
		callbacks:       []func(){},
		exe:             exe,
	}
}

func (vm *VarManager) ApplyToVar(fun func(*Var, error), createIfMissing bool, uuid *common.VarUUId) {
	v, err := vm.find(uuid)
	if err == mdb.NotFound && createIfMissing {
		v = NewVar(uuid, vm.exe, vm.disk, vm)
		vm.active[*v.UUId] = v
		server.Log(uuid, "New var")
	} else if err != nil {
		fun(nil, err)
		return
	}
	fun(v, nil)
	if _, found := vm.active[*uuid]; !found && !v.isIdle() {
		panic(fmt.Sprintf("Var is not active, yet is not idle! %v %v", uuid, fun))
	} else if vm.onIdle != nil {
		if found {
			v.ForceToIdle()
		}
		vm.checkAllIdle()
	}
}

func (vm *VarManager) ForceToIdle(onIdle func()) {
	vm.onIdle = onIdle
	if !vm.checkAllIdle() {
		for uuid, v := range vm.active {
			if configuration.TopologyVarUUId.Compare(&uuid) == common.EQ {
				continue
			}
			v.ForceToIdle()
		}
		vm.checkAllIdle()
	}
}

func (vm *VarManager) checkAllIdle() bool {
	onIdle := vm.onIdle
	if onIdle == nil {
		return true
	} else if l := len(vm.active); l == 0 {
		vm.onIdle = nil
		onIdle()
		return true
	} else if _, found := vm.active[*configuration.TopologyVarUUId]; l == 1 && found {
		vm.onIdle = nil
		onIdle()
		return true
	}
	return false
}

// var.VarLifecycle interface
func (vm *VarManager) SetInactive(v *Var) {
	server.Log(v.UUId, "is now inactive")
	v1, found := vm.active[*v.UUId]
	switch {
	case !found:
		panic(fmt.Sprintf("%v inactive but doesn't exist!\n", v.UUId))
	case v1 != v:
		panic(fmt.Sprintf("%v inactive but different var! %p %p\n", v.UUId, v, v1))
	default:
		//fmt.Printf("%v is now inactive. ", v.UUId)
		delete(vm.active, *v.UUId)
		if funcs, found := vm.onVarIdle[*v.UUId]; found {
			delete(vm.onVarIdle, *v.UUId)
			for _, f := range funcs {
				f()
			}
		}
	}
}

func (vm *VarManager) Immigrate(uuid *common.VarUUId, varCap *msgs.Var, txnId *common.TxnId, txnCap *msgs.Txn, cont func(error)) {
	if _, found := vm.active[*uuid]; found {
		funcs := vm.onVarIdle[*uuid]
		vm.onVarIdle[*uuid] = append(funcs, func() { vm.Immigrate(uuid, varCap, txnId, txnCap, cont) })
		return
	}

	txnBites := db.TxnToRootBytes(txnCap)
	varSeg := capn.NewBuffer(nil)
	varCapRoot := msgs.NewRootVar(varSeg)
	varCapRoot.SetId(varCap.Id())
	varCapRoot.SetPositions(varCap.Positions())
	varCapRoot.SetWriteTxnId(varCap.WriteTxnId())
	varCapRoot.SetWriteTxnClock(varCap.WriteTxnClock())
	varCapRoot.SetWritesClock(varCap.WritesClock())
	varBites := server.SegToBytes(varSeg)

	future := vm.disk.ReadWriteTransaction(false, func(rwtxn *mdbs.RWTxn) interface{} {
		if varBitesOld, err := rwtxn.Get(db.DB.Vars, uuid[:]); err != nil && err != mdb.NotFound {
			return nil
		} else if err == nil {
			seg, _, err := capn.ReadFromMemoryZeroCopy(varBitesOld)
			if err != nil {
				rwtxn.Error(err)
				return nil
			}
			varCapOld := msgs.ReadRootVar(seg)
			txnIdOld := common.MakeTxnId(varCapOld.WriteTxnId())
			txnIdCmp := txnId.Compare(txnIdOld)
			if txnIdCmp == common.EQ {
				server.Log(uuid, "Immigration ignored: duplicate", txnId)
				return nil
			}
			txnClock := VectorClockFromCap(varCap.WriteTxnClock())
			txnClockOld := VectorClockFromCap(varCapOld.WriteTxnClock())
			elem := txnClock.Clock[*uuid]
			elemOld := txnClockOld.Clock[*uuid]
			if elem < elemOld || (elem == elemOld && txnIdCmp == common.LT) {
				server.Log("Immigration ignored as local disk contains newer version. Assuming it arrived late.", uuid)
				return nil
			}
		}
		if err := db.WriteTxnToDisk(rwtxn, txnId, txnBites); err == nil {
			rwtxn.Put(db.DB.Vars, uuid[:], varBites, 0)
		}
		return nil
	})
	go func() {
		_, err := future.ResultError()
		if err == nil {
			server.Log("Immigration of", uuid, "completed", txnId)
		}
		cont(err)
	}()
}

func (vm *VarManager) find(uuid *common.VarUUId) (*Var, error) {
	if v, found := vm.active[*uuid]; found {
		return v, nil
	}

	result, err := vm.disk.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		// rtxn.Get returns a copy of the data, so we don't need to
		// worry about pointers into the disk
		if bites, err := rtxn.Get(db.DB.Vars, uuid[:]); err == nil {
			return bites
		} else {
			return err
		}
	}).ResultError()

	if err != nil {
		return nil, err
	}
	if nf, ok := result.(mdb.Errno); ok && nf == mdb.NotFound {
		return nil, nf
	}
	v, err := VarFromData(result.([]byte), vm.exe, vm.disk, vm)
	if err == nil {
		vm.active[*v.UUId] = v
	}
	return v, err
}

func (vm *VarManager) Status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("- Active Vars: %v", len(vm.active)))
	sc.Emit(fmt.Sprintf("- Callbacks: %v", len(vm.callbacks)))
	sc.Emit(fmt.Sprintf("- Beater live? %v", vm.beaterLive))
	for _, v := range vm.active {
		v.Status(sc.Fork())
	}
	sc.Join()
}

func (vm *VarManager) ScheduleCallback(fun func()) {
	vm.callbacks = append(vm.callbacks, fun)
	if !vm.beaterLive {
		vm.beaterLive = true
		terminate := make(chan struct{})
		go vm.beater(terminate)
	}
}

func (vm *VarManager) beat(terminate chan struct{}) {
	if len(vm.callbacks) != 0 {
		callbacks := vm.callbacks
		vm.callbacks = make([]func(), 0, len(callbacks))
		for _, fun := range callbacks {
			fun()
		}
	}
	if len(vm.callbacks) == 0 {
		close(terminate)
		vm.beaterLive = false
	}
}

func (vm *VarManager) beater(terminate chan struct{}) {
	barrier := make(chan server.EmptyStruct, 1)
	fun := func() {
		vm.beat(terminate)
		barrier <- server.EmptyStructVal
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		time.Sleep(server.VarIdleTimeoutMin + (time.Duration(rng.Intn(server.VarIdleTimeoutRange)) * time.Millisecond))
		select {
		case <-terminate:
			return
		default:
			vm.exe.Enqueue(fun)
			<-barrier
		}
	}
}
