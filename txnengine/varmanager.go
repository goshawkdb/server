package txnengine

import (
	"fmt"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	"math/rand"
	"time"
)

type VarManager struct {
	LocalConnection
	Topology    *configuration.Topology
	RMId        common.RMId
	db          *db.Databases
	active      map[common.VarUUId]*Var
	RollAllowed bool
	onDisk      func(bool)
	lc          LocalConnection
	callbacks   []func()
	beaterLive  bool
	exe         *dispatcher.Executor
}

func init() {
	db.DB.Vars = &mdbs.DBISettings{Flags: mdb.CREATE}
}

func NewVarManager(exe *dispatcher.Executor, rmId common.RMId, tp TopologyPublisher, db *db.Databases, lc LocalConnection) *VarManager {
	vm := &VarManager{
		LocalConnection: lc,
		RMId:            rmId,
		db:              db,
		active:          make(map[common.VarUUId]*Var),
		RollAllowed:     false,
		callbacks:       []func(){},
		exe:             exe,
	}
	exe.Enqueue(func() {
		vm.Topology = tp.AddTopologySubscriber(VarSubscriber, vm)
		vm.RollAllowed = vm.Topology == nil || !vm.Topology.NextBarrierReached1(rmId)
	})
	return vm
}

func (vm *VarManager) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	resultChan := make(chan struct{})
	enqueued := vm.exe.Enqueue(func() {
		if od := vm.onDisk; od != nil {
			vm.onDisk = nil
			od(false)
		}
		vm.Topology = topology
		oldRollAllowed := vm.RollAllowed
		if !vm.RollAllowed {
			vm.RollAllowed = topology == nil || !topology.NextBarrierReached1(vm.RMId)
		}
		server.Log("VarManager", fmt.Sprintf("%p", vm), "rollAllowed:", oldRollAllowed, "->", vm.RollAllowed, fmt.Sprintf("%p", topology))

		goingToDisk := topology != nil && topology.NextBarrierReached1(vm.RMId) && !topology.NextBarrierReached2(vm.RMId)

		doneWrapped := func(result bool) {
			close(resultChan)
			done(result)
		}
		if goingToDisk {
			vm.onDisk = doneWrapped
			vm.checkAllDisk()
		} else {
			server.Log("VarManager", fmt.Sprintf("%p", vm), "calling done", fmt.Sprintf("%p", topology))
			doneWrapped(true)
		}
	})
	if enqueued {
		go vm.exe.WithTerminatedChan(func(terminated chan struct{}) {
			select {
			case <-resultChan:
			case <-terminated:
				select {
				case <-resultChan:
				default:
					done(false)
				}
			}
		})
	} else {
		done(false)
	}
}

func (vm *VarManager) ApplyToVar(fun func(*Var, error), createIfMissing bool, uuid *common.VarUUId) {
	v, err := vm.find(uuid)
	if err == mdb.NotFound && createIfMissing {
		v = NewVar(uuid, vm.exe, vm.db, vm)
		vm.active[*v.UUId] = v
		server.Log(uuid, "New var")
	} else if err != nil {
		fun(nil, err)
		return
	}
	fun(v, nil)
	if _, found := vm.active[*uuid]; !found && !v.isIdle() {
		panic(fmt.Sprintf("Var is not active, yet is not idle! %v %v", uuid, fun))
	} else {
		vm.checkAllDisk()
	}
}

func (vm *VarManager) checkAllDisk() {
	if od := vm.onDisk; od != nil {
		for _, v := range vm.active {
			if v.UUId.Compare(configuration.TopologyVarUUId) != common.EQ && !v.isOnDisk(true) {
				if !vm.RollAllowed {
					server.Log("VarManager", fmt.Sprintf("%p", vm), "WTF?! rolls are banned, but have var", v.UUId, "not on disk!")
				}
				return
			}
		}
		vm.onDisk = nil
		vm.RollAllowed = false
		server.Log("VarManager", fmt.Sprintf("%p", vm), "Rolls banned; calling done", fmt.Sprintf("%p", od))
		od(true)
	}
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
	}
}

func (vm *VarManager) find(uuid *common.VarUUId) (*Var, error) {
	if v, found := vm.active[*uuid]; found {
		return v, nil
	}

	result, err := vm.db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		// rtxn.Get returns a copy of the data, so we don't need to
		// worry about pointers into the db
		if bites, err := rtxn.Get(vm.db.Vars, uuid[:]); err == nil {
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
	v, err := VarFromData(result.([]byte), vm.exe, vm.db, vm)
	if err == nil {
		vm.active[*v.UUId] = v
	}
	return v, err
}

func (vm *VarManager) Status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("- Active Vars: %v", len(vm.active)))
	sc.Emit(fmt.Sprintf("- Callbacks: %v", len(vm.callbacks)))
	sc.Emit(fmt.Sprintf("- Beater live? %v", vm.beaterLive))
	sc.Emit(fmt.Sprintf("- Roll allowed? %v", vm.RollAllowed))
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
