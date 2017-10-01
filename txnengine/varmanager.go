package txnengine

import (
	"fmt"
	"github.com/go-kit/kit/log"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	tw "github.com/msackman/gotimerwheel"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	"goshawkdb.io/server/types/connectionmanager"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/types/localconnection"
	"goshawkdb.io/server/types/topology"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/proxy"
	"goshawkdb.io/server/utils/status"
	"time"
)

type VarManager struct {
	sconn.ServerConnectionPublisher
	localconnection.LocalConnection
	logger           log.Logger
	Topology         *configuration.Topology
	RMId             common.RMId
	db               *db.Databases
	active           map[common.VarUUId]*Var
	RollAllowed      bool
	onDisk           func(bool)
	tw               *tw.TimerWheel
	beaterTerminator chan struct{}
	exe              *dispatcher.Executor
}

func init() {
	db.DB.Vars = &mdbs.DBISettings{Flags: mdb.CREATE}
}

func NewVarManager(exe *dispatcher.Executor, rmId common.RMId, cm connectionmanager.ConnectionManager, db *db.Databases, lc localconnection.LocalConnection, logger log.Logger) *VarManager {
	vm := &VarManager{
		ServerConnectionPublisher: proxy.NewServerConnectionPublisherProxy(exe, cm, logger),
		LocalConnection:           lc,
		logger:                    logger, // varDispatcher creates the context for us
		RMId:                      rmId,
		db:                        db,
		active:                    make(map[common.VarUUId]*Var),
		RollAllowed:               false,
		tw:                        tw.NewTimerWheel(time.Now(), 25*time.Millisecond),
		exe:                       exe,
	}
	vm.Topology = cm.AddTopologySubscriber(topology.VarSubscriber, vm)
	vm.RollAllowed = vm.Topology != nil && vm.Topology.NextConfiguration == nil
	return vm
}

type vmTopologyChanged struct {
	actor.MsgSyncQuery
	vm       *VarManager
	topology *configuration.Topology
	outcome  bool
}

func (tc *vmTopologyChanged) Exec() (bool, error) {
	if od := tc.vm.onDisk; od != nil {
		tc.vm.onDisk = nil
		od(false)
	}
	tc.vm.Topology = tc.topology
	utils.DebugLog(tc.vm.logger, "debug", "TopologyChanged.", "topology", tc.topology)

	if tc.topology.NextConfiguration == nil {
		tc.vm.RollAllowed = true
		utils.DebugLog(tc.vm.logger, "debug", "TopologyChanged. Calling done.", "topology", tc.topology)
		tc.done(true)
	} else {
		tc.vm.onDisk = tc.done
		tc.vm.checkAllDisk()
	}
	return false, nil
}

func (tc *vmTopologyChanged) done(result bool) {
	tc.outcome = result
	tc.MustClose()
}

func (vm *VarManager) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	tc := &vmTopologyChanged{
		vm:       vm,
		topology: topology,
	}
	tc.InitMsg(vm.exe.Mailbox)
	if vm.exe.Mailbox.EnqueueMsg(tc) {
		go func() { done(tc.Wait() && tc.outcome) }()
	} else {
		done(false)
	}
}

func (vm *VarManager) ApplyToVar(fun func(*Var), createIfMissing bool, uuid *common.VarUUId) {
	v, shutdown := vm.find(uuid)
	if shutdown {
		return
	}
	if v == nil && createIfMissing {
		v = NewVar(uuid, vm.exe, vm.db, vm)
		vm.active[*v.UUId] = v
		utils.DebugLog(vm.logger, "debug", "New var.", "VarUUId", uuid)
	}
	fun(v)
	if _, found := vm.active[*uuid]; v != nil && !found && !v.isIdle() {
		panic(fmt.Sprintf("Var is not active, yet is not idle! %v %p", uuid, fun))
	} else {
		vm.checkAllDisk()
	}
}

func (vm *VarManager) checkAllDisk() {
	if od := vm.onDisk; od != nil {
		for _, v := range vm.active {
			if v.UUId.Compare(configuration.TopologyVarUUId) != common.EQ && !v.isOnDisk() {
				return
			}
		}
		vm.onDisk = nil
		vm.RollAllowed = false
		utils.DebugLog(vm.logger, "debug", "Rolls now banned. Calling done.", "topology", vm.Topology)
		od(true)
	}
}

// var.VarLifecycle interface
func (vm *VarManager) SetInactive(v *Var) {
	utils.DebugLog(vm.logger, "debug", "Var now inactive.", "VarUUId", v.UUId)
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

func (vm *VarManager) find(uuid *common.VarUUId) (*Var, bool) {
	if v, found := vm.active[*uuid]; found {
		return v, false
	}

	// This is blocking which you might think is a mistake. But LMDB
	// reads are so very fast that as far as I can tell, right now,
	// it's not worth the extra pain to go to an async model. Async is
	// perfectly possible - just change find to take a continuation,
	// and bounce back into this go-routine when necessary. Only
	// complication is you need to track in-flight loads. But I can't
	// measure any advantage yet for doing that.
	result, err := vm.db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		// rtxn.Get returns a copy of the data, so we don't need to
		// worry about pointers into the db
		if bites, err := rtxn.Get(vm.db.Vars, uuid[:]); err == nil {
			return bites
		} else {
			return true
		}
	}).ResultError()

	if err != nil {
		panic(fmt.Sprintf("Error when loading %v from disk: %v", uuid, err))
	} else if result == nil { // shutdown
		return nil, true
	} else if bites, ok := result.([]byte); ok {
		v, err := VarFromData(bites, vm.exe, vm.db, vm)
		if err != nil {
			panic(fmt.Sprintf("Error when recreating %v: %v", uuid, err))
		} else if v == nil { // shutdown
			return v, true
		} else {
			vm.active[*v.UUId] = v
			return v, false
		}
	} else { // not found
		return nil, false
	}
}

func (vm *VarManager) Status(sc *status.StatusConsumer) {
	sc.Emit(fmt.Sprintf("- Active Vars: %v", len(vm.active)))
	sc.Emit(fmt.Sprintf("- Callbacks: %v", vm.tw.Length()))
	sc.Emit(fmt.Sprintf("- Beater live? %v", vm.beaterTerminator != nil))
	sc.Emit(fmt.Sprintf("- Roll allowed? %v", vm.RollAllowed))
	for _, v := range vm.active {
		v.Status(sc.Fork())
	}
	sc.Join()
}

func (vm *VarManager) ScheduleCallback(interval time.Duration, fun tw.Event) {
	if err := vm.tw.ScheduleEventIn(interval, fun); err != nil {
		panic(err)
	}
	if vm.beaterTerminator == nil {
		vm.beaterTerminator = make(chan struct{})
		go vm.beater(vm.beaterTerminator)
	}
}

func (vm *VarManager) beat() (bool, error) {
	vm.tw.AdvanceTo(time.Now(), 32)
	// fmt.Println("done:", )
	if vm.tw.IsEmpty() && vm.beaterTerminator != nil {
		close(vm.beaterTerminator)
		vm.beaterTerminator = nil
	}
	return false, nil
}

func (vm *VarManager) beater(terminate chan struct{}) {
	sleep := 100 * time.Millisecond
	for {
		time.Sleep(sleep)
		select {
		case <-terminate:
			return
		default:
			vm.exe.EnqueueFuncAsync(vm.beat)
		}
	}
}
