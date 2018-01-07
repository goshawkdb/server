package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	"goshawkdb.io/server/types"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/poisson"
	"goshawkdb.io/server/utils/status"
	vc "goshawkdb.io/server/utils/vectorclock"
	"math/rand"
	"time"
)

type Var struct {
	UUId            *common.VarUUId
	poisson         *poisson.Poisson
	curFrame        *frame
	curFrameOnDisk  *frame
	writeInProgress func()
	subscriptions   *Subscriptions
	exe             *dispatcher.Executor
	db              *db.Databases
	vm              *VarManager
	rng             *rand.Rand
}

func VarFromData(data []byte, exe *dispatcher.Executor, db *db.Databases, vm *VarManager) (*Var, error) {
	seg, _, err := capn.ReadFromMemoryZeroCopy(data)
	if err != nil {
		return nil, err
	}
	varCap := msgs.ReadRootVar(seg)

	subs, err := NewSubscriptionsFromData(vm, varCap.Subscriptions())
	if err != nil {
		return nil, err
	}
	v := newVar(common.MakeVarUUId(varCap.Id()), exe, db, vm, subs)
	positions := varCap.Positions()
	positionsPtr := (*common.Positions)(&positions)
	if positions.Len() == 0 {
		positionsPtr = nil
	}

	writeTxnId := common.MakeTxnId(varCap.WriteTxnId())
	valueTxnId := common.MakeTxnId(varCap.ValueTxnId())
	writeTxnClock := vc.VectorClockFromData(varCap.WriteTxnClock(), true).AsMutable()
	writesClock := vc.VectorClockFromData(varCap.WritesClock(), true).AsMutable()
	utils.DebugLog(vm.logger, "debug", "Restored.", "VarUUId", v.UUId, "TxnId", writeTxnId, "ValueTxnId", valueTxnId)

	v.curFrame = NewFrame(v, nil, positionsPtr, writeTxnId, valueTxnId, nil, nil, writeTxnClock, writesClock)
	v.curFrameOnDisk = v.curFrame
	v.curFrame.onDisk = true // don't call NowOnDisk because this is not an edge.
	return v, nil
}

func NewVar(uuid *common.VarUUId, exe *dispatcher.Executor, db *db.Databases, vm *VarManager) *Var {
	v := newVar(uuid, exe, db, vm, NewSubscriptions(vm))

	clock := vc.NewVectorClock().AsMutable().Bump(v.UUId, 1)
	written := vc.NewVectorClock().AsMutable().Bump(v.UUId, 1)
	v.curFrame = NewFrame(v, nil, nil, nil, nil, nil, nil, clock, written)

	return v
}

func newVar(uuid *common.VarUUId, exe *dispatcher.Executor, db *db.Databases, vm *VarManager, subscriptions *Subscriptions) *Var {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &Var{
		UUId:            uuid,
		poisson:         poisson.NewPoisson(),
		curFrame:        nil,
		writeInProgress: nil,
		subscriptions:   subscriptions,
		exe:             exe,
		db:              db,
		vm:              vm,
		rng:             rng,
	}
}

func (v *Var) ReceiveTxn(action *localAction, enqueuedAt time.Time) {
	utils.DebugLog(v.vm.logger, "debug", "ReceiveTxn.", "VarUUId", v.UUId, "action", action)
	v.poisson.AddThen(enqueuedAt)

	isRead, isWrite := action.IsRead(), action.IsWrite()

	switch {
	case isRead && isWrite:
		v.curFrame.AddReadWrite(action)
	case isRead:
		v.curFrame.AddRead(action)
	case isWrite: // includes roll
		v.curFrame.AddWrite(action)
	default:
		panic(fmt.Sprintf("Received txn action I don't understand: %v", action))
	}
}

func (v *Var) ReceiveTxnOutcome(action *localAction, enqueuedAt time.Time) {
	utils.DebugLog(v.vm.logger, "debug", "ReceiveTxnOutcome.", "VarUUId", v.UUId, "action", action)
	v.poisson.AddThen(enqueuedAt)

	isRead, isWrite := action.IsRead(), action.IsWrite()

	switch {
	case action.frame == nil:
		if (isWrite && !v.curFrame.WriteLearnt(action)) ||
			(!isWrite && isRead && !v.curFrame.ReadLearnt(action)) {
			action.LocallyComplete()
			v.maybeMakeInactive()
		}

	case action.frame.v != v:
		panic(fmt.Sprintf("%v frame var has changed %p -> %p (%v)", v.UUId, action.frame.v, v, action))

	case action.aborted:
		switch {
		case isRead && isWrite:
			action.frame.ReadWriteAborted(action, true)
		case isRead:
			action.frame.ReadAborted(action)
		case isWrite:
			action.frame.WriteAborted(action, true)
		default:
			panic(fmt.Sprintf("Received txn abort outcome I don't understand: %v", action))
		}

	default:
		switch {
		case isRead && isWrite:
			action.frame.ReadWriteCommitted(action)
		case isRead:
			action.frame.ReadCommitted(action)
		case isWrite:
			action.frame.WriteCommitted(action)
		default:
			panic(fmt.Sprintf("Received txn commit outcome I don't understand: %v", action))
		}
	}
}

func (v *Var) SetCurFrame(f *frame, frameAction *localAction) {
	utils.DebugLog(v.vm.logger, "debug", "SetCurFrame.", "VarUUId", v.UUId, "frameTxnId", f.frameTxnId, "frameValueTxnId", f.frameValueTxnId)

	v.curFrame = f

	// diffLen := action.outcomeClock.Len() - action.TxnReader.Actions(true).Actions().Len()
	// fmt.Printf("d%v ", diffLen)

	v.maybeWriteFrame(f)
}

func (v *Var) maybeWriteFrame(f *frame) {
	if v.writeInProgress != nil {
		v.writeInProgress = func() {
			v.writeInProgress = nil
			v.maybeWriteFrame(f)
		}
		return
	}
	v.writeInProgress = func() {
		v.writeInProgress = nil
		v.subscriptions.Verify()
		v.maybeMakeInactive()
	}

	varSeg := capn.NewBuffer(nil)
	varCap := msgs.NewRootVar(varSeg)

	varCap.SetId(v.UUId[:])
	varCap.SetPositions(capn.UInt8List(*f.positions))
	varCap.SetWriteTxnId(f.frameTxnId[:])
	varCap.SetValueTxnId(f.frameValueTxnId[:])
	varCap.SetWriteTxnClock(f.frameTxnClock.AsData())
	varCap.SetWritesClock(f.frameWritesClock.AsData())
	varCap.SetSubscriptions(v.subscriptions.AsData())
	varData := common.SegToBytes(varSeg)

	// NB v.curFrameOnDisk cannot change whilst we're in here, so it's
	// safe to access it directly. But, frameValueTxn can change so we
	// need to copy that out now:
	frameValueTxn := f.frameValueTxn

	// to ensure correct order of writes, schedule the write from
	// the current go-routine...
	future := v.db.ReadWriteTransaction(func(rwtxn *mdbs.RWTxn) interface{} {
		if err := rwtxn.Put(v.db.Vars, v.UUId[:], varData, 0); err != nil {
			return types.EmptyStructVal
		}

		if frameValueTxn == nil {
			// we are confident that it's already on disk, so we need to
			// just bump the ref count.
			if err := v.db.IncrTxnRefCount(rwtxn, f.frameValueTxnId); err != nil {
				return types.EmptyStructVal
			}
		} else {
			if err := v.db.WriteTxnToDisk(rwtxn, frameValueTxn.Id, frameValueTxn.Data); err != nil {
				return types.EmptyStructVal
			}
		}

		if v.curFrameOnDisk != nil {
			// DeleteTxnFromDisk is idempotent and does not error if the txn is not on disk.
			if err := v.db.DeleteTxnFromDisk(rwtxn, v.curFrameOnDisk.frameValueTxnId); err != nil {
				return types.EmptyStructVal
			}
		}
		return types.EmptyStructVal
	})
	go func() {
		// ... but process the result in a new go-routine to avoid blocking the executor.
		if ran, err := future.ResultError(); err != nil {
			panic(fmt.Sprintf("Var error when writing to disk: %v\n", err))
		} else if ran != nil {
			// Switch back to the right go-routine
			v.applyToSelf(func() {
				utils.DebugLog(v.vm.logger, "debug", "Written to disk.", "VarUUId", v.UUId, "TxnId", f.frameTxnId)
				v.curFrameOnDisk = f
				f.FrameNowOnDisk()
				v.writeInProgress()
			})
		}
	}()
}

func (v *Var) TxnGloballyComplete(action *localAction, enqueuedAt time.Time) {
	utils.DebugLog(v.vm.logger, "debug", "Txn globally complete.", "VarUUId", v.UUId, "action", action)
	if action.frame.v != v {
		panic(fmt.Sprintf("%v frame var has changed %p -> %p (%v)", v.UUId, action.frame.v, v, action))
	}
	v.poisson.AddThen(enqueuedAt)
	if action.IsWrite() {
		action.frame.WriteGloballyComplete(action)
	} else {
		action.frame.ReadGloballyComplete(action)
	}
}

func (v *Var) maybeMakeInactive() {
	if v.isIdle() {
		v.vm.SetInactive(v)
	}
}

func (v *Var) isIdle() bool {
	return v.writeInProgress == nil && !v.subscriptions.IsDirty() && v.curFrame.isIdle()
}

func (v *Var) isOnDisk() bool {
	return v.writeInProgress == nil && !v.subscriptions.IsDirty() && v.curFrame == v.curFrameOnDisk && v.curFrame.isEmpty()
}

func (v *Var) applyToSelf(fun func()) {
	v.exe.EnqueueFuncAsync(func() (bool, error) {
		v.vm.ApplyToVar(func(v1 *Var) {
			switch {
			case v1 == nil:
				panic(fmt.Sprintf("%v not found!", v.UUId))
			case v1 != v:
				utils.DebugLog(v.vm.logger, "debug", "Ignoring callback as var object has changed.", "VarUUId", v.UUId)
				v1.maybeMakeInactive()
			default:
				fun()
			}
		}, false, v.UUId)
		return false, nil
	})
}

func (v *Var) Status(sc *status.StatusConsumer) {
	sc.Emit(v.UUId.String())
	v.subscriptions.Status(sc.Fork())
	sc.Emit("- CurFrame:")
	v.curFrame.Status(sc.Fork())
	sc.Emit(fmt.Sprintf("- Idle? %v", v.isIdle()))
	sc.Emit(fmt.Sprintf("- IsOnDisk? %v", v.isOnDisk()))
	sc.Join()
}
