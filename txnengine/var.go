package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	"math/rand"
	"time"
)

type VarWriteSubscriber func(v *Var, value []byte, references *msgs.VarIdPos_List, txn *Txn)

type Var struct {
	UUId            *common.VarUUId
	positions       *common.Positions
	curFrame        *frame
	curFrameOnDisk  *frame
	writeInProgress func()
	subscribers     map[common.TxnId]VarWriteSubscriber
	exe             *dispatcher.Executor
	disk            *mdbs.MDBServer
	vm              *VarManager
	varCap          *msgs.Var
	rng             *rand.Rand
}

func VarFromData(data []byte, exe *dispatcher.Executor, disk *mdbs.MDBServer, vm *VarManager) (*Var, error) {
	seg, _, err := capn.ReadFromMemoryZeroCopy(data)
	if err != nil {
		return nil, err
	}
	varCap := msgs.ReadRootVar(seg)

	v := newVar(common.MakeVarUUId(varCap.Id()), exe, disk, vm)
	positions := varCap.Positions()
	if positions.Len() != 0 {
		v.positions = (*common.Positions)(&positions)
	}

	writeTxnId := common.MakeTxnId(varCap.WriteTxnId())
	writeTxnClock := VectorClockFromCap(varCap.WriteTxnClock())
	writesClock := VectorClockFromCap(varCap.WritesClock())
	server.Log(v.UUId, "Restored", writeTxnId)

	if result, err := disk.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		return db.ReadTxnBytesFromDisk(rtxn, writeTxnId)
	}).ResultError(); err == nil {
		bites := result.([]byte)
		if seg, _, err := capn.ReadFromMemoryZeroCopy(bites); err == nil {
			txn := msgs.ReadRootTxn(seg)
			actions := txn.Actions()
			v.curFrame = NewFrame(nil, v, writeTxnId, &actions, writeTxnClock, writesClock)
			v.curFrameOnDisk = v.curFrame
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}

	v.varCap = &varCap

	return v, nil
}

func NewVar(uuid *common.VarUUId, exe *dispatcher.Executor, disk *mdbs.MDBServer, vm *VarManager) *Var {
	v := newVar(uuid, exe, disk, vm)

	clock := NewVectorClock().Bump(*v.UUId, 0)
	written := NewVectorClock().Bump(*v.UUId, 0)
	v.curFrame = NewFrame(nil, v, nil, nil, clock, written)

	seg := capn.NewBuffer(nil)
	varCap := msgs.NewRootVar(seg)
	varCap.SetId(v.UUId[:])
	v.varCap = &varCap

	return v
}

func newVar(uuid *common.VarUUId, exe *dispatcher.Executor, disk *mdbs.MDBServer, vm *VarManager) *Var {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &Var{
		UUId:            uuid,
		positions:       nil,
		curFrame:        nil,
		curFrameOnDisk:  nil,
		writeInProgress: nil,
		subscribers:     make(map[common.TxnId]VarWriteSubscriber),
		exe:             exe,
		disk:            disk,
		vm:              vm,
		rng:             rng,
	}
}

func (v *Var) AddWriteSubscriber(txnId *common.TxnId, fun VarWriteSubscriber) {
	v.subscribers[*txnId] = fun
}

func (v *Var) RemoveWriteSubscriber(txnId *common.TxnId) {
	delete(v.subscribers, *txnId)
	v.maybeMakeInactive()
}

func (v *Var) ReceiveTxn(action *localAction) {
	server.Log(v.UUId, "ReceiveTxn", action)
	isRead, isWrite := action.IsRead(), action.IsWrite()

	if isRead && action.Retry {
		if voted := v.curFrame.ReadRetry(action); !voted {
			v.AddWriteSubscriber(action.Id,
				func(v *Var, value []byte, refs *msgs.VarIdPos_List, newtxn *Txn) {
					if voted := v.curFrame.ReadRetry(action); voted {
						v.RemoveWriteSubscriber(action.Id)
					}
				})
		}
		return
	}

	switch {
	case isRead && isWrite:
		v.curFrame.AddReadWrite(action)
	case isRead:
		v.curFrame.AddRead(action)
	default:
		v.curFrame.AddWrite(action)
	}
}

func (v *Var) ReceiveTxnOutcome(action *localAction) {
	server.Log(v.UUId, "ReceiveTxnOutcome", action)
	isRead, isWrite := action.IsRead(), action.IsWrite()

	switch {
	case action.Retry:
		v.RemoveWriteSubscriber(action.Id)

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
		default:
			action.frame.WriteAborted(action, true)
		}

	default:
		switch {
		case isRead && isWrite:
			action.frame.ReadWriteCommitted(action)
		case isRead:
			action.frame.ReadCommitted(action)
		default:
			action.frame.WriteCommitted(action)
		}
	}
}

func (v *Var) SetCurFrame(f *frame, action *localAction, positions *common.Positions) {
	server.Log(v.UUId, "SetCurFrame", action)
	v.curFrame = f

	if positions != nil {
		v.positions = positions
	}

	actionCap := action.writeAction
	var (
		value      []byte
		references msgs.VarIdPos_List
	)
	switch actionCap.Which() {
	case msgs.ACTION_WRITE:
		write := actionCap.Write()
		value = write.Value()
		references = write.References()
	case msgs.ACTION_READWRITE:
		rw := actionCap.Readwrite()
		value = rw.Value()
		references = rw.References()
	case msgs.ACTION_CREATE:
		create := actionCap.Create()
		value = create.Value()
		references = create.References()
	case msgs.ACTION_ROLL: // deliberately do nothing
	default:
		panic(fmt.Sprintf("Unexpected action type: %v", actionCap.Which()))
	}
	for _, sub := range v.subscribers {
		sub(v, value, &references, action.Txn)
	}

	// diffLen := len(action.outcomeClock.Clock) - action.TxnCap.Actions().Len()
	// fmt.Printf("%v ", diffLen)

	v.maybeWriteFrame(f, action, positions)
}

func (v *Var) maybeWriteFrame(f *frame, action *localAction, positions *common.Positions) {
	if v.writeInProgress != nil {
		v.writeInProgress = func() {
			v.writeInProgress = nil
			v.maybeWriteFrame(f, action, positions)
		}
		return
	}
	v.writeInProgress = func() {
		v.writeInProgress = nil
		v.maybeMakeInactive()
	}

	oldVarCap := *v.varCap

	varSeg := capn.NewBuffer(nil)
	varCap := msgs.NewRootVar(varSeg)
	v.varCap = &varCap
	varCap.SetId(oldVarCap.Id())

	if positions != nil {
		varCap.SetPositions(capn.UInt8List(*positions))
	} else {
		varCap.SetPositions(oldVarCap.Positions())
	}

	varCap.SetWriteTxnId(f.frameTxnId[:])
	varCap.SetWriteTxnClock(f.frameTxnClock.AddToSeg(varSeg))
	varCap.SetWritesClock(f.frameWritesClock.AddToSeg(varSeg))
	varData := server.SegToBytes(varSeg)

	txnBytes := action.TxnRootBytes()

	// to ensure correct order of writes, schedule the write from
	// the current go-routine...
	future := v.disk.ReadWriteTransaction(false, func(rwtxn *mdbs.RWTxn) interface{} {
		if err := db.WriteTxnToDisk(rwtxn, f.frameTxnId, txnBytes); err == nil {
			if err = rwtxn.Put(db.DB.Vars, v.UUId[:], varData, 0); err == nil {
				if v.curFrameOnDisk != nil {
					db.DeleteTxnFromDisk(rwtxn, v.curFrameOnDisk.frameTxnId)
				}
			}
		}
		return nil
	})
	go func() {
		// ... but process the result in a new go-routine to avoid blocking the executor.
		if _, err := future.ResultError(); err != nil {
			panic(fmt.Sprintf("Var error when writing to disk: %v\n", err))
			return
		}
		// Switch back to the right go-routine
		v.applyToVar(func() {
			server.Log(v.UUId, "Wrote", f.frameTxnId)
			v.curFrameOnDisk = f
			for ancestor := f.parent; ancestor != nil && ancestor.DescendentOnDisk(); ancestor = ancestor.parent {
			}
			v.writeInProgress()
		})
	}()
}

func (v *Var) TxnGloballyComplete(action *localAction) {
	server.Log(v.UUId, "Txn globally complete", action)
	if action.frame.v != v {
		panic(fmt.Sprintf("%v frame var has changed %p -> %p (%v)", v.UUId, action.frame.v, v, action))
	}
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
	return len(v.subscribers) == 0 && v.writeInProgress == nil && v.curFrame.isIdle()
}

func (v *Var) applyToVar(fun func()) {
	v.exe.Enqueue(func() {
		v.vm.ApplyToVar(func(v1 *Var, err error) {
			switch {
			case err != nil:
				panic(err)
			case v1 != v:
				v1.maybeMakeInactive()
			default:
				fun()
			}
		}, false, v.UUId)
	})
}

func (v *Var) Status(sc *server.StatusConsumer) {
	sc.Emit(v.UUId.String())
	if v.positions == nil {
		sc.Emit("- Positions: unknown")
	} else {
		sc.Emit(fmt.Sprintf("- Positions: %v", v.positions))
	}
	sc.Emit("- CurFrame:")
	v.curFrame.Status(sc.Fork())
	sc.Emit(fmt.Sprintf("- Subscribers: %v", len(v.subscribers)))
	sc.Emit(fmt.Sprintf("- Idle? %v", v.isIdle()))
	sc.Emit(fmt.Sprintf("- OnDisk? %v", v.curFrame.IsOnDisk()))
	sc.Join()
}
