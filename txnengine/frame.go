package txnengine

import (
	"bytes"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	sl "github.com/msackman/skiplist"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/types"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/binarybackoff"
	"goshawkdb.io/server/utils/status"
	"goshawkdb.io/server/utils/txnreader"
	vc "goshawkdb.io/server/utils/vectorclock"
	"sort"
	"time"
)

var AbortRollNotFirst = errors.New("AbortRollNotFirst")
var AbortRollNotInPermutation = errors.New("AbortRollNotInPermutation")

type frame struct {
	parent           *frame
	child            *frame
	v                *Var
	frameTxnId       *common.TxnId
	frameTxnActions  *txnreader.TxnActions
	frameTxnClock    *vc.VectorClockMutable // the clock (including merge missing) of the frame txn
	frameWritesClock *vc.VectorClockMutable // max elems from all writes of all txns in parent frame
	readVoteClock    *vc.VectorClockMutable
	positionsFound   bool
	mask             *vc.VectorClockMutable
	scheduleBackoff  *binarybackoff.BinaryBackoffEngine
	frameOpen
	frameClosed
	frameErase
	currentState frameStateMachineComponent
}

func NewFrame(parent *frame, v *Var, txnId *common.TxnId, txnActions *txnreader.TxnActions, txnClock, writesClock *vc.VectorClockMutable) *frame {
	f := &frame{
		parent:           parent,
		v:                v,
		frameTxnId:       txnId,
		frameTxnActions:  txnActions,
		frameTxnClock:    txnClock,
		frameWritesClock: writesClock,
		positionsFound:   false,
	}
	if parent == nil {
		f.mask = vc.NewVectorClock().AsMutable()
		f.scheduleBackoff = binarybackoff.NewBinaryBackoffEngine(v.rng, server.VarRollDelayMin, server.VarRollDelayMax)
	} else {
		f.mask = parent.mask
		f.scheduleBackoff = parent.scheduleBackoff
		f.scheduleBackoff.Shrink(server.VarRollDelayMin)
	}
	f.init()
	utils.DebugLog(f.v.vm.logger, "debug", "NewFrame.", "VarUUId", f.v.UUId)
	f.maybeStartRoll()
	return f
}

func (f *frame) init() {
	f.frameOpen.init(f)
	f.frameClosed.init(f)
	f.frameErase.init(f)

	f.currentState = &f.frameOpen
	f.currentState.start()
}

func (f *frame) nextState() {
	switch f.currentState {
	case &f.frameOpen:
		f.currentState = &f.frameClosed
	case &f.frameClosed:
		f.currentState = &f.frameErase
	case &f.frameErase:
		f.currentState = nil
		return
	}
	f.currentState.start()
}

func (f *frame) String() string {
	return fmt.Sprintf("%v Frame %v (%v) r%v w%v", f.v.UUId, f.frameTxnId, f.frameTxnClock.Len(), f.readVoteClock, f.writeVoteClock)
}

func (f *frame) Status(sc *status.StatusConsumer) {
	sc.Emit(f.String())
	readHistogram := make([]int, 4)
	for node := f.reads.First(); node != nil; node = node.Next() {
		readHistogram[int(node.Value.(txnStatus))]++
	}
	writeHistogram := make([]int, 4)
	for node := f.writes.First(); node != nil; node = node.Next() {
		writeHistogram[int(node.Value.(txnStatus))]++
	}
	sc.Emit(fmt.Sprintf("- Read Count: %v %v", f.reads.Len(), readHistogram))
	sc.Emit(fmt.Sprintf("- Uncommitted Read Count: %v", f.uncommittedReads))
	sc.Emit(fmt.Sprintf("- Learnt future reads: %v", len(f.learntFutureReads)))
	sc.Emit(fmt.Sprintf("- Write Count: %v %v", f.writes.Len(), writeHistogram))
	sc.Emit(fmt.Sprintf("- Uncommitted Write Count: %v", f.uncommittedWrites))
	sc.Emit(fmt.Sprintf("- RW Present: %v", f.rwPresent))
	sc.Emit(fmt.Sprintf("- Mask: %v", f.mask))
	sc.Emit(fmt.Sprintf("- Current State: %v", f.currentState))
	sc.Emit(fmt.Sprintf("- Roll scheduled/active? %v/%v", f.rollScheduled != nil, f.rollActive))
	sc.Emit(fmt.Sprintf("- DescendentOnDisk? %v", f.onDisk))
	sc.Emit(fmt.Sprintf("- Child == nil? %v", f.child == nil))
	sc.Emit(fmt.Sprintf("- Parent == nil? %v", f.parent == nil))
	if f.parent != nil {
		f.parent.Status(sc.Fork())
	}
	sc.Join()
}

type txnStatus uint8

const (
	postponed   txnStatus = iota
	uncommitted txnStatus = iota
	committed   txnStatus = iota
	completing  txnStatus = iota
)

// State machine

type frameStateMachineComponent interface {
	init(*frame)
	start()
}

// open

type frameOpen struct {
	*frame
	reads              *sl.SkipList
	learntFutureReads  []*localAction
	maxUncommittedRead *localAction
	uncommittedReads   uint
	writeVoteClock     *vc.VectorClockMutable
	writes             *sl.SkipList
	clientWrites       map[common.ClientId]types.EmptyStruct
	uncommittedWrites  uint
	rwPresent          bool
	rollScheduled      *time.Time
	rollActive         bool
	rollTxn            *cmsgs.ClientTxn
	rollTxnPos         map[common.VarUUId]*types.PosCapVer
}

func (fo *frameOpen) init(f *frame) {
	fo.frame = f
	fo.reads = sl.New(f.v.rng)
	fo.learntFutureReads = []*localAction{}
	fo.writes = sl.New(f.v.rng)
	fo.clientWrites = make(map[common.ClientId]types.EmptyStruct)
}

func (fo *frameOpen) start()         {}
func (fo *frameOpen) String() string { return "frameOpen" }

func (fo *frameOpen) AddRead(action *localAction) {
	txn := action.Txn
	utils.DebugLog(fo.v.vm.logger, "debug", "AddRead", "frame", fo.frame, "TxnId", txn.Id, "vsn", action.readVsn)
	switch {
	case fo.currentState != fo:
		panic(fmt.Sprintf("%v AddRead called for %v with frame in state %v", fo.v, txn, fo.currentState))
	case fo.writes.Len() != 0 || fo.frameTxnActions == nil:
		// We could have learnt a write at this point but we're still fine to accept smaller reads.
		action.VoteDeadlock(fo.frameTxnClock)
	case fo.frameTxnId.Compare(action.readVsn) != common.EQ:
		action.VoteBadRead(fo.frameTxnClock, fo.frameTxnId, fo.frameTxnActions)
		fo.v.maybeMakeInactive()
	case fo.reads.Get(action) == nil:
		fo.uncommittedReads++
		fo.reads.Insert(action, uncommitted)
		if fo.maxUncommittedRead == nil || fo.maxUncommittedRead.Compare(action) == sl.LT {
			fo.maxUncommittedRead = action
		}
		action.frame = fo.frame
		fo.calculateReadVoteClock()
		if !action.VoteCommit(fo.readVoteClock, nil) {
			fo.ReadAborted(action)
		}
	default:
		panic(fmt.Sprintf("%v AddRead called for known txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) ReadAborted(action *localAction) {
	txn := action.Txn
	utils.DebugLog(fo.v.vm.logger, "debug", "ReadAborted", "frame", fo.frame, "TxnId", txn.Id)
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v ReadAborted called for %v with frame in state %v", fo.frame, txn, fo.currentState))
	}
	if node := fo.reads.Get(action); node != nil && node.Value == uncommitted {
		prev := node.Prev()
		node.Remove()
		fo.uncommittedReads--
		action.frame = nil
		fo.maybeFindMaxReadFrom(action, prev)
		fo.v.maybeMakeInactive()
	} else {
		panic(fmt.Sprintf("%v ReadAborted called for unknown txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) ReadCommitted(action *localAction) {
	txn := action.Txn
	utils.DebugLog(fo.v.vm.logger, "debug", "ReadCommitted", "frame", fo.frame, "TxnId", txn.Id)
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v ReadAborted called for %v with frame in state %v", fo.v, txn, fo.currentState))
	}
	if node := fo.reads.Get(action); node != nil && node.Value == uncommitted {
		node.Value = committed
		fo.uncommittedReads--
		fo.maybeFindMaxReadFrom(action, node.Prev())
	} else {
		panic(fmt.Sprintf("%v ReadCommitted called for unknown txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) AddWrite(action *localAction) {
	txn := action.Txn
	utils.DebugLog(fo.v.vm.logger, "debug", "AddWrite", "frame", fo.frame, "TxnId", txn.Id)
	cid := txn.Id.ClientId(common.RMIdEmpty)
	_, found := fo.clientWrites[cid]
	switch {
	case fo.currentState != fo:
		panic(fmt.Sprintf("%v AddWrite called for %v with frame in state %v", fo.v, txn, fo.currentState))
	case fo.rwPresent || (fo.maxUncommittedRead != nil && action.Compare(fo.maxUncommittedRead) == sl.LT) || found || len(fo.learntFutureReads) != 0:
		action.VoteDeadlock(fo.frameTxnClock)
	case fo.writes.Get(action) == nil:
		fo.uncommittedWrites++
		fo.clientWrites[cid] = types.EmptyStructVal
		action.frame = fo.frame
		if fo.uncommittedReads == 0 {
			fo.writes.Insert(action, uncommitted)
			fo.calculateWriteVoteClock()
			if !action.VoteCommit(fo.writeVoteClock, fo.v.subscriptions) {
				fo.WriteAborted(action, true)
			}
		} else {
			fo.writes.Insert(action, postponed)
		}
	default:
		panic(fmt.Sprintf("%v AddWrite called for known txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) WriteAborted(action *localAction, permitInactivate bool) {
	txn := action.Txn
	utils.DebugLog(fo.v.vm.logger, "debug", "WriteAborted", "frame", fo.frame, "TxnId", txn.Id)
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v WriteAborted called for %v with frame in state %v", fo.v, txn, fo.currentState))
	}
	if node := fo.writes.Get(action); node != nil && node.Value == uncommitted {
		node.Remove()
		fo.uncommittedWrites--
		delete(fo.clientWrites, txn.Id.ClientId(common.RMIdEmpty))
		action.frame = nil
		if fo.writes.Len() == 0 {
			fo.writeVoteClock = nil
			fo.maybeStartRoll()
			if permitInactivate {
				fo.v.maybeMakeInactive()
			}
		} else {
			fo.maybeCreateChild()
		}
	} else {
		panic(fmt.Sprintf("%v WriteAborted called for unknown txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) WriteCommitted(action *localAction) {
	txn := action.Txn
	utils.DebugLog(fo.v.vm.logger, "debug", "WriteCommitted", "frame", fo.frame, "TxnId", txn.Id)
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v WriteCommitted called for %v with frame in state %v", fo.v, txn, fo.currentState))
	}
	if node := fo.writes.Get(action); node != nil && node.Value == uncommitted {
		node.Value = committed
		fo.uncommittedWrites--
		fo.positionsFound = fo.positionsFound || (fo.frameTxnActions == nil && action.createPositions != nil)
		fo.maybeCreateChild()
	} else {
		panic(fmt.Sprintf("%v WriteCommitted called for unknown txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) AddReadWrite(action *localAction) {
	txn := action.Txn
	utils.DebugLog(fo.v.vm.logger, "debug", "AddReadWrite", "frame", fo.frame, "TxnId", txn.Id, "vsn", action.readVsn)
	switch {
	case fo.currentState != fo:
		panic(fmt.Sprintf("%v AddReadWrite called for %v with frame in state %v", fo.v, txn, fo.currentState))
	case fo.writes.Len() != 0 || (fo.maxUncommittedRead != nil && action.Compare(fo.maxUncommittedRead) == sl.LT) || fo.frameTxnActions == nil || len(fo.learntFutureReads) != 0:
		action.VoteDeadlock(fo.frameTxnClock)
	case fo.frameTxnId.Compare(action.readVsn) != common.EQ:
		action.VoteBadRead(fo.frameTxnClock, fo.frameTxnId, fo.frameTxnActions)
		fo.v.maybeMakeInactive()
	case fo.writes.Get(action) == nil:
		fo.rwPresent = true
		fo.uncommittedWrites++
		action.frame = fo.frame
		if fo.uncommittedReads == 0 {
			fo.writes.Insert(action, uncommitted)
			fo.calculateWriteVoteClock()
			if !action.VoteCommit(fo.writeVoteClock, fo.v.subscriptions) {
				fo.ReadWriteAborted(action, true)
			}
		} else {
			fo.writes.Insert(action, postponed)
		}
	default:
		panic(fmt.Sprintf("%v AddReadWrite called for known txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) ReadWriteAborted(action *localAction, permitInactivate bool) {
	txn := action.Txn
	utils.DebugLog(fo.v.vm.logger, "debug", "ReadWriteAborted", "frame", fo.frame, "TxnId", txn.Id)
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v ReadWriteAborted called for %v with frame in state %v", fo.v, txn, fo.currentState))
	}
	if node := fo.writes.Get(action); node != nil && node.Value == uncommitted {
		node.Remove()
		fo.uncommittedWrites--
		fo.rwPresent = false
		action.frame = nil
		if fo.writes.Len() == 0 {
			fo.writeVoteClock = nil
			fo.maybeStartRoll()
			if permitInactivate {
				fo.v.maybeMakeInactive()
			}
		} else {
			fo.maybeCreateChild()
		}
	} else {
		panic(fmt.Sprintf("%v ReadWriteAborted called for unknown txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) ReadWriteCommitted(action *localAction) {
	txn := action.Txn
	utils.DebugLog(fo.v.vm.logger, "debug", "ReadWriteCommitted", "frame", fo.frame, "TxnId", txn.Id)
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v ReadWriteCommitted called for %v with frame in state %v", fo.v, txn, fo.currentState))
	}
	if node := fo.writes.Get(action); node != nil && node.Value == uncommitted {
		node.Value = committed
		fo.uncommittedWrites--
		fo.maybeCreateChild()
	} else {
		panic(fmt.Sprintf("%v ReadWriteCommitted called for unknown txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) ReadLearnt(action *localAction) bool {
	txn := action.Txn
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v ReadLearnt called for %v with frame in state %v", fo.v, txn, fo.currentState))
	}
	actClockElem := action.outcomeClock.At(fo.v.UUId)
	if actClockElem == 0 {
		panic("About to do 0 - 1 in uint64")
	}
	actClockElem--
	reqClockElem := fo.frameTxnClock.At(fo.v.UUId)
	if action.readVsn.Compare(fo.frameTxnId) != common.EQ {
		// The write would be one less than the read. We want to know if
		// this read is of a write before or after our current frame
		// write. If the clock elems are equal then the read _must_ be
		// of a write that is after this frame write and we created this
		// frame "early", so we should store the read. So that means we
		// only should ignore this read if its write clock elem is < our
		// frame write clock elem.
		if actClockElem < reqClockElem {
			utils.DebugLog(fo.v.vm.logger, "debug", "ReadLearnt. Ignored. Too Old.", "frame", fo.frame, "TxnId", txn.Id)
			fo.maybeStartRoll()
			return false
		} else {
			utils.DebugLog(fo.v.vm.logger, "debug", "ReadLearnt. Future frame.", "frame", fo.frame, "TxnId", txn.Id)
			fo.learntFutureReads = append(fo.learntFutureReads, action)
			action.frame = fo.frame
			return true
		}
	}
	if actClockElem != reqClockElem {
		panic(fmt.Sprintf("%v oddness in read learnt: read is of right version, but clocks differ (action=%v != frame=%v) (%v)", fo.frame, actClockElem, reqClockElem, action))
	}
	if fo.reads.Get(action) == nil {
		fo.reads.Insert(action, committed)
		action.frame = fo.frame
		// If we had voted on this txn (rather than learning it), then
		// every element within our readVoteClock we would find within
		// the action.outcomeClock (albeit possibly at a later
		// version). Thus if anything within our readVoteClock is _not_
		// in the action.outcomeClock then we know that we must be
		// missing some TGCs - essentially we can infer TGCs by
		// observing the outcome clocks on future txns we learn.
		fo.calculateReadVoteClock()
		mask := vc.NewVectorClock().AsMutable()
		fo.readVoteClock.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
			if action.outcomeClock.At(vUUId) == 0 {
				mask.SetVarIdMax(vUUId, v)
			}
			return true
		})
		fo.subtractClock(mask)
		utils.DebugLog(fo.v.vm.logger, "debug", "ReadLearnt", "frame", fo.frame, "TxnId", txn.Id, "uncommittedReads", fo.uncommittedReads, "uncommittedWrites", fo.uncommittedWrites)
		fo.maybeStartRoll()
		return true
	} else {
		panic(fmt.Sprintf("%v ReadLearnt called for known txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) WriteLearnt(action *localAction) bool {
	txn := action.Txn
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v WriteLearnt called for %v with frame in state %v", fo.v, txn, fo.currentState))
	}
	actClockElem := action.outcomeClock.At(fo.v.UUId)
	reqClockElem := fo.frameTxnClock.At(fo.v.UUId)
	if actClockElem < reqClockElem || (actClockElem == reqClockElem && action.Id.Compare(fo.frameTxnId) == common.LT) {
		utils.DebugLog(fo.v.vm.logger, "debug", "WriteLearnt. Ignored. Too Old.", "frame", fo.frame, "TxnId", txn.Id)
		fo.maybeStartRoll()
		return false
	}
	if action.Id.Compare(fo.frameTxnId) == common.EQ {
		utils.DebugLog(fo.v.vm.logger, "debug", "WriteLearnt. Duplicate of current frame.", "frame", fo.frame, "TxnId", txn.Id)
		fo.maybeStartRoll()
		return false
	}
	if actClockElem == reqClockElem {
		// ok, so ourself and this txn were actually siblings, but we
		// created this frame before we knew that. By definition, there
		// cannot be any committed reads of us.
		if fo.reads.Len() > fo.uncommittedReads {
			panic(fmt.Sprintf("%v (%v) Found committed reads where there should have been none for action %v (%v)", fo.frame, fo.frameTxnClock, action, action.outcomeClock))
		}
	}
	if fo.writes.Get(action) == nil {
		fo.writes.Insert(action, committed)
		action.frame = fo.frame
		fo.positionsFound = fo.positionsFound || (fo.frameTxnActions == nil && action.createPositions != nil)
		// See corresponding comment in ReadLearnt. We only force the
		// readvoteclock here because we cannot calculate the
		// writevoteclock because we may have uncommitted reads.
		clock := fo.writeVoteClock
		if clock == nil {
			fo.calculateReadVoteClock()
			clock = fo.readVoteClock
		}
		mask := vc.NewVectorClock().AsMutable()
		clock.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
			if action.outcomeClock.At(vUUId) == 0 {
				mask.SetVarIdMax(vUUId, v)
			}
			return true
		})
		fo.subtractClock(mask)
		utils.DebugLog(fo.v.vm.logger, "debug", "WriteLearnt", "frame", fo.frame, "TxnId", txn.Id, "uncommittedReads", fo.uncommittedReads, "uncommittedWrites", fo.uncommittedWrites)
		fo.maybeCreateChild()
		return true
	} else {
		panic(fmt.Sprintf("%v WriteLearnt called for known txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) maybeFindMaxReadFrom(action *localAction, node *sl.Node) {
	if fo.uncommittedReads == 0 {
		fo.maxUncommittedRead = nil
		fo.maybeStartWrites()
	} else if fo.maxUncommittedRead == action {
		for {
			if node.Value == uncommitted {
				fo.maxUncommittedRead = node.Key.(*localAction)
				break
			}
			node = node.Prev()
		}
	}
}

func (fo *frameOpen) maybeStartWrites() {
	fo.maybeStartRoll()
	if fo.writes.Len() == 0 || fo.uncommittedReads != 0 {
		return
	}
	if fo.uncommittedWrites == 0 {
		// fo.writes must be full of learnt writes only
		fo.maybeCreateChild()
	} else {
		fo.calculateWriteVoteClock()
		for node := fo.writes.First(); node != nil; {
			next := node.Next()
			if node.Value == postponed {
				node.Value = uncommitted
				if action := node.Key.(*localAction); !action.VoteCommit(fo.writeVoteClock, fo.v.subscriptions) {
					if action.IsRead() {
						fo.ReadWriteAborted(action, false)
					} else {
						fo.WriteAborted(action, false)
					}
				}
			}
			node = next
		}
	}
}

func (fo *frameOpen) calculateReadVoteClock() {
	if fo.readVoteClock == nil {
		if fo.frameWritesClock.At(fo.v.UUId) == 0 {
			panic(fmt.Sprintf("%v no write to self! %v", fo.frame, fo.frameWritesClock))
		}

		// see notes below in calculateWriteVoteClock!
		clock := fo.frameTxnClock.Clone()
		if fo.mask.Len()+fo.frameWritesClock.Len() > clock.Len() {
			// mask is bigger, so look through clock
			clock.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
				if m := fo.mask.At(vUUId); m >= v {
					clock.Delete(vUUId)
				} else if w := fo.frameWritesClock.At(vUUId); w == v {
					clock.Bump(vUUId, 1)
				}
				return true
			})

		} else {
			fo.mask.ForEach(func(vUUId *common.VarUUId, m uint64) bool {
				if v := clock.At(vUUId); m >= v {
					clock.Delete(vUUId)
				}
				return true
			})
			fo.frameWritesClock.ForEach(func(vUUId *common.VarUUId, w uint64) bool {
				if v := clock.At(vUUId); v == w {
					clock.Bump(vUUId, 1)
				}
				return true
			})
		}

		fo.readVoteClock = clock
	}
}

func (fo *frameOpen) calculateWriteVoteClock() {
	if fo.writeVoteClock == nil {
		fo.calculateReadVoteClock()
		clock := fo.readVoteClock.Clone()
		written := vc.NewVectorClock().AsMutable()
		for node := fo.reads.First(); node != nil; node = node.Next() {
			action := node.Key.(*localAction)
			clock.MergeInMax(action.outcomeClock)
			for _, k := range action.writes {
				written.SetVarIdMax(k, action.outcomeClock.At(k))
			}
		}

		// Everything in written is also in clock. But the value in
		// written can be lower than in clock because a txn may have a
		// future read of a var with a higher clock elem.  But if the
		// mask is > then the value in clock then it can't be the case
		// that the value in mask is <= the value in written.

		if fo.mask.Len()+written.Len() > clock.Len() {
			// mask is bigger, so loop through clock
			clock.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
				if m := fo.mask.At(vUUId); m >= v {
					clock.Delete(vUUId)
				} else if w := written.At(vUUId); w == v {
					clock.Bump(vUUId, 1)
				}
				return true
			})

		} else {
			// mask is smaller, so loop through mask. But this means we
			// have to do written separately
			fo.mask.ForEach(func(vUUId *common.VarUUId, m uint64) bool {
				if v := clock.At(vUUId); m >= v {
					// there is no risk we will add this back in in the
					// written loop (see above)
					clock.Delete(vUUId)
				}
				return true
			})
			written.ForEach(func(vUUId *common.VarUUId, w uint64) bool {
				if v := clock.At(vUUId); v == w {
					clock.Bump(vUUId, 1)
				}
				return true
			})
		}

		fo.writeVoteClock = clock
	}
}

func (fo *frameOpen) maybeCreateChild() {
	// still working on reads   || still working on writes   || never done any writes || first frame on var creation and we've not yet seen the actual create yet
	if fo.uncommittedReads != 0 || fo.uncommittedWrites != 0 || fo.writes.Len() == 0 || (fo.frameTxnActions == nil && !fo.positionsFound) {
		return
	}

	// fmt.Printf("r%vw%v ", fo.reads.Len(), fo.writes.Len())

	// First we need to order by local elem. Because writes is a
	// skiplist of txns in txnid order, the lists we append to will
	// also remain in txnid order.
	vUUId := fo.v.UUId
	localElemValToTxns := make(map[uint64]*[]*localAction)
	localElemVals := uint64s([]uint64{})
	for node := fo.writes.First(); node != nil; node = node.Next() {
		action := node.Key.(*localAction)
		localElemVal := action.outcomeClock.At(vUUId)
		if listPtr, found := localElemValToTxns[localElemVal]; found {
			*listPtr = append(*listPtr, action)
		} else {
			list := []*localAction{action}
			localElemValToTxns[localElemVal] = &list
			localElemVals = append(localElemVals, localElemVal)
		}
	}
	localElemVals.Sort()

	var clock, written *vc.VectorClockMutable

	elem := fo.frameTxnClock.At(fo.v.UUId)
	switch {
	case len(localElemVals) == 1 && localElemVals[0] == elem:
		// We must have learnt one or more writes that have the same
		// local elem as the frame txn so they were siblings of the
		// frame txn. By dfn, there can have been no successful reads of
		// this frame txn.
		clock = fo.frameTxnClock.Clone()
		written = fo.frameWritesClock.Clone()
		if fo.reads.Len() != 0 {
			panic(fmt.Sprintf("%v has committed reads even though frame has younger siblings", fo.frame))
		}

	case localElemVals[0] == elem:
		// We learnt of some siblings to this frame txn, but we also did
		// further work. Again, there can not have been any reads of
		// this frame txn. We can also ignore our siblings because the
		// further work will by definition include the consequences of
		// the siblings to this frame.
		localElemVals = localElemVals[1:]
		if fo.reads.Len() != 0 {
			panic(fmt.Sprintf("%v has committed reads even though frame has younger siblings", fo.frame))
		}
		fo.calculateWriteVoteClock()
		clock = fo.writeVoteClock
		written = vc.NewVectorClock().AsMutable()

	default:
		fo.calculateWriteVoteClock()
		clock = fo.writeVoteClock
		written = vc.NewVectorClock().AsMutable()
	}

	var winner *localAction
	var positions *common.Positions

	for _, localElemVal := range localElemVals {
		actions := localElemValToTxns[localElemVal]
		for _, action := range *actions {
			if positions == nil && action.createPositions != nil {
				positions = action.createPositions
			}

			outcomeClock := action.outcomeClock.AsMutable()
			action.outcomeClock = outcomeClock

			clock.MergeInMax(outcomeClock)
			outcomeClock.MergeInMissing(clock)
			winner = maxTxnByOutcomeClock(winner, action)

			if action.writesClock == nil {
				for _, k := range action.writes {
					written.SetVarIdMax(k, outcomeClock.At(k))
				}
			} else {
				written.MergeInMax(action.writesClock)
			}
		}
	}

	fo.child = NewFrame(fo.frame, fo.v, winner.Id, winner.writeTxnActions, winner.outcomeClock.AsMutable(), written)
	fo.v.SetCurFrame(fo.child, winner, positions)
	for _, action := range fo.learntFutureReads {
		action.frame = nil
		utils.DebugLog(fo.v.vm.logger, "debug", "New frame learns future reads.", "frame", fo.frame)
		if !fo.child.ReadLearnt(action) {
			action.LocallyComplete()
		}
	}
	fo.learntFutureReads = nil
	fo.nextState()
	fo.readVoteClock = nil
	fo.writeVoteClock = nil
	fo.clientWrites = nil
	fo.rollTxn = nil
}

func (fo *frameOpen) basicRollCondition(rescheduling bool) bool {
	return (rescheduling || fo.rollScheduled == nil) && !fo.rollActive && fo.currentState == fo && fo.child == nil && fo.writes.Len() == 0 && fo.v.positions != nil && fo.v.curFrame == fo.frame &&
		(fo.reads.Len() > fo.uncommittedReads || (fo.frameTxnClock.Len() > fo.frameTxnActions.Actions().Len() && fo.parent == nil && fo.reads.Len() == 0 && len(fo.learntFutureReads) == 0))
}

func (fo *frameOpen) maybeStartRoll() {
	fo.maybeStartRollFrom(false)
}

func (fo *frameOpen) maybeStartRollFrom(rescheduling bool) {
	if fo.basicRollCondition(rescheduling) {
		multiplier := 0
		for node := fo.reads.First(); node != nil; node = node.Next() {
			if node.Value == committed {
				multiplier += node.Key.(*localAction).TxnReader.Actions(true).Actions().Len()
			}
		}
		now := time.Now()
		quietDuration := server.VarRollTimeExpectation * time.Duration(multiplier)
		probOfZero := fo.v.poisson.P(quietDuration, 0, now)
		elapsed := time.Duration(0)
		if fo.rollScheduled == nil {
			fo.rollScheduled = &now
		} else {
			elapsed = now.Sub(*fo.rollScheduled)
		}
		// fmt.Printf("s%v(%v|%v)\n", fo.v.UUId, probOfZero, fo.scheduleBackoff.Cur)
		if fo.v.vm.RollAllowed && (probOfZero > server.VarRollPRequirement || (elapsed > server.VarRollDelayMax)) {
			// fmt.Printf("%v r%v %v\n", now, fo.v.UUId, elapsed)
			fo.startRoll(rollCallback{
				frameOpen: fo,
				forceRoll: elapsed > server.VarRollForceNotFirstAfter,
			})
		} else {
			fo.scheduleRoll()
		}
	} else if rescheduling {
		fo.rollScheduled = nil
	}
}

func (fo *frameOpen) scheduleRoll() {
	utils.DebugLog(fo.v.vm.logger, "debug", "Roll callback scheduled.", "frame", fo.frame)
	fo.v.vm.ScheduleCallback(fo.scheduleBackoff.Advance(), func(*time.Time) {
		fo.v.applyToSelf(func() {
			fo.maybeStartRollFrom(true)
		})
	})
}

func (fo *frameOpen) startRoll(rollCB rollCallback) {
	fo.rollActive = true
	// must do roll txn creation in the main go-routine
	ctxn, varPosVerMap := fo.createRollClientTxn()
	utils.DebugLog(fo.v.vm.logger, "debug", "Starting roll.", "frame", fo.frame)
	go func() {
		// Yes, we really must mark these as topology txns so that they
		// are allowed through during topology changes.
		_, outcome, err := fo.v.vm.RunClientTransaction(ctxn, true, varPosVerMap, rollCB.rollTranslationCallback)
		ow := ""
		if outcome != nil {
			ow = fmt.Sprint(outcome.Which())
			if outcome.Which() == msgs.OUTCOME_ABORT {
				ow += fmt.Sprintf("-%v", outcome.Abort().Which())
			}
		}
		// fmt.Printf("%v r%v (%v)\n", fo.v.UUId, ow, err == AbortRollNotFirst)
		fo.v.applyToSelf(func() {
			utils.DebugLog(fo.v.vm.logger, "debug", "Roll finished.", "frame", fo.frame, "outcome", ow, "error", err)
			if fo.v.curFrame != fo.frame {
				return
			}
			fo.rollActive = false
			if (outcome == nil && err != nil) || (outcome != nil && outcome.Which() != msgs.OUTCOME_COMMIT) {
				if err == AbortRollNotInPermutation {
					// we need to go to sleep - this var has been removed from this RM
					fo.rollScheduled = nil
					fo.v.maybeMakeInactive()
				} else {
					fo.scheduleRoll()
				}
			}
		})
	}()
}

type rollCallback struct {
	*frameOpen
	forceRoll bool
}

// careful in here: we'll be running this inside localConnection's actor.
func (rc rollCallback) rollTranslationCallback(cAction *cmsgs.ClientAction, action *msgs.Action, hashCodes []common.RMId, connections map[common.RMId]*sconn.ServerConnection) error {
	// We cannot roll for anyone else. This could try to happen during
	// immigration, which is very bad because we will probably have the
	// wrong hashcodes so could cause divergence.
	foundSelf := false
	for _, rmId := range hashCodes {
		if foundSelf = rmId == rc.v.vm.RMId; foundSelf {
			break
		} else if _, found := connections[rmId]; !rc.forceRoll && found {
			// we're not forced, and there is someone else alive ahead of us who should be doing the roll.
			return AbortRollNotFirst
		}
	}
	if !foundSelf {
		return AbortRollNotInPermutation
	}
	return nil
}

func (fo *frameOpen) createRollClientTxn() (*cmsgs.ClientTxn, map[common.VarUUId]*types.PosCapVer) {
	if fo.rollTxn != nil {
		return fo.rollTxn, fo.rollTxnPos
	}
	var origAction *msgs.Action
	vUUIdBytes := fo.v.UUId[:]
	txnActions := fo.frameTxnActions.Actions()
	for idx, l := 0, txnActions.Len(); idx < l; idx++ {
		action := txnActions.At(idx)
		if bytes.Equal(action.VarId(), vUUIdBytes) {
			origAction = &action
			break
		}
	}

	posMap := make(map[common.VarUUId]*types.PosCapVer)

	seg := capn.NewBuffer(nil)
	ctxn := cmsgs.NewClientTxn(seg)
	ctxn.SetRetry(false)
	actions := cmsgs.NewClientActionList(seg, 1)
	ctxn.SetActions(actions)
	action := actions.At(0)
	action.SetVarId(fo.v.UUId[:])
	action.SetActionType(cmsgs.CLIENTACTIONTYPE_ROLL)
	action.SetModified()
	modified := action.Modified()
	origModified := origAction.Modified()
	modified.SetValue(origModified.Value())
	origRefs := origModified.References()

	refVarList := cmsgs.NewClientVarIdPosList(seg, origRefs.Len())
	modified.SetReferences(refVarList)
	for idx, l := 0, origRefs.Len(); idx < l; idx++ {
		origRef := origRefs.At(idx)
		vUUId := common.MakeVarUUId(origRef.Id())
		// these are needed because the translation verifies the refs are valid
		pos := common.Positions(origRef.Positions())
		posMap[*vUUId] = &types.PosCapVer{
			Positions:  &pos,
			Capability: common.NewCapability(origRef.Capability()),
		}
		varIdPos := refVarList.At(idx)
		varIdPos.SetVarId(vUUId[:])
		varIdPos.SetCapability(origRef.Capability())
	}
	fo.rollTxn = &ctxn
	// we do this one last in case our own refs point at ourself and so
	// we need to overwrite it to include the correct version
	posMap[*fo.v.UUId] = &types.PosCapVer{
		Positions:  fo.v.positions,
		Capability: common.ReadWriteCapability,
		Version:    fo.frameTxnId,
	}
	fo.rollTxnPos = posMap
	return &ctxn, posMap
}

func (fo *frameOpen) subtractClock(clock vc.VectorClock) {
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v subtractClock called with frame in state %v", fo.v, fo.currentState))
	}
	if changed := fo.mask.MergeInMax(clock); changed {
		fo.mask.Delete(fo.v.UUId)
		if fo.writes.Len() == 0 && len(fo.learntFutureReads) == 0 {
			fo.writeVoteClock = nil
			if fo.reads.Len() == 0 {
				fo.readVoteClock = nil
			}
		}
	}
}

func (fo *frameOpen) isIdle() bool {
	return fo.parent == nil && fo.rollScheduled == nil && fo.isEmpty()
}

func (fo *frameOpen) isEmpty() bool {
	return fo.currentState == fo && !fo.rollActive && fo.child == nil && fo.writes.Len() == 0 && fo.reads.Len() == 0 && len(fo.learntFutureReads) == 0
}

// closed

type frameClosed struct {
	*frame
	onDisk bool
}

func (fc *frameClosed) init(f *frame) {
	fc.frame = f
}

func (fc *frameClosed) start() {
	fc.MaybeCompleteTxns()
}

func (fc *frameClosed) String() string { return "frameClosed" }

func (fc *frameClosed) DescendentOnDisk() bool {
	if !fc.onDisk {
		utils.DebugLog(fc.v.vm.logger, "debug", "DescendentOnDisk", "frame", fc.frame)
		fc.onDisk = true
		fc.MaybeCompleteTxns()
		return true
	}
	return false
}

func (fc *frameClosed) MaybeCompleteTxns() {
	if fc.currentState == fc && fc.onDisk && fc.parent == nil {
		utils.DebugLog(fc.v.vm.logger, "debug", "MaybeCompleteTxns", "frame", fc.frame)
		fc.nextState()
		for node := fc.reads.First(); node != nil; node = node.Next() {
			if node.Value == committed {
				node.Key.(*localAction).LocallyComplete()
				node.Value = completing
			} else {
				panic(fmt.Sprintf("Not committed! %v %v %v", fc.frame, node.Key, node.Value))
			}
		}
		for node := fc.writes.First(); node != nil; node = node.Next() {
			if node.Value == committed {
				node.Key.(*localAction).LocallyComplete()
				node.Value = completing
			} else {
				panic(fmt.Sprintf("Not committed! %v %v %v", fc.frame, node.Key, node.Value))
			}
		}
	}
	fc.maybeStartRoll()
	fc.v.maybeMakeInactive()
}

// erase

type frameErase struct {
	*frame
}

func (fe *frameErase) init(f *frame) {
	fe.frame = f
}

func (fe *frameErase) start()         {}
func (fe *frameErase) String() string { return "frameErase" }

func (fe *frameErase) ReadGloballyComplete(action *localAction) {
	txn := action.Txn
	utils.DebugLog(fe.v.vm.logger, "debug", "ReadGloballyComplete", "frame", fe.frame, "TxnId", txn.Id)
	if fe.currentState != fe {
		panic(fmt.Sprintf("%v ReadGloballyComplete called for %v with frame in state %v", fe.v, txn, fe.currentState))
	}
	if node := fe.reads.Get(action); node != nil && node.Value == completing {
		node.Remove()
		fe.v.curFrame.subtractClock(action.outcomeClock)
		fe.maybeErase()
	} else {
		panic(fmt.Sprintf("ReadGloballyComplete for invalid action %v %v", fe.frame, node))
	}
}

func (fe *frameErase) WriteGloballyComplete(action *localAction) {
	txn := action.Txn
	utils.DebugLog(fe.v.vm.logger, "debug", "WriteGloballyComplete", "frame", fe.frame, "TxnId", txn.Id)
	if fe.currentState != fe {
		panic(fmt.Sprintf("%v WriteGloballyComplete called for %v with frame in state %v", fe.v, txn, fe.currentState))
	}
	if node := fe.writes.Get(action); node != nil && node.Value == completing {
		node.Remove()
		fe.v.curFrame.subtractClock(action.outcomeClock)
		fe.maybeErase()
	} else {
		panic(fmt.Sprintf("ReadGloballyComplete for invalid action %v %v", fe.frame, node))
	}
}

func (fe *frameErase) maybeErase() {
	// (we won't receive TGCs for learnt writes)
	if fe.reads.Len() == 0 && fe.writes.Len() == 0 {
		utils.DebugLog(fe.v.vm.logger, "debug", "maybeErase", "frame", fe.frame)
		child := fe.child
		child.parent = nil
		child.MaybeCompleteTxns() // child may be in frame open!
		fe.nextState()
	}
}

// uint64s

type uint64s []uint64

func (l uint64s) Len() int           { return len(l) }
func (l uint64s) Less(i, j int) bool { return l[i] < l[j] }
func (l uint64s) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l uint64s) Sort()              { sort.Sort(l) }

func maxTxnByOutcomeClock(a, b *localAction) *localAction {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	default:
		agt := b.outcomeClock.LessThan(a.outcomeClock)
		bgt := a.outcomeClock.LessThan(b.outcomeClock)
		switch {
		case agt == bgt:
			if a.Id.Compare(b.Id) == common.LT {
				return b
			} else {
				return a
			}
		case agt:
			return a
		default:
			return b
		}
	}
}
