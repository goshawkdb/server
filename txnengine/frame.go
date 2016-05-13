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
	"sort"
)

var AbortRollNotFirst = errors.New("AbortRollNotFirst")
var AbortRollNotInPermutation = errors.New("AbortRollNotInPermutation")

type frame struct {
	parent           *frame
	child            *frame
	v                *Var
	frameTxnId       *common.TxnId
	frameTxnActions  *msgs.Action_List
	frameTxnClock    *VectorClock
	frameWritesClock *VectorClock
	readVoteClock    *VectorClock
	positionsFound   bool
	mask             *VectorClock
	frameOpen
	frameClosed
	frameErase
	currentState frameStateMachineComponent
}

func NewFrame(parent *frame, v *Var, txnId *common.TxnId, txnActions *msgs.Action_List, txnClock *VectorClock, writesClock *VectorClock) *frame {
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
		f.mask = NewVectorClock()
	} else {
		f.mask = parent.mask
	}
	f.init()
	server.Log(f, "NewFrame")
	f.calculateReadVoteClock()
	f.maybeScheduleRoll()
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
	return fmt.Sprintf("%v Frame %v (%v) r%v w%v", f.v.UUId, f.frameTxnId, f.frameTxnClock.Len, f.readVoteClock, f.writeVoteClock)
}

func (f *frame) Status(sc *server.StatusConsumer) {
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
	sc.Emit(fmt.Sprintf("- Locked? %v", f.isLocked()))
	sc.Emit(fmt.Sprintf("- Roll scheduled/active? %v/%v", f.rollScheduled, f.rollActive))
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
	frameStateMachineWitness()
}

// open

type frameOpen struct {
	*frame
	reads              *sl.SkipList
	learntFutureReads  []*localAction
	maxUncommittedRead *localAction
	uncommittedReads   uint
	writeVoteClock     *VectorClock
	writes             *sl.SkipList
	clientWrites       map[[common.ClientLen]byte]server.EmptyStruct
	uncommittedWrites  uint
	rwPresent          bool
	rollScheduled      bool
	rollActive         bool
	rollTxn            *cmsgs.ClientTxn
	rollTxnPos         map[common.VarUUId]*common.Positions
}

func (fo *frameOpen) init(f *frame) {
	fo.frame = f
	fo.reads = sl.New(f.v.rng)
	fo.learntFutureReads = []*localAction{}
	fo.writes = sl.New(f.v.rng)
	fo.clientWrites = make(map[[common.ClientLen]byte]server.EmptyStruct)
}

func (fo *frameOpen) start()                    {}
func (fo *frameOpen) frameStateMachineWitness() {}
func (fo *frameOpen) String() string            { return "frameOpen" }

func (fo *frameOpen) ReadRetry(action *localAction) bool {
	txn := action.Txn
	server.Log(fo.frame, "ReadRetry", txn)
	switch {
	case fo.currentState != fo:
		panic(fmt.Sprintf("%v ReadRetry called for %v with frame in state %v", fo.v, txn, fo.currentState))
	case fo.frameTxnActions == nil || fo.frameTxnId.Compare(action.readVsn) == common.EQ:
		return false
	default:
		action.VoteBadRead(fo.frameTxnClock, fo.frameTxnId, fo.frameTxnActions)
		fo.v.maybeMakeInactive()
		return true
	}
}

func (fo *frameOpen) AddRead(action *localAction) {
	txn := action.Txn
	server.Log(fo.frame, "AddRead", txn, action.readVsn)
	switch {
	case fo.currentState != fo:
		panic(fmt.Sprintf("%v AddRead called for %v with frame in state %v", fo.v, txn, fo.currentState))
	case fo.writeVoteClock != nil || (fo.writes.Len() != 0 && fo.writes.First().Key.Compare(action) == sl.LT) || fo.frameTxnActions == nil || fo.isLocked():
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
		if !action.VoteCommit(fo.readVoteClock) {
			fo.ReadAborted(action)
		}
	default:
		panic(fmt.Sprintf("%v AddRead called for known txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) ReadAborted(action *localAction) {
	txn := action.Txn
	server.Log(fo.frame, "ReadAborted", txn)
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
	server.Log(fo.frame, "ReadCommitted", txn)
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
	server.Log(fo.frame, "AddWrite", txn)
	cid := txn.Id.ClientId()
	_, found := fo.clientWrites[cid]
	switch {
	case fo.currentState != fo:
		panic(fmt.Sprintf("%v AddWrite called for %v with frame in state %v", fo.v, txn, fo.currentState))
	case fo.rwPresent || (fo.maxUncommittedRead != nil && action.Compare(fo.maxUncommittedRead) == sl.LT) || found || len(fo.learntFutureReads) != 0 || fo.isLocked():
		action.VoteDeadlock(fo.frameTxnClock)
	case fo.writes.Get(action) == nil:
		fo.uncommittedWrites++
		fo.clientWrites[cid] = server.EmptyStructVal
		action.frame = fo.frame
		if fo.uncommittedReads == 0 {
			fo.writes.Insert(action, uncommitted)
			fo.calculateWriteVoteClock()
			if !action.VoteCommit(fo.writeVoteClock) {
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
	server.Log(fo.frame, "WriteAborted", txn)
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v WriteAborted called for %v with frame in state %v", fo.v, txn, fo.currentState))
	}
	if node := fo.writes.Get(action); node != nil && node.Value == uncommitted {
		node.Remove()
		fo.uncommittedWrites--
		delete(fo.clientWrites, txn.Id.ClientId())
		action.frame = nil
		if fo.writes.Len() == 0 {
			fo.writeVoteClock = nil
			fo.maybeScheduleRoll()
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
	server.Log(fo.frame, "WriteCommitted", txn)
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
	server.Log(fo.frame, "AddReadWrite", txn, action.readVsn)
	switch {
	case fo.currentState != fo:
		panic(fmt.Sprintf("%v AddReadWrite called for %v with frame in state %v", fo.v, txn, fo.currentState))
	case fo.writeVoteClock != nil || fo.writes.Len() != 0 || (fo.maxUncommittedRead != nil && action.Compare(fo.maxUncommittedRead) == sl.LT) || fo.frameTxnActions == nil || len(fo.learntFutureReads) != 0 || (!action.IsRoll() && fo.isLocked()):
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
			if !action.VoteCommit(fo.writeVoteClock) {
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
	server.Log(fo.frame, "ReadWriteAborted", txn)
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
			fo.maybeScheduleRoll()
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
	server.Log(fo.frame, "ReadWriteCommitted", txn)
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
		panic("Just did 0 - 1 in int64")
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
			server.Log(fo.frame, "ReadLearnt", txn, "ignored, too old")
			return false
		} else {
			server.Log(fo.frame, "ReadLearnt", txn, "of future frame")
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
		fo.readVoteClock.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
			if action.outcomeClock.At(vUUId) == 0 {
				fo.mask.SetVarIdMax(vUUId, v)
			}
			return true
		})
		server.Log(fo.frame, "ReadLearnt", txn, "uncommittedReads:", fo.uncommittedReads, "uncommittedWrites:", fo.uncommittedWrites)
		fo.maybeScheduleRoll()
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
		server.Log(fo.frame, "WriteLearnt", txn, "ignored, too old")
		return false
	}
	if action.Id.Compare(fo.frameTxnId) == common.EQ {
		server.Log(fo.frame, "WriteLearnt", txn, "is duplicate of current frame")
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
		// See corresponding comment in ReadLearnt
		clock := fo.writeVoteClock
		if clock == nil {
			clock = fo.readVoteClock
		}
		clock.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
			if action.outcomeClock.At(vUUId) == 0 {
				fo.mask.SetVarIdMax(vUUId, v)
			}
			return true
		})
		server.Log(fo.frame, "WriteLearnt", txn, "uncommittedReads:", fo.uncommittedReads, "uncommittedWrites:", fo.uncommittedWrites)
		if fo.uncommittedReads == 0 {
			fo.maybeCreateChild()
		}
		return true
	} else {
		panic(fmt.Sprintf("%v WriteLearnt called for known txn %v", fo.frame, txn))
	}
}

func (fo *frameOpen) isLocked() bool {
	return false
	// Locking is disabled because it's unsafe when there are temporary
	// node failures around: with node failures, TGCs don't get issued
	// so the clocks don't get tidied up, so the frame can lock itself
	// and then we can't make progress. TODO FIXME.
	/*
		if fo.frameTxnActions == nil || fo.parent == nil {
			return false
		}
		rvcLen := fo.readVoteClock.Len
		actionsLen := fo.frameTxnActions.Len()
		excess := rvcLen - actionsLen
		return excess > server.FrameLockMinExcessSize && rvcLen > actionsLen*server.FrameLockMinRatio
	*/
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
	fo.maybeScheduleRoll()
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
				if action := node.Key.(*localAction); !action.VoteCommit(fo.writeVoteClock) {
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
		clock := fo.frameTxnClock.Clone()
		written := fo.frameWritesClock.Clone()
		clock.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
			if fo.mask.At(vUUId) >= v {
				clock.Delete(vUUId)
			}
			return true
		})
		written.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
			if fo.mask.At(vUUId) < v || fo.v.UUId.Compare(vUUId) == common.EQ {
				clock.SetVarIdMax(vUUId, v+1)
			}
			return true
		})
		fo.readVoteClock = clock
		if fo.frameWritesClock.At(fo.v.UUId) == 0 {
			panic(fmt.Sprintf("%v no write to self! %v", fo.frame, fo.frameWritesClock))
		}
	}
}

func (fo *frameOpen) calculateWriteVoteClock() {
	if fo.writeVoteClock == nil {
		clock := fo.readVoteClock.Clone()
		written := NewVectorClock()
		for node := fo.reads.First(); node != nil; node = node.Next() {
			action := node.Key.(*localAction)
			clock.MergeInMax(action.outcomeClock)
			for _, k := range action.writes {
				written.SetVarIdMax(k, action.outcomeClock.At(k))
			}
		}
		clock.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
			if fo.mask.At(vUUId) >= v {
				clock.Delete(vUUId)
			}
			return true
		})
		written.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
			if fo.mask.At(vUUId) < v {
				clock.SetVarIdMax(vUUId, v+1)
			}
			return true
		})
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

	var clock, written *VectorClock

	elem := fo.frameTxnClock.At(fo.v.UUId)
	switch {
	case len(localElemVals) == 1 && localElemVals[0] == elem:
		clock = fo.frameTxnClock.Clone()
		written = fo.frameWritesClock.Clone()
		for fo.reads.Len() != 0 {
			panic(fmt.Sprintf("%v has committed reads even though frame has younger siblings", fo.frame))
		}

	case localElemVals[0] == elem:
		localElemVals = localElemVals[1:]
		for fo.reads.Len() != 0 {
			panic(fmt.Sprintf("%v has committed reads even though frame has younger siblings", fo.frame))
		}
		fo.calculateWriteVoteClock()
		clock = fo.writeVoteClock.Clone()
		written = NewVectorClock()

	default:
		fo.calculateWriteVoteClock()
		clock = fo.writeVoteClock.Clone()
		written = NewVectorClock()
	}

	var winner *localAction
	var positions *common.Positions

	for _, localElemVal := range localElemVals {
		actions := localElemValToTxns[localElemVal]
		for _, action := range *actions {
			action.outcomeClock = action.outcomeClock.Clone()
			action.outcomeClock.MergeInMissing(clock)
			winner = maxTxnByOutcomeClock(winner, action)

			if positions == nil && action.createPositions != nil {
				positions = action.createPositions
			}

			clock.MergeInMax(action.outcomeClock)
			if action.writesClock == nil {
				for _, k := range action.writes {
					written.SetVarIdMax(k, action.outcomeClock.At(k))
				}
			} else {
				written.MergeInMax(action.writesClock)
			}
		}
	}

	fo.child = NewFrame(fo.frame, fo.v, winner.Id, winner.writeTxnActions, winner.outcomeClock, written)
	for _, action := range fo.learntFutureReads {
		action.frame = nil
		if !fo.child.ReadLearnt(action) {
			action.LocallyComplete()
		}
	}
	fo.learntFutureReads = nil
	fo.nextState()
	fo.v.SetCurFrame(fo.child, winner, positions)
}

func (fo *frameOpen) maybeScheduleRoll() {
	// do not check vm.RollAllowed here.
	if !fo.rollScheduled && !fo.rollActive && fo.currentState == fo && fo.child == nil && fo.writes.Len() == 0 && fo.v.positions != nil &&
		(fo.reads.Len() > fo.uncommittedReads || (fo.frameTxnClock.Len > fo.frameTxnActions.Len() && fo.parent == nil && fo.reads.Len() == 0 && len(fo.learntFutureReads) == 0)) {
		fo.rollScheduled = true
		fo.v.vm.ScheduleCallback(func() {
			fo.v.applyToVar(func() {
				fo.rollScheduled = false
				fo.maybeStartRoll()
			})
		})
	}
}

func (fo *frameOpen) maybeStartRoll() {
	if fo.v.vm.RollAllowed && !fo.rollActive && fo.currentState == fo && fo.child == nil && fo.writes.Len() == 0 && fo.v.positions != nil &&
		(fo.reads.Len() > fo.uncommittedReads || (fo.frameTxnClock.Len > fo.frameTxnActions.Len() && fo.parent == nil && fo.reads.Len() == 0 && len(fo.learntFutureReads) == 0)) {
		fo.rollActive = true
		ctxn, varPosMap := fo.createRollClientTxn()
		go func() {
			server.Log(fo.frame, "Starting roll")
			outcome, err := fo.v.vm.RunClientTransaction(ctxn, varPosMap, true)
			ow := ""
			if outcome != nil {
				ow = fmt.Sprint(outcome.Which())
				if outcome.Which() == msgs.OUTCOME_ABORT {
					ow += fmt.Sprintf("-%v", outcome.Abort().Which())
				}
			}
			// fmt.Printf("r%v ", ow)
			server.Log(fo.frame, "Roll finished: outcome", ow, "; err:", err)
			if outcome == nil || outcome.Which() != msgs.OUTCOME_COMMIT {
				fo.v.applyToVar(func() {
					fo.rollActive = false
					if err == AbortRollNotInPermutation {
						fo.v.maybeMakeInactive()
					} else {
						fo.maybeScheduleRoll()
					}
				})
			}
		}()
	} else {
		fo.maybeScheduleRoll()
	}
}

func (fo *frameOpen) createRollClientTxn() (*cmsgs.ClientTxn, map[common.VarUUId]*common.Positions) {
	if fo.rollTxn != nil {
		return fo.rollTxn, fo.rollTxnPos
	}
	var origWrite *msgs.Action
	vUUIdBytes := fo.v.UUId[:]
	for idx, l := 0, fo.frameTxnActions.Len(); idx < l; idx++ {
		action := fo.frameTxnActions.At(idx)
		if bytes.Equal(action.VarId(), vUUIdBytes) {
			origWrite = &action
			break
		}
	}
	seg := capn.NewBuffer(nil)
	ctxn := cmsgs.NewClientTxn(seg)
	ctxn.SetRetry(false)
	actions := cmsgs.NewClientActionList(seg, 1)
	ctxn.SetActions(actions)
	action := actions.At(0)
	action.SetVarId(fo.v.UUId[:])
	action.SetRoll()
	roll := action.Roll()
	roll.SetVersion(fo.frameTxnId[:])
	var refs msgs.VarIdPos_List
	switch origWrite.Which() {
	case msgs.ACTION_WRITE:
		ow := origWrite.Write()
		roll.SetValue(ow.Value())
		refs = ow.References()
	case msgs.ACTION_READWRITE:
		owr := origWrite.Readwrite()
		roll.SetValue(owr.Value())
		refs = owr.References()
	case msgs.ACTION_CREATE:
		oc := origWrite.Create()
		roll.SetValue(oc.Value())
		refs = oc.References()
	case msgs.ACTION_ROLL:
		owr := origWrite.Roll()
		roll.SetValue(owr.Value())
		refs = owr.References()
	default:
		panic(fmt.Sprintf("%v unexpected action type when building roll: %v", fo.frame, origWrite.Which()))
	}
	posMap := make(map[common.VarUUId]*common.Positions)
	posMap[*fo.v.UUId] = fo.v.positions
	refVarList := seg.NewDataList(refs.Len())
	roll.SetReferences(refVarList)
	for idx, l := 0, refs.Len(); idx < l; idx++ {
		ref := refs.At(idx)
		vUUId := common.MakeVarUUId(ref.Id())
		pos := common.Positions(ref.Positions())
		posMap[*vUUId] = &pos
		refVarList.Set(idx, vUUId[:])
	}
	fo.rollTxn = &ctxn
	fo.rollTxnPos = posMap
	return &ctxn, posMap
}

func (fo *frameOpen) subtractClock(clock *VectorClock) {
	if fo.currentState != fo {
		panic(fmt.Sprintf("%v subtractClock called with frame in state %v", fo.v, fo.currentState))
	}
	if changed := fo.mask.MergeInMax(clock); changed && fo.reads.Len() == 0 && fo.writeVoteClock == nil {
		fo.readVoteClock = nil
		fo.calculateReadVoteClock()
	}
}

func (fo *frameOpen) isIdle() bool {
	return fo.parent == nil && !fo.rollScheduled && fo.isEmpty()
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

func (fc *frameClosed) frameStateMachineWitness() {}
func (fc *frameClosed) String() string            { return "frameClosed" }

func (fc *frameClosed) DescendentOnDisk() bool {
	if !fc.onDisk {
		server.Log(fc.frame, "DescendentOnDisk")
		fc.onDisk = true
		fc.MaybeCompleteTxns()
		return true
	}
	return false
}

func (fc *frameClosed) MaybeCompleteTxns() {
	if fc.currentState == fc && fc.onDisk && fc.parent == nil {
		server.Log(fc.frame, "MaybeCompleteTxns")
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
	fc.maybeScheduleRoll()
	fc.v.maybeMakeInactive()
}

// erase

type frameErase struct {
	*frame
}

func (fe *frameErase) init(f *frame) {
	fe.frame = f
}

func (fe *frameErase) start()                    {}
func (fe *frameErase) frameStateMachineWitness() {}
func (fe *frameErase) String() string            { return "frameErase" }

func (fe *frameErase) ReadGloballyComplete(action *localAction) {
	txn := action.Txn
	server.Log(fe.frame, "ReadGloballyComplete", txn)
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
	server.Log(fe.frame, "WriteGloballyComplete", txn)
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
		server.Log(fe.frame, "maybeErase")
		child := fe.child
		child.parent = nil
		child.MaybeCompleteTxns()
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
