package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdb "github.com/msackman/gomdb"
	sl "github.com/msackman/skiplist"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	"sync"
	"sync/atomic"
)

type TxnLocalStateChange interface {
	TxnBallotsComplete(*Txn, ...*Ballot)
	TxnLocallyComplete(*Txn)
	TxnFinished(*Txn)
}

type Txn struct {
	Id           *common.TxnId
	Retry        bool
	writes       []*common.VarUUId
	localActions []localAction
	voter        bool
	TxnCap       *msgs.Txn
	txnRootBytes struct {
		sync.RWMutex
		bites []byte
	}
	exe         *dispatcher.Executor
	vd          *VarDispatcher
	stateChange TxnLocalStateChange
	txnDetermineLocalBallots
	txnAwaitLocalBallots
	txnReceiveOutcome
	txnAwaitLocallyComplete
	txnReceiveCompletion
	currentState txnStateMachineComponent
}

func (txnA *Txn) Compare(txnB *Txn) common.Cmp {
	switch {
	case txnA == txnB:
		return common.EQ
	case txnA == nil:
		return common.LT
	case txnB == nil:
		return common.GT
	default:
		return txnA.Id.Compare(txnB.Id)
	}
}

type localAction struct {
	*Txn
	vUUId           *common.VarUUId
	ballot          *Ballot
	frame           *frame
	readVsn         *common.TxnId
	writeTxnActions *msgs.Action_List
	writeAction     *msgs.Action
	createPositions *common.Positions
	roll            bool
	outcomeClock    *VectorClock
	writesClock     *VectorClock
}

func (action *localAction) IsRead() bool {
	return action.readVsn != nil
}

func (action *localAction) IsWrite() bool {
	return action.writeTxnActions != nil
}

func (action *localAction) IsRoll() bool {
	return action.roll
}

func (action *localAction) IsImmigrant() bool {
	return action.writesClock != nil
}

func (action *localAction) VoteDeadlock(clock *VectorClock) {
	if action.ballot == nil {
		action.ballot = NewBallot(action.vUUId, AbortDeadlock, clock)
		action.voteCast(action.ballot, true)
	}
}

func (action *localAction) VoteBadRead(clock *VectorClock, txnId *common.TxnId, actions *msgs.Action_List) {
	if action.ballot == nil {
		action.ballot = NewBallot(action.vUUId, AbortBadRead, clock)
		action.ballot.CreateBadReadCap(txnId, actions)
		action.voteCast(action.ballot, true)
	}
}

func (action *localAction) VoteCommit(clock *VectorClock) bool {
	if action.ballot == nil {
		action.ballot = NewBallot(action.vUUId, Commit, clock)
		return !action.voteCast(action.ballot, false)
	}
	return false
}

// sl.Comparable interface
func (a *localAction) Compare(bC sl.Comparable) sl.Cmp {
	if bC == nil {
		if a == nil {
			return sl.EQ
		} else {
			return sl.GT
		}
	} else {
		b := bC.(*localAction)
		switch {
		case a == b:
			return sl.EQ
		case a == nil:
			return sl.LT
		case b == nil:
			return sl.GT
		default:
			return sl.Cmp(a.Txn.Compare(b.Txn))
		}
	}
}

func (action localAction) String() string {
	isCreate := action.createPositions != nil
	isWrite := action.writeTxnActions != nil
	f := ""
	if action.frame != nil {
		f = "|f"
	}
	b := ""
	if action.ballot != nil {
		b = "|b"
	}
	i := ""
	if action.writesClock != nil {
		i = "|i"
	}
	return fmt.Sprintf("Action from %v for %v: create:%v|read:%v|write:%v|roll:%v%s%s%s", action.Id, action.vUUId, isCreate, action.readVsn, isWrite, action.roll, f, b, i)
}

func ImmigrationTxnFromCap(exe *dispatcher.Executor, vd *VarDispatcher, stateChange TxnLocalStateChange, ourRMId common.RMId, txnCap *msgs.Txn, varCaps *msgs.Var_List) {
	txn := TxnFromCap(exe, vd, stateChange, ourRMId, txnCap)
	txnActions := txnCap.Actions()
	txn.localActions = make([]localAction, varCaps.Len())
	actionsMap := make(map[common.VarUUId]*localAction)
	for idx, l := 0, varCaps.Len(); idx < l; idx++ {
		varCap := varCaps.At(idx)
		action := &txn.localActions[idx]
		action.Txn = txn
		action.vUUId = common.MakeVarUUId(varCap.Id())
		action.writeTxnActions = &txnActions
		positions := varCap.Positions()
		action.createPositions = (*common.Positions)(&positions)
		action.outcomeClock = VectorClockFromCap(varCap.WriteTxnClock())
		action.writesClock = VectorClockFromCap(varCap.WritesClock())
		actionsMap[*action.vUUId] = action
	}

	for idx, l := 0, txnActions.Len(); idx < l; idx++ {
		actionCap := txnActions.At(idx)
		vUUId := common.MakeVarUUId(actionCap.VarId())
		if action, found := actionsMap[*vUUId]; found {
			action.writeAction = &actionCap
		}
	}

	txn.Start(false)
	txn.nextState()
	for idx := range txn.localActions {
		action := &txn.localActions[idx]
		f := func(v *Var, err error) {
			if err != nil {
				panic(fmt.Sprintf("%v immigration error: %v", txn.Id, err))
			} else {
				v.ReceiveTxnOutcome(action)
			}
		}
		vd.ApplyToVar(f, true, action.vUUId)
	}
}

func TxnFromCap(exe *dispatcher.Executor, vd *VarDispatcher, stateChange TxnLocalStateChange, ourRMId common.RMId, txnCap *msgs.Txn) *Txn {
	txnId := common.MakeTxnId(txnCap.Id())
	actions := txnCap.Actions()
	txn := &Txn{
		Id:          txnId,
		Retry:       txnCap.Retry(),
		writes:      make([]*common.VarUUId, 0, actions.Len()),
		TxnCap:      txnCap,
		exe:         exe,
		vd:          vd,
		stateChange: stateChange,
	}

	allocations := txnCap.Allocations()
	for idx, l := 0, allocations.Len(); idx < l; idx++ {
		alloc := allocations.At(idx)
		rmId := common.RMId(alloc.RmId())
		if ourRMId == rmId {
			txn.populate(alloc.ActionIndices(), actions)
			break
		}
	}

	return txn
}

func (txn *Txn) populate(actionIndices capn.UInt16List, actions msgs.Action_List) {
	localActions := make([]localAction, actionIndices.Len())
	txn.localActions = localActions
	var action *localAction

	actionIndicesIdx := 0
	actionIndex := -1
	if actionIndicesIdx < actionIndices.Len() {
		actionIndex = int(actionIndices.At(actionIndicesIdx))
		action = &localActions[actionIndicesIdx]
	}

	for idx, l := 0, actions.Len(); idx < l; idx++ {
		actionCap := actions.At(idx)

		if idx == actionIndex {
			action.Txn = txn
			action.vUUId = common.MakeVarUUId(actionCap.VarId())
		}

		switch actionCap.Which() {
		case msgs.ACTION_READ:
			if idx == actionIndex {
				readCap := actionCap.Read()
				readVsn := common.MakeTxnId(readCap.Version())
				action.readVsn = readVsn
			}

		case msgs.ACTION_WRITE:
			if idx == actionIndex {
				action.writeTxnActions = &actions
				action.writeAction = &actionCap
				txn.writes = append(txn.writes, action.vUUId)
			} else {
				txn.writes = append(txn.writes, common.MakeVarUUId(actionCap.VarId()))
			}

		case msgs.ACTION_READWRITE:
			if idx == actionIndex {
				readWriteCap := actionCap.Readwrite()
				readVsn := common.MakeTxnId(readWriteCap.Version())
				action.readVsn = readVsn
				action.writeTxnActions = &actions
				action.writeAction = &actionCap
				txn.writes = append(txn.writes, action.vUUId)
			} else {
				txn.writes = append(txn.writes, common.MakeVarUUId(actionCap.VarId()))
			}

		case msgs.ACTION_CREATE:
			if idx == actionIndex {
				createCap := actionCap.Create()
				positions := common.Positions(createCap.Positions())
				action.writeTxnActions = &actions
				action.writeAction = &actionCap
				action.createPositions = &positions
				txn.writes = append(txn.writes, action.vUUId)
			} else {
				txn.writes = append(txn.writes, common.MakeVarUUId(actionCap.VarId()))
			}

		case msgs.ACTION_ROLL:
			if idx == actionIndex {
				rollCap := actionCap.Roll()
				readVsn := common.MakeTxnId(rollCap.Version())
				action.readVsn = readVsn
				action.writeTxnActions = &actions
				action.writeAction = &actionCap
				action.roll = true
				txn.writes = append(txn.writes, action.vUUId)
			} else {
				txn.writes = append(txn.writes, common.MakeVarUUId(actionCap.VarId()))
			}

		default:
			panic(fmt.Sprintf("Unexpected action type: %v", actionCap.Which()))
		}

		if idx == actionIndex {
			actionIndicesIdx++
			if actionIndicesIdx < actionIndices.Len() {
				actionIndex = int(actionIndices.At(actionIndicesIdx))
				action = &localActions[actionIndicesIdx]
			}
		}
	}
	if actionIndicesIdx != actionIndices.Len() {
		panic(fmt.Sprintf("Expected to find %v local actions, but only found %v", actionIndices.Len(), actionIndicesIdx))
	}
}

func (txn *Txn) TxnRootBytes() []byte {
	trb := &txn.txnRootBytes
	trb.RLock()
	bites := trb.bites
	trb.RUnlock()
	if bites == nil {
		trb.Lock()
		if trb.bites == nil {
			trb.bites = db.TxnToRootBytes(txn.TxnCap)
		}
		bites = trb.bites
		trb.Unlock()
	}
	return bites
}

func (txn *Txn) Start(voter bool) {
	txn.voter = voter
	if voter {
		txn.txnDetermineLocalBallots.init(txn)
		txn.txnAwaitLocalBallots.init(txn)
	}
	txn.txnReceiveOutcome.init(txn)
	txn.txnAwaitLocallyComplete.init(txn)
	txn.txnReceiveCompletion.init(txn)

	if voter {
		txn.currentState = &txn.txnDetermineLocalBallots
	} else {
		txn.currentState = &txn.txnReceiveOutcome
	}
	txn.currentState.start()
}

func (txn *Txn) nextState() {
	switch txn.currentState {
	case &txn.txnDetermineLocalBallots:
		txn.currentState = &txn.txnAwaitLocalBallots
	case &txn.txnAwaitLocalBallots:
		txn.currentState = &txn.txnReceiveOutcome
	case &txn.txnReceiveOutcome:
		txn.currentState = &txn.txnAwaitLocallyComplete
	case &txn.txnAwaitLocallyComplete:
		txn.currentState = &txn.txnReceiveCompletion
	case &txn.txnReceiveCompletion:
		txn.currentState = nil
		return
	default:
		panic(fmt.Sprintf("%v Next state called on txn with txn in terminal state: %v\n", txn.Id, txn.currentState))
		return
	}
	txn.currentState.start()
}

func (txn *Txn) String() string {
	return txn.Id.String()
}

func (txn *Txn) Status(sc *server.StatusConsumer) {
	sc.Emit(txn.Id.String())
	sc.Emit(fmt.Sprintf("- Local Actions: %v", txn.localActions))
	sc.Emit(fmt.Sprintf("- Current State: %v", txn.currentState))
	sc.Emit(fmt.Sprintf("- Retry? %v", txn.Retry))
	sc.Emit(fmt.Sprintf("- PreAborted? %v", txn.preAbortedBool))
	sc.Emit(fmt.Sprintf("- Aborted? %v", txn.aborted))
	sc.Emit(fmt.Sprintf("- Outcome Clock: %v", txn.outcomeClock))
	sc.Emit(fmt.Sprintf("- Active Frames Count: %v", atomic.LoadInt32(&txn.activeFramesCount)))
	sc.Emit(fmt.Sprintf("- Completed? %v", txn.completed))
	sc.Join()
}

// State machine

type txnStateMachineComponent interface {
	init(*Txn)
	start()
	txnStateMachineComponentWitness()
}

// Determine Local Ballots
type txnDetermineLocalBallots struct {
	*Txn
	pendingVote int32
}

func (tdb *txnDetermineLocalBallots) txnStateMachineComponentWitness() {}
func (tdb *txnDetermineLocalBallots) String() string                   { return "txnDetermineLocalBallots" }

func (tdb *txnDetermineLocalBallots) init(txn *Txn) {
	tdb.Txn = txn
	if !tdb.Retry {
		atomic.StoreInt32(&tdb.pendingVote, int32(len(tdb.localActions)))
	}
}

func (tdb *txnDetermineLocalBallots) start() {
	tdb.nextState() // advance state FIRST!
	for idx := 0; idx < len(tdb.localActions); idx++ {
		action := &tdb.localActions[idx]
		f := func(v *Var, err error) {
			if err != nil {
				panic(fmt.Sprintf("%v error (%v): %v", tdb.Id, tdb, err))
			} else {
				v.ReceiveTxn(action)
			}
		}
		tdb.vd.ApplyToVar(f, true, action.vUUId)
	}
}

// Await Local Ballots
type txnAwaitLocalBallots struct {
	*Txn
	preAborted     int32
	preAbortedBool bool
}

func (talb *txnAwaitLocalBallots) txnStateMachineComponentWitness() {}
func (talb *txnAwaitLocalBallots) String() string                   { return "txnAwaitLocalBallots" }

func (talb *txnAwaitLocalBallots) init(txn *Txn) {
	talb.Txn = txn
}

func (talb *txnAwaitLocalBallots) start() {}

func (talb *txnAwaitLocalBallots) voteCast(ballot *Ballot, abort bool) bool {
	if talb.Retry {
		talb.exe.Enqueue(func() { talb.retryTxnBallotComplete(ballot) })
		return true
	}
	if abort && atomic.CompareAndSwapInt32(&talb.preAborted, 0, 1) {
		talb.exe.Enqueue(talb.preAbort)
	}
	abort = abort || atomic.LoadInt32(&talb.preAborted) == 1
	if atomic.AddInt32(&talb.pendingVote, -1) == 0 {
		talb.exe.Enqueue(talb.allTxnBallotsComplete)
	}
	return abort
}

func (talb *txnAwaitLocalBallots) preAbort() {
	if talb.currentState == talb && !talb.preAbortedBool {
		talb.preAbortedBool = true
		for idx := 0; idx < len(talb.localActions); idx++ {
			action := &talb.localActions[idx]
			f := func(v *Var, err error) {
				if err == mdb.NotFound && action.ballot != nil && action.frame == nil {
					// no problem - we've already voted to abort
				} else if err == nil && action.ballot != nil && action.frame == nil {
					v.maybeMakeInactive()
				} else if err != nil {
					panic(fmt.Sprintf("%v error (%v): %v", talb.Id, talb, err))
				} else if action.ballot != nil && action.frame != nil {
					if action.frame.v != v {
						panic("var has gone idle in the meantime somehow")
					}
					switch {
					case action.IsRead() && action.IsWrite():
						action.frame.ReadWriteAborted(action, true)
					case action.IsRead():
						action.frame.ReadAborted(action)
					default:
						action.frame.WriteAborted(action, true)
					}
				}
			}
			talb.vd.ApplyToVar(f, false, action.vUUId)
		}
	} else {
		panic(fmt.Sprintf("%v error: preAbort with txn in wrong state (or preAbort called multiple times: %v): %v\n", talb.Id, talb.currentState, talb.preAbortedBool))
	}
}

func (talb *txnAwaitLocalBallots) allTxnBallotsComplete() {
	if talb.currentState == talb {
		talb.nextState() // advance state FIRST!
		ballots := make([]*Ballot, len(talb.localActions))
		for idx := 0; idx < len(talb.localActions); idx++ {
			action := &talb.localActions[idx]
			ballots[idx] = action.ballot
		}
		talb.stateChange.TxnBallotsComplete(talb.Txn, ballots...)
	} else {
		panic(fmt.Sprintf("%v error: Ballots completed with txn in wrong state: %v\n", talb.Id, talb.currentState))
	}
}

func (talb *txnAwaitLocalBallots) retryTxnBallotComplete(ballot *Ballot) {
	if talb.currentState == talb {
		talb.nextState()
	}
	// Up until we actually receive the outcome, we should pass on all
	// of these to the proposer.
	if talb.currentState == &talb.txnReceiveOutcome {
		talb.stateChange.TxnBallotsComplete(talb.Txn, ballot)
	}
}

// Receive Outcome
type txnReceiveOutcome struct {
	*Txn
	outcomeClock *VectorClock
	aborted      bool
}

func (tro *txnReceiveOutcome) txnStateMachineComponentWitness() {}
func (tro *txnReceiveOutcome) String() string                   { return "txnReceiveOutcome" }

func (tro *txnReceiveOutcome) init(txn *Txn) {
	tro.Txn = txn
}

func (tro *txnReceiveOutcome) start() {}

// Callback (from network/paxos)
func (tro *txnReceiveOutcome) BallotOutcomeReceived(outcome *msgs.Outcome) {
	if tro.outcomeClock != nil || tro.aborted {
		// We've already been here. Be silent if we receive extra outcomes.
		return
	}
	if tro.Retry && tro.currentState == &tro.txnAwaitLocalBallots {
		tro.nextState()
	}
	if tro.currentState != tro {
		// We've received the outcome too early! Be noisy!
		panic(fmt.Sprintf("%v error: Ballot outcome received with txn in wrong state: %v\n", tro.Id, tro.currentState))
		return
	}
	switch outcome.Which() {
	case msgs.OUTCOME_COMMIT:
		tro.outcomeClock = VectorClockFromCap(outcome.Commit())
	default:
		tro.aborted = true
	}
	tro.nextState() // advance state FIRST!
	if tro.preAbortedBool {
		if !tro.aborted {
			panic(fmt.Sprintf("%v We preAborted the txn, but the txn outcome is to commit!", tro.Id))
		}
		return
	}
	for idx := 0; idx < len(tro.localActions); idx++ {
		action := &tro.localActions[idx]
		action.outcomeClock = tro.outcomeClock
		f := func(v *Var, err error) {
			if err != nil {
				panic(fmt.Sprintf("%v error (%v, aborted? %v, preAborted? %v, frame == nil? %v): %v", tro.Id, tro, tro.aborted, tro.preAbortedBool, action.frame == nil, err))
			} else {
				v.ReceiveTxnOutcome(action)
			}
		}
		// Should only have to create missing vars if we're a learner (i.e. !voter).
		tro.vd.ApplyToVar(f, !tro.voter, action.vUUId)
	}
}

// Await Locally Complete
type txnAwaitLocallyComplete struct {
	*Txn
	activeFramesCount int32
}

func (talc *txnAwaitLocallyComplete) txnStateMachineComponentWitness() {}
func (talc *txnAwaitLocallyComplete) String() string                   { return "txnAwaitLocallyComplete" }

func (talc *txnAwaitLocallyComplete) init(txn *Txn) {
	talc.Txn = txn
	atomic.StoreInt32(&talc.activeFramesCount, int32(len(talc.localActions)))
}

func (talc *txnAwaitLocallyComplete) start() {
	if talc.aborted || atomic.LoadInt32(&talc.activeFramesCount) == 0 {
		talc.locallyComplete()
	}
}

// Callback (from var-dispatcher (frames) back into txn)
func (talc *txnAwaitLocallyComplete) LocallyComplete() {
	result := atomic.AddInt32(&talc.activeFramesCount, -1)
	server.Log(talc.Id, "LocallyComplete called, pending frame count:", result)
	if result == 0 {
		talc.exe.Enqueue(talc.locallyComplete)
	} else if result < 0 {
		panic(fmt.Sprintf("%v activeFramesCount went -1!", talc.Id))
	}
}

func (talc *txnAwaitLocallyComplete) locallyComplete() {
	if talc.currentState == talc {
		talc.nextState() // do state first!
		talc.stateChange.TxnLocallyComplete(talc.Txn)
	}
}

// Receive Completion
type txnReceiveCompletion struct {
	*Txn
	completed bool
}

func (trc *txnReceiveCompletion) txnStateMachineComponentWitness() {}
func (trc *txnReceiveCompletion) String() string                   { return "txnReceiveCompletion" }

func (trc *txnReceiveCompletion) init(txn *Txn) {
	trc.Txn = txn
}

func (trc *txnReceiveCompletion) start() {}

// Callback (from network/paxos)
func (trc *txnReceiveCompletion) CompletionReceived() {
	server.Log(trc.Id, "CompletionReceived; already completed?", trc.completed, "state:", trc.currentState, "aborted?", trc.aborted)
	if trc.completed {
		// Be silent in this case.
		return
	}
	if trc.currentState != trc {
		// We've been completed early! Be noisy!
		panic(fmt.Sprintf("%v error: Txn completion received with txn in wrong state: %v\n", trc.Id, trc.currentState))
		return
	}
	trc.completed = true
	trc.maybeFinish()
	if trc.aborted {
		return
	}
	for idx := 0; idx < len(trc.localActions); idx++ {
		action := &trc.localActions[idx]
		if action.frame == nil {
			// Could be the case if !aborted and we're a learner, but
			// when we learnt, we never assigned a frame.
			continue
		}
		f := func(v *Var, err error) {
			if err != nil {
				panic(fmt.Sprintf("%v error (%v, aborted? %v, frame == nil? %v): %v", trc.Id, trc, trc.aborted, action.frame == nil, err))
			} else {
				v.TxnGloballyComplete(action)
			}
		}
		trc.vd.ApplyToVar(f, false, action.vUUId)
	}
}

func (trc *txnReceiveCompletion) maybeFinish() {
	if trc.currentState == trc && trc.completed {
		trc.nextState()
		trc.stateChange.TxnFinished(trc.Txn)
	}
}
