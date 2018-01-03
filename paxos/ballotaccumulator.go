package paxos

import (
	"bytes"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	eng "goshawkdb.io/server/txnengine"
	"goshawkdb.io/server/types"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/status"
	"goshawkdb.io/server/utils/txnreader"
	"goshawkdb.io/server/utils/vectorclock"
	"sort"
)

type BallotAccumulator struct {
	logger         log.Logger
	txn            *txnreader.TxnReader
	vUUIdToBallots map[common.VarUUId]*varBallot
	outcome        *outcomeEqualId
	subscribers    common.TxnIds
	incompleteVars int
	dirty          bool
}

// You get one BallotAccumulator per txn. Which means the remaining
// paxos instance namespace is {rmId,varId}. So for each var, we
// expect to see ballots from fInc distinct rms.

func NewBallotAccumulator(txn *txnreader.TxnReader, logger log.Logger) *BallotAccumulator {
	actions := txn.Actions(true).Actions()
	ba := &BallotAccumulator{
		logger:         logger,
		txn:            txn,
		vUUIdToBallots: make(map[common.VarUUId]*varBallot),
		outcome:        nil,
		incompleteVars: actions.Len(),
		dirty:          false,
	}

	vBallots := make([]varBallot, ba.incompleteVars)
	for idx := 0; idx < ba.incompleteVars; idx++ {
		action := actions.At(idx)
		vUUId := common.MakeVarUUId(action.VarId())
		vBallot := &vBallots[idx]
		vBallot.vUUId = vUUId
		ba.vUUIdToBallots[*vUUId] = vBallot
	}

	allocs := txn.Txn.Allocations()
	for idx, l := 0, allocs.Len(); idx < l; idx++ {
		alloc := allocs.At(idx)
		if alloc.Active() == 0 {
			break
		}
		indices := alloc.ActionIndices()
		for idy, m := 0, indices.Len(); idy < m; idy++ {
			vBallots[int(indices.At(idy))].voters++
		}
	}

	return ba
}

type varBallot struct {
	vUUId      *common.VarUUId
	result     *eng.Ballot
	rmToBallot rmBallots
	voters     int
}

func (vBallot *varBallot) String() string {
	return fmt.Sprintf("varBallot %v with %v ballots (%v required); result=%v",
		vBallot.vUUId, len(vBallot.rmToBallot), vBallot.voters, vBallot.result)
}

type rmBallots []*rmBallot

func (rmBals rmBallots) Len() int           { return len(rmBals) }
func (rmBals rmBallots) Less(i, j int) bool { return rmBals[i].instanceRMId < rmBals[j].instanceRMId }
func (rmBals rmBallots) Swap(i, j int)      { rmBals[i], rmBals[j] = rmBals[j], rmBals[i] }
func (rmBals rmBallots) Sort()              { sort.Sort(rmBals) }

type rmBallot struct {
	instanceRMId common.RMId
	ballot       *eng.Ballot
	roundNumber  paxosNumber
}

func BallotAccumulatorFromData(txn *txnreader.TxnReader, outcome *outcomeEqualId, subsCap [][]byte, instances *msgs.InstancesForVar_List, logger log.Logger) *BallotAccumulator {
	ba := NewBallotAccumulator(txn, logger)
	ba.outcome = outcome
	// All instances that went to disk must be complete.
	if ba.incompleteVars != instances.Len() {
		panic(fmt.Sprintf("%v: Expected to find %d instances, but found %d.", txn.Id, ba.incompleteVars, instances.Len()))
	}
	ba.incompleteVars = 0

	for idx, l := 0, instances.Len(); idx < l; idx++ {
		instancesForVar := instances.At(idx)
		vUUId := common.MakeVarUUId(instancesForVar.VarId())
		vBallot := ba.vUUIdToBallots[*vUUId]
		acceptedInstances := instancesForVar.Instances()
		rmBals := make(rmBallots, acceptedInstances.Len())
		vBallot.rmToBallot = rmBals
		for idy, m := 0, acceptedInstances.Len(); idy < m; idy++ {
			acceptedInstance := acceptedInstances.At(idy)
			rmBal := &rmBallot{
				instanceRMId: common.RMId(acceptedInstance.RmId()),
				ballot:       eng.BallotFromData(acceptedInstance.Ballot()),
				roundNumber:  paxosNumber(acceptedInstance.RoundNumber()),
			}
			rmBals[idy] = rmBal
		}
		vBallot.result = eng.BallotFromData(instancesForVar.Result())
	}

	subscribers := make(common.TxnIds, len(subsCap))
	for idx, bites := range subsCap {
		subscribers[idx] = common.MakeTxnId(bites)
	}
	ba.subscribers = subscribers

	return ba
}

// For every vUUId involved in this txn, we should see fInc * ballots:
// one from each RM voting for each vUUId.
func (ba *BallotAccumulator) BallotReceived(instanceRMId common.RMId, inst *instance, vUUId *common.VarUUId, txn *txnreader.TxnReader) (*outcomeEqualId, common.TxnIds) {
	ba.txn = ba.txn.Combine(txn)

	vBallot := ba.vUUIdToBallots[*vUUId]
	if vBallot.rmToBallot == nil {
		vBallot.rmToBallot = make(rmBallots, 0, vBallot.voters)
	}
	found := false
	for idx, rBal := range vBallot.rmToBallot {
		if found = rBal.instanceRMId == instanceRMId; found {
			vBallot.rmToBallot[idx].ballot = inst.accepted
			break
		}
	}
	if !found {
		rmBal := &rmBallot{
			instanceRMId: instanceRMId,
			ballot:       inst.accepted,
			roundNumber:  inst.acceptedNum,
		}
		vBallot.rmToBallot = append(vBallot.rmToBallot, rmBal)
		if len(vBallot.rmToBallot) == vBallot.voters {
			ba.incompleteVars--
		}
		if len(vBallot.rmToBallot) >= vBallot.voters {
			vBallot.rmToBallot.Sort()
		}
	}
	if len(vBallot.rmToBallot) >= vBallot.voters {
		vBallot.result = nil
		ba.dirty = true
	}
	return ba.determineOutcome()
}

func (ba *BallotAccumulator) determineOutcome() (*outcomeEqualId, common.TxnIds) {
	// We must wait until we have at least F+1 results for all vars,
	// otherwise we run the risk of timetravel: a slow learner could
	// issue a badread based on not being caught up. By waiting for at
	// least F+1 ballots for a var (they don't have to be the same
	// ballot!), we avoid this as there must be at least one voter who
	// isn't in the past.
	if !(ba.dirty && ba.incompleteVars == 0) {
		return nil, nil
	}
	ba.dirty = false

	combinedClock := vectorclock.NewVectorClock().AsMutable()
	aborted, deadlock := false, false

	vUUIds := make(common.VarUUIds, 0, len(ba.vUUIdToBallots))
	commitSubscribers := make(map[common.TxnId]types.EmptyStruct)
	br := NewBadReads()
	utils.DebugLog(ba.logger, "debug", "determineOutcome")
	for _, vBallot := range ba.vUUIdToBallots {
		vUUIds = append(vUUIds, vBallot.vUUId)
		if vBallot.result == nil {
			vBallot.CalculateResult(br, combinedClock, commitSubscribers)
		} else if !vBallot.result.Aborted() {
			combinedClock.MergeInMax(vBallot.result.Clock)
			for _, subId := range vBallot.result.Subscribers {
				commitSubscribers[*subId] = types.EmptyStructVal
			}
		}
		aborted = aborted || vBallot.result.Aborted()
		deadlock = deadlock || vBallot.result.Vote == eng.AbortDeadlock
	}

	vUUIds.Sort()

	seg := capn.NewBuffer(nil)
	outcome := msgs.NewOutcome(seg)
	outcomeIdList := msgs.NewOutcomeIdList(seg, len(vUUIds))
	for idx, vUUId := range vUUIds {
		outcomeId := outcomeIdList.At(idx)
		outcomeId.SetVarId(vUUId[:])
		vBallot := ba.vUUIdToBallots[*vUUId]
		instanceIdList := msgs.NewAcceptedInstanceIdList(seg, len(vBallot.rmToBallot))
		for idy, rmBal := range vBallot.rmToBallot {
			instanceId := instanceIdList.At(idy)
			instanceId.SetRmId(uint32(rmBal.instanceRMId))
			instanceId.SetVote(rmBal.ballot.Vote.ToVoteEnum())
		}
		outcomeId.SetAcceptedInstances(instanceIdList)
	}
	outcome.SetId(outcomeIdList)

	if aborted {
		outcome.SetTxn(ba.txn.AsDeflated().Data)
		outcome.SetAbort()
		abort := outcome.Abort()
		if deadlock {
			abort.SetResubmit()
		} else {
			abort.SetRerun(br.AddToSeg(seg))
		}
		ba.subscribers = common.TxnIds{}

	} else {
		outcome.SetTxn(ba.txn.Data)
		outcome.SetCommit(combinedClock.AsData())
		if len(ba.vUUIdToBallots) > combinedClock.Len() {
			panic(fmt.Sprintf("Ballot outcome clock too short! %v, %v, %v", ba.txn.Id, ba.vUUIdToBallots, combinedClock))
		}
		subscribers := make(common.TxnIds, 0, len(commitSubscribers))
		for subId := range commitSubscribers {
			subIdCopy := subId
			subscribers = append(subscribers, &subIdCopy)
		}
		ba.subscribers = subscribers
	}

	ba.outcome = (*outcomeEqualId)(&outcome)
	return ba.outcome, ba.subscribers
}

func (ba *BallotAccumulator) AddInstancesToSeg(seg *capn.Segment) msgs.InstancesForVar_List {
	instances := msgs.NewInstancesForVarList(seg, len(ba.vUUIdToBallots)-ba.incompleteVars)
	idx := 0
	for vUUId, vBallot := range ba.vUUIdToBallots {
		vUUIdCopy := vUUId
		instancesForVar := instances.At(idx)
		idx++
		instancesForVar.SetVarId(vUUIdCopy[:])
		instancesForVar.SetResult(vBallot.result.Data)
		acceptedInstances := msgs.NewAcceptedInstanceList(seg, len(vBallot.rmToBallot))
		instancesForVar.SetInstances(acceptedInstances)
		for idy, rmBal := range vBallot.rmToBallot {
			acceptedInstance := acceptedInstances.At(idy)
			acceptedInstance.SetRmId(uint32(rmBal.instanceRMId))
			acceptedInstance.SetRoundNumber(uint64(rmBal.roundNumber))
			acceptedInstance.SetBallot(rmBal.ballot.Data)
		}
	}
	return instances
}

func (ba *BallotAccumulator) Status(sc *status.StatusConsumer) {
	sc.Emit(fmt.Sprintf("Ballot Accumulator for %v", ba.txn.Id))
	sc.Emit(fmt.Sprintf("- incomplete var count: %v", ba.incompleteVars))
	sc.Join()
}

type varBallotReducer struct {
	vUUId *common.VarUUId
	*eng.BallotBuilder
	badReads
}

func (vb *varBallot) CalculateResult(br badReads, clock *vectorclock.VectorClockMutable, commitSubscribers map[common.TxnId]types.EmptyStruct) {
	reducer := &varBallotReducer{
		vUUId:         vb.vUUId,
		BallotBuilder: eng.NewBallotBuilder(vb.vUUId, eng.Commit, vectorclock.NewVectorClock().AsMutable(), nil),
		badReads:      br,
	}
	for _, rmBal := range vb.rmToBallot {
		reducer.combineVote(rmBal)
	}
	if !reducer.Aborted() {
		clock.MergeInMax(reducer.Clock)
		for _, subId := range reducer.Subscribers {
			commitSubscribers[*subId] = types.EmptyStructVal
		}
	}
	vb.result = reducer.ToBallot()
}

func (cur *varBallotReducer) combineVote(rmBal *rmBallot) {
	new := rmBal.ballot

	if new.Vote == eng.AbortBadRead {
		cur.badReads.combine(rmBal)
	}

	curClock := cur.Clock
	newClock := rmBal.ballot.Clock

	switch {
	case cur.Vote == eng.Commit && new.Vote == eng.Commit:
		curClock.MergeInMax(newClock)
		cur.Subscribers = append(cur.Subscribers, new.Subscribers...)

	case cur.Vote == eng.AbortDeadlock && curClock.Len() == 0:
		// Do nothing - ignore the new ballot
	case new.Vote == eng.AbortDeadlock && newClock.Len() == 0:
		// This has been created by abort proposer. This trumps everything.
		cur.Vote = eng.AbortDeadlock
		cur.VoteCap = new.VoteCap
		cur.Clock = newClock.AsMutable()
		cur.Subscribers = nil

	case cur.Vote == eng.Commit:
		// new.Vote != eng.Commit otherwise we'd have hit first case.
		cur.Vote = new.Vote
		cur.VoteCap = new.VoteCap
		cur.Clock = newClock.AsMutable()
		cur.Subscribers = nil

	case new.Vote == eng.Commit:
		// But we know cur.Vote != eng.Commit. Do nothing.

	case new.Vote == eng.AbortDeadlock && cur.Vote == eng.AbortDeadlock:
		curClock.MergeInMax(newClock)

	case new.Vote == eng.AbortDeadlock && cur.Vote == eng.AbortBadRead &&
		newClock.At(cur.vUUId) < curClock.At(cur.vUUId):
		// The new Deadlock is strictly in the past of the current
		// BadRead, so we stay on the badread.
		curClock.MergeInMax(newClock)

	case new.Vote == eng.AbortDeadlock && cur.Vote == eng.AbortBadRead:
		// The new Deadlock is equal or greater than (by clock local
		// elem) than the current Badread. We should switch to the
		// Deadlock
		cur.Vote = eng.AbortDeadlock
		cur.VoteCap = new.VoteCap
		curClock.MergeInMax(newClock)

	case cur.Vote == eng.AbortBadRead: // && new.Vote == eng.AbortBadRead
		curClock.MergeInMax(newClock)

	case newClock.At(cur.vUUId) > curClock.At(cur.vUUId):
		// && cur.Vote == AbortDeadlock && new.Vote == AbortBadRead. The
		// new BadRead is strictly in the future of the cur Deadlock, so
		// we should switch to the BadRead.
		cur.Vote = eng.AbortBadRead
		cur.VoteCap = new.VoteCap
		curClock.MergeInMax(newClock)

	default:
		// cur.Vote == AbortDeadlock && new.Vote == AbortBadRead.
		curClock.MergeInMax(newClock)
	}
}

type badReads map[common.VarUUId]*badReadAction

func NewBadReads() badReads {
	return make(map[common.VarUUId]*badReadAction)
}

func (br badReads) combine(rmBal *rmBallot) {
	badRead := rmBal.ballot.VoteCap.AbortBadRead()
	clock := rmBal.ballot.Clock
	txnId := common.MakeTxnId(badRead.TxnId())
	badReadData := badRead.TxnActions()
	actions := txnreader.TxnActionsFromData(badReadData, true).Actions()

	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		vUUId := common.MakeVarUUId(action.VarId())
		clockElem := clock.At(vUUId)

		if bra, found := br[*vUUId]; found {
			bra.combine(&action, rmBal, txnId, clockElem)
		} else if action.ActionType() == msgs.ACTIONTYPE_READONLY {
			br[*vUUId] = &badReadAction{
				rmBallot:  rmBal,
				vUUId:     vUUId,
				txnId:     common.MakeTxnId(action.Version()),
				clockElem: clockElem - 1,
				action:    &action,
			}
			if clockElem == 0 {
				panic(fmt.Sprintf("Just did 0 - 1 in int64 (%v, %v) (%v)", vUUId, clock, txnId))
			}
		} else {
			br[*vUUId] = &badReadAction{
				rmBallot:  rmBal,
				vUUId:     vUUId,
				txnId:     txnId,
				clockElem: clockElem,
				action:    &action,
			}
		}
	}
}

type badReadAction struct {
	*rmBallot
	vUUId     *common.VarUUId
	txnId     *common.TxnId
	clockElem uint64
	action    *msgs.Action
}

func (bra *badReadAction) set(action *msgs.Action, rmBal *rmBallot, txnId *common.TxnId, clockElem uint64) {
	bra.rmBallot = rmBal
	bra.txnId = txnId
	bra.clockElem = clockElem
	bra.action = action
}

func (bra *badReadAction) combine(action *msgs.Action, rmBal *rmBallot, txnId *common.TxnId, clockElem uint64) {
	newActionType := action.ActionType()
	braActionType := bra.action.ActionType()

	switch {
	case braActionType != msgs.ACTIONTYPE_READONLY && newActionType != msgs.ACTIONTYPE_READONLY:
		// They're both writes in some way. Just order the txns
		if clockElem > bra.clockElem || (clockElem == bra.clockElem && bra.txnId.Compare(txnId) == common.LT) {
			bra.set(action, rmBal, txnId, clockElem)
		}

	case braActionType == msgs.ACTIONTYPE_READONLY && newActionType == msgs.ACTIONTYPE_READONLY:
		clockElem--
		// If they read the same version, we really don't care.
		if !bytes.Equal(bra.action.Version(), action.Version()) {
			// They read different versions, but which version was the latter?
			if clockElem > bra.clockElem {
				bra.set(action, rmBal, common.MakeTxnId(action.Version()), clockElem)
			}
		}

	case braActionType == msgs.ACTIONTYPE_READONLY:
		if bytes.Equal(bra.txnId[:], txnId[:]) {
			// The write will obviously be in the past of the
			// existing read, but it's better to have the write
			// as we can update the client with the actual
			// value.
			bra.set(action, rmBal, txnId, clockElem)
		} else if clockElem > bra.clockElem {
			// The write is after than the read
			bra.set(action, rmBal, txnId, clockElem)
		}

	default: // Existing is not a read, but new is a read.
		clockElem--
		// If the read is a read of the existing write, better to keep the write
		if !bytes.Equal(bra.txnId[:], action.Version()) {
			if clockElem > bra.clockElem {
				// The read must be of some value which was written after our existing write.
				bra.set(action, rmBal, common.MakeTxnId(action.Version()), clockElem)
			}
		}
	}
}

func (br badReads) AddToSeg(seg *capn.Segment) msgs.Update_List {
	txnIdToBadReadActions := make(map[common.TxnId]*[]*badReadAction, len(br))
	for _, bra := range br {
		if bras, found := txnIdToBadReadActions[*bra.txnId]; found {
			*bras = append(*bras, bra)
		} else {
			list := []*badReadAction{bra}
			txnIdToBadReadActions[*bra.txnId] = &list
		}
	}

	updates := msgs.NewUpdateList(seg, len(txnIdToBadReadActions))
	idx := 0
	for txnId, badReadActions := range txnIdToBadReadActions {
		update := updates.At(idx)
		idx++
		update.SetTxnId(txnId[:])
		actionsListSeg := capn.NewBuffer(nil)
		actionsListWrapper := msgs.NewRootActionListWrapper(actionsListSeg)
		actionsList := msgs.NewActionList(actionsListSeg, len(*badReadActions))
		actionsListWrapper.SetActions(actionsList)
		clock := vectorclock.NewVectorClock().AsMutable()
		for idy, bra := range *badReadActions {
			action := bra.action
			switch action.ActionType() {
			case msgs.ACTIONTYPE_READONLY:
				newAction := actionsList.At(idy)
				newAction.SetVarId(action.VarId())
				newAction.SetUnmodified()
				newAction.SetActionType(msgs.ACTIONTYPE_MISSING)
			case msgs.ACTIONTYPE_WRITEONLY:
				actionsList.Set(idy, *action)
			case msgs.ACTIONTYPE_CREATE, msgs.ACTIONTYPE_READWRITE, msgs.ACTIONTYPE_ROLL:
				newAction := actionsList.At(idy)
				newAction.SetVarId(action.VarId())
				newAction.SetActionType(msgs.ACTIONTYPE_WRITEONLY)
				newAction.SetModified()
				newMod := newAction.Modified()
				mod := action.Modified()
				newMod.SetValue(mod.Value())
				newMod.SetReferences(mod.References())
			default:
				panic(fmt.Sprintf("Unexpected action type (%v) for badread of %v at %v",
					action.Which(), action.VarId(), txnId))
			}
			clock.SetVarIdMax(bra.vUUId, bra.clockElem)
		}
		update.SetActions(common.SegToBytes(actionsListSeg))
		update.SetClock(clock.AsData())
	}

	return updates
}
