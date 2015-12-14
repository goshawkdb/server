package paxos

import (
	"bytes"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	eng "goshawkdb.io/server/txnengine"
	"sort"
)

type BallotAccumulator struct {
	Txn            *msgs.Txn
	txnId          *common.TxnId
	vUUIdToBallots map[common.VarUUId]*varBallot
	outcome        *outcomeEqualId
	incompleteVars int
	dirty          bool
}

// You get one BallotAccumulator per txn. Which means the remaining
// paxos instance namespace is {rmId,varId}. So for each var, we
// expect to see ballots from fInc distinct rms.

func NewBallotAccumulator(txnId *common.TxnId, txn *msgs.Txn) *BallotAccumulator {
	actions := txn.Actions()
	ba := &BallotAccumulator{
		Txn:            txn,
		txnId:          txnId,
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

	allocs := txn.Allocations()
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

func BallotAccumulatorFromData(txnId *common.TxnId, txn *msgs.Txn, outcome *outcomeEqualId, instances *msgs.InstancesForVar_List) *BallotAccumulator {
	ba := NewBallotAccumulator(txnId, txn)
	ba.outcome = outcome

	for idx, l := 0, instances.Len(); idx < l; idx++ {
		// All instances that went to disk must be complete. But in the
		// case of a retry, not all instances must be complete before
		// going to disk.
		ba.incompleteVars--
		instancesForVar := instances.At(idx)
		acceptedInstances := instancesForVar.Instances()
		vUUId := common.MakeVarUUId(instancesForVar.VarId())
		vBallot := ba.vUUIdToBallots[*vUUId]
		rmBals := rmBallots(make([]*rmBallot, acceptedInstances.Len()))
		vBallot.rmToBallot = rmBals
		for idy, m := 0, acceptedInstances.Len(); idy < m; idy++ {
			acceptedInstance := acceptedInstances.At(idy)
			ballot := acceptedInstance.Ballot()
			rmBal := &rmBallot{
				instanceRMId: common.RMId(acceptedInstance.RmId()),
				ballot:       eng.BallotFromCap(&ballot),
				roundNumber:  paxosNumber(acceptedInstance.RoundNumber()),
			}
			rmBals[idy] = rmBal
		}
		result := instancesForVar.Result()
		vBallot.result = eng.BallotFromCap(&result)
	}

	return ba
}

// For every vUUId involved in this txn, we should see fInc * ballots:
// one from each RM voting for each vUUId. rmId is the paxos
// instanceRMId.
func (ba *BallotAccumulator) BallotReceived(instanceRMId common.RMId, inst *instance, vUUId *common.VarUUId, txn *msgs.Txn) *outcomeEqualId {
	if isDeflated(ba.Txn) && !isDeflated(txn) {
		ba.Txn = txn
	}

	vBallot := ba.vUUIdToBallots[*vUUId]
	if vBallot.rmToBallot == nil {
		vBallot.rmToBallot = rmBallots(make([]*rmBallot, 0, vBallot.voters))
	}
	ballot := eng.BallotFromCap(inst.accepted)
	found := false
	for idx, rBal := range vBallot.rmToBallot {
		if found = rBal.instanceRMId == instanceRMId; found {
			vBallot.rmToBallot[idx].ballot = ballot
			break
		}
	}
	if !found {
		rmBal := &rmBallot{
			instanceRMId: instanceRMId,
			ballot:       ballot,
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

func (ba *BallotAccumulator) determineOutcome() *outcomeEqualId {
	// Even in the case of retries, we must wait until we have at least
	// F+1 results for one var, otherwise we run the risk of
	// timetravel: a slow learner could issue a badread based on not
	// being caught up. By waiting for at least F+1 ballots for a var
	// (they don't have to be the same ballot!), we avoid this as there
	// must be at least one voter who isn't in the past.
	if !(ba.dirty && (ba.incompleteVars == 0 || ba.Txn.Retry())) {
		return nil
	}
	ba.dirty = false

	combinedClock := eng.NewVectorClock()
	aborted, deadlock := false, false

	vUUIds := common.VarUUIds(make([]*common.VarUUId, 0, len(ba.vUUIdToBallots)))
	br := NewBadReads()
	server.Log(ba.txnId, "Calculating result")
	for _, vBallot := range ba.vUUIdToBallots {
		if len(vBallot.rmToBallot) < vBallot.voters {
			continue
		}
		vUUIds = append(vUUIds, vBallot.vUUId)
		if vBallot.result == nil {
			vBallot.CalculateResult(br, combinedClock)
		}
		aborted = aborted || vBallot.result.Aborted()
		deadlock = deadlock || vBallot.result.Vote == eng.AbortDeadlock
	}

	vUUIds.Sort()

	seg := capn.NewBuffer(nil)
	outcome := msgs.NewOutcome(seg)
	outcomeIdList := msgs.NewOutcomeIdList(seg, len(vUUIds))
	outcome.SetId(outcomeIdList)
	for idx, vUUId := range vUUIds {
		outcomeId := outcomeIdList.At(idx)
		outcomeId.SetVarId(vUUId[:])
		vBallot := ba.vUUIdToBallots[*vUUId]
		instanceIdList := msgs.NewAcceptedInstanceIdList(seg, len(vBallot.rmToBallot))
		outcomeId.SetAcceptedInstances(instanceIdList)
		for idy, rmBal := range vBallot.rmToBallot {
			instanceId := instanceIdList.At(idy)
			instanceId.SetRmId(uint32(rmBal.instanceRMId))
			instanceId.SetVote(rmBal.ballot.Vote.ToVoteEnum())
		}
	}

	if aborted {
		deflatedTxn := deflateTxn(ba.Txn, seg)
		outcome.SetTxn(*deflatedTxn)
		outcome.SetAbort()
		abort := outcome.Abort()
		if deadlock {
			abort.SetResubmit()
		} else {
			abort.SetRerun(br.AddToSeg(seg))
		}

	} else {
		outcome.SetTxn(*ba.Txn)
		outcome.SetCommit(combinedClock.AddToSeg(seg))
	}

	ba.outcome = (*outcomeEqualId)(&outcome)
	return ba.outcome
}

func (ba *BallotAccumulator) AddInstancesToSeg(seg *capn.Segment) msgs.InstancesForVar_List {
	instances := msgs.NewInstancesForVarList(seg, len(ba.vUUIdToBallots)-ba.incompleteVars)
	idx := 0
	for vUUId, vBallot := range ba.vUUIdToBallots {
		if len(vBallot.rmToBallot) < vBallot.voters {
			continue
		}
		vUUIdCopy := vUUId
		instancesForVar := instances.At(idx)
		idx++
		instancesForVar.SetVarId(vUUIdCopy[:])
		instancesForVar.SetResult(vBallot.result.AddToSeg(seg))
		acceptedInstances := msgs.NewAcceptedInstanceList(seg, len(vBallot.rmToBallot))
		instancesForVar.SetInstances(acceptedInstances)
		for idy, rmBal := range vBallot.rmToBallot {
			acceptedInstance := acceptedInstances.At(idy)
			acceptedInstance.SetRmId(uint32(rmBal.instanceRMId))
			acceptedInstance.SetRoundNumber(uint64(rmBal.roundNumber))
			acceptedInstance.SetBallot(*rmBal.ballot.BallotCap)
		}
	}
	return instances
}

func (ba *BallotAccumulator) Status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("Ballot Accumulator for %v", ba.txnId))
	sc.Emit(fmt.Sprintf("- incomplete var count: %v", ba.incompleteVars))
	sc.Emit(fmt.Sprintf("- retry? %v", ba.Txn.Retry()))
	sc.Join()
}

func (vb *varBallot) CalculateResult(br badReads, clock *eng.VectorClock) {
	vb.result = eng.NewBallot(vb.vUUId, eng.Commit, eng.NewVectorClock())
	for _, rmBal := range vb.rmToBallot {
		vb.combineVote(rmBal, br)
	}
	if !vb.result.Aborted() {
		clock.MergeInMax(vb.result.Clock)
	}
}

func (vb *varBallot) combineVote(rmBal *rmBallot, br badReads) {
	cur := vb.result
	new := rmBal.ballot

	if new.Vote == eng.AbortBadRead {
		br.combine(rmBal)
	}

	switch {
	case cur.Vote == eng.Commit && new.Vote == eng.Commit:
		cur.Clock.MergeInMax(new.Clock)

	case cur.Vote == eng.AbortDeadlock && len(cur.Clock.Clock) == 0:
		// Do nothing - ignore the new ballot
	case new.Vote == eng.AbortDeadlock && len(new.Clock.Clock) == 0:
		// This has been created by abort proposer. This trumps everything.
		cur.Vote = eng.AbortDeadlock
		cur.Clock = new.Clock

	case cur.Vote == eng.Commit:
		// new.Vote != eng.Commit otherwise we'd have hit first case.
		cur.Vote = new.Vote
		cur.Clock = new.Clock.Clone()

	case new.Vote == eng.Commit:
		// But we know cur.Vote != eng.Commit. Do nothing.

	case new.Vote == eng.AbortDeadlock && cur.Vote == eng.AbortDeadlock:
		cur.Clock.MergeInMax(new.Clock)

	case new.Vote == eng.AbortDeadlock && cur.Vote == eng.AbortBadRead &&
		new.Clock.Clock[*vb.vUUId] < cur.Clock.Clock[*vb.vUUId]:
		// The new Deadlock is strictly in the past of the current
		// BadRead, so we stay on the badread.
		cur.Clock.MergeInMax(new.Clock)

	case new.Vote == eng.AbortDeadlock && cur.Vote == eng.AbortBadRead:
		// The new Deadlock is equal or greater than (by clock local
		// elem) than the current Badread. We should switch to the
		// Deadlock
		cur.Vote = eng.AbortDeadlock
		cur.Clock.MergeInMax(new.Clock)

	case cur.Vote == eng.AbortBadRead: // && new.Vote == eng.AbortBadRead
		cur.Clock.MergeInMax(new.Clock)

	case new.Clock.Clock[*vb.vUUId] > cur.Clock.Clock[*vb.vUUId]:
		// && cur.Vote == AbortDeadlock && new.Vote == AbortBadRead. The
		// new BadRead is strictly in the future of the cur Deadlock, so
		// we should switch to the BadRead.
		cur.Vote = eng.AbortBadRead
		cur.Clock.MergeInMax(new.Clock)

	default:
		// cur.Vote == AbortDeadlock && new.Vote == AbortBadRead.
		cur.Clock.MergeInMax(new.Clock)
	}
}

func deflateTxn(txn *msgs.Txn, seg *capn.Segment) *msgs.Txn {
	if isDeflated(txn) {
		return txn
	}
	deflatedTxn := msgs.NewTxn(seg)
	deflatedTxn.SetId(txn.Id())
	deflatedTxn.SetRetry(txn.Retry())
	deflatedTxn.SetSubmitter(txn.Submitter())
	deflatedTxn.SetSubmitterBootCount(txn.SubmitterBootCount())
	deflatedTxn.SetFInc(txn.FInc())
	deflatedTxn.SetTopologyVersion(txn.TopologyVersion())

	deflatedTxn.SetAllocations(txn.Allocations())

	actionsList := txn.Actions()
	deflatedActionsList := msgs.NewActionList(seg, actionsList.Len())
	deflatedTxn.SetActions(deflatedActionsList)
	for idx, l := 0, actionsList.Len(); idx < l; idx++ {
		deflatedAction := deflatedActionsList.At(idx)
		deflatedAction.SetVarId(actionsList.At(idx).VarId())
		deflatedAction.SetMissing()
	}

	return &deflatedTxn
}

func isDeflated(txn *msgs.Txn) bool {
	actions := txn.Actions()
	return actions.Len() != 0 && actions.At(0).Which() == msgs.ACTION_MISSING
}

type badReads map[common.VarUUId]*badReadAction

func NewBadReads() badReads {
	return make(map[common.VarUUId]*badReadAction)
}

func (br badReads) combine(rmBal *rmBallot) {
	clock := rmBal.ballot.Clock
	badRead := rmBal.ballot.VoteCap.AbortBadRead()
	txnId := common.MakeTxnId(badRead.TxnId())
	actions := badRead.TxnActions()

	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		vUUId := common.MakeVarUUId(action.VarId())

		if bra, found := br[*vUUId]; found {
			bra.combine(&action, rmBal, txnId, clock.Clock[*vUUId])
		} else if action.Which() == msgs.ACTION_READ {
			br[*vUUId] = &badReadAction{
				rmBallot:  rmBal,
				vUUId:     vUUId,
				txnId:     common.MakeTxnId(action.Read().Version()),
				clockElem: clock.Clock[*vUUId] - 1,
				action:    &action,
			}
		} else {
			br[*vUUId] = &badReadAction{
				rmBallot:  rmBal,
				vUUId:     vUUId,
				txnId:     txnId,
				clockElem: clock.Clock[*vUUId],
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
	newActionType := action.Which()
	braActionType := bra.action.Which()

	switch {
	case braActionType != msgs.ACTION_READ && newActionType != msgs.ACTION_READ:
		// They're both writes in some way. Just order the txns
		if clockElem > bra.clockElem || (clockElem == bra.clockElem && bra.txnId.LessThan(txnId)) {
			bra.set(action, rmBal, txnId, clockElem)
		}

	case braActionType == msgs.ACTION_READ && newActionType == msgs.ACTION_READ:
		braRead := bra.action.Read()
		newRead := action.Read()
		clockElem--
		// If they read the same version, we really don't care.
		if !bytes.Equal(braRead.Version(), newRead.Version()) {
			// They read different versions, but which version was the latter?
			if clockElem > bra.clockElem {
				bra.set(action, rmBal, common.MakeTxnId(newRead.Version()), clockElem)
			}
		}

	case braActionType == msgs.ACTION_READ:
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
		newRead := action.Read()
		clockElem--
		// If the read is a read of the existing write, better to keep the write
		if !bytes.Equal(bra.txnId[:], newRead.Version()) {
			if clockElem > bra.clockElem {
				// The read must be of some value which was written after our existing write.
				bra.set(action, rmBal, common.MakeTxnId(newRead.Version()), clockElem)
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
		actionList := msgs.NewActionList(seg, len(*badReadActions))
		update.SetActions(actionList)
		clock := eng.NewVectorClock()
		for idy, bra := range *badReadActions {
			action := bra.action
			switch action.Which() {
			case msgs.ACTION_READ:
				newAction := actionList.At(idy)
				newAction.SetVarId(action.VarId())
				newAction.SetMissing()
			case msgs.ACTION_WRITE:
				actionList.Set(idy, *action)
			case msgs.ACTION_READWRITE:
				readWrite := action.Readwrite()
				newAction := actionList.At(idy)
				newAction.SetVarId(action.VarId())
				newAction.SetWrite()
				newWrite := newAction.Write()
				newWrite.SetValue(readWrite.Value())
				newWrite.SetReferences(readWrite.References())
			case msgs.ACTION_CREATE:
				create := action.Create()
				newAction := actionList.At(idy)
				newAction.SetVarId(action.VarId())
				newAction.SetWrite()
				newWrite := newAction.Write()
				newWrite.SetValue(create.Value())
				newWrite.SetReferences(create.References())
			case msgs.ACTION_ROLL:
				roll := action.Roll()
				newAction := actionList.At(idy)
				newAction.SetVarId(action.VarId())
				newAction.SetWrite()
				newWrite := newAction.Write()
				newWrite.SetValue(roll.Value())
				newWrite.SetReferences(roll.References())
			default:
				panic(fmt.Sprintf("Unexpected action type (%v) for badread of %v at %v",
					action.Which(), action.VarId(), txnId))
			}
			clock.SetVarIdMax(*bra.vUUId, bra.clockElem)
		}
		update.SetClock(clock.AddToSeg(seg))
	}

	return updates
}
