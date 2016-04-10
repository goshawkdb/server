package paxos

import (
	"bytes"
	"fmt"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
)

// OutcomeAccumulator groups together all the different outcomes we've
// received for a given txn. Once we have at least fInc outcomes from
// distinct acceptors which all have equal Ids, we know we have a
// consensus on the result.
type OutcomeAccumulator struct {
	acceptors              common.RMIds
	acceptorIdToTxnOutcome map[common.RMId]*acceptorIdxWithTxnOutcome
	winningOutcome         *txnOutcome
	allKnownOutcomes       []*txnOutcome
	fInc                   int
	pendingTGC             int
}

type acceptorIdxWithTxnOutcome struct {
	idx         int
	tgcReceived bool
	tOut        *txnOutcome
}

type txnOutcome struct {
	outcome              *outcomeEqualId
	acceptors            common.RMIds
	outcomeReceivedCount int
}

func NewOutcomeAccumulator(fInc int, acceptors common.RMIds) *OutcomeAccumulator {
	acceptorIdToTxnOutcome := make(map[common.RMId]*acceptorIdxWithTxnOutcome, len(acceptors))
	ids := make([]acceptorIdxWithTxnOutcome, len(acceptors))
	for idx, rmId := range acceptors {
		ptr := &ids[idx]
		ptr.idx = idx
		acceptorIdToTxnOutcome[rmId] = ptr
	}
	return &OutcomeAccumulator{
		acceptors:              acceptors,
		acceptorIdToTxnOutcome: acceptorIdToTxnOutcome,
		winningOutcome:         nil,
		allKnownOutcomes:       make([]*txnOutcome, 0, 1),
		fInc:                   fInc,
		pendingTGC:             len(acceptors),
	}
}

func (oa *OutcomeAccumulator) TopologyChange(topology *configuration.Topology) bool {
	for rmId := range topology.RMsRemoved() {
		if accIdxTOut, found := oa.acceptorIdToTxnOutcome[rmId]; found {
			delete(oa.acceptorIdToTxnOutcome, rmId)
			oa.acceptors[accIdxTOut.idx] = common.RMIdEmpty
			if l := len(oa.acceptors); l > oa.fInc {
				oa.fInc = l
			}

			accIdxTOut.tgcReceived = true
			if tOut := accIdxTOut.tOut; tOut != nil {
				accIdxTOut.tOut = nil
				tOut.outcomeReceivedCount--
				tOut.acceptors[accIdxTOut.idx] = common.RMIdEmpty
			}
		}
	}
	return oa.winningOutcome != nil && oa.winningOutcome.outcomeReceivedCount == oa.acceptors.NonEmptyLen()
}

func (oa *OutcomeAccumulator) BallotOutcomeReceived(acceptorId common.RMId, outcome *msgs.Outcome) (*msgs.Outcome, common.RMIds, bool) {
	if accIdxTOut, found := oa.acceptorIdToTxnOutcome[acceptorId]; found {
		outcomeEq := (*outcomeEqualId)(outcome)
		tOut := accIdxTOut.tOut

		if tOut != nil {
			if tOut.outcome.Equal(outcomeEq) {
				// It's completely a duplicate msg. No change to our state so just return
				return nil, nil, false
			} else {
				// The acceptor has changed its mind.
				tOut.outcomeReceivedCount--
				tOut.acceptors[accIdxTOut.idx] = common.RMIdEmpty
				// Paxos guarantees that in this case, tOut != oa.winningOutcome
			}
		}

		tOut = oa.getOutcome(outcomeEq)
		if tOut == nil {
			tOut = &txnOutcome{
				outcome:              outcomeEq,
				acceptors:            make([]common.RMId, len(oa.acceptors)),
				outcomeReceivedCount: 1,
			}
			tOut.acceptors[accIdxTOut.idx] = acceptorId
			oa.addToOutcomes(tOut)

		} else {
			// We've checked for duplicate msgs above, so we don't need to
			// worry about that here.
			tOut.outcomeReceivedCount++
			tOut.acceptors[accIdxTOut.idx] = acceptorId
		}
		accIdxTOut.tOut = tOut

		if oa.winningOutcome == nil && oa.fInc == tOut.outcomeReceivedCount {
			oa.winningOutcome = tOut
			return (*msgs.Outcome)(oa.winningOutcome.outcome),
				tOut.acceptors.NonEmpty(),
				tOut.outcomeReceivedCount == oa.acceptors.NonEmptyLen()
		} else if oa.winningOutcome == tOut {
			return nil, []common.RMId{acceptorId}, tOut.outcomeReceivedCount == oa.acceptors.NonEmptyLen()
		}
	}
	return nil, nil, false
}

func (oa *OutcomeAccumulator) TxnGloballyCompleteReceived(acceptorId common.RMId) bool {
	server.Log("TGC received from", acceptorId)
	if accIdxTOut, found := oa.acceptorIdToTxnOutcome[acceptorId]; found && !accIdxTOut.tgcReceived {
		accIdxTOut.tgcReceived = true
		oa.pendingTGC--
		return oa.pendingTGC == 0
	}
	return false
}

func (oa *OutcomeAccumulator) addToOutcomes(tOut *txnOutcome) {
	oa.allKnownOutcomes = append(oa.allKnownOutcomes, tOut)
}

func (oa *OutcomeAccumulator) getOutcome(outcome *outcomeEqualId) *txnOutcome {
	for _, tOut := range oa.allKnownOutcomes {
		if outcome.Equal(tOut.outcome) {
			return tOut
		}
	}
	return nil
}

func (oa *OutcomeAccumulator) IsAllAborts() []common.RMId {
	var nonEmpty *txnOutcome
	for _, tOut := range oa.allKnownOutcomes {
		if tOut.outcomeReceivedCount != 0 {
			if nonEmpty == nil && (*msgs.Outcome)(tOut.outcome).Which() == msgs.OUTCOME_ABORT {
				nonEmpty = tOut
			} else {
				return nil
			}
		}
	}

	if nonEmpty != nil {
		return nonEmpty.acceptors.NonEmpty()
	}
	return nil
}

func (oa *OutcomeAccumulator) Status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("- unique outcomes: %v", oa.allKnownOutcomes))
	sc.Emit(fmt.Sprintf("- outcome decided? %v", oa.winningOutcome != nil))
	sc.Emit(fmt.Sprintf("- pending TGC count: %v", oa.pendingTGC))
	sc.Join()
}

func (to *txnOutcome) String() string {
	return fmt.Sprintf("%v:%v", to.outcome, to.acceptors.NonEmpty())
}

type outcomeEqualId msgs.Outcome

func (id *outcomeEqualId) String() string {
	idList := (*msgs.Outcome)(id).Id()
	buf := "OutcomeId["
	for idx, l := 0, idList.Len(); idx < l; idx++ {
		outId := idList.At(idx)
		buf += fmt.Sprintf("%v{", common.MakeVarUUId(outId.VarId()))
		instList := outId.AcceptedInstances()
		for idy, m := 0, instList.Len(); idy < m; idy++ {
			inst := instList.At(idy)
			buf += fmt.Sprintf("(instance %v: vote %v)", common.RMId(inst.RmId()), inst.Vote())
		}
		buf += "} "
	}
	buf += "]"
	return buf
}

func (a *outcomeEqualId) Equal(b *outcomeEqualId) bool {
	switch {
	case a == b:
		return true
	case a == nil || b == nil:
		return false
	default:
		aIdList, bIdList := (*msgs.Outcome)(a).Id(), (*msgs.Outcome)(b).Id()
		if aIdList.Len() != bIdList.Len() {
			return false
		}
		for idx, l := 0, aIdList.Len(); idx < l; idx++ {
			aOutId, bOutId := aIdList.At(idx), bIdList.At(idx)
			if !bytes.Equal(aOutId.VarId(), bOutId.VarId()) {
				return false
			}
			aAccInstList, bAccInstList := aOutId.AcceptedInstances(), bOutId.AcceptedInstances()
			if aAccInstList.Len() != bAccInstList.Len() {
				return false
			}
			for idy, m := 0, aAccInstList.Len(); idy < m; idy++ {
				aAccInstId, bAccInstId := aAccInstList.At(idy), bAccInstList.At(idy)
				if !(aAccInstId.RmId() == bAccInstId.RmId() && aAccInstId.Vote() == bAccInstId.Vote()) {
					return false
				}
			}
		}
		return true
	}
}
