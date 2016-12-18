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
// distinct acceptors which all have equal Clocks, we know we have a
// consensus on the result.
type OutcomeAccumulator struct {
	acceptors        common.RMIds
	acceptorOutcomes map[common.RMId]*acceptorIndexWithTxnOutcome
	winningOutcome   *txnOutcome
	allKnownOutcomes []*txnOutcome
	pendingTGC       int
	fInc             int
}

type acceptorIndexWithTxnOutcome struct {
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
	acceptorOutcomes := make(map[common.RMId]*acceptorIndexWithTxnOutcome, len(acceptors))
	ids := make([]acceptorIndexWithTxnOutcome, len(acceptors))
	for idx, rmId := range acceptors {
		ptr := &ids[idx]
		ptr.idx = idx
		acceptorOutcomes[rmId] = ptr
	}
	return &OutcomeAccumulator{
		acceptors:        acceptors,
		acceptorOutcomes: acceptorOutcomes,
		winningOutcome:   nil,
		allKnownOutcomes: make([]*txnOutcome, 0, 1),
		pendingTGC:       len(acceptors),
		fInc:             fInc,
	}
}

func (oa *OutcomeAccumulator) TopologyChange(topology *configuration.Topology) bool {
	// We can only gain more RMsRemoved when a new topology is
	// installed post barrier2 and migration. To get to barrier2, every
	// live transaction must have its outcome known. Therefore by this
	// point we should not have to deal with the removal of nodes
	// causing winningOutcome needing to go from nil to non-nil.

	// The above holds for user txns, but not for txns which are
	// actually involved in a topology change. For example, a node
	// which is being removed could start a topology txn, and then
	// observe that the topology has changed and it has been
	// removed. It then shuts down. This could result in a loss of
	// acceptors and proposers. It's the loss of acceptors that's the
	// biggest problem because we have no way to replace them.

	for rmId := range topology.RMsRemoved {
		if acceptorOutcome, found := oa.acceptorOutcomes[rmId]; found {
			delete(oa.acceptorOutcomes, rmId)
			server.Log("OutcomeAccumulator deleting acceptor", rmId)
			oa.acceptors[acceptorOutcome.idx] = common.RMIdEmpty
			if l := oa.acceptors.NonEmptyLen(); l < oa.fInc {
				oa.fInc = l
			}
			if !acceptorOutcome.tgcReceived {
				acceptorOutcome.tgcReceived = true
				oa.pendingTGC--
			}
			if tOut := acceptorOutcome.tOut; tOut != nil {
				acceptorOutcome.tOut = nil
				tOut.outcomeReceivedCount--
				if tOut.outcomeReceivedCount == 0 {
					oa.deleteFromOutcomes(tOut)
				} else {
					tOut.acceptors[acceptorOutcome.idx] = common.RMIdEmpty
				}
			}
		}
	}
	return oa.winningOutcome != nil && oa.winningOutcome.outcomeReceivedCount == len(oa.acceptorOutcomes)
}

func (oa *OutcomeAccumulator) BallotOutcomeReceived(acceptorId common.RMId, outcome *msgs.Outcome) (*msgs.Outcome, bool) {
	outcomeEq := (*outcomeEqualId)(outcome)
	acceptorOutcome, found := oa.acceptorOutcomes[acceptorId]
	if !found {
		// It must have been removed due to a topology change. See notes
		// in TopologyChange
		if oa.winningOutcome == nil {
			return nil, false
		} else {
			return (*msgs.Outcome)(oa.winningOutcome.outcome), oa.winningOutcome.outcomeReceivedCount == len(oa.acceptorOutcomes)
		}
	}

	if tOut := acceptorOutcome.tOut; tOut != nil {
		if tOut.outcome.Equal(outcomeEq) {
			// It's completely a duplicate msg. No change to our state so just return
			return nil, false
		} else {
			// The acceptor has changed its mind.
			tOut.outcomeReceivedCount--
			if tOut.outcomeReceivedCount == 0 {
				oa.deleteFromOutcomes(tOut)
			} else {
				tOut.acceptors[acceptorOutcome.idx] = common.RMIdEmpty
			}
			// Paxos guarantees that in this case, tOut != oa.winningOutcome
		}
	}

	tOut := oa.getOutcome(outcomeEq)
	// We've checked for duplicate msgs above, so we don't need to
	// worry about that here.
	tOut.outcomeReceivedCount++
	tOut.acceptors[acceptorOutcome.idx] = acceptorId
	acceptorOutcome.tOut = tOut

	allAgreed := tOut.outcomeReceivedCount == len(oa.acceptorOutcomes)
	if oa.winningOutcome == nil && tOut.outcomeReceivedCount == oa.fInc {
		oa.winningOutcome = tOut
		return (*msgs.Outcome)(oa.winningOutcome.outcome), allAgreed
	}
	return nil, allAgreed
}

func (oa *OutcomeAccumulator) TxnGloballyCompleteReceived(acceptorId common.RMId) bool {
	server.Log("TGC received from", acceptorId, "; pending:", oa.pendingTGC)
	acceptorOutcome, found := oa.acceptorOutcomes[acceptorId]
	if !found {
		// It must have been removed due to a topology change. See notes
		// in TopologyChange
		return oa.pendingTGC == 0
	}
	if !acceptorOutcome.tgcReceived {
		acceptorOutcome.tgcReceived = true
		oa.pendingTGC--
	}
	return oa.pendingTGC == 0
}

func (oa *OutcomeAccumulator) getOutcome(outcome *outcomeEqualId) *txnOutcome {
	var empty *txnOutcome
	for _, tOut := range oa.allKnownOutcomes {
		if tOut.outcome.Equal(outcome) {
			return tOut
		} else if empty == nil && tOut.outcome == nil {
			empty = tOut
		}
	}
	if empty == nil {
		empty = &txnOutcome{
			outcome:              outcome,
			acceptors:            make([]common.RMId, len(oa.acceptors)),
			outcomeReceivedCount: 0,
		}
		oa.allKnownOutcomes = append(oa.allKnownOutcomes, empty)
	} else {
		empty.outcome = outcome
		empty.acceptors = make([]common.RMId, len(oa.acceptors))
		empty.outcomeReceivedCount = 0
	}
	return empty
}

func (oa *OutcomeAccumulator) deleteFromOutcomes(tOut *txnOutcome) {
	tOut.outcome = nil
}

func (oa *OutcomeAccumulator) IsAllAborts() []common.RMId {
	count := len(oa.acceptorOutcomes)
	for _, tOut := range oa.allKnownOutcomes {
		if tOut.outcome != nil && tOut.outcomeReceivedCount == count && (*msgs.Outcome)(tOut.outcome).Which() == msgs.OUTCOME_ABORT {
			return tOut.acceptors.NonEmpty()
		}
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
