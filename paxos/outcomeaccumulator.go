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
	acceptorIdToTxnOutcome map[common.RMId]*txnOutcome
	outcomes               []*txnOutcome
	decidingOutcome        *txnOutcome
	pendingTGC             map[common.RMId]server.EmptyStruct
	fInc                   int
	acceptorCount          int
}

func NewOutcomeAccumulator(fInc int, acceptors common.RMIds) *OutcomeAccumulator {
	pendingTGC := make(map[common.RMId]server.EmptyStruct, len(acceptors))
	for _, rmId := range acceptors {
		pendingTGC[rmId] = server.EmptyStructVal
	}
	return &OutcomeAccumulator{
		acceptorIdToTxnOutcome: make(map[common.RMId]*txnOutcome),
		outcomes:               []*txnOutcome{},
		pendingTGC:             pendingTGC,
		fInc:                   fInc,
		acceptorCount:          len(acceptors),
	}
}

func (oa *OutcomeAccumulator) TopologyChange(topology *configuration.Topology) bool {
	result := false
	for rmId := range topology.RMsRemoved() {
		if _, found := oa.pendingTGC[rmId]; found {
			delete(oa.pendingTGC, rmId)
			if outcome, found := oa.acceptorIdToTxnOutcome[rmId]; found {
				delete(oa.acceptorIdToTxnOutcome, rmId)
				outcome.outcomeReceivedCount--
			}
			oa.acceptorCount--
			if oa.acceptorCount > oa.fInc {
				oa.fInc = oa.acceptorCount
			}
			if oa.decidingOutcome != nil {
				result = result || oa.decidingOutcome.outcomeReceivedCount == oa.acceptorCount
			}
		}
	}
	return result
}

func (oa *OutcomeAccumulator) BallotOutcomeReceived(acceptorId common.RMId, outcome *msgs.Outcome) (*msgs.Outcome, bool) {
	outcomeEq := (*outcomeEqualId)(outcome)
	if tOut, found := oa.acceptorIdToTxnOutcome[acceptorId]; found {
		if tOut.outcome.Equal(outcomeEq) {
			// It's completely a duplicate msg. No change to our state so just return
			return nil, false
		} else {
			// The acceptor has changed its mind.
			tOut.outcomeReceivedCount--
			// Paxos guarantees that in this case, tOut != oa.decidingOutcome
		}
	}

	tOut := oa.getOutcome(outcomeEq)
	if tOut == nil {
		tOut = &txnOutcome{
			outcome:              outcomeEq,
			outcomeReceivedCount: 1,
		}
		oa.addToOutcomes(tOut)

	} else {
		// We've checked for duplicate msgs above, so we don't need to
		// worry about that here.
		tOut.outcomeReceivedCount++
	}
	oa.acceptorIdToTxnOutcome[acceptorId] = tOut

	allAgreed := tOut.outcomeReceivedCount == oa.acceptorCount
	if oa.decidingOutcome == nil && oa.fInc == tOut.outcomeReceivedCount {
		oa.decidingOutcome = tOut
		return (*msgs.Outcome)(oa.decidingOutcome.outcome), allAgreed
	}
	return nil, allAgreed
}

func (oa *OutcomeAccumulator) TxnGloballyCompleteReceived(acceptorId common.RMId) bool {
	server.Log("TGC received from", acceptorId, "; pending:", oa.pendingTGC)
	delete(oa.pendingTGC, acceptorId)
	return len(oa.pendingTGC) == 0
}

func (oa *OutcomeAccumulator) addToOutcomes(tOut *txnOutcome) {
	oa.outcomes = append(oa.outcomes, tOut)
}

func (oa *OutcomeAccumulator) getOutcome(outcome *outcomeEqualId) *txnOutcome {
	for _, tOut := range oa.outcomes {
		if tOut.outcome.Equal(outcome) {
			return tOut
		}
	}
	return nil
}

func (oa *OutcomeAccumulator) IsAllAborts() []common.RMId {
	count := len(oa.acceptorIdToTxnOutcome)
	for _, outcome := range oa.outcomes {
		if outcome.outcomeReceivedCount == count && (*msgs.Outcome)(outcome.outcome).Which() == msgs.OUTCOME_ABORT {
			acceptors := make([]common.RMId, 0, count)
			for rmId := range oa.acceptorIdToTxnOutcome {
				acceptors = append(acceptors, rmId)
			}
			return acceptors
		}
	}
	return nil
}

func (oa *OutcomeAccumulator) Status(sc *server.StatusConsumer) {
	outcomeToAcceptors := make(map[*txnOutcome][]common.RMId)
	acceptors := make([]common.RMId, 0, len(oa.acceptorIdToTxnOutcome))
	for rmId, outcome := range oa.acceptorIdToTxnOutcome {
		acceptors = append(acceptors, rmId)
		if list, found := outcomeToAcceptors[outcome]; found {
			outcomeToAcceptors[outcome] = append(list, rmId)
		} else {
			outcomeToAcceptors[outcome] = []common.RMId{rmId}
		}
	}
	sc.Emit(fmt.Sprintf("- known outcomes from acceptors: %v", acceptors))
	sc.Emit(fmt.Sprintf("- unique outcomes: %v", outcomeToAcceptors))
	sc.Emit(fmt.Sprintf("- outcome decided? %v", oa.decidingOutcome != nil))
	sc.Emit(fmt.Sprintf("- pending TGCs from: %v", oa.pendingTGC))
	sc.Join()
}

type txnOutcome struct {
	outcome              *outcomeEqualId
	outcomeReceivedCount int
}

func (to *txnOutcome) String() string {
	return fmt.Sprintf("%v:%v", to.outcome, to.outcomeReceivedCount)
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
