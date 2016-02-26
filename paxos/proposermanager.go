package paxos

import (
	"encoding/binary"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	eng "goshawkdb.io/server/txnengine"
	"log"
)

func init() {
	db.DB.Proposers = &mdbs.DBISettings{Flags: mdb.CREATE}
}

const ( //                  txnId  rmId
	instanceIdPrefixLen = common.KeyLen + 4
)

type instanceIdPrefix [instanceIdPrefixLen]byte

type ProposerManager struct {
	RMId              common.RMId
	VarDispatcher     *eng.VarDispatcher
	Exe               *dispatcher.Executor
	ConnectionManager ConnectionManager
	Disk              *mdbs.MDBServer
	proposals         map[instanceIdPrefix]*proposal
	proposers         map[common.TxnId]*Proposer
}

func NewProposerManager(rmId common.RMId, exe *dispatcher.Executor, varDispatcher *eng.VarDispatcher, cm ConnectionManager, server *mdbs.MDBServer) *ProposerManager {
	pm := &ProposerManager{
		RMId:              rmId,
		proposals:         make(map[instanceIdPrefix]*proposal),
		proposers:         make(map[common.TxnId]*Proposer),
		VarDispatcher:     varDispatcher,
		Exe:               exe,
		ConnectionManager: cm,
		Disk:              server,
	}
	return pm
}

func (pm *ProposerManager) loadFromData(txnId *common.TxnId, data []byte) error {
	if _, found := pm.proposers[*txnId]; !found {
		proposer, err := ProposerFromData(pm, txnId, data)
		if err != nil {
			return err
		}
		pm.proposers[*txnId] = proposer
		proposer.Start()
	}
	return nil
}

func (pm *ProposerManager) TxnReceived(txnId *common.TxnId, txnCap *msgs.Txn) {
	// Due to failures, we can actually receive outcomes (2Bs) first,
	// before we get the txn to vote on it - due to failures, other
	// proposers will have created abort proposals, and consensus may
	// have already been reached. If this is the case, it is correct to
	// ignore this message.
	if _, found := pm.proposers[*txnId]; !found {
		server.Log(txnId, "Received")
		proposer := NewProposer(pm, txnId, txnCap, ProposerActiveVoter)
		pm.proposers[*txnId] = proposer
		proposer.Start()
	}
}

func (pm *ProposerManager) NewPaxosProposals(txnId *common.TxnId, txn *msgs.Txn, fInc int, ballots []*eng.Ballot, acceptors []common.RMId, rmId common.RMId, skipPhase1 bool) {
	instId := instanceIdPrefix([instanceIdPrefixLen]byte{})
	instIdSlice := instId[:]
	copy(instIdSlice, txnId[:])
	binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], uint32(rmId))
	if _, found := pm.proposals[instId]; !found {
		server.Log(txnId, "NewPaxos; acceptors:", acceptors, "; instance:", rmId)
		prop := NewProposal(pm, txnId, txn, fInc, ballots, rmId, acceptors, skipPhase1)
		pm.proposals[instId] = prop
		prop.Start()
	}
}

func (pm *ProposerManager) AddToPaxosProposals(txnId *common.TxnId, ballots []*eng.Ballot, rmId common.RMId) {
	server.Log(txnId, "Adding ballot to Paxos; instance:", rmId)
	instId := instanceIdPrefix([instanceIdPrefixLen]byte{})
	instIdSlice := instId[:]
	copy(instIdSlice, txnId[:])
	binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], uint32(rmId))
	if prop, found := pm.proposals[instId]; found {
		prop.AddBallots(ballots)
	} else {
		log.Printf("Error: Adding ballot to Paxos, unable to find proposals. %v %v\n", txnId, rmId)
	}
}

// from network
func (pm *ProposerManager) OneBTxnVotesReceived(sender common.RMId, txnId *common.TxnId, oneBTxnVotes *msgs.OneBTxnVotes) {
	server.Log(txnId, "1B received from", sender, "; instance:", common.RMId(oneBTxnVotes.RmId()))
	instId := instanceIdPrefix([instanceIdPrefixLen]byte{})
	instIdSlice := instId[:]
	copy(instIdSlice, txnId[:])
	binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], oneBTxnVotes.RmId())
	if prop, found := pm.proposals[instId]; found {
		prop.OneBTxnVotesReceived(sender, oneBTxnVotes)
	}
	// If not found, it should be safe to ignore - it's just a delayed
	// 1B that we clearly don't need to complete the paxos instances
	// anyway.
}

// from network
func (pm *ProposerManager) TwoBTxnVotesReceived(sender common.RMId, txnId *common.TxnId, twoBTxnVotes *msgs.TwoBTxnVotes) {
	instId := instanceIdPrefix([instanceIdPrefixLen]byte{})
	instIdSlice := instId[:]
	copy(instIdSlice, txnId[:])

	switch twoBTxnVotes.Which() {
	case msgs.TWOBTXNVOTES_FAILURES:
		failures := twoBTxnVotes.Failures()
		server.Log(txnId, "2B received from", sender, "; instance:", common.RMId(failures.RmId()))
		binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], failures.RmId())
		if prop, found := pm.proposals[instId]; found {
			prop.TwoBFailuresReceived(sender, &failures)
		}

	case msgs.TWOBTXNVOTES_OUTCOME:
		binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], uint32(pm.RMId))
		outcome := twoBTxnVotes.Outcome()

		if proposer, found := pm.proposers[*txnId]; found {
			server.Log(txnId, "2B outcome received from", sender, "(known active)")
			proposer.BallotOutcomeReceived(sender, &outcome)
			return
		}

		txnCap := outcome.Txn()

		alloc := AllocForRMId(&txnCap, pm.RMId)

		if alloc.Active() != 0 {
			// We have no record of this, but we were active - we must
			// have died and recovered (or we may have never received
			// this yet - see above - if we were down, other proposers
			// may have started abort proposers). Thus this could be
			// abort (abort proposers out there) or commit (we previously
			// voted, and that vote got recorded, but we have since died
			// and restarted).
			server.Log(txnId, "2B outcome received from", sender, "(unknown active)")

			// There's a possibility the acceptor that sent us this 2B is
			// one of only a few acceptors that got enough 2As to
			// determine the outcome. We must set up new paxos instances
			// to ensure the result is propogated to all. All we need to
			// do is to start a proposal for our own vars. The proposal
			// itself will detect any further absences and take care of
			// them.
			acceptors := GetAcceptorsFromTxn(&txnCap)
			server.Log(txnId, "Starting abort proposals with acceptors", acceptors)
			fInc := int(txnCap.FInc())
			ballots := MakeAbortBallots(&txnCap, alloc)
			pm.NewPaxosProposals(txnId, &txnCap, fInc, ballots, acceptors, pm.RMId, false)

			proposer := NewProposer(pm, txnId, &txnCap, ProposerActiveLearner)
			pm.proposers[*txnId] = proposer
			proposer.Start()
			proposer.BallotOutcomeReceived(sender, &outcome)
		} else {
			// Not active, so we are a learner
			if outcome.Which() == msgs.OUTCOME_COMMIT {
				server.Log(txnId, "2B outcome received from", sender, "(unknown learner)")
				// we must be a learner.
				proposer := NewProposer(pm, txnId, &txnCap, ProposerPassiveLearner)
				pm.proposers[*txnId] = proposer
				proposer.Start()
				proposer.BallotOutcomeReceived(sender, &outcome)

			} else {
				// Whilst it's an abort now, at some point in the past it
				// was a commit and as such we received that
				// outcome. However, we must have since died and so lost
				// that state/proposer. We should now immediately reply
				// with a TLC.
				server.Log(txnId, "Sending immediate TLC for unknown abort learner")
				// We have no state here, and if we receive further 2Bs
				// from the repeating sender at the acceptor then we will
				// send further TLCs. So the use of OSS here is correct.
				NewOneShotSender(MakeTxnLocallyCompleteMsg(txnId), pm.ConnectionManager, sender)
			}
		}

	default:
		panic(fmt.Sprintf("Unexpected 2BVotes type: %v", twoBTxnVotes.Which()))
	}
}

// from network
func (pm *ProposerManager) TxnGloballyCompleteReceived(sender common.RMId, txnId *common.TxnId) {
	if proposer, found := pm.proposers[*txnId]; found {
		server.Log(txnId, "TGC received from", sender, "(proposer found)")
		proposer.TxnGloballyCompleteReceived(sender)
	} else {
		server.Log(txnId, "TGC received from", sender, "(ignored)")
	}
}

// from network
func (pm *ProposerManager) TxnSubmissionAbortReceived(sender common.RMId, txnId *common.TxnId) {
	if proposer, found := pm.proposers[*txnId]; found {
		server.Log(txnId, "TSA received from", sender, "(proposer found)")
		proposer.Abort()
	} else {
		server.Log(txnId, "TSA received from", sender, "(ignored)")
	}
}

// from proposer
func (pm *ProposerManager) TxnFinished(txnId *common.TxnId) {
	delete(pm.proposers, *txnId)
}

// We have an outcome by this point, so we should stop sending proposals.
func (pm *ProposerManager) FinishProposers(txnId *common.TxnId) {
	instId := instanceIdPrefix([instanceIdPrefixLen]byte{})
	instIdSlice := instId[:]
	copy(instIdSlice, txnId[:])
	binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], uint32(pm.RMId))
	if prop, found := pm.proposals[instId]; found {
		delete(pm.proposals, instId)
		abortInstances := prop.FinishProposing()
		for _, rmId := range abortInstances {
			binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], uint32(rmId))
			if prop, found := pm.proposals[instId]; found {
				delete(pm.proposals, instId)
				prop.FinishProposing()
			}
		}
	}
}

func (pm *ProposerManager) Status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("Live proposers: %v", len(pm.proposers)))
	for _, prop := range pm.proposers {
		prop.Status(sc.Fork())
	}
	sc.Emit(fmt.Sprintf("Live proposals: %v", len(pm.proposals)))
	for _, prop := range pm.proposals {
		prop.Status(sc.Fork())
	}
	sc.Join()
}

func GetAcceptorsFromTxn(txnCap *msgs.Txn) common.RMIds {
	fInc := int(txnCap.FInc())
	twoFInc := fInc + fInc - 1
	acceptors := make([]common.RMId, twoFInc)
	allocations := txnCap.Allocations()
	idx := 0
	for l := allocations.Len(); idx < l && idx < twoFInc; idx++ {
		alloc := allocations.At(idx)
		acceptors[idx] = common.RMId(alloc.RmId())
	}
	// Danger! For the initial topology txns, there are _not_ twoFInc acceptors
	return acceptors[:idx]
}

func MakeTxnLocallyCompleteMsg(txnId *common.TxnId) []byte {
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	tlc := msgs.NewTxnLocallyComplete(seg)
	msg.SetTxnLocallyComplete(tlc)
	tlc.SetTxnId(txnId[:])
	return server.SegToBytes(seg)
}

func MakeTxnSubmissionCompleteMsg(txnId *common.TxnId) []byte {
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	tsc := msgs.NewTxnSubmissionComplete(seg)
	msg.SetSubmissionComplete(tsc)
	tsc.SetTxnId(txnId[:])
	return server.SegToBytes(seg)
}

func MakeTxnSubmissionAbortMsg(txnId *common.TxnId) []byte {
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	tsa := msgs.NewTxnSubmissionAbort(seg)
	msg.SetSubmissionAbort(tsa)
	tsa.SetTxnId(txnId[:])
	return server.SegToBytes(seg)
}

func AllocForRMId(txn *msgs.Txn, rmId common.RMId) *msgs.Allocation {
	allocs := txn.Allocations()
	for idx, l := 0, allocs.Len(); idx < l; idx++ {
		alloc := allocs.At(idx)
		if common.RMId(alloc.RmId()) == rmId {
			return &alloc
		}
	}
	return nil
}

func MakeAbortBallots(txn *msgs.Txn, alloc *msgs.Allocation) []*eng.Ballot {
	actions := txn.Actions()
	actionIndices := alloc.ActionIndices()
	ballots := make([]*eng.Ballot, actionIndices.Len())
	for idx, l := 0, actionIndices.Len(); idx < l; idx++ {
		action := actions.At(int(actionIndices.At(idx)))
		vUUId := common.MakeVarUUId(action.VarId())
		ballots[idx] = eng.NewBallot(vUUId, eng.AbortDeadlock, nil)
	}
	return ballots
}
