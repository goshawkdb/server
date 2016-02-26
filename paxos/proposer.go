package paxos

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/db"
	eng "goshawkdb.io/server/txnengine"
	"log"
)

type ProposerMode uint8

const (
	ProposerActiveVoter    ProposerMode = iota
	ProposerActiveLearner  ProposerMode = iota
	ProposerPassiveLearner ProposerMode = iota
	proposerTLCSender      ProposerMode = iota
)

type Proposer struct {
	proposerManager *ProposerManager
	mode            ProposerMode
	txn             *eng.Txn
	txnId           *common.TxnId
	acceptors       common.RMIds
	fInc            int
	currentState    proposerStateMachineComponent
	proposerAwaitBallots
	proposerReceiveOutcomes
	proposerAwaitLocallyComplete
	proposerReceiveGloballyComplete
	proposerAwaitFinished
}

// NB, active just means that in the txn allocs we're active. But if
// we receive outcomes before the txn itself, we do not vote. So you
// can be active, but not a voter.

func NewProposer(pm *ProposerManager, txnId *common.TxnId, txnCap *msgs.Txn, mode ProposerMode) *Proposer {
	p := &Proposer{
		proposerManager: pm,
		mode:            mode,
		txnId:           txnId,
		acceptors:       GetAcceptorsFromTxn(txnCap),
		fInc:            int(txnCap.FInc()),
	}
	if mode == ProposerActiveVoter {
		p.txn = eng.TxnFromCap(pm.Exe, pm.VarDispatcher, p, pm.RMId, txnCap)
	}
	p.init()
	return p
}

func ProposerFromData(pm *ProposerManager, txnId *common.TxnId, data []byte) (*Proposer, error) {
	seg, _, err := capn.ReadFromMemoryZeroCopy(data)
	if err != nil {
		return nil, err
	}
	// If we were on disk, then that means we must be locally complete
	// and just need to send out TLCs.
	state := msgs.ReadRootProposerState(seg)
	acceptorsCap := state.Acceptors()
	acceptors := make([]common.RMId, acceptorsCap.Len())
	for idx := range acceptors {
		acceptors[idx] = common.RMId(acceptorsCap.At(idx))
	}
	// We were on disk. Thus we received outcomes from all
	// acceptors. So we don't need to worry about the outcome
	// accumulator's fInc, hence just use -1 here.
	p := &Proposer{
		proposerManager: pm,
		mode:            proposerTLCSender,
		txnId:           txnId,
		acceptors:       acceptors,
		fInc:            -1,
	}
	p.init()
	p.allAcceptorsAgreed = true
	return p, nil
}

func (p *Proposer) init() {
	p.proposerAwaitBallots.init(p)
	p.proposerReceiveOutcomes.init(p)
	p.proposerAwaitLocallyComplete.init(p)
	p.proposerReceiveGloballyComplete.init(p)
	p.proposerAwaitFinished.init(p)
}

func (p *Proposer) Start() {
	if p.currentState != nil {
		return
	}

	switch p.mode {
	case ProposerActiveVoter:
		p.currentState = &p.proposerAwaitBallots
	case ProposerActiveLearner:
		p.currentState = &p.proposerReceiveOutcomes
	case ProposerPassiveLearner:
		p.currentState = &p.proposerReceiveOutcomes
	case proposerTLCSender:
		p.currentState = &p.proposerReceiveGloballyComplete
	}

	p.currentState.start()
}

func (p *Proposer) Status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("Proposer for %v", p.txnId))
	sc.Emit(fmt.Sprintf("- Mode: %v", p.mode))
	sc.Emit(fmt.Sprintf("- Current state: %v", p.currentState))
	sc.Emit("- Outcome Accumulator")
	p.outcomeAccumulator.Status(sc.Fork())
	sc.Emit(fmt.Sprintf("- Locally Complete? %v", p.locallyCompleted))
	if p.txn != nil {
		sc.Emit("- Txn")
		p.txn.Status(sc.Fork())
	}
	sc.Join()
}

type proposerStateMachineComponent interface {
	init(*Proposer)
	start()
	proposerStateMachineComponentWitness()
}

func (p *Proposer) nextState() {
	switch p.currentState {
	case &p.proposerAwaitBallots:
		p.currentState = &p.proposerReceiveOutcomes
	case &p.proposerReceiveOutcomes:
		p.currentState = &p.proposerAwaitLocallyComplete
	case &p.proposerAwaitLocallyComplete:
		p.currentState = &p.proposerReceiveGloballyComplete
	case &p.proposerReceiveGloballyComplete:
		p.currentState = &p.proposerAwaitFinished
	case &p.proposerAwaitFinished:
		p.currentState = nil
		return
	}
	p.currentState.start()
}

// await ballots

type proposerAwaitBallots struct {
	*Proposer
	submitter          common.RMId
	submitterBootCount uint32
}

func (pab *proposerAwaitBallots) init(proposer *Proposer) {
	pab.Proposer = proposer
}

func (pab *proposerAwaitBallots) start() {
	pab.txn.Start(true)
	pab.submitter = common.RMId(pab.txn.TxnCap.Submitter())
	pab.submitterBootCount = pab.txn.TxnCap.SubmitterBootCount()
	if pab.txn.Retry {
		pab.proposerManager.ConnectionManager.AddSender(pab)
	}
}

func (pab *proposerAwaitBallots) proposerStateMachineComponentWitness() {}
func (pab *proposerAwaitBallots) String() string {
	return "proposerAwaitBallots"
}

func (pab *proposerAwaitBallots) TxnBallotsComplete(ballots ...*eng.Ballot) {
	if pab.currentState == pab {
		server.Log(pab.txnId, "TxnBallotsComplete callback. Acceptors:", pab.acceptors)
		if !pab.allAcceptorsAgreed {
			pab.proposerManager.NewPaxosProposals(pab.txnId, pab.txn.TxnCap, pab.fInc, ballots, pab.acceptors, pab.proposerManager.RMId, true)
		}
		pab.nextState()

	} else if pab.txn.Retry && pab.currentState == &pab.proposerReceiveOutcomes {
		server.Log(pab.txnId, "TxnBallotsComplete (retry) callback with existing proposals")
		if !pab.allAcceptorsAgreed {
			pab.proposerManager.AddToPaxosProposals(pab.txnId, ballots, pab.proposerManager.RMId)
		}

	} else if !pab.txn.Retry {
		log.Printf("Error: %v TxnBallotsComplete callback invoked in wrong state (%v)\n",
			pab.txnId, pab.currentState)
	}
}

func (pab *proposerAwaitBallots) Abort() {
	if pab.currentState == pab && !pab.allAcceptorsAgreed {
		server.Log(pab.txnId, "Proposer Aborting")
		txnCap := pab.txn.TxnCap
		alloc := AllocForRMId(txnCap, pab.proposerManager.RMId)
		ballots := MakeAbortBallots(txnCap, alloc)
		pab.TxnBallotsComplete(ballots...)
	}
}

func (pab *proposerAwaitBallots) ConnectedRMs(conns map[common.RMId]Connection) {
	if conn, found := conns[pab.submitter]; !found || conn.BootCount() != pab.submitterBootCount {
		pab.maybeAbortRetry()
	}
}
func (pab *proposerAwaitBallots) ConnectionLost(rmId common.RMId, conns map[common.RMId]Connection) {
	if rmId == pab.submitter {
		pab.maybeAbortRetry()
	}
}
func (pab *proposerAwaitBallots) ConnectionEstablished(rmId common.RMId, conn Connection, conns map[common.RMId]Connection) {
	if rmId == pab.submitter && conn.BootCount() != pab.submitterBootCount {
		pab.maybeAbortRetry()
	}
}

func (pab *proposerAwaitBallots) maybeAbortRetry() {
	pab.proposerManager.Exe.Enqueue(pab.Abort)
}

// receive outcomes

type proposerReceiveOutcomes struct {
	*Proposer
	outcomeAccumulator *OutcomeAccumulator
	outcome            *msgs.Outcome
}

func (pro *proposerReceiveOutcomes) init(proposer *Proposer) {
	pro.Proposer = proposer
	pro.outcomeAccumulator = NewOutcomeAccumulator(pro.fInc, pro.acceptors)
}

func (pro *proposerReceiveOutcomes) start() {
	if pro.txn != nil && pro.txn.Retry {
		pro.proposerManager.ConnectionManager.RemoveSenderAsync(&pro.proposerAwaitBallots)
	}
	if pro.outcome != nil {
		// we've received enough outcomes already!
		pro.nextState()
	}
}

func (pro *proposerReceiveOutcomes) proposerStateMachineComponentWitness() {}
func (pro *proposerReceiveOutcomes) String() string {
	return "proposerReceiveOutcomes"
}

func (pro *proposerReceiveOutcomes) BallotOutcomeReceived(sender common.RMId, outcome *msgs.Outcome) {
	server.Log(pro.txnId, "Ballot outcome received from", sender)
	if pro.mode == proposerTLCSender {
		// Consensus already reached and we've been to disk. So this
		// *must* be a duplicate: safe to ignore.

		// Even in the case where it's a retry, we actually don't care
		// that we could be receiving this *after* sending a TLC because
		// all we need to know is that it aborted, not the details.
		return
	}

	outcome, allAgreed := pro.outcomeAccumulator.BallotOutcomeReceived(sender, outcome)
	if allAgreed {
		pro.allAcceptorsAgree()
	}
	if outcome == nil && pro.mode == ProposerPassiveLearner {
		if knownAcceptors := pro.outcomeAccumulator.IsAllAborts(); knownAcceptors != nil {
			// As a passiveLearner, we started this proposer through
			// receiving a commit outcome. However, that has changed, due
			// to failures and every outcome we have is for the same
			// abort. Therefore we're abandoning this learner, and
			// sending TLCs immediately to everyone we've received the
			// abort outcome from.
			server.Log(pro.txnId, "abandoning learner with all aborts", knownAcceptors)
			pro.proposerManager.FinishProposers(pro.txnId)
			pro.proposerManager.TxnFinished(pro.txnId)
			tlcMsg := MakeTxnLocallyCompleteMsg(pro.txnId)
			// This is wrong - should be repeating sender. FIXME.
			NewOneShotSender(tlcMsg, pro.proposerManager.ConnectionManager, knownAcceptors...)
			return
		}
	}
	if pro.outcome == nil && outcome != nil {
		pro.outcome = outcome
		// It's possible that we're an activeVoter, and whilst our vars
		// are figuring out their votes, we receive enough ballot
		// outcomes from acceptors to determine the overall outcome. We
		// should only advance to the next state if we're currently
		// waiting for ballot outcomes.
		if pro.currentState == pro {
			pro.nextState()
		} else if pro.currentState == &pro.proposerAwaitBallots && pro.txn.Retry {
			// Advance currentState to proposerReceiveOutcomes, the
			// start() of which will immediately call nextState() again.
			pro.nextState()
		}
	}
}

// await locally complete

type proposerAwaitLocallyComplete struct {
	*Proposer
	allAcceptorsAgreed bool
	callbackInvoked    bool
}

func (palc *proposerAwaitLocallyComplete) init(proposer *Proposer) {
	palc.Proposer = proposer
}

func (palc *proposerAwaitLocallyComplete) start() {
	server.Log(palc.txnId, "Outcome for txn determined")
	if palc.txn == nil && palc.outcome.Which() == msgs.OUTCOME_COMMIT {
		// We are a learner (either active or passive), and the result
		// has turned out to be a commit.
		txnCap := palc.outcome.Txn()
		pm := palc.proposerManager
		palc.txn = eng.TxnFromCap(pm.Exe, pm.VarDispatcher, palc.Proposer, pm.RMId, &txnCap)
		palc.txn.Start(false)
	}
	if palc.txn == nil {
		palc.TxnLocallyComplete()
	} else {
		palc.txn.BallotOutcomeReceived(palc.outcome)
	}
}

func (palc *proposerAwaitLocallyComplete) proposerStateMachineComponentWitness() {}
func (palc *proposerAwaitLocallyComplete) String() string {
	return "proposerAwaitLocallyComplete"
}

func (palc *proposerAwaitLocallyComplete) TxnLocallyComplete() {
	if palc.currentState == palc && !palc.callbackInvoked {
		server.Log(palc.txnId, "Txn locally completed")
		palc.callbackInvoked = true
		palc.maybeWriteToDisk()
	}
}

func (palc *proposerAwaitLocallyComplete) allAcceptorsAgree() {
	if !palc.allAcceptorsAgreed {
		palc.allAcceptorsAgreed = true
		palc.proposerManager.FinishProposers(palc.txnId)
		palc.maybeWriteToDisk()
	}
}

func (palc *proposerAwaitLocallyComplete) maybeWriteToDisk() {
	if !(palc.currentState == palc && palc.callbackInvoked && palc.allAcceptorsAgreed) {
		return
	}

	stateSeg := capn.NewBuffer(nil)
	state := msgs.NewRootProposerState(stateSeg)
	acceptorsCap := stateSeg.NewUInt32List(len(palc.acceptors))
	state.SetAcceptors(acceptorsCap)
	for idx, rmId := range palc.acceptors {
		acceptorsCap.Set(idx, uint32(rmId))
	}

	data := server.SegToBytes(stateSeg)

	future := palc.proposerManager.Disk.ReadWriteTransaction(false, func(rwtxn *mdbs.RWTxn) interface{} {
		rwtxn.Put(db.DB.Proposers, palc.txnId[:], data, 0)
		return nil
	})
	go func() {
		if _, err := future.ResultError(); err != nil {
			log.Printf("Error: %v when writing proposer to disk: %v\n", palc.txnId, err)
			return
		}
		palc.proposerManager.Exe.Enqueue(palc.writeDone)
	}()
}

func (palc *proposerAwaitLocallyComplete) writeDone() {
	if palc.currentState == palc {
		palc.nextState()
	}
}

// receive globally complete

type proposerReceiveGloballyComplete struct {
	*Proposer
	tlcSender        *RepeatingSender
	locallyCompleted bool
}

func (prgc *proposerReceiveGloballyComplete) init(proposer *Proposer) {
	prgc.Proposer = proposer
}

func (prgc *proposerReceiveGloballyComplete) start() {
	if !prgc.locallyCompleted {
		prgc.locallyCompleted = true
		prgc.mode = proposerTLCSender
		tlcMsg := MakeTxnLocallyCompleteMsg(prgc.txnId)
		prgc.tlcSender = NewRepeatingSender(tlcMsg, prgc.acceptors...)
		server.Log(prgc.txnId, "Adding TLC Sender to", prgc.acceptors)
		prgc.proposerManager.ConnectionManager.AddSender(prgc.tlcSender)
	}
}

func (prgc *proposerReceiveGloballyComplete) proposerStateMachineComponentWitness() {}
func (prgc *proposerReceiveGloballyComplete) String() string {
	return "proposerReceiveGloballyComplete"
}

func (prgc *proposerReceiveGloballyComplete) TxnGloballyCompleteReceived(sender common.RMId) {
	if prgc.currentState == prgc {
		if prgc.outcomeAccumulator.TxnGloballyCompleteReceived(sender) {
			prgc.nextState()
		}
	}
	// If currentState != proposerReceiveGloballyComplete then this TGC
	// could just be a duplicate from some acceptor that's got bounced.
	// But we should not receive any TGC until we've issued TLCs.
	if !prgc.locallyCompleted {
		log.Printf("Error: %v globally complete received from %v without us issuing locally complete. (%v)\n",
			prgc.txnId, sender, prgc.currentState)
	}
}

// await finished

type proposerAwaitFinished struct {
	*Proposer
}

func (paf *proposerAwaitFinished) init(proposer *Proposer) {
	paf.Proposer = proposer
}

func (paf *proposerAwaitFinished) start() {
	if paf.txn == nil {
		paf.TxnFinished()
	} else {
		paf.txn.CompletionReceived()
	}
}

func (paf *proposerAwaitFinished) proposerStateMachineComponentWitness() {}
func (paf *proposerAwaitFinished) String() string {
	return "proposerAwaitFinished"
}

func (paf *proposerAwaitFinished) TxnFinished() {
	server.Log(paf.txnId, "Txn Finished Callback")
	if paf.currentState == paf {
		paf.nextState()
		future := paf.proposerManager.Disk.ReadWriteTransaction(false, func(rwtxn *mdbs.RWTxn) interface{} {
			rwtxn.Del(db.DB.Proposers, paf.txnId[:], nil)
			return nil
		})
		go func() {
			if _, err := future.ResultError(); err != nil {
				log.Printf("Error: %v when deleting proposer from disk: %v\n", paf.txnId, err)
				return
			}
			paf.proposerManager.Exe.Enqueue(func() {
				paf.proposerManager.ConnectionManager.RemoveSenderAsync(paf.tlcSender)
				paf.tlcSender = nil
				paf.proposerManager.TxnFinished(paf.txnId)
			})
		}()
	} else {
		log.Printf("Error: %v TxnFinished callback invoked with proposer in wrong state: %v",
			paf.txnId, paf.currentState)
	}
}
