package paxos

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	eng "goshawkdb.io/server/txnengine"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/senders"
	"goshawkdb.io/server/utils/status"
	"goshawkdb.io/server/utils/txnreader"
	"os"
	"time"
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
	logger          log.Logger
	mode            ProposerMode
	initialMode     ProposerMode
	txn             *eng.Txn
	txnId           *common.TxnId
	birthday        time.Time
	acceptors       common.RMIds
	topology        *configuration.Topology
	twoFInc         int
	createdFromDisk bool
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

func NewProposer(pm *ProposerManager, txn *txnreader.TxnReader, mode ProposerMode, topology *configuration.Topology) *Proposer {
	txnCap := txn.Txn
	p := &Proposer{
		proposerManager: pm,
		mode:            mode,
		initialMode:     mode,
		txnId:           txn.Id,
		birthday:        time.Now(),
		acceptors:       GetAcceptorsFromTxn(txnCap),
		topology:        topology,
		twoFInc:         int(txnCap.TwoFInc()),
	}
	if mode == ProposerActiveVoter {
		p.txn = eng.TxnFromReader(pm.Exe, pm.VarDispatcher, p, pm.RMId, txn, p)
	}
	p.init()
	return p
}

func ProposerFromData(pm *ProposerManager, txnId *common.TxnId, data []byte, topology *configuration.Topology) (*Proposer, error) {
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
	// accumulator's twoFInc, hence just use -1 here.
	p := &Proposer{
		proposerManager: pm,
		mode:            proposerTLCSender,
		initialMode:     proposerTLCSender,
		txnId:           txnId,
		birthday:        time.Now(),
		acceptors:       acceptors,
		topology:        topology,
		twoFInc:         -1,
		createdFromDisk: true,
	}
	p.init()
	p.allAcceptorsAgreed = true
	return p, nil
}

func (p *Proposer) Log(keyvals ...interface{}) error {
	if p.logger == nil {
		p.logger = log.With(p.proposerManager.logger, "TxnId", p.txnId)
	}
	return p.logger.Log(keyvals...)
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

	if p.topology != nil {
		topology := p.topology
		p.topology = nil
		p.TopologyChanged(topology)
	}

	p.currentState.start()
}

func (p *Proposer) Status(sc *status.StatusConsumer) {
	sc.Emit(fmt.Sprintf("Proposer for %v", p.txnId))
	sc.Emit(fmt.Sprintf("- Born: %v", p.birthday))
	sc.Emit(fmt.Sprintf("- Created from disk: %v", p.createdFromDisk))
	sc.Emit(fmt.Sprintf("- Mode: %v", p.mode))
	sc.Emit(fmt.Sprintf("- Initial Mode: %v", p.initialMode))
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

func (p *Proposer) TopologyChanged(topology *configuration.Topology) {
	if topology == p.topology {
		return
	}
	p.topology = topology
	rmsRemoved := topology.RMsRemoved
	utils.DebugLog(p, "debug", "TopologyChanged.",
		"currentState", p.currentState, "RMsRemoved", rmsRemoved)
	if _, found := rmsRemoved[p.proposerManager.RMId]; found {
		return
	}
	// create new acceptors slice because the initial slice can be
	// shared with proposals.
	acceptors := make([]common.RMId, 0, len(p.acceptors))
	for _, rmId := range p.acceptors {
		if _, found := rmsRemoved[rmId]; !found {
			acceptors = append(acceptors, rmId)
		}
	}
	p.acceptors = acceptors

	switch p.currentState {
	case &p.proposerAwaitBallots, &p.proposerReceiveOutcomes, &p.proposerAwaitLocallyComplete:
		if p.outcomeAccumulator.TopologyChanged(topology) {
			p.allAcceptorsAgree()
		}
	case &p.proposerReceiveGloballyComplete:
		for rmId := range rmsRemoved {
			p.TxnGloballyCompleteReceived(rmId)
		}
	case &p.proposerAwaitFinished:
		// do nothing
	}
}

func (p *Proposer) TLCDone() bool {
	return p.currentState == &p.proposerReceiveGloballyComplete ||
		p.currentState == &p.proposerAwaitFinished ||
		p.currentState == nil
}

func (p *Proposer) IsTopologyTxn() bool {
	if p.txn == nil {
		return false
	}
	actions := p.txn.TxnReader.Actions(true).Actions()
	if actions.Len() != 1 {
		return false
	}
	vUUId := common.MakeVarUUId(actions.At(1).VarId())
	return vUUId.Compare(configuration.TopologyVarUUId) == common.EQ
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
	txnId := pab.txn.TxnReader.Id
	pab.submitter = txnId.RMId(pab.proposerManager.RMId)
	pab.submitterBootCount = txnId.BootCount()
}

func (pab *proposerAwaitBallots) proposerStateMachineComponentWitness() {}
func (pab *proposerAwaitBallots) String() string {
	return "proposerAwaitBallots"
}

func (pab *proposerAwaitBallots) TxnBallotsComplete(ballots ...*eng.Ballot) {
	if pab.currentState == pab {
		utils.DebugLog(pab, "debug", "TxnBallotsComplete callback.", "acceptors", pab.acceptors)
		if !pab.allAcceptorsAgreed {
			pab.proposerManager.NewPaxosProposals(pab.txn.TxnReader, pab.twoFInc, ballots, pab.acceptors, pab.proposerManager.RMId, true)
		}
		pab.nextState()

	} else {
		pab.Log("error", "TxnBallotsComplete callback invoked in wrong state.",
			"currentState", pab.currentState)
	}
}

func (pab *proposerAwaitBallots) Abort() (bool, error) {
	if pab.currentState == pab && !pab.allAcceptorsAgreed {
		utils.DebugLog(pab, "debug", "Proposer Aborting.")
		txn := pab.txn.TxnReader
		alloc := AllocForRMId(txn.Txn, pab.proposerManager.RMId)
		ballots := MakeAbortBallots(txn, alloc)
		pab.TxnBallotsComplete(ballots...)
	}
	return false, nil
}

// receive outcomes

type proposerReceiveOutcomes struct {
	*Proposer
	outcomeAccumulator *OutcomeAccumulator
	outcome            *msgs.Outcome
}

func (pro *proposerReceiveOutcomes) init(proposer *Proposer) {
	pro.Proposer = proposer
	pro.outcomeAccumulator = NewOutcomeAccumulator(pro.twoFInc, pro.acceptors, pro.Proposer)
}

func (pro *proposerReceiveOutcomes) start() {
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
	utils.DebugLog(pro, "debug", "Ballot outcome received.", "sender", sender)
	if pro.mode == proposerTLCSender {
		// Consensus already reached and we've been to disk. So this
		// *must* be a duplicate: safe to ignore.
		return
	}

	outcome, allAgreed := pro.outcomeAccumulator.BallotOutcomeReceived(sender, outcome)
	if allAgreed {
		pro.allAcceptorsAgree()
	}
	if outcome == nil && pro.mode == ProposerPassiveLearner {
		if knownAcceptors := pro.outcomeAccumulator.IsAllAborts(); len(knownAcceptors) != 0 {
			// As a passiveLearner, we started this proposer through
			// receiving a commit outcome. However, that has changed, due
			// to failures and every outcome we have is for the same
			// abort. Therefore we're abandoning this learner, and
			// sending TLCs immediately to everyone we've received the
			// abort outcome from.
			utils.DebugLog(pro, "debug", "Abandoning learner with all aborts.", "knownAcceptors", knownAcceptors)
			pro.proposerManager.FinishProposals(pro.txnId)
			pro.proposerManager.TxnFinished(pro.Proposer)
			tlcMsg := MakeTxnLocallyCompleteMsg(pro.txnId)
			// We are destroying our state here. Thus even if this msg
			// goes missing, if the acceptor sends us further 2Bs then
			// we'll send back further TLCs from proposer manager. So the
			// use of OSS here is correct.
			senders.NewOneShotSender(pro.Proposer, tlcMsg, pro.proposerManager, knownAcceptors...)
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
	utils.DebugLog(palc, "debug", "Outcome for txn determined.")
	if palc.txn == nil && palc.outcome.Which() == msgs.OUTCOME_COMMIT {
		// We are a learner (either active or passive), and the result
		// has turned out to be a commit.
		defer func() {
			if r := recover(); r != nil {
				palc.Log("msg", "Recovered!", "error", fmt.Sprint(r), "outcomeWhich", palc.outcome.Which())
				sc := status.NewStatusConsumer()
				palc.outcomeAccumulator.Status(sc)
				str := sc.Wait()
				os.Stderr.WriteString(str + "\n")
				panic("repanic")
			}
		}()
		txn := txnreader.TxnReaderFromData(palc.outcome.Txn())
		pm := palc.proposerManager
		palc.txn = eng.TxnFromReader(pm.Exe, pm.VarDispatcher, palc.Proposer, pm.RMId, txn, palc.Proposer)
		palc.txn.Start(false)
	}
	if palc.txn == nil {
		palc.TxnLocallyComplete(palc.txn)
	} else {
		palc.txn.BallotOutcomeReceived(palc.outcome)
	}
}

func (palc *proposerAwaitLocallyComplete) proposerStateMachineComponentWitness() {}
func (palc *proposerAwaitLocallyComplete) String() string {
	return "proposerAwaitLocallyComplete"
}

func (palc *proposerAwaitLocallyComplete) TxnLocallyComplete(*eng.Txn) {
	if palc.currentState == palc && !palc.callbackInvoked {
		utils.DebugLog(palc, "debug", "Txn locally completed.")
		palc.callbackInvoked = true
		palc.maybeWriteToDisk()
	}
}

func (palc *proposerAwaitLocallyComplete) allAcceptorsAgree() {
	if !palc.allAcceptorsAgreed {
		palc.allAcceptorsAgreed = true
		palc.proposerManager.FinishProposals(palc.txnId)
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

	data := common.SegToBytes(stateSeg)

	future := palc.proposerManager.DB.ReadWriteTransaction(func(rwtxn *mdbs.RWTxn) interface{} {
		rwtxn.Put(palc.proposerManager.DB.Proposers, palc.txnId[:], data, 0)
		return true
	})
	go func() {
		if ran, err := future.ResultError(); err != nil {
			panic(fmt.Sprintf("Error: %v when writing proposer to disk: %v\n", palc.txnId, err))
		} else if ran != nil {
			palc.proposerManager.Exe.EnqueueFuncAsync(palc.writeDone)
		}
	}()
}

func (palc *proposerAwaitLocallyComplete) writeDone() (bool, error) {
	if palc.currentState == palc {
		palc.nextState()
	}
	return false, nil
}

// receive globally complete

type proposerReceiveGloballyComplete struct {
	*Proposer
	tlcSender        *senders.RepeatingSender
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
		prgc.tlcSender = senders.NewRepeatingSender(tlcMsg, prgc.acceptors...)
		utils.DebugLog(prgc, "debug", "Adding TLC Sender.", "acceptors", prgc.acceptors)
		prgc.proposerManager.AddServerConnectionSubscriber(prgc.tlcSender)
		prgc.proposerManager.TxnLocallyComplete(prgc.Proposer)
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
		prgc.Log("error", "Globally complete received without us issuing locally complete.",
			"sender", sender, "currentState", prgc.currentState)
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
		paf.TxnFinished(paf.txn)
	} else {
		paf.txn.CompletionReceived()
	}
}

func (paf *proposerAwaitFinished) proposerStateMachineComponentWitness() {}
func (paf *proposerAwaitFinished) String() string {
	return "proposerAwaitFinished"
}

func (paf *proposerAwaitFinished) TxnFinished(*eng.Txn) {
	utils.DebugLog(paf, "debug", "Txn Finished Callback.")
	if paf.currentState == paf {
		paf.nextState()
		future := paf.proposerManager.DB.ReadWriteTransaction(func(rwtxn *mdbs.RWTxn) interface{} {
			rwtxn.Del(paf.proposerManager.DB.Proposers, paf.txnId[:], nil)
			return true
		})
		go func() {
			if ran, err := future.ResultError(); err != nil {
				panic(fmt.Sprintf("Error: %v when deleting proposer from disk: %v\n", paf.txnId, err))
			} else if ran != nil {
				paf.proposerManager.Exe.EnqueueFuncAsync(func() (bool, error) {
					paf.proposerManager.RemoveServerConnectionSubscriber(paf.tlcSender)
					paf.tlcSender = nil
					paf.proposerManager.TxnFinished(paf.Proposer)
					return false, nil
				})
			}
		}()
	} else {
		paf.Log("error", "TxnFinished callback invoked with proposer in wrong state.",
			"currentState", paf.currentState)
	}
}
