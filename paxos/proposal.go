package paxos

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	eng "goshawkdb.io/server/txnengine"
)

type proposal struct {
	proposerManager    *ProposerManager
	instanceRMId       common.RMId
	acceptors          []common.RMId
	activeRMIds        map[common.RMId]uint32
	twoFInc            int
	fInc               int
	txn                *eng.TxnReader
	submitter          common.RMId
	submitterBootCount uint32
	skipPhase1         bool
	instances          map[common.VarUUId]*proposalInstance
	pending            []*proposalInstance
	abortInstances     []common.RMId
	finished           bool
}

func NewProposal(pm *ProposerManager, txn *eng.TxnReader, twoFInc int, ballots []*eng.Ballot, instanceRMId common.RMId, acceptors []common.RMId, skipPhase1 bool) *proposal {
	txnCap := txn.Txn
	allocs := txnCap.Allocations()
	activeRMIds := make(map[common.RMId]uint32, allocs.Len())
	for idx, l := 0, allocs.Len(); idx < l; idx++ {
		alloc := allocs.At(idx)
		bootCount := alloc.Active()
		if bootCount == 0 {
			break
		}
		rmId := common.RMId(alloc.RmId())
		activeRMIds[rmId] = bootCount
	}
	p := &proposal{
		proposerManager:    pm,
		instanceRMId:       instanceRMId,
		acceptors:          acceptors,
		activeRMIds:        activeRMIds,
		twoFInc:            twoFInc,
		fInc:               (twoFInc >> 1) + 1,
		txn:                txn,
		submitter:          txn.Id.RMId(pm.RMId),
		submitterBootCount: txn.Id.BootCount(),
		skipPhase1:         skipPhase1,
		instances:          make(map[common.VarUUId]*proposalInstance, len(ballots)),
		pending:            make([]*proposalInstance, 0, len(ballots)),
		finished:           false,
	}
	for _, ballot := range ballots {
		pi := newProposalInstance(p, ballot)
		p.instances[*ballot.VarUUId] = pi
		pi.init()
		pi.start()
	}
	return p
}

func (p *proposal) Start() {
	p.maybeSendOneA()
	p.maybeSendTwoA()
}

func (p *proposal) AddBallots(ballots []*eng.Ballot) {
	added := false
	for _, ballot := range ballots {
		if _, found := p.instances[*ballot.VarUUId]; found {
			continue
		}
		pi := newProposalInstance(p, ballot)
		p.instances[*ballot.VarUUId] = pi
		pi.init()
		pi.start()
		added = true
	}
	if added {
		p.maybeSendOneA()
		p.maybeSendTwoA()
	}
}

func (p *proposal) maybeSendOneA() {
	pendingPromises := p.pending[:0]
	for _, pi := range p.instances {
		if pi.currentState == &pi.proposalOneA {
			pendingPromises = append(pendingPromises, pi)
		}
	}
	if len(pendingPromises) == 0 {
		return
	}
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	sender := newProposalSender(p, pendingPromises)
	oneACap := msgs.NewOneATxnVotes(seg)
	msg.SetOneATxnVotes(oneACap)
	txnId := p.txn.Id
	oneACap.SetTxnId(txnId[:])
	oneACap.SetRmId(uint32(p.instanceRMId))
	proposals := msgs.NewTxnVoteProposalList(seg, len(pendingPromises))
	oneACap.SetProposals(proposals)
	for idx, pi := range pendingPromises {
		proposal := proposals.At(idx)
		pi.addOneAToProposal(&proposal, sender)
	}
	sender.msg = common.SegToBytes(seg)
	server.DebugLog(p.proposerManager.logger, "debug", "Adding sender for 1A.", "TxnId", txnId)
	p.proposerManager.AddServerConnectionSubscriber(sender)
}

func (p *proposal) OneBTxnVotesReceived(sender common.RMId, oneBTxnVotes *msgs.OneBTxnVotes) {
	promises := oneBTxnVotes.Promises()
	for idx, l := 0, promises.Len(); idx < l; idx++ {
		promise := promises.At(idx)
		vUUId := common.MakeVarUUId(promise.VarId())
		pi := p.instances[*vUUId]
		pi.oneBTxnVotesReceived(sender, &promise)
	}
	p.maybeSendOneA()
	p.maybeSendTwoA()
}

func (p *proposal) maybeSendTwoA() {
	pendingAccepts := p.pending[:0]
	for _, pi := range p.instances {
		if pi.currentState == &pi.proposalTwoA {
			pendingAccepts = append(pendingAccepts, pi)
		}
	}
	if len(pendingAccepts) == 0 {
		return
	}
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	sender := newProposalSender(p, pendingAccepts)
	twoACap := msgs.NewTwoATxnVotes(seg)
	msg.SetTwoATxnVotes(twoACap)
	twoACap.SetRmId(uint32(p.instanceRMId))
	acceptRequests := msgs.NewTxnVoteAcceptRequestList(seg, len(pendingAccepts))
	twoACap.SetAcceptRequests(acceptRequests)
	for idx, pi := range pendingAccepts {
		acceptRequest := acceptRequests.At(idx)
		pi.addTwoAToAcceptRequest(seg, &acceptRequest, sender)
	}
	twoACap.SetTxn(p.txn.Data)
	sender.msg = common.SegToBytes(seg)
	server.DebugLog(p.proposerManager.logger, "debug", "Adding sender for 2A.", "TxnId", p.txn.Id)
	p.proposerManager.AddServerConnectionSubscriber(sender)
}

func (p *proposal) TwoBFailuresReceived(sender common.RMId, failures *msgs.TwoBTxnVotesFailures) {
	nacks := failures.Nacks()
	for idx, l := 0, nacks.Len(); idx < l; idx++ {
		nack := nacks.At(idx)
		vUUId := common.MakeVarUUId(nack.VarId())
		pi := p.instances[*vUUId]
		pi.twoBNackReceived(&nack)
	}
	p.maybeSendOneA()
	p.maybeSendTwoA()
}

func (p *proposal) FinishProposing() []common.RMId {
	if p.finished {
		return nil
	}
	p.finished = true
	for _, pi := range p.instances {
		if sender := pi.oneASender; sender != nil {
			pi.oneASender = nil
			server.DebugLog(p.proposerManager.logger, "debug", "Finishing sender for 1A.", "TxnId", p.txn.Id)
			sender.finished()
		}
		if sender := pi.twoASender; sender != nil {
			pi.twoASender = nil
			server.DebugLog(p.proposerManager.logger, "debug", "Finishing sender for 2A.", "TxnId", p.txn.Id,
				"VarUUId", pi.ballot.VarUUId)
			sender.finished()
		}
	}
	return p.abortInstances
}

func (p *proposal) Status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("Proposal for %v-%v", p.txn.Id, p.instanceRMId))
	sc.Emit(fmt.Sprintf("- Acceptors: %v", p.acceptors))
	sc.Emit(fmt.Sprintf("- Instances: %v", len(p.instances)))
	sc.Emit(fmt.Sprintf("- Finished? %v", p.finished))
	sc.Join()
}

type proposalInstance struct {
	*proposal
	ballot       *eng.Ballot
	currentState proposalInstanceComponent
	proposalOneA
	proposalOneB
	proposalTwoA
	proposalTwoB
}

func newProposalInstance(p *proposal, ballot *eng.Ballot) *proposalInstance {
	return &proposalInstance{
		proposal: p,
		ballot:   ballot,
	}
}

func (pi *proposalInstance) init() {
	pi.proposalOneA.init(pi)
	pi.proposalOneB.init(pi)
	pi.proposalTwoA.init(pi)
	pi.proposalTwoB.init(pi)

	if pi.skipPhase1 {
		pi.currentState = &pi.proposalTwoA
	} else {
		pi.currentState = &pi.proposalOneA
	}
}

func (pi *proposalInstance) start() {
	pi.currentState.start()
}

func (pi *proposalInstance) nextState(requestedState proposalInstanceComponent) {
	if requestedState == nil {
		switch pi.currentState {
		case &pi.proposalOneA:
			pi.currentState = &pi.proposalOneB
		case &pi.proposalOneB:
			pi.currentState = &pi.proposalTwoA
		case &pi.proposalTwoA:
			pi.currentState = &pi.proposalTwoB
		default:
			return
		}
	} else {
		pi.currentState = requestedState
	}
	pi.currentState.start()
}

type proposalInstanceComponent interface {
	init(*proposalInstance)
	start()
	proposalInstanceComponentWitness()
}

// oneA
type proposalOneA struct {
	*proposalInstance
	currentRoundNumber paxosNumber
	oneASender         *proposalSender
}

func (oneA *proposalOneA) proposalInstanceComponentWitness() {}
func (oneA *proposalOneA) String() string                    { return "ProposalInstanceOneA" }

func (oneA *proposalOneA) init(p *proposalInstance) {
	oneA.proposalInstance = p
	top := uint64(1)
	if oneA.skipPhase1 {
		top = 0
	}
	oneA.currentRoundNumber = paxosNumber((top << 32) | uint64(oneA.proposerManager.RMId))
}

func (oneA *proposalOneA) start() {}

func (oneA *proposalOneA) addOneAToProposal(proposalCap *msgs.TxnVoteProposal, sender *proposalSender) {
	proposalCap.SetVarId(oneA.ballot.VarUUId[:])
	proposalCap.SetRoundNumber(uint64(oneA.currentRoundNumber))
	oneA.oneASender = sender
	oneA.nextState(nil)
}

// oneB
type proposalOneB struct {
	*proposalInstance
	promisesReceivedFrom []common.RMId
	winningRound         paxosNumber
	winningBallot        []byte
}

func (oneB *proposalOneB) proposalInstanceComponentWitness() {}
func (oneB *proposalOneB) String() string                    { return "ProposalInstanceOneB" }

func (oneB *proposalOneB) init(pi *proposalInstance) {
	oneB.proposalInstance = pi
	oneB.promisesReceivedFrom = make([]common.RMId, 0, oneB.fInc)
}

func (oneB *proposalOneB) start() {
	oneB.promisesReceivedFrom = oneB.promisesReceivedFrom[:0]
	oneB.winningRound = 0
	oneB.winningBallot = nil
}

func (oneB *proposalOneB) oneBTxnVotesReceived(sender common.RMId, promise *msgs.TxnVotePromise) {
	roundNumber := paxosNumber(promise.RoundNumber())
	if oneB.currentState != oneB || roundNumber < oneB.currentRoundNumber {
		return
	}
	switch promise.Which() {
	case msgs.TXNVOTEPROMISE_ROUNDNUMBERTOOLOW:
		roundNumber = paxosNumber((uint64(promise.RoundNumberTooLow()+1) << 32) | uint64(oneB.proposerManager.RMId))
		if roundNumber > oneB.currentRoundNumber {
			oneB.currentRoundNumber = roundNumber
			oneB.oneASender.instanceComplete(oneB.proposalInstance)
			oneB.oneASender = nil
			oneB.nextState(&oneB.proposalOneA)
			return
		}
	case msgs.TXNVOTEPROMISE_FREECHOICE:
		// do nothing
	case msgs.TXNVOTEPROMISE_ACCEPTED:
		accepted := promise.Accepted()
		if roundNumber = paxosNumber(accepted.RoundNumber()); roundNumber > oneB.winningRound {
			oneB.winningRound = roundNumber
			oneB.winningBallot = accepted.Ballot()
		}
	default:
		panic(fmt.Sprintf("Unexpected promise type: %v", promise.Which()))
	}
	found := false
	for _, rmId := range oneB.promisesReceivedFrom {
		if found = rmId == sender; found {
			break
		}
	}
	if !found {
		oneB.promisesReceivedFrom = append(oneB.promisesReceivedFrom, sender)
		if len(oneB.promisesReceivedFrom) == oneB.fInc {
			oneB.oneASender.instanceComplete(oneB.proposalInstance)
			oneB.oneASender = nil
			oneB.nextState(nil)
		}
	}
}

// twoA
type proposalTwoA struct {
	*proposalInstance
	twoASender *proposalSender
}

func (twoA *proposalTwoA) proposalInstanceComponentWitness() {}
func (twoA *proposalTwoA) String() string                    { return "ProposalInstanceTwoA" }

func (twoA *proposalTwoA) init(pi *proposalInstance) {
	twoA.proposalInstance = pi
}

func (twoA *proposalTwoA) start() {}

func (twoA *proposalTwoA) addTwoAToAcceptRequest(seg *capn.Segment, acceptRequest *msgs.TxnVoteAcceptRequest, sender *proposalSender) {
	var ballotData []byte
	if twoA.winningBallot == nil { // free choice from everyone
		ballotData = twoA.ballot.Data
	} else {
		ballotData = twoA.winningBallot
	}
	acceptRequest.SetBallot(ballotData)

	acceptRequest.SetRoundNumber(uint64(twoA.currentRoundNumber))
	twoA.twoASender = sender
	twoA.nextState(nil)
}

// twoB
type proposalTwoB struct {
	*proposalInstance
}

func (twoB *proposalTwoB) proposalInstanceComponentWitness() {}
func (twoB *proposalTwoB) String() string                    { return "ProposalInstanceTwoB" }

func (twoB *proposalTwoB) init(pi *proposalInstance) {
	twoB.proposalInstance = pi
}

func (twoB *proposalTwoB) start() {}

func (twoB *proposalTwoB) twoBNackReceived(nack *msgs.TxnVoteTwoBFailure) {
	roundNumber := paxosNumber(nack.RoundNumber())
	if twoB.currentState != twoB || roundNumber < twoB.winningRound {
		return
	}
	if twoB.twoASender != nil {
		twoB.twoASender.instanceComplete(twoB.proposalInstance)
		twoB.twoASender = nil
	}
	roundNumber = paxosNumber((uint64(nack.RoundNumberTooLow()+1) << 32) | uint64(twoB.proposerManager.RMId))
	if roundNumber > twoB.currentRoundNumber {
		twoB.currentRoundNumber = roundNumber
		twoB.nextState(&twoB.proposalOneA)
	}
}

// proposalSender

// Despite the fact that we're only sending to acceptors here, we also
// monitor all other active proposers for death and boot-count issues,
// even though proposers never need to send to each other. Arguably,
// it would be better to do such monitoring and formation of abort
// proposers on the death of active proposers (who are not acceptors)
// in the acceptor side, but it becomes tricky to ensure all such
// abort proposers are cancelled before TLCs are sent and received.
// In any case, even if we spot an acceptor dying, our reaction is to
// form abort proposers for the acceptor iff it's also an active
// proposer, so the same reaction to the death of non-acceptor active
// proposers is hardly unexpected.
type proposalSender struct {
	*proposal
	msg                      []byte
	done                     bool
	incompleteInstances      []*proposalInstance
	incompleteInstancesCount int
	proposeAborts            bool
}

func newProposalSender(p *proposal, instances []*proposalInstance) *proposalSender {
	instancesList := make([]*proposalInstance, len(instances))
	copy(instancesList, instances)

	return &proposalSender{
		proposal:                 p,
		done:                     false,
		incompleteInstances:      instancesList,
		incompleteInstancesCount: len(instances),
		proposeAborts:            p.instanceRMId == p.proposerManager.RMId,
	}
}

func (s *proposalSender) instanceComplete(pi *proposalInstance) {
	for idx, i := range s.incompleteInstances {
		if i == pi {
			s.incompleteInstances[idx] = nil
			s.incompleteInstancesCount--
			if s.incompleteInstancesCount == 0 {
				s.finished()
			}
			break
		}
	}
}

func (s *proposalSender) finished() {
	if !s.done {
		s.done = true
		server.DebugLog(s.proposerManager.logger, "debug", "Removing proposal sender.")
		s.proposerManager.RemoveServerConnectionSubscriber(s)
	}
}

func (s *proposalSender) ConnectedRMs(conns map[common.RMId]Connection) {
	for _, rmId := range s.proposal.acceptors {
		if conn, found := conns[rmId]; found {
			conn.Send(s.msg)
		}
	}
	for rmId, bootCount := range s.proposal.activeRMIds {
		if conn, found := conns[rmId]; !found || conn.BootCount() != bootCount {
			s.ConnectionLost(rmId, conns)
		}
	}
	if conn, found := conns[s.proposal.submitter]; !found || (conn.BootCount() != s.submitterBootCount && s.submitterBootCount > 0) {
		s.ConnectionLost(s.proposal.submitter, conns)
	}
}

func (s *proposalSender) ConnectionLost(lost common.RMId, conns map[common.RMId]Connection) {
	if !s.proposeAborts {
		return
	}

	if lost == s.proposal.submitter {
		// There's a chance that only we received this txn, so we need
		// to abort for all other active RMs.
		s.proposal.proposerManager.Exe.Enqueue(func() {
			// Only start a new proposal if we're not finished. If we are
			// finished, we're either fully finished (i.e. all acceptors
			// agree on the outcome and there's no more proposal work to
			// do), or the current proposal is finished, though there may
			// be later stages. In the first case, the point is we must
			// have made contact with at least F+1 acceptors and got
			// answers back from them, in which case we know that all the
			// active voters have also contacted acceptors (if they have
			// not, we would definitely not have F+1 results), which
			// means they have all received the txn itself. In the second
			// case, we know there are going to be future proposal
			// stages, so we will pick up any failures at that time. So
			// in these cases, if the submitter dies, we have no work to
			// do because the txn has already made it out as far as
			// necessary (or will do one way or another).
			//
			// However, if we are not finished, then there's the
			// possibility that not all active voters have even received
			// the txn (which could very well be _why_ we're not
			// finished). Therefore, we attempt to abort for everyone
			// other than ourselves. We could of course try to just
			// resend the txn submission instead, but that's just more
			// code paths and more complexity. In general, we always opt
			// for a fail-fast solution.
			if s.proposal.finished {
				return
			}
			allocs := s.proposal.txn.Txn.Allocations()
			for idx, l := 0, allocs.Len(); idx < l; idx++ {
				alloc := allocs.At(idx)
				rmId := common.RMId(alloc.RmId())
				if alloc.Active() == 0 {
					break
				} else if rmId == s.proposal.proposerManager.RMId {
					continue
				} else {
					found := false
					// slightly horrible N^2, but not on critical path. Ok for now.
					for _, alreadyAborted := range s.proposal.abortInstances {
						if found = alreadyAborted == rmId; found {
							break
						}
					}
					if found {
						break
					}
					ballots := MakeAbortBallots(s.proposal.txn, &alloc)
					server.DebugLog(s.proposerManager.logger, "debug", "Trying to abort due to lost submitter.",
						"TxnId", s.proposal.txn.Id, "RMId", rmId, "lost", lost, "actionCount", len(ballots))
					s.proposal.abortInstances = append(s.proposal.abortInstances, rmId)
					s.proposal.proposerManager.NewPaxosProposals(
						s.txn, s.twoFInc, ballots, s.proposal.acceptors, rmId, false)
				}
			}
		})
		return
	}

	alloc := AllocForRMId(s.proposal.txn.Txn, lost)
	if alloc == nil || alloc.Active() == 0 {
		return
	}
	s.proposal.proposerManager.Exe.Enqueue(func() {
		if s.proposal.finished { // see above equiv
			return
		}
		for _, alreadyAborted := range s.proposal.abortInstances {
			if alreadyAborted == lost {
				return // already done!
			}
		}
		ballots := MakeAbortBallots(s.proposal.txn, alloc)
		server.DebugLog(s.proposerManager.logger, "debug", "Trying to abort.", "TxnId", s.proposal.txn.Id,
			"lost", lost, "actionCount", len(ballots))
		s.proposal.abortInstances = append(s.proposal.abortInstances, lost)
		s.proposal.proposerManager.NewPaxosProposals(
			s.txn, s.twoFInc, ballots, s.proposal.acceptors, lost, false)
	})
}

func (s *proposalSender) ConnectionEstablished(rmId common.RMId, conn Connection, conns map[common.RMId]Connection, done func()) {
	for _, acc := range s.proposal.acceptors {
		if acc == rmId {
			conn.Send(s.msg)
			break
		}
	}
	if bootCount, found := s.proposal.activeRMIds[rmId]; found && bootCount != conn.BootCount() {
		s.ConnectionLost(rmId, conns) // at worst, this just enqueues some functinos so nothing to worry about
	}
	done()
}
