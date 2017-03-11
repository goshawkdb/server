package paxos

import (
	"encoding/binary"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"github.com/prometheus/client_golang/prometheus"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	eng "goshawkdb.io/server/txnengine"
	"time"
)

func init() {
	db.DB.Proposers = &mdbs.DBISettings{Flags: mdb.CREATE}
}

const ( //                  txnId  rmId
	instanceIdPrefixLen = common.KeyLen + 4
)

type instanceIdPrefix [instanceIdPrefixLen]byte

type ProposerManager struct {
	ServerConnectionPublisher
	logger        log.Logger
	RMId          common.RMId
	BootCount     uint32
	VarDispatcher *eng.VarDispatcher
	Exe           *dispatcher.Executor
	DB            *db.Databases
	proposals     map[instanceIdPrefix]*proposal
	proposers     map[common.TxnId]*Proposer
	topology      *configuration.Topology
	metrics       *ProposerMetrics
}

type ProposerMetrics struct {
	Gauge    prometheus.Gauge
	Lifespan prometheus.Histogram
}

func NewProposerManager(exe *dispatcher.Executor, rmId common.RMId, bootCount uint32, cm ConnectionManager, db *db.Databases, varDispatcher *eng.VarDispatcher, logger log.Logger) *ProposerManager {
	pm := &ProposerManager{
		ServerConnectionPublisher: NewServerConnectionPublisherProxy(exe, cm, logger),
		logger:        logger, // proposerDispatcher creates the context
		RMId:          rmId,
		BootCount:     bootCount,
		proposals:     make(map[instanceIdPrefix]*proposal),
		proposers:     make(map[common.TxnId]*Proposer),
		VarDispatcher: varDispatcher,
		Exe:           exe,
		DB:            db,
		topology:      nil,
	}
	exe.Enqueue(func() { pm.topology = cm.AddTopologySubscriber(eng.ProposerSubscriber, pm) })
	return pm
}

func (pm *ProposerManager) loadFromData(txnId *common.TxnId, data []byte) error {
	if _, found := pm.proposers[*txnId]; found {
		panic(fmt.Sprintf("ProposerManager loadFromData: For %v, proposer already exists!", txnId))
	}
	proposer, err := ProposerFromData(pm, txnId, data, pm.topology)
	if err != nil {
		return err
	}
	pm.proposers[*txnId] = proposer
	if pm.metrics != nil {
		pm.metrics.Gauge.Inc()
	}
	proposer.Start()
	return nil
}

func (pm *ProposerManager) TopologyChanged(topology *configuration.Topology, done func(bool)) {
	finished := make(chan struct{})
	enqueued := pm.Exe.Enqueue(func() {
		pm.topology = topology
		for _, proposer := range pm.proposers {
			proposer.TopologyChange(topology)
		}
		close(finished)
	})
	if enqueued {
		go pm.Exe.WithTerminatedChan(func(terminated chan struct{}) {
			select {
			case <-finished:
				done(true)
			case <-terminated:
				done(false)
			}
		})
	} else {
		done(false)
	}
}

func (pm *ProposerManager) SetMetrics(metrics *ProposerMetrics) {
	l := float64(len(pm.proposers))
	if pm.metrics != nil {
		pm.metrics.Gauge.Sub(l)
	}
	if metrics != nil {
		metrics.Gauge.Add(l)
	}
	pm.metrics = metrics
}

func (pm *ProposerManager) ImmigrationReceived(txn *eng.TxnReader, varCaps *msgs.Var_List, stateChange eng.TxnLocalStateChange) {
	eng.ImmigrationTxnFromCap(pm.Exe, pm.VarDispatcher, stateChange, pm.RMId, txn, varCaps, pm.logger)
}

func (pm *ProposerManager) TxnReceived(sender common.RMId, txn *eng.TxnReader) {
	// Due to failures, we can actually receive outcomes (2Bs) first,
	// before we get the txn to vote on it - due to failures, other
	// proposers will have created abort proposals on our behalf, and
	// consensus may have already been reached. If this is the case, it
	// is correct to ignore this message.
	txnId := txn.Id
	txnCap := txn.Txn
	if _, found := pm.proposers[*txnId]; !found {
		server.DebugLog(pm.logger, "debug", "Received.", "TxnId", txnId)
		accept := true
		if pm.topology != nil {
			accept = (pm.topology.NextConfiguration == nil && pm.topology.Version == txnCap.TopologyVersion()) ||
				// Could also do pm.topology.BarrierReached1(sender), but
				// would need to specialise that to rolls rather than
				// topology txns, and it's enforced on the sending side
				// anyway. Once the sender has received the next topology,
				// it'll do the right thing and locally block until it's
				// in barrier1.
				(pm.topology.NextConfiguration != nil && pm.topology.NextConfiguration.Version == txnCap.TopologyVersion())
			if accept {
				_, found := pm.topology.RMsRemoved[sender]
				accept = !found
				if accept {
					accept = false
					allocations := txn.Txn.Allocations()
					for idx, l := 0, allocations.Len(); idx < l; idx++ {
						alloc := allocations.At(idx)
						rmId := common.RMId(alloc.RmId())
						if rmId == pm.RMId {
							accept = alloc.Active() == pm.BootCount
							break
						}
					}
					if !accept {
						server.DebugLog(pm.logger, "debug", "Aborting received txn as it was submitted for an older version of us so we may have already voted on it.",
							"TxnId", txnId, "BootCount", pm.BootCount)
					}
				} else {
					server.DebugLog(pm.logger, "debug", "Aborting received txn as sender has been removed from topology.",
						"TxnId", txnId, "sender", sender)
				}
			} else {
				server.DebugLog(pm.logger, "debug", "Aborting received txn due to non-matching topology.",
					"TxnId", txnId, "topologyVersion", txnCap.TopologyVersion())
			}
		}
		if accept {
			pm.createProposerStart(txn, ProposerActiveVoter, pm.topology)

		} else {
			acceptors := GetAcceptorsFromTxn(txnCap)
			twoFInc := int(txnCap.TwoFInc())
			alloc := AllocForRMId(txnCap, pm.RMId)
			ballots := MakeAbortBallots(txn, alloc)
			// We must not skip phase 1 - it's possible in a previous
			// life we did vote on this.
			pm.NewPaxosProposals(txn, twoFInc, ballots, acceptors, pm.RMId, false)
			// ActiveLearner is right - we don't want the proposer to
			// vote, but it should exist to collect the 2Bs that should
			// come back.
			pm.createProposerStart(txn, ProposerActiveLearner, pm.topology)
		}
	}
}

func (pm *ProposerManager) NewPaxosProposals(txn *eng.TxnReader, twoFInc int, ballots []*eng.Ballot, acceptors []common.RMId, rmId common.RMId, skipPhase1 bool) {
	instId := instanceIdPrefix([instanceIdPrefixLen]byte{})
	instIdSlice := instId[:]
	txnId := txn.Id
	copy(instIdSlice, txnId[:])
	binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], uint32(rmId))
	if _, found := pm.proposals[instId]; !found {
		server.DebugLog(pm.logger, "debug", "NewPaxos.", "TxnId", txnId, "acceptors", acceptors, "instance", rmId)
		prop := NewProposal(pm, txn, twoFInc, ballots, rmId, acceptors, skipPhase1)
		pm.proposals[instId] = prop
		prop.Start()
	}
}

func (pm *ProposerManager) AddToPaxosProposals(txnId *common.TxnId, ballots []*eng.Ballot, rmId common.RMId) {
	server.DebugLog(pm.logger, "debug", "Adding ballot to Paxos.", "TxnId", txnId, "instance", rmId)
	instId := instanceIdPrefix([instanceIdPrefixLen]byte{})
	instIdSlice := instId[:]
	copy(instIdSlice, txnId[:])
	binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], uint32(rmId))
	if prop, found := pm.proposals[instId]; found {
		prop.AddBallots(ballots)
	} else {
		pm.logger.Log("error", "Adding ballot to Paxos, unable to find proposals.", "TxnId", txnId, "RMId", rmId)
	}
}

// from network
func (pm *ProposerManager) OneBTxnVotesReceived(sender common.RMId, txnId *common.TxnId, oneBTxnVotes *msgs.OneBTxnVotes) {
	server.DebugLog(pm.logger, "debug", "1B received.", "TxnId", txnId, "sender", sender, "instance", common.RMId(oneBTxnVotes.RmId()))
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
func (pm *ProposerManager) TwoBTxnVotesReceived(sender common.RMId, txnId *common.TxnId, txn *eng.TxnReader, twoBTxnVotes *msgs.TwoBTxnVotes) {
	instId := instanceIdPrefix([instanceIdPrefixLen]byte{})
	instIdSlice := instId[:]
	copy(instIdSlice, txnId[:])

	switch twoBTxnVotes.Which() {
	case msgs.TWOBTXNVOTES_FAILURES:
		failures := twoBTxnVotes.Failures()
		server.DebugLog(pm.logger, "debug", "2B failures received.", "TxnId", txnId, "sender", sender, "instance", common.RMId(failures.RmId()))
		binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], failures.RmId())
		if prop, found := pm.proposals[instId]; found {
			prop.TwoBFailuresReceived(sender, &failures)
		}

	case msgs.TWOBTXNVOTES_OUTCOME:
		binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], uint32(pm.RMId))
		outcome := twoBTxnVotes.Outcome()

		if proposer, found := pm.proposers[*txnId]; found {
			server.DebugLog(pm.logger, "debug", "2B outcome received. Known.", "TxnId", txnId, "sender", sender)
			proposer.BallotOutcomeReceived(sender, &outcome)
			return
		}

		txnCap := txn.Txn

		alloc := AllocForRMId(txnCap, pm.RMId)

		if alloc.Active() != 0 {
			// We have no record of this, but we were active - we must
			// have died and recovered (or we may have never received
			// this yet - see above - if we were down, other proposers
			// may have started abort proposers). Thus this could be
			// abort (abort proposers out there) or commit (we previously
			// voted, and that vote got recorded, but we have since died
			// and restarted).
			server.DebugLog(pm.logger, "debug", "2B outcome received. Unknown Active.", "TxnId", txnId, "sender", sender)

			// There's a possibility the acceptor that sent us this 2B is
			// one of only a few acceptors that got enough 2As to
			// determine the outcome. We must set up new paxos instances
			// to ensure the result is propogated to all. All we need to
			// do is to start a proposal for our own vars. The proposal
			// itself will detect any further absences and take care of
			// them.
			acceptors := GetAcceptorsFromTxn(txnCap)
			server.DebugLog(pm.logger, "debug", "Starting abort proposals.", "TxnId", txnId, "acceptors", acceptors)
			twoFInc := int(txnCap.TwoFInc())
			ballots := MakeAbortBallots(txn, alloc)
			pm.NewPaxosProposals(txn, twoFInc, ballots, acceptors, pm.RMId, false)

			proposer := pm.createProposerStart(txn, ProposerActiveLearner, pm.topology)
			proposer.BallotOutcomeReceived(sender, &outcome)
		} else {
			// Not active, so we are a learner
			if outcome.Which() == msgs.OUTCOME_COMMIT {
				server.DebugLog(pm.logger, "debug", "2B outcome received. Unknown Learner.", "TxnId", txnId, "sender", sender)
				// we must be a learner.
				proposer := pm.createProposerStart(txn, ProposerPassiveLearner, pm.topology)
				proposer.BallotOutcomeReceived(sender, &outcome)

			} else {
				// Whilst it's an abort now, at some point in the past it
				// was a commit and as such we received that
				// outcome. However, we must have since died and so lost
				// that state/proposer. We should now immediately reply
				// with a TLC.
				server.DebugLog(pm.logger, "debug", "Sending immediate TLC for unknown abort learner.", "TxnId", txnId)
				// We have no state here, and if we receive further 2Bs
				// from the repeating sender at the acceptor then we will
				// send further TLCs. So the use of OSS here is correct.
				NewOneShotSender(pm.logger, MakeTxnLocallyCompleteMsg(txnId), pm, sender)
			}
		}

	default:
		panic(fmt.Sprintf("Unexpected 2BVotes type: %v", twoBTxnVotes.Which()))
	}
}

// from network
func (pm *ProposerManager) TxnGloballyCompleteReceived(sender common.RMId, txnId *common.TxnId) {
	if proposer, found := pm.proposers[*txnId]; found {
		server.DebugLog(pm.logger, "debug", "TGC received. Proposer found.", "TxnId", txnId, "sender", sender)
		proposer.TxnGloballyCompleteReceived(sender)
	} else {
		server.DebugLog(pm.logger, "debug", "TGC received. Ignored.", "TxnId", txnId, "sender", sender)
	}
}

// from network
func (pm *ProposerManager) TxnSubmissionAbortReceived(sender common.RMId, txnId *common.TxnId) {
	if proposer, found := pm.proposers[*txnId]; found {
		server.DebugLog(pm.logger, "debug", "TSA received. Proposer found.", "TxnId", txnId, "sender", sender)
		proposer.Abort()
	} else {
		server.DebugLog(pm.logger, "debug", "TSA received. Ignored.", "TxnId", txnId, "sender", sender)
	}
}

func (pm *ProposerManager) createProposerStart(txn *eng.TxnReader, mode ProposerMode, topology *configuration.Topology) *Proposer {
	if _, found := pm.proposers[*txn.Id]; found {
		panic(fmt.Sprintf("ProposerManager createProposerStart: Proposer for %v already exists!", txn.Id))
	}
	proposer := NewProposer(pm, txn, mode, topology)
	pm.proposers[*txn.Id] = proposer
	if pm.metrics != nil {
		pm.metrics.Gauge.Inc()
	}
	proposer.Start()
	return proposer
}

// from proposer
func (pm *ProposerManager) TxnFinished(proposer *Proposer) {
	if prop, found := pm.proposers[*proposer.txnId]; !found || prop != proposer {
		panic(fmt.Sprintf("ProposerManager TxnFinished: No such proposer found! %v %v %v %v",
			proposer.txnId, proposer, found, prop))
	}
	delete(pm.proposers, *proposer.txnId)
	if pm.metrics != nil {
		pm.metrics.Gauge.Dec()
		elapsed := time.Now().Sub(proposer.birthday)
		pm.metrics.Lifespan.Observe(float64(elapsed) / float64(time.Second))
	}
}

// We have an outcome by this point, so we should stop sending proposals.
func (pm *ProposerManager) FinishProposals(txnId *common.TxnId) {
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

func GetAcceptorsFromTxn(txnCap msgs.Txn) common.RMIds {
	twoFInc := int(txnCap.TwoFInc())
	acceptors := make([]common.RMId, twoFInc)
	allocations := txnCap.Allocations()
	idx := 0
	for l := allocations.Len(); idx < l && idx < twoFInc; idx++ {
		alloc := allocations.At(idx)
		acceptors[idx] = common.RMId(alloc.RmId())
	}
	// Danger! For the topology txns, there are generally _not_ twoFInc
	// acceptors!
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

func AllocForRMId(txn msgs.Txn, rmId common.RMId) *msgs.Allocation {
	allocs := txn.Allocations()
	for idx, l := 0, allocs.Len(); idx < l; idx++ {
		alloc := allocs.At(idx)
		if common.RMId(alloc.RmId()) == rmId {
			return &alloc
		}
	}
	return nil
}

func MakeAbortBallots(txn *eng.TxnReader, alloc *msgs.Allocation) []*eng.Ballot {
	actions := txn.Actions(true).Actions()
	actionIndices := alloc.ActionIndices()
	ballots := make([]*eng.Ballot, actionIndices.Len())
	for idx, l := 0, actionIndices.Len(); idx < l; idx++ {
		action := actions.At(int(actionIndices.At(idx)))
		vUUId := common.MakeVarUUId(action.VarId())
		ballots[idx] = eng.NewBallotBuilder(vUUId, eng.AbortDeadlock, nil).ToBallot()
	}
	return ballots
}
