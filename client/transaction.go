package client

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	"goshawkdb.io/server/types"
	"goshawkdb.io/server/types/actor"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/binarybackoff"
	ch "goshawkdb.io/server/utils/consistenthash"
	"goshawkdb.io/server/utils/senders"
	"goshawkdb.io/server/utils/status"
	"goshawkdb.io/server/utils/txnreader"
	"math/rand"
	"time"
)

type TransactionSubmitter struct {
	logger              log.Logger
	rmId                common.RMId
	bootCount           uint32
	connPub             sconn.ServerConnectionPublisher
	disabledHashCodes   map[common.RMId]types.EmptyStruct
	connections         map[common.RMId]*sconn.ServerConnection
	txns                map[common.TxnId]*TransactionRecord
	topology            *configuration.Topology
	actor               actor.EnqueueActor
	rng                 *rand.Rand
	bufferedSubmissions []func() // only needed for client txns
}

func NewTransactionSubmitter(self common.RMId, bootCount uint32, connPub sconn.ServerConnectionPublisher, actor actor.EnqueueActor, rng *rand.Rand, logger log.Logger) *TransactionSubmitter {
	return &TransactionSubmitter{
		logger:    logger,
		rmId:      self,
		bootCount: bootCount,
		connPub:   connPub,
		txns:      make(map[common.TxnId]*TransactionRecord),
		actor:     actor,
		rng:       rng,
	}
}

func (ts *TransactionSubmitter) Shutdown() {
	for _, tr := range ts.txns {
		if err := tr.terminate(nil); err != nil {
			ts.logger.Log("error", err)
		}
	}
	ts.txns = nil
}

func (ts *TransactionSubmitter) Status(sc *status.StatusConsumer) {
	txnIds := make([]common.TxnId, 0, len(ts.txns))
	for txnId := range ts.txns {
		txnIds = append(txnIds, txnId)
	}
	sc.Emit(fmt.Sprintf("TransactionSubmitter: live TxnIds: %v", txnIds))
	sc.Emit(fmt.Sprintf("TransactionSubmitter: buffered Txns: %v", len(ts.bufferedSubmissions)))
	sc.Join()
}

func (ts *TransactionSubmitter) SubmissionOutcomeReceived(sender common.RMId, txn *txnreader.TxnReader, outcome *msgs.Outcome) error {
	if tr, found := ts.txns[*txn.Id]; found {
		return tr.SubmissionOutcomeReceived(sender, txn, outcome)
	} else {
		// OSS is safe here - it's the default action on receipt of an unknown txnid
		senders.NewOneShotSender(ts.logger, paxos.MakeTxnSubmissionCompleteMsg(txn.Id), ts.connPub, sender)
		return nil
	}
}

func (ts *TransactionSubmitter) TopologyChanged(topology *configuration.Topology) error {
	utils.DebugLog(ts.logger, "debug", "TS Topology Changed.", "topology", topology, "blank", topology.IsBlank())
	if topology.IsBlank() {
		// topology is needed for client txns. As we're booting up, we
		// just don't care.
		return nil
	}
	ts.topology = topology
	for _, tr := range ts.txns {
		tr.TopologyChanged(topology)
	}
	return ts.calculateDisabledHashcodes()
}

func (ts *TransactionSubmitter) ServerConnectionsChanged(servers map[common.RMId]*sconn.ServerConnection) error {
	utils.DebugLog(ts.logger, "debug", "TS ServerConnectionsChanged.", "servers", servers)
	ts.connections = servers
	return ts.calculateDisabledHashcodes()
}

func (ts *TransactionSubmitter) calculateDisabledHashcodes() error {
	if ts.topology == nil || ts.connections == nil {
		return nil
	}
	ts.disabledHashCodes = make(map[common.RMId]types.EmptyStruct, len(ts.topology.RMs))
	for _, rmId := range ts.topology.RMs {
		if rmId == common.RMIdEmpty {
			continue
		} else if _, found := ts.connections[rmId]; !found {
			ts.disabledHashCodes[rmId] = types.EmptyStructVal
		}
	}
	utils.DebugLog(ts.logger, "debug", "TS disabled hash codes.", "disabledHashCodes", ts.disabledHashCodes)
	// need to wait until we've updated disabledHashCodes before
	// starting up any buffered txns.
	if !ts.topology.IsBlank() && len(ts.bufferedSubmissions) != 0 {
		funs := ts.bufferedSubmissions
		ts.bufferedSubmissions = nil
		for _, fun := range funs {
			fun()
		}
	}
	return nil
}

func (ts *TransactionSubmitter) AddTransactionRecord(txnId *common.TxnId, tr *TransactionRecord) {
	if _, found := ts.txns[*txnId]; found {
		panic("Transaction already exists! " + txnId.String())
	}
	ts.txns[*txnId] = tr
}

type transactionOutcomeReceiver interface {
	Terminated(*TransactionRecord) error
	Committed(*txnreader.TxnReader, *TransactionRecord) error
	Aborted(*txnreader.TxnReader, *TransactionRecord) error
}

type TransactionRecord struct {
	*TransactionSubmitter
	transactionOutcomeReceiver
	cache       *Cache
	birthday    time.Time
	origId      *common.TxnId
	client      *cmsgs.ClientTxn
	server      *msgs.Txn
	objs        map[common.VarUUId]*Cached
	active      common.RMIds
	acceptors   common.RMIds
	sender      *senders.RepeatingSender
	accumulator *paxos.OutcomeAccumulator
	outcome     *msgs.Outcome
	bbe         *binarybackoff.BinaryBackoffEngine
}

func (tr *TransactionRecord) Submit() {
	tr.birthday = time.Now()
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	msg.SetTxnSubmission(common.SegToBytes(tr.server.Segment))
	tr.acceptors = paxos.GetAcceptorsFromTxn(*tr.server)
	tr.accumulator = paxos.NewOutcomeAccumulator(int(tr.server.TwoFInc()), tr.acceptors, tr.logger)
	utils.DebugLog(tr.logger, "debug", "Submitting txn.", "OrigTxnId", tr.origId, "active", tr.active)

	tr.sender = senders.NewRepeatingSender(common.SegToBytes(seg), tr.active...)
	if tr.bbe == nil || tr.bbe.Cur == 0 {
		tr.connPub.AddServerConnectionSubscriber(tr.sender)
	} else {
		tr.bbe.After(func() {
			tr.actor.EnqueueFuncAsync(func() (bool, error) {
				// if it's already nil then we must have been terminated
				if tr.sender != nil {
					tr.connPub.AddServerConnectionSubscriber(tr.sender)
				}
				return false, nil
			})
		})
	}
}

func (tr *TransactionRecord) SubmissionOutcomeReceived(sender common.RMId, txn *txnreader.TxnReader, outcome *msgs.Outcome) error {
	if tr.outcome, _ = tr.accumulator.BallotOutcomeReceived(sender, outcome); tr.outcome != nil {
		delete(tr.txns, *txn.Id)
		return tr.terminate(txn)
	}
	return nil
}

func (tr *TransactionRecord) terminate(txn *txnreader.TxnReader) error {
	// If we're a retry then we have a specific Abort message to
	// send which helps tidy up garbage in the case that this node
	// *isn't* going down (connection dying only, and !completed).
	//
	// But, if we're not a retry, then we must *not* cancel the
	// sender (unless completed!): in the case that this node
	// *isn't* going down then no one else will react to the loss of
	// this connection and so we could end up with a dangling
	// transaction if the txnSender only manages to send to some but
	// not all active voters.

	// OSS is safe here - see above. In the case that the node's
	// exiting, this informs acceptors that we don't care about the
	// answer so they shouldn't hang around waiting for the node to
	// return. Of course, it's best effort.
	senders.NewOneShotSender(tr.logger, paxos.MakeTxnSubmissionCompleteMsg(txn.Id), tr.connPub, tr.acceptors...)
	if tr.sender != nil {
		tr.connPub.RemoveServerConnectionSubscriber(tr.sender)
		tr.sender = nil
	}
	completed := tr.outcome != nil
	if !completed && tr.server.Retry() {
		// If this msg doesn't make it then proposers should
		// observe our death and tidy up anyway. If it's just this
		// connection shutting down then there should be no
		// problem with these msgs getting to the proposers.
		senders.NewOneShotSender(tr.logger, paxos.MakeTxnSubmissionAbortMsg(txn.Id), tr.connPub, tr.active...)
	}

	switch {
	case !completed:
		return tr.Terminated(tr)
	case tr.outcome.Which() == msgs.OUTCOME_COMMIT:
		return tr.Committed(txn, tr)
	default:
		return tr.Aborted(txn, tr)
	}
}

func (tr *TransactionRecord) TopologyChanged(topology *configuration.Topology) {
	if tr.cache != nil {
		tr.cache.SetResolver(ch.NewResolver(topology.RMs, topology.TwoFInc))
	}
	if tr.accumulator != nil {
		tr.accumulator.TopologyChanged(topology)
		// TODO the above returns true iff we now have a winning
		// outcome! Handle this. But this is also all linked through to
		// the remaining T88 issues, so wait for that.
	}
}
