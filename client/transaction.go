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
	shuttingDown        func()
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

func (ts *TransactionSubmitter) Shutdown(onceEmpty func()) {
	ts.shuttingDown = onceEmpty
	if len(ts.txns) == 0 {
		onceEmpty()
	} else {
		for subId, tr := range ts.txns {
			subIdCopy := subId
			if err := tr.terminate(nil, &subIdCopy); err != nil {
				ts.logger.Log("error", err)
			}
		}
	}
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

func (ts *TransactionSubmitter) SubmissionOutcomeReceived(sender common.RMId, subId *common.TxnId, txn *txnreader.TxnReader, outcome *msgs.Outcome) error {
	if tr, found := ts.txns[*subId]; found {
		return tr.SubmissionOutcomeReceived(sender, subId, txn, outcome)
	} else {
		// OSS is safe here - it's the default action on receipt of an unknown txnid
		senders.NewOneShotSender(ts.logger, paxos.MakeTxnSubmissionCompleteMsg(txn.Id, subId), ts.connPub, sender)
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

func (ts *TransactionSubmitter) AddTransactionRecord(tr *TransactionRecord) {
	if ts.txns == nil { // shutdown
		return
	}
	if _, found := ts.txns[*tr.Id]; found {
		panic("Transaction already exists! " + tr.Id.String())
	}
	if tr.shuttingDown == nil {
		ts.txns[*tr.Id] = tr
	}
}

type transactionOutcomeReceiver interface {
	Committed(*txnreader.TxnReader, *TransactionRecord) error
	Aborted(*txnreader.TxnReader, *TransactionRecord) error
}

type TransactionRecord struct {
	*TransactionSubmitter
	transactionOutcomeReceiver
	subManager  *SubscriptionManager
	cache       *Cache
	birthday    time.Time
	Id          *common.TxnId
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
	if tr.shuttingDown != nil {
		return
	}
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

func (tr *TransactionRecord) SubmissionOutcomeReceived(sender common.RMId, subId *common.TxnId, txn *txnreader.TxnReader, outcome *msgs.Outcome) error {
	// Be aware of exciting possibilities here. For example, for a
	// subscription txn, it's possible that we're going to receive
	// updates for that subscription *before* we know here whether or
	// not the subscription txn committed.
	if subId.Compare(txn.Id) == common.EQ { // normal txn
		if tr.outcome, _ = tr.accumulator.BallotOutcomeReceived(sender, outcome); tr.outcome != nil {
			err := tr.terminate(txn, subId)
			if tr.shuttingDown == nil {
				if tr.subManager == nil || tr.outcome.Which() == msgs.OUTCOME_ABORT {
					delete(tr.txns, *txn.Id) // not a subscription, or it aborted, so tidy up
				}
			} else { // we're shutting down, let's tidy up
				delete(tr.txns, *txn.Id)
				if len(tr.txns) == 0 {
					tr.shuttingDown()
				}
			}
			return err
		}
	} else if tr.subManager == nil {
		panic(fmt.Sprintf("Recevied update transaction for non-subscription! %v %v", subId, txn.Id))
	} else {
		return tr.subManager.SubmissionOutcomeReceived(sender, txn, outcome)
	}
	return nil
}

func (tr *TransactionRecord) terminate(txn *txnreader.TxnReader, subId *common.TxnId) error {
	completed := txn != nil
	// If this node is going down then we have to rely on others
	// noticing, and tidying up for us.
	//
	// If the node is staying up, but this client connection is going
	// down then we must hang around to tidy up ourselves (otherwise we
	// could be left with dangling txns if our txnsubmitter only sent
	// to some but not all proposers).
	//
	// So we only remove stuff when we really are completed.
	if completed {
		if tr.sender != nil {
			tr.connPub.RemoveServerConnectionSubscriber(tr.sender)
			tr.sender = nil
		}

		// We now send TSC to all acceptors. There is a chance that some
		// will not have received any votes yet so we have the risk of
		// TSCs arriving first. But the acceptors will ignore that, so
		// it's safe, and eventually when they do send us an outcome,
		// we'll respond with another TSC - if we're completed then this
		// tr will have been removed from tr.txns - so this is all safe
		// really.
		senders.NewOneShotSender(tr.logger, paxos.MakeTxnSubmissionCompleteMsg(tr.Id, subId), tr.connPub, tr.acceptors...)

		if tr.outcome.Which() == msgs.OUTCOME_COMMIT {
			return tr.Committed(txn, tr)
		} else {
			return tr.Aborted(txn, tr)
		}
	} else {
		/* TODO adjust to subscribe
		if !completed && tr.server.Retry() {
			// If this msg doesn't make it then proposers should
			// observe our death and tidy up anyway. If it's just this
			// connection shutting down then there should be no
			// problem with these msgs getting to the proposers.
			senders.NewOneShotSender(tr.logger, paxos.MakeTxnSubmissionAbortMsg(tr.Id), tr.connPub, tr.active...)
		}
		*/
		return nil
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
