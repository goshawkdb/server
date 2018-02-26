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
	connPub             sconn.ServerConnectionPublisher
	disabledHashCodes   map[common.RMId]types.EmptyStruct
	connections         map[common.RMId]*sconn.ServerConnection
	txns                map[common.TxnId]*TransactionRecord
	topology            *configuration.Topology
	actor               actor.EnqueueActor
	rng                 *rand.Rand
	bufferedSubmissions []func() // only needed for client txns
	shuttingDown        func([]*SubscriptionManager)
	shuttingDownSubs    []*SubscriptionManager
}

func NewTransactionSubmitter(connPub sconn.ServerConnectionPublisher, actor actor.EnqueueActor, rng *rand.Rand, logger log.Logger) *TransactionSubmitter {
	return &TransactionSubmitter{
		logger:  logger,
		connPub: connPub,
		txns:    make(map[common.TxnId]*TransactionRecord),
		actor:   actor,
		rng:     rng,
	}
}

func (ts *TransactionSubmitter) Shutdown(onceEmpty func([]*SubscriptionManager)) {
	if ts.shuttingDown != nil {
		return
	}
	ts.shuttingDown = onceEmpty
	if len(ts.txns) == 0 {
		onceEmpty(nil)
	} else {
		// NB, we could have live txns which are trying to delete
		// subscriptions, but the subscriptions they are trying to
		// delete are safe to "terminate" first. So they would end up in
		// shuttingDownSubs. So the content of shuttingDownSubs may be
		// greater than necessary, but it shouldn't miss anything: if
		// there's an addSubscription in flight, then when the outcome
		// is known for that, it'll be added in.
		ts.shuttingDownSubs = make([]*SubscriptionManager, 0, len(ts.txns))
		for _, tr := range ts.txns {
			tr.terminate()
		}
	}
}

func (ts *TransactionSubmitter) Status(sc *status.StatusConsumer) {
	txnIds := make([]string, 0, len(ts.txns))
	for txnId, tr := range ts.txns {
		str := txnId.String()
		if tr.subManager != nil {
			str += "(s)"
		}
		txnIds = append(txnIds, str)
	}
	sc.Emit(fmt.Sprintf("TransactionSubmitter: live TxnIds: %s", txnIds))
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
	utils.DebugLog(ts.logger, "debug", "TS Topology Changed.", "topology", topology)
	if topology == nil {
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
	if ts.topology != nil && len(ts.bufferedSubmissions) != 0 {
		funs := ts.bufferedSubmissions
		ts.bufferedSubmissions = nil
		for _, fun := range funs {
			fun()
		}
	}
	return nil
}

func (ts *TransactionSubmitter) AddTransactionRecord(tr *TransactionRecord, force bool) {
	if _, found := ts.txns[*tr.Id]; found {
		panic("Transaction already exists! " + tr.Id.String())
	}
	if force || tr.shuttingDown == nil {
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
		outcome, _ := tr.accumulator.BallotOutcomeReceived(sender, outcome)

		var err error
		// BallotOutcomeReceived is actually edge triggered: we will
		// only get a non-nil outcome once, so we really are safe here:
		if outcome != nil {
			tr.outcome = outcome
			if outcome.Which() == msgs.OUTCOME_COMMIT {
				tr.deleteSubscriptions(txn)
				err = tr.Committed(txn, tr)
			} else {
				err = tr.Aborted(txn, tr)
			}

			// We can now tidy up the txn submission sender: If the node
			// is staying up, but this client connection is going down
			// then we must hang around to tidy up ourselves (otherwise
			// we could be left with dangling txns if our txn sender only
			// sent to some but not all proposers).
			if tr.sender != nil {
				tr.connPub.RemoveServerConnectionSubscriber(tr.sender)
				tr.sender = nil
			}

			// We now send TSC to all acceptors. There is a chance that
			// some will not have received any votes yet so we have the
			// risk of TSCs arriving first. But the acceptors will ignore
			// that, so it's safe, and eventually when they do send us an
			// outcome, we'll respond with another TSC - if we're
			// completed then this tr will have been removed from tr.txns
			// - so this is all safe really.
			senders.NewOneShotSender(tr.logger, paxos.MakeTxnSubmissionCompleteMsg(tr.Id, subId), tr.connPub, tr.acceptors...)

		} else if tr.outcome != nil {
			// We already have a result, but we need to make sure the
			// sending acceptor gets a TSC from us. It is possible for us
			// to receive outcomes multiple times from the same acceptor
			// and thus be here multiple times. An acceptor receiving a
			// TSC is idempotent, so we're safe. We could optimise this
			// however. TODO.

			// We will only be here if we're a sub which committed and so
			// we can't rely on the default handling to send out TSCs.
			senders.NewOneShotSender(tr.logger, paxos.MakeTxnSubmissionCompleteMsg(tr.Id, subId), tr.connPub, sender)
		}

		// NB test tr.outcome and not outcome because outcome will be non-nil only once!
		// NB If we aborted, then by dfn, even if we are a sub, subManager will be empty.
		// I.e. we're done here unless we committed and we're a sub.
		if tr.outcome != nil &&
			(tr.subManager == nil || tr.outcome.Which() == msgs.OUTCOME_ABORT || tr.shuttingDown != nil) {
			tr.terminate()
		}

		return err

	} else if tr.subManager == nil {
		panic(fmt.Sprintf("Recevied update transaction for non-subscription! %v %v", subId, txn.Id))
	} else {
		return tr.subManager.SubmissionOutcomeReceived(sender, txn, outcome)
	}
}

func (tr *TransactionRecord) terminate() {
	// Ordering of this test is specific to ensure subManager records we want to terminate.
	if (tr.subManager == nil || tr.subManager.terminate()) && tr.outcome != nil {
		delete(tr.txns, *tr.Id)
		if tr.shuttingDown != nil {
			if tr.subManager != nil && tr.outcome.Which() == msgs.OUTCOME_COMMIT {
				tr.shuttingDownSubs = append(tr.shuttingDownSubs, tr.subManager)
			}
			if len(tr.txns) == 0 {
				tr.shuttingDown(tr.shuttingDownSubs)
			}
		}
	}
}

func (tr *TransactionRecord) deleteSubscriptions(txn *txnreader.TxnReader) {
	actions := txn.Actions(true).Actions()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		meta := action.Meta()
		if len(meta.DelSub()) == 0 {
			continue
		}
		vUUId := common.MakeVarUUId(action.VarId())
		subId := common.MakeTxnId(meta.DelSub())
		// it is impossible for us to be here *before* the sub record
		// has been added.
		if subTr, found := tr.txns[*subId]; found && subTr.subManager != nil {
			subTr.subManager.Deleted(vUUId)
		}
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
