package client

import (
	"fmt"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/paxos"
	"goshawkdb.io/server/utils/senders"
	"goshawkdb.io/server/utils/txnreader"
)

type SubscriptionConsumer func(txn *txnreader.TxnReader, tr *TransactionRecord) error

func NewSubscriptionManager(subId *common.TxnId, tr *TransactionRecord, consumer SubscriptionConsumer) *SubscriptionManager {
	return &SubscriptionManager{
		TransactionRecord: tr,
		subId:             subId,
		consumer:          consumer,
		incomplete:        make(map[common.TxnId]*subscriptionUpdate),
	}
}

type SubscriptionManager struct {
	*TransactionRecord
	subId      *common.TxnId
	consumer   SubscriptionConsumer
	incomplete map[common.TxnId]*subscriptionUpdate
}

type subscriptionUpdate struct {
	acceptors   common.RMIds
	accumulator *paxos.OutcomeAccumulator
}

func (sm *SubscriptionManager) SubmissionOutcomeReceived(sender common.RMId, txn *txnreader.TxnReader, outcome *msgs.Outcome) error {
	if outcome.Which() != msgs.OUTCOME_COMMIT {
		panic(fmt.Sprintf("SubId %v received non-commit outcome in txn %v", sm.subId, txn.Id))
	}

	su, found := sm.incomplete[*txn.Id]
	if !found {
		twoFInc := int(txn.Txn.TwoFInc())
		acceptors := paxos.GetAcceptorsFromTxn(txn.Txn)
		acc := paxos.NewOutcomeAccumulator(twoFInc, acceptors, sm.logger)
		su = &subscriptionUpdate{
			acceptors:   acceptors,
			accumulator: acc,
		}
		sm.incomplete[*txn.Id] = su
	}

	if result, _ := su.accumulator.BallotOutcomeReceived(sender, outcome); result != nil {
		delete(sm.incomplete, *txn.Id)
		// see notes in TransactionRecord.terminate
		senders.NewOneShotSender(sm.logger, paxos.MakeTxnSubmissionCompleteMsg(txn.Id, sm.subId), sm.connPub, su.acceptors...)

		return sm.consumer(txn, sm.TransactionRecord)
	}

	return nil
}
