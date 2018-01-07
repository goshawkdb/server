package client

import (
	"fmt"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/paxos"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/senders"
	"goshawkdb.io/server/utils/txnreader"
	"goshawkdb.io/server/utils/vectorclock"
)

type SubscriptionConsumer func(txn *txnreader.TxnReader, tr *TransactionRecord) error

func NewSubscriptionManager(subId *common.TxnId, tr *TransactionRecord, consumer SubscriptionConsumer) *SubscriptionManager {
	actions := txnreader.TxnActionsFromData(tr.server.Actions(), true).Actions()
	cache := make(map[common.VarUUId]*VerClock, actions.Len())
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if action.ActionType() == msgs.ACTIONTYPE_ADDSUBSCRIPTION {
			vUUId := common.MakeVarUUId(action.VarId())
			version := common.MakeTxnId(action.Version())
			cache[*vUUId] = &VerClock{version: version}
		}
	}
	return &SubscriptionManager{
		TransactionRecord: tr,
		subId:             subId,
		consumer:          consumer,
		incomplete:        make(map[common.TxnId]*subscriptionUpdate),
		cache:             cache,
	}
}

type SubscriptionManager struct {
	*TransactionRecord
	subId      *common.TxnId
	consumer   SubscriptionConsumer
	incomplete map[common.TxnId]*subscriptionUpdate
	cache      map[common.VarUUId]*VerClock
}

type VerClock struct {
	version   *common.TxnId
	clockElem uint64
}

type subscriptionUpdate struct {
	acceptors   common.RMIds
	accumulator *paxos.OutcomeAccumulator
	outcome     *msgs.Outcome
}

func (sm *SubscriptionManager) SubmissionOutcomeReceived(sender common.RMId, txn *txnreader.TxnReader, outcome *msgs.Outcome) error {
	if outcome.Which() != msgs.OUTCOME_COMMIT {
		panic(fmt.Sprintf("SubId %v received non-commit outcome in txn %v", sm.subId, txn.Id))
	}

	su, found := sm.incomplete[*txn.Id]
	if !found {
		newer := false
		actions := txn.Actions(true).Actions()
		clock := vectorclock.VectorClockFromData(outcome.Commit(), true)
		for idx, l := 0, actions.Len(); idx < l; idx++ {
			action := actions.At(idx)

			// A normal update via a badread would never contain a ROLL
			// action. But this isn't a badread - this is the committed
			// txn, and so that really can contain rolls,
			// addSubscriptions, delSubscriptions. This then becomes a
			// problem when we factor in that we can receive txns in any
			// order: we could receive a roll and judge it newer, but not
			// have the value to which it refers. So to keep matters
			// simple, we ignore all actions which don't contain the real
			// value:
			actionType := action.ActionType()
			if actionType != msgs.ACTIONTYPE_CREATE &&
				actionType != msgs.ACTIONTYPE_WRITEONLY &&
				actionType != msgs.ACTIONTYPE_READWRITE {
				continue
			}

			vUUId := common.MakeVarUUId(action.VarId())
			if verClock, found := sm.cache[*vUUId]; found {
				clockElem := clock.At(vUUId)
				if clockElem > verClock.clockElem || (clockElem == verClock.clockElem && txn.Id.Compare(verClock.version) == common.GT) {
					newer = true
					verClock.version = txn.Id
					verClock.clockElem = clockElem
				}
			}
		}

		if !newer {
			// ignore it - either we've already processed this and for
			// some reason there's been a dupe of the outcome message, or
			// we received a newer outcome first.
			senders.NewOneShotSender(sm.logger, paxos.MakeTxnSubmissionCompleteMsg(txn.Id, sm.subId), sm.connPub, sender)
			utils.DebugLog(sm.logger, "debug", "Ignoring non-newer subscription txn.", "SubId", sm.subId, "TxnId", txn.Id)
			return nil
		}

		twoFInc := int(txn.Txn.TwoFInc())
		acceptors := paxos.GetAcceptorsFromTxn(txn.Txn)
		acc := paxos.NewOutcomeAccumulator(twoFInc, acceptors, sm.logger)
		su = &subscriptionUpdate{
			acceptors:   acceptors,
			accumulator: acc,
		}
		sm.incomplete[*txn.Id] = su
	}

	outcome, allAgreed := su.accumulator.BallotOutcomeReceived(sender, outcome)
	if outcome != nil {
		su.outcome = outcome

		senders.NewOneShotSender(sm.logger, paxos.MakeTxnSubmissionCompleteMsg(txn.Id, sm.subId), sm.connPub, su.acceptors...)

		utils.DebugLog(sm.logger, "debug", "Outcome known for subscription txn.", "SubId", sm.subId, "TxnId", txn.Id)
		return sm.consumer(txn, sm.TransactionRecord)
	} else if su.outcome != nil {
		senders.NewOneShotSender(sm.logger, paxos.MakeTxnSubmissionCompleteMsg(txn.Id, sm.subId), sm.connPub, sender)
	}

	if allAgreed {
		delete(sm.incomplete, *txn.Id)
	}

	return nil
}
