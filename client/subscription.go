package client

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/paxos"
	"goshawkdb.io/server/types"
	"goshawkdb.io/server/types/localconnection"
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
	subId       *common.TxnId
	consumer    SubscriptionConsumer
	incomplete  map[common.TxnId]*subscriptionUpdate
	cache       map[common.VarUUId]*VerClock
	terminating bool
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

func (sm *SubscriptionManager) terminate() bool {
	sm.terminating = true
	for _, su := range sm.incomplete {
		if su.outcome == nil {
			return false
		}
	}
	return true
}

func (sm *SubscriptionManager) createUnsubscribeTxn(cache *Cache) (*cmsgs.ClientTxn, map[common.VarUUId]*types.PosCapVer) {
	roots := make(map[common.VarUUId]*types.PosCapVer, len(sm.cache))
	seg := capn.NewBuffer(nil)
	ctxn := cmsgs.NewClientTxn(seg)
	actions := cmsgs.NewClientActionList(seg, len(sm.cache))
	ctxn.SetActions(actions)
	idx := 0
	for vUUId := range sm.cache {
		c, err := cache.Find(&vUUId, true)
		if err != nil {
			panic(err)
		}
		roots[vUUId] = &types.PosCapVer{
			Positions:  c.positions,
			Capability: common.ReadOnlyCapability,
			Version:    c.version, // use the version from c, not sm.cache, as c may be more up to date!
		}
		action := actions.At(idx)
		idx++
		action.SetActionType(cmsgs.CLIENTACTIONTYPE_DELSUBSCRIPTION)
		action.SetVarId(vUUId[:])
		action.SetModified()
		mod := action.Modified()
		mod.SetValue(sm.Id[:])
	}

	return &ctxn, roots
}

func (sm *SubscriptionManager) Unsubscribe(lc localconnection.LocalConnection, cache *Cache) error {
	for {
		ctxn, roots := sm.createUnsubscribeTxn(cache)
		_, outcome, err := lc.RunClientTransaction(ctxn, false, roots, nil)
		if err != nil {
			return err
		}
		if outcome.Which() == msgs.OUTCOME_COMMIT {
			return nil
		}
		abort := outcome.Abort()
		if abort.Which() == msgs.OUTCOMEABORT_RESUBMIT {
			continue
		}
		updates := abort.Rerun()
		for idx, l := 0, updates.Len(); idx < l; idx++ {
			update := updates.At(idx)
			txnId := common.MakeTxnId(update.TxnId())
			clock := vectorclock.VectorClockFromData(update.Clock(), true)
			actions := txnreader.TxnActionsFromData(update.Actions(), true).Actions()
			for idy, m := 0, actions.Len(); idy < m; idy++ {
				action := actions.At(idy)
				vUUId := common.MakeVarUUId(action.VarId())
				_, found := sm.cache[*vUUId]
				if !found {
					continue
				}
				c := cache.m[*vUUId]
				if action.ActionType() != msgs.ACTIONTYPE_WRITEONLY {
					continue
				}
				cmp := c.version.Compare(txnId)
				clockElem := clock.At(vUUId)
				if clockElem > c.clockElem || (clockElem == c.clockElem && cmp == common.LT) {
					c.version = txnId
					c.clockElem = clockElem
					// we don't care about updating refs or caps or anything like that at this point.
				}
			}
		}
		// now with the updated cache, just go around again
	}
}

func (sm *SubscriptionManager) Deleted(vUUId *common.VarUUId) {
	// One of the most surprising things about how subscriptions are
	// implemented is that individual vars can be removed from a
	// subscription. This was not a conscious design decision...
	delete(sm.cache, *vUUId)
	if len(sm.cache) == 0 {
		// unattach ourself first so that we don't risk ending up in
		// shuttingDownSubs: clearly, all our subscriptions have been
		// removed.
		sm.subManager = nil
		sm.TransactionRecord.terminate()
	}
}

func (sm *SubscriptionManager) SubmissionOutcomeReceived(sender common.RMId, txn *txnreader.TxnReader, outcome *msgs.Outcome) (err error) {
	if outcome.Which() != msgs.OUTCOME_COMMIT {
		panic(fmt.Sprintf("SubId %v received non-commit outcome in txn %v", sm.subId, txn.Id))
	}

	su, found := sm.incomplete[*txn.Id]
	if !found {
		if sm.terminating {
			// if we're terminating then don't create any new updaters,
			// just reply and ignore.
			senders.NewOneShotSender(sm.logger, paxos.MakeTxnSubmissionCompleteMsg(txn.Id, sm.subId), sm.connPub, sender)
			return
		}

		// This can still do the wrong thing, and I've no idea how to
		// fix this properly. The problem is that we should be able to
		// cope with a duplicate receipt of a 2B/outcome. So, we could
		// test to see whether the outcome fails the "newer" test below,
		// but this is flawed: we can't act on the outcome until we have
		// consensus as an acceptor can change its mind. So we then have
		// a race - we could have reached consensus (in fact further -
		// allAgreed), sent back the TSC, and deleted the record. But
		// due to a network issue at this point, just before the TSC
		// arrives at one of the acceptors, the acceptor decides to send
		// us a duplicate 2B/outcome. Without keeping a total history of
		// all outcomes we've received, which would be very expensive, I
		// can't quite see how to avoid accidentally creating a new
		// subscriptionUpdate in such a case, or how we could ever clear
		// it up. TODO.

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

		if sm.terminating {
			sm.TransactionRecord.terminate()
		}

		if newer {
			err = sm.consumer(txn, sm.TransactionRecord)
		} else {
			utils.DebugLog(sm.logger, "debug", "Ignoring non-newer outcome.", "SubId", sm.subId, "TxnId", txn.Id)
		}

	} else if su.outcome != nil {
		senders.NewOneShotSender(sm.logger, paxos.MakeTxnSubmissionCompleteMsg(txn.Id, sm.subId), sm.connPub, sender)
	}

	if allAgreed {
		delete(sm.incomplete, *txn.Id)
	}

	return err
}
