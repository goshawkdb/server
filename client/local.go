package client

import (
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/types"
	loco "goshawkdb.io/server/types/localconnection"
	"goshawkdb.io/server/utils/binarybackoff"
	ch "goshawkdb.io/server/utils/consistenthash"
	"goshawkdb.io/server/utils/txnreader"
)

type LocalTxnCompletionContinuation func(*txnreader.TxnReader, *msgs.Outcome, error) error

func (cont LocalTxnCompletionContinuation) Committed(txn *txnreader.TxnReader, tr *TransactionRecord) error {
	return cont(txn, tr.outcome, nil)
}

func (cont LocalTxnCompletionContinuation) Aborted(txn *txnreader.TxnReader, tr *TransactionRecord) error {
	return cont(txn, tr.outcome, nil)
}

func (ts *TransactionSubmitter) SubmitLocalServerTransaction(txnId *common.TxnId, txn *msgs.Txn, subscriptionConsumer SubscriptionConsumer, active common.RMIds, bbe *binarybackoff.BinaryBackoffEngine, cont LocalTxnCompletionContinuation) {
	tr := &TransactionRecord{
		TransactionSubmitter:       ts,
		transactionOutcomeReceiver: cont,
		Id:     txnId,
		origId: txnId,
		server: txn,
		active: active,
		bbe:    bbe,
	}
	if subscriptionConsumer != nil {
		tr.subManager = NewSubscriptionManager(txnId, tr, subscriptionConsumer)
	}
	ts.AddTransactionRecord(tr)
	tr.Submit()
}

func (ts *TransactionSubmitter) SubmitLocalClientTransaction(txnId *common.TxnId, txn *cmsgs.ClientTxn, isTopologyTxn bool, roots map[common.VarUUId]*types.PosCapVer, translationCallback loco.TranslationCallback, cont LocalTxnCompletionContinuation) error {
	if ts.topology.IsBlank() {
		ts.bufferedSubmissions = append(ts.bufferedSubmissions, func() {
			ts.SubmitLocalClientTransaction(txnId, txn, isTopologyTxn, roots, translationCallback, cont)
		})
		return nil
	} else {
		tr := &TransactionRecord{
			TransactionSubmitter:       ts,
			transactionOutcomeReceiver: cont,
			cache:  NewCache(ts.rng, roots),
			Id:     txnId,
			origId: txnId,
			client: txn,
		}
		tr.cache.SetResolver(ch.NewResolver(ts.topology.RMs, ts.topology.TwoFInc))
		if err := tr.formServerTxn(translationCallback, isTopologyTxn); err != nil {
			return cont(nil, nil, err)
		}
		ts.AddTransactionRecord(tr)
		tr.Submit()
		return nil
	}
}
