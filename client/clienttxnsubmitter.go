package client

import (
	"encoding/binary"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/types/actor"
	cconn "goshawkdb.io/server/types/connections/client"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/binarybackoff"
	"goshawkdb.io/server/utils/status"
	"goshawkdb.io/server/utils/txnreader"
	"math/rand"
	"time"
)

type ClientTxnCompletionConsumer func(*cmsgs.ClientTxnOutcome, error) error

type ClientTxnSubmitter struct {
	*SimpleTxnSubmitter
	versionCache *versionCache
	txnLive      bool
	rng          *rand.Rand
	backoff      *binarybackoff.BinaryBackoffEngine
	metrics      *cconn.ClientTxnMetrics
}

func NewClientTxnSubmitter(rmId common.RMId, bootCount uint32, roots map[common.VarUUId]*common.Capability, namespace []byte, cm sconn.ServerConnectionPublisher, actor actor.EnqueueActor, logger log.Logger, metrics *cconn.ClientTxnMetrics) *ClientTxnSubmitter {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	sts := NewSimpleTxnSubmitter(rmId, bootCount, cm, actor, rng, logger)
	return &ClientTxnSubmitter{
		SimpleTxnSubmitter: sts,
		versionCache:       NewVersionCache(roots, namespace),
		txnLive:            false,
		rng:                rng,
		backoff:            binarybackoff.NewBinaryBackoffEngine(rng, server.SubmissionMinSubmitDelay, server.SubmissionMaxSubmitDelay),
		metrics:            metrics,
	}
}

func (cts *ClientTxnSubmitter) Status(sc *status.StatusConsumer) {
	sc.Emit(fmt.Sprintf("ClientTxnSubmitter: txnLive? %v", cts.txnLive))
	cts.SimpleTxnSubmitter.Status(sc.Fork())
	sc.Join()
}

func (cts *ClientTxnSubmitter) SubmitClientTransaction(ctxnCap *cmsgs.ClientTxn, continuation ClientTxnCompletionConsumer) error {
	if cts.txnLive {
		return continuation(nil, fmt.Errorf("Cannot submit client as a live txn already exists"))
	}

	curTxnId := common.MakeTxnId(ctxnCap.Id())
	if err := cts.versionCache.ValidateTransaction(curTxnId, ctxnCap); err != nil {
		return continuation(nil, err)
	}

	if cts.metrics != nil {
		cts.metrics.TxnSubmit.Inc()
	}
	start := time.Now()
	resubmitCount := 1

	seg := capn.NewBuffer(nil)
	clientOutcome := cmsgs.NewClientTxnOutcome(seg)
	clientOutcome.SetId(ctxnCap.Id())

	cts.backoff.Shrink(server.SubmissionMinSubmitDelay)

	var cont TxnCompletionConsumer
	cont = func(txn *txnreader.TxnReader, outcome *msgs.Outcome, err error) error {
		if outcome == nil || err != nil { // node is shutting down or error
			cts.txnLive = false
			return continuation(nil, err)
		}
		if cts.metrics != nil {
			cts.metrics.TxnLatency.Observe(float64(int64(time.Now().Sub(start))) / float64(time.Second))
		}
		txnId := txn.Id
		switch outcome.Which() {
		case msgs.OUTCOME_COMMIT:
			cts.versionCache.UpdateFromCommit(txn, outcome)
			clientOutcome.SetFinalId(txnId[:])
			clientOutcome.SetCommit()
			cts.addCreatesToCache(txn)
			cts.txnLive = false
			if cts.metrics != nil {
				cts.metrics.TxnResubmit.Add(float64(resubmitCount))
			}
			return continuation(&clientOutcome, nil)

		default:
			abort := outcome.Abort()
			resubmit := abort.Which() == msgs.OUTCOMEABORT_RESUBMIT
			if !resubmit {
				updates := abort.Rerun()
				validUpdates := cts.versionCache.UpdateFromAbort(&updates)
				utils.DebugLog(cts.logger, "debug", "Txn Outcome.", "TxnId", txnId,
					"updatesLen", updates.Len(), "validLen", len(validUpdates))
				resubmit = len(validUpdates) == 0
				if !resubmit {
					clientOutcome.SetFinalId(txnId[:])
					clientOutcome.SetAbort(cts.translateUpdates(seg, validUpdates))
					cts.txnLive = false
					if cts.metrics != nil {
						cts.metrics.TxnRerun.Inc()
						cts.metrics.TxnResubmit.Add(float64(resubmitCount))
					}
					return continuation(&clientOutcome, nil)
				}
			}
			utils.DebugLog(cts.logger, "debug", "Resubmitting Txn.", "TxnId", txnId,
				"origResubmit", abort.Which() == msgs.OUTCOMEABORT_RESUBMIT)

			cts.backoff.Advance()
			//fmt.Printf("%v ", cts.backoff.Cur)
			resubmitCount++
			start = time.Now()

			curTxnIdNum := binary.BigEndian.Uint64(txnId[:8])
			curTxnIdNum += 1 + uint64(cts.rng.Intn(8))
			binary.BigEndian.PutUint64(curTxnId[:8], curTxnIdNum)
			newSeg := capn.NewBuffer(nil)
			newCtxnCap := cmsgs.NewClientTxn(newSeg)
			newCtxnCap.SetId(curTxnId[:])
			newCtxnCap.SetRetry(ctxnCap.Retry())
			newCtxnCap.SetActions(ctxnCap.Actions())

			return cts.SimpleTxnSubmitter.SubmitClientTransaction(nil, &newCtxnCap, curTxnId, cont, cts.backoff, false, cts.versionCache)
		}
	}

	cts.txnLive = true
	// fmt.Printf("%v ", delay)
	return cts.SimpleTxnSubmitter.SubmitClientTransaction(nil, ctxnCap, curTxnId, cont, cts.backoff, false, cts.versionCache)
}

func (cts *ClientTxnSubmitter) addCreatesToCache(txn *txnreader.TxnReader) {
	actions := txn.Actions(true).Actions()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if action.Which() == msgs.ACTION_CREATE {
			varUUId := common.MakeVarUUId(action.VarId())
			positions := common.Positions(action.Create().Positions())
			cts.hashCache.AddPosition(varUUId, &positions)
		}
	}
}

func (cts *ClientTxnSubmitter) translateUpdates(seg *capn.Segment, updates map[common.TxnId]*[]*update) cmsgs.ClientUpdate_List {
	clientUpdates := cmsgs.NewClientUpdateList(seg, len(updates))
	idx := 0
	for txnId, actions := range updates {
		clientUpdate := clientUpdates.At(idx)
		idx++
		clientUpdate.SetVersion(txnId[:])
		clientActions := cmsgs.NewClientActionList(seg, len(*actions))
		clientUpdate.SetActions(clientActions)

		for idy, action := range *actions {
			clientAction := clientActions.At(idy)
			action.AddToClientAction(cts.hashCache, seg, &clientAction)
		}
	}
	return clientUpdates
}
