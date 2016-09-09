package client

import (
	"encoding/binary"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"time"
)

type ClientTxnCompletionConsumer func(*cmsgs.ClientTxnOutcome, error) error

type ClientTxnSubmitter struct {
	*SimpleTxnSubmitter
	versionCache versionCache
	txnLive      bool
	initialDelay time.Duration
}

func NewClientTxnSubmitter(rmId common.RMId, bootCount uint32, roots map[common.VarUUId]*common.Capability, cm paxos.ConnectionManager) *ClientTxnSubmitter {
	return &ClientTxnSubmitter{
		SimpleTxnSubmitter: NewSimpleTxnSubmitter(rmId, bootCount, cm),
		versionCache:       NewVersionCache(roots),
		txnLive:            false,
		initialDelay:       time.Duration(0),
	}
}

func (cts *ClientTxnSubmitter) Status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("ClientTxnSubmitter: txnLive? %v", cts.txnLive))
	cts.SimpleTxnSubmitter.Status(sc.Fork())
	sc.Join()
}

func (cts *ClientTxnSubmitter) SubmitClientTransaction(ctxnCap *cmsgs.ClientTxn, continuation ClientTxnCompletionConsumer) error {
	if cts.txnLive {
		return continuation(nil, fmt.Errorf("Cannot submit client as a live txn already exists"))
	}

	if err := cts.versionCache.ValidateTransaction(ctxnCap); err != nil {
		return continuation(nil, err)
	}

	seg := capn.NewBuffer(nil)
	clientOutcome := cmsgs.NewClientTxnOutcome(seg)
	clientOutcome.SetId(ctxnCap.Id())

	curTxnId := common.MakeTxnId(ctxnCap.Id())

	delay := cts.initialDelay
	if delay < time.Millisecond {
		delay = time.Duration(0)
	}
	start := time.Now()

	var cont TxnCompletionConsumer
	cont = func(txn *eng.TxnReader, outcome *msgs.Outcome, err error) error {
		if outcome == nil || err != nil { // node is shutting down or error
			cts.txnLive = false
			return continuation(nil, err)
		}
		txnId := txn.Id
		end := time.Now()
		elapsed := end.Sub(start)
		start = end
		switch outcome.Which() {
		case msgs.OUTCOME_COMMIT:
			cts.versionCache.UpdateFromCommit(txn, outcome)
			clientOutcome.SetFinalId(txnId[:])
			clientOutcome.SetCommit()
			cts.addCreatesToCache(txn)
			cts.txnLive = false
			cts.initialDelay = delay >> 1
			return continuation(&clientOutcome, nil)

		default:
			abort := outcome.Abort()
			resubmit := abort.Which() == msgs.OUTCOMEABORT_RESUBMIT
			if !resubmit {
				updates := abort.Rerun()
				validUpdates := cts.versionCache.UpdateFromAbort(&updates)
				server.Log("Updates:", updates.Len(), "; valid: ", len(validUpdates))
				resubmit = len(validUpdates) == 0
				if !resubmit {
					clientOutcome.SetFinalId(txnId[:])
					clientOutcome.SetAbort(cts.translateUpdates(seg, validUpdates))
					cts.txnLive = false
					cts.initialDelay = delay >> 1
					return continuation(&clientOutcome, nil)
				}
			}
			server.Log("Resubmitting", txnId, "; orig resubmit?", abort.Which() == msgs.OUTCOMEABORT_RESUBMIT)

			delay = delay + time.Duration(cts.rng.Intn(int(elapsed)))
			if delay > server.SubmissionMaxSubmitDelay {
				delay = server.SubmissionMaxSubmitDelay + time.Duration(cts.rng.Intn(int(server.SubmissionMaxSubmitDelay)))
			}
			//fmt.Printf("%v ", delay)

			curTxnIdNum := binary.BigEndian.Uint64(txnId[:8])
			curTxnIdNum += 1 + uint64(cts.rng.Intn(8))
			binary.BigEndian.PutUint64(curTxnId[:8], curTxnIdNum)
			ctxnCap.SetId(curTxnId[:])

			return cts.SimpleTxnSubmitter.SubmitClientTransaction(ctxnCap, curTxnId, cont, delay, false, cts.versionCache)
		}
	}

	cts.txnLive = true
	// fmt.Printf("%v ", delay)
	return cts.SimpleTxnSubmitter.SubmitClientTransaction(ctxnCap, curTxnId, cont, delay, false, cts.versionCache)
}

func (cts *ClientTxnSubmitter) addCreatesToCache(txn *eng.TxnReader) {
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
