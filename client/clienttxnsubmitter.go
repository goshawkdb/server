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
	"time"
)

type ClientTxnCompletionConsumer func(*cmsgs.ClientTxnOutcome, error)

type ClientTxnSubmitter struct {
	*SimpleTxnSubmitter
	versionCache versionCache
	txnLive      bool
	initialDelay time.Duration
}

func NewClientTxnSubmitter(rmId common.RMId, bootCount uint32, cm paxos.ConnectionManager) *ClientTxnSubmitter {
	return &ClientTxnSubmitter{
		SimpleTxnSubmitter: NewSimpleTxnSubmitter(rmId, bootCount, cm),
		versionCache:       NewVersionCache(),
		txnLive:            false,
		initialDelay:       time.Duration(0),
	}
}

func (cts *ClientTxnSubmitter) Status(sc *server.StatusConsumer) {
	sc.Emit(fmt.Sprintf("ClientTxnSubmitter: txnLive? %v", cts.txnLive))
	cts.SimpleTxnSubmitter.Status(sc.Fork())
	sc.Join()
}

func (cts *ClientTxnSubmitter) SubmitClientTransaction(ctxnCap *cmsgs.ClientTxn, continuation ClientTxnCompletionConsumer) {
	if cts.txnLive {
		continuation(nil, fmt.Errorf("Cannot submit client as a live txn already exists"))
		return
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
	retryCount := 0

	var cont TxnCompletionConsumer
	cont = func(txnId *common.TxnId, outcome *msgs.Outcome, err error) {
		if outcome == nil || err != nil { // node is shutting down or error
			cts.txnLive = false
			continuation(nil, err)
			return
		}
		end := time.Now()
		elapsed := end.Sub(start)
		start = end
		switch outcome.Which() {
		case msgs.OUTCOME_COMMIT:
			cts.versionCache.UpdateFromCommit(txnId, outcome)
			clientOutcome.SetFinalId(txnId[:])
			clientOutcome.SetCommit()
			cts.addCreatesToCache(outcome)
			cts.txnLive = false
			cts.initialDelay = delay >> 1
			fmt.Printf("¬%v ", retryCount)
			continuation(&clientOutcome, nil)
			return

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
					fmt.Printf("¬%v ", retryCount)
					continuation(&clientOutcome, nil)
					return
				}
			}
			server.Log("Resubmitting", txnId, "; orig resubmit?", abort.Which() == msgs.OUTCOMEABORT_RESUBMIT)
			retryCount++

			delay = delay + time.Duration(cts.rng.Intn(int(elapsed)))
			if delay > server.SubmissionMaxSubmitDelay {
				delay = server.SubmissionMaxSubmitDelay + time.Duration(cts.rng.Intn(int(server.SubmissionMaxSubmitDelay)))
			}
			fmt.Printf("%v|%v ", retryCount, delay)

			curTxnIdNum := binary.BigEndian.Uint64(txnId[:8])
			curTxnIdNum += 1 + uint64(cts.rng.Intn(8))
			binary.BigEndian.PutUint64(curTxnId[:8], curTxnIdNum)
			ctxnCap.SetId(curTxnId[:])

			cts.SimpleTxnSubmitter.SubmitClientTransaction(ctxnCap, cont, delay, false)
		}
	}

	cts.txnLive = true
	fmt.Printf("%v|%v ", retryCount, delay)
	cts.SimpleTxnSubmitter.SubmitClientTransaction(ctxnCap, cont, delay, false)
}

func (cts *ClientTxnSubmitter) addCreatesToCache(outcome *msgs.Outcome) {
	actions := outcome.Txn().Actions()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if action.Which() == msgs.ACTION_CREATE {
			varUUId := common.MakeVarUUId(action.VarId())
			positions := common.Positions(action.Create().Positions())
			cts.hashCache.AddPosition(varUUId, &positions)
		}
	}
}

func (cts *ClientTxnSubmitter) translateUpdates(seg *capn.Segment, updates map[*msgs.Update][]*msgs.Action) cmsgs.ClientUpdate_List {
	clientUpdates := cmsgs.NewClientUpdateList(seg, len(updates))
	idx := 0
	for update, actions := range updates {
		clientUpdate := clientUpdates.At(idx)
		idx++
		clientUpdate.SetVersion(update.TxnId())
		clientActions := cmsgs.NewClientActionList(seg, len(actions))
		clientUpdate.SetActions(clientActions)

		for idy, action := range actions {
			clientAction := clientActions.At(idy)
			clientAction.SetVarId(action.VarId())
			switch action.Which() {
			case msgs.ACTION_MISSING:
				clientAction.SetDelete()
			case msgs.ACTION_WRITE:
				clientAction.SetWrite()
				write := action.Write()
				clientWrite := clientAction.Write()
				clientWrite.SetValue(write.Value())
				references := write.References()
				clientReferences := seg.NewDataList(references.Len())
				clientWrite.SetReferences(clientReferences)
				for idz, n := 0, references.Len(); idz < n; idz++ {
					ref := references.At(idz)
					clientReferences.Set(idz, ref.Id())
					positions := common.Positions(ref.Positions())
					cts.hashCache.AddPosition(common.MakeVarUUId(ref.Id()), &positions)
				}
			default:
				panic(fmt.Sprintf("Unexpected action type: %v", action.Which()))
			}
		}
	}
	return clientUpdates
}
