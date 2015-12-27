package paxos

import (
	"fmt"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	eng "goshawkdb.io/server/txnengine"
	"log"
)

type ProposerDispatcher struct {
	dispatcher.Dispatcher
	proposermanagers []*ProposerManager
}

func NewProposerDispatcher(count uint8, rmId common.RMId, varDispatcher *eng.VarDispatcher, cm ConnectionManager, server *mdbs.MDBServer) *ProposerDispatcher {
	pd := &ProposerDispatcher{
		proposermanagers: make([]*ProposerManager, count),
	}
	pd.Dispatcher.Init(count)
	for idx, exe := range pd.Executors {
		pd.proposermanagers[idx] = NewProposerManager(rmId, exe, varDispatcher, cm, server)
	}
	pd.loadFromDisk(server)
	return pd
}

func (pd *ProposerDispatcher) TxnReceived(sender common.RMId, txn *msgs.Txn) {
	txnId := common.MakeTxnId(txn.Id())
	pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.TxnReceived(txnId, txn) })
}

func (pd *ProposerDispatcher) OneBTxnVotesReceived(sender common.RMId, oneBTxnVotes *msgs.OneBTxnVotes) {
	txnId := common.MakeTxnId(oneBTxnVotes.TxnId())
	pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.OneBTxnVotesReceived(sender, txnId, oneBTxnVotes) })
}

func (pd *ProposerDispatcher) TwoBTxnVotesReceived(sender common.RMId, twoBTxnVotes *msgs.TwoBTxnVotes) {
	var txnId *common.TxnId
	switch twoBTxnVotes.Which() {
	case msgs.TWOBTXNVOTES_FAILURES:
		txnId = common.MakeTxnId(twoBTxnVotes.Failures().TxnId())
	case msgs.TWOBTXNVOTES_OUTCOME:
		txnId = common.MakeTxnId(twoBTxnVotes.Outcome().Txn().Id())
	default:
		panic(fmt.Sprintf("Unexpected 2BVotes type: %v", twoBTxnVotes.Which()))
	}
	pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.TwoBTxnVotesReceived(sender, txnId, twoBTxnVotes) })
}

func (pd *ProposerDispatcher) TxnGloballyCompleteReceived(sender common.RMId, tgc *msgs.TxnGloballyComplete) {
	txnId := common.MakeTxnId(tgc.TxnId())
	pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.TxnGloballyCompleteReceived(sender, txnId) })
}

func (pd *ProposerDispatcher) TxnSubmissionAbortReceived(sender common.RMId, tsa *msgs.TxnSubmissionAbort) {
	txnId := common.MakeTxnId(tsa.TxnId())
	pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.TxnSubmissionAbortReceived(sender, txnId) })
}

func (pd *ProposerDispatcher) Status(sc *server.StatusConsumer) {
	sc.Emit("Proposers")
	for idx, executor := range pd.Executors {
		s := sc.Fork()
		s.Emit(fmt.Sprintf("Proposer Manager %v", idx))
		manager := pd.proposermanagers[idx]
		executor.Enqueue(func() { manager.Status(s) })
	}
	sc.Join()
}

func (pd *ProposerDispatcher) loadFromDisk(server *mdbs.MDBServer) {
	res, err := server.ReadonlyTransaction(func(rtxn *mdbs.RTxn) (interface{}, error) {
		return rtxn.WithCursor(db.DB.Proposers, func(cursor *mdb.Cursor) (interface{}, error) {
			// cursor.Get returns a copy of the data. So it's fine for us
			// to store and process this later - it's not about to be
			// overwritten on disk.
			count := 0
			txnIdData, proposerState, err := cursor.Get(nil, nil, mdb.FIRST)
			for ; err == nil; txnIdData, proposerState, err = cursor.Get(nil, nil, mdb.NEXT) {
				count++
				txnId := common.MakeTxnId(txnIdData)
				proposerStateCopy := proposerState
				pd.withProposerManager(txnId, func(pm *ProposerManager) {
					pm.loadFromData(txnId, proposerStateCopy)
				})
			}
			if err == mdb.NotFound {
				// fine, we just fell off the end as expected.
				return count, nil
			} else {
				return count, err
			}
		})
	}).ResultError()
	if err == nil {
		log.Printf("Loaded %v proposers from disk\n", res.(int))
	} else {
		log.Println("ProposerDispatcher error loading from disk:", err)
	}
}

func (pd *ProposerDispatcher) withProposerManager(txnId *common.TxnId, fun func(*ProposerManager)) bool {
	idx := uint8(txnId[server.MostRandomByteIndex]) % pd.ExecutorCount
	executor := pd.Executors[idx]
	manager := pd.proposermanagers[idx]
	return executor.Enqueue(func() { fun(manager) })
}
