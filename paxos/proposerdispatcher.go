package paxos

import (
	"fmt"
	"github.com/go-kit/kit/log"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
	eng "goshawkdb.io/server/txnengine"
	"goshawkdb.io/server/types/connectionmanager"
	"goshawkdb.io/server/utils"
)

type ProposerDispatcher struct {
	dispatcher.Dispatcher
	logger           log.Logger
	proposermanagers []*ProposerManager
}

func NewProposerDispatcher(count uint8, rmId common.RMId, bootCount uint32, cm connectionmanager.ConnectionManager, db *db.Databases, varDispatcher *eng.VarDispatcher, logger log.Logger) *ProposerDispatcher {
	pd := &ProposerDispatcher{
		logger:           log.With(logger, "subsystem", "proposerDispatcher"),
		proposermanagers: make([]*ProposerManager, count),
	}
	logger = log.With(logger, "subsystem", "proposerManager")
	pd.Dispatcher.Init(count, logger)
	for idx, exe := range pd.Executors {
		pd.proposermanagers[idx] = NewProposerManager(exe, rmId, bootCount, cm, db, varDispatcher,
			log.With(logger, "instance", idx))
	}
	pd.loadFromDisk(db)
	return pd
}

func (pd *ProposerDispatcher) TxnReceived(sender common.RMId, txn *utils.TxnReader) {
	txnId := txn.Id
	pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.TxnReceived(sender, txn) })
}

func (pd *ProposerDispatcher) OneBTxnVotesReceived(sender common.RMId, oneBTxnVotes msgs.OneBTxnVotes) {
	txnId := common.MakeTxnId(oneBTxnVotes.TxnId())
	pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.OneBTxnVotesReceived(sender, txnId, oneBTxnVotes) })
}

func (pd *ProposerDispatcher) TwoBTxnVotesReceived(sender common.RMId, twoBTxnVotes msgs.TwoBTxnVotes) {
	var txnId *common.TxnId
	var txn *utils.TxnReader
	switch twoBTxnVotes.Which() {
	case msgs.TWOBTXNVOTES_FAILURES:
		txnId = common.MakeTxnId(twoBTxnVotes.Failures().TxnId())
	case msgs.TWOBTXNVOTES_OUTCOME:
		txn = utils.TxnReaderFromData(twoBTxnVotes.Outcome().Txn())
		txnId = txn.Id
	default:
		panic(fmt.Sprintf("Unexpected 2BVotes type: %v", twoBTxnVotes.Which()))
	}
	pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.TwoBTxnVotesReceived(sender, txnId, txn, twoBTxnVotes) })
}

func (pd *ProposerDispatcher) TxnGloballyCompleteReceived(sender common.RMId, tgc msgs.TxnGloballyComplete) {
	txnId := common.MakeTxnId(tgc.TxnId())
	pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.TxnGloballyCompleteReceived(sender, txnId) })
}

func (pd *ProposerDispatcher) TxnSubmissionAbortReceived(sender common.RMId, tsa msgs.TxnSubmissionAbort) {
	txnId := common.MakeTxnId(tsa.TxnId())
	pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.TxnSubmissionAbortReceived(sender, txnId) })
}

func (pd *ProposerDispatcher) ImmigrationReceived(migration msgs.Migration, stateChange eng.TxnLocalStateChange) {
	elemsList := migration.Elems()
	elemsCount := elemsList.Len()
	for idx := 0; idx < elemsCount; idx++ {
		elem := elemsList.At(idx)
		txn := utils.TxnReaderFromData(elem.Txn())
		txnId := txn.Id
		varCaps := elem.Vars()
		pd.withProposerManager(txnId, func(pm *ProposerManager) { pm.ImmigrationReceived(txn, varCaps, stateChange) })
	}
}

func (pd *ProposerDispatcher) SetMetrics(metrics *ProposerMetrics) {
	for idx, exe := range pd.Executors {
		manager := pd.proposermanagers[idx]
		exe.EnqueueFuncAsync(func() (bool, error) {
			manager.SetMetrics(metrics)
			return false, nil
		})
	}
}

func (pd *ProposerDispatcher) Status(sc *utils.StatusConsumer) {
	sc.Emit("Proposers")
	for idx, exe := range pd.Executors {
		s := sc.Fork()
		s.Emit(fmt.Sprintf("Proposer Manager %v", idx))
		manager := pd.proposermanagers[idx]
		exe.EnqueueFuncAsync(func() (bool, error) {
			manager.Status(s)
			return false, nil
		})
	}
	sc.Join()
}

func (pd *ProposerDispatcher) loadFromDisk(db *db.Databases) {
	res, err := db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		res, _ := rtxn.WithCursor(db.Proposers, func(cursor *mdbs.Cursor) interface{} {
			// cursor.Get returns a copy of the data. So it's fine for us
			// to store and process this later - it's not about to be
			// overwritten on disk.
			proposerStates := make(map[*common.TxnId][]byte)
			txnIdData, proposerState, err := cursor.Get(nil, nil, mdb.FIRST)
			for ; err == nil; txnIdData, proposerState, err = cursor.Get(nil, nil, mdb.NEXT) {
				txnId := common.MakeTxnId(txnIdData)
				proposerStates[txnId] = proposerState
			}
			if err == mdb.NotFound {
				// fine, we just fell off the end as expected.
				return proposerStates
			} else {
				cursor.Error(err)
				return nil
			}
		})
		return res
	}).ResultError()
	if err != nil {
		panic(fmt.Sprintf("ProposerDispatcher error loading from disk: %v", err))
	} else if res != nil {
		proposerStates := res.(map[*common.TxnId][]byte)
		for txnId, proposerState := range proposerStates {
			proposerStateCopy := proposerState
			txnIdCopy := txnId
			pd.withProposerManager(txnIdCopy, func(pm *ProposerManager) {
				if err := pm.loadFromData(txnIdCopy, proposerStateCopy); err != nil {
					panic(fmt.Sprintf("ProposerDispatcher error loading %v from disk: %v\n", txnIdCopy, err))
				}
			})
		}
		pd.logger.Log("msg", "Loaded proposers from disk.", "count", len(proposerStates))
	}
}

func (pd *ProposerDispatcher) withProposerManager(txnId *common.TxnId, fun func(*ProposerManager)) bool {
	idx := uint8(txnId[server.MostRandomByteIndex]) % pd.ExecutorCount
	exe := pd.Executors[idx]
	manager := pd.proposermanagers[idx]
	return exe.EnqueueFuncAsync(func() (bool, error) {
		fun(manager)
		return false, nil
	})
}
