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
	"goshawkdb.io/server/types/connectionmanager"
	"goshawkdb.io/server/utils"
)

type AcceptorDispatcher struct {
	dispatcher.Dispatcher
	logger            log.Logger
	connectionManager connectionmanager.ConnectionManager
	acceptormanagers  []*AcceptorManager
}

func NewAcceptorDispatcher(count uint8, rmId common.RMId, cm connectionmanager.ConnectionManager, db *db.Databases, logger log.Logger) *AcceptorDispatcher {
	ad := &AcceptorDispatcher{
		logger:           log.With(logger, "subsystem", "acceptorDispatcher"),
		acceptormanagers: make([]*AcceptorManager, count),
	}
	logger = log.With(logger, "subsystem", "acceptorManager")
	ad.Dispatcher.Init(count, logger)
	for idx, exe := range ad.Executors {
		ad.acceptormanagers[idx] = NewAcceptorManager(rmId, exe, cm, db,
			log.With(logger, "instance", idx))
	}
	ad.loadFromDisk(db)
	return ad
}

func (ad *AcceptorDispatcher) OneATxnVotesReceived(sender common.RMId, oneATxnVotes msgs.OneATxnVotes) {
	txnId := common.MakeTxnId(oneATxnVotes.TxnId())
	ad.withAcceptorManager(txnId, func(am *AcceptorManager) { am.OneATxnVotesReceived(sender, txnId, oneATxnVotes) })
}

func (ad *AcceptorDispatcher) TwoATxnVotesReceived(sender common.RMId, twoATxnVotes msgs.TwoATxnVotes) {
	txn := utils.TxnReaderFromData(twoATxnVotes.Txn())
	txnId := txn.Id
	ad.withAcceptorManager(txnId, func(am *AcceptorManager) { am.TwoATxnVotesReceived(sender, txn, twoATxnVotes) })
}

func (ad *AcceptorDispatcher) TxnLocallyCompleteReceived(sender common.RMId, tlc msgs.TxnLocallyComplete) {
	txnId := common.MakeTxnId(tlc.TxnId())
	ad.withAcceptorManager(txnId, func(am *AcceptorManager) { am.TxnLocallyCompleteReceived(sender, txnId, tlc) })
}

func (ad *AcceptorDispatcher) TxnSubmissionCompleteReceived(sender common.RMId, tsc msgs.TxnSubmissionComplete) {
	txnId := common.MakeTxnId(tsc.TxnId())
	ad.withAcceptorManager(txnId, func(am *AcceptorManager) { am.TxnSubmissionCompleteReceived(sender, txnId, tsc) })
}

func (ad *AcceptorDispatcher) SetMetrics(metrics *AcceptorMetrics) {
	for idx, executor := range ad.Executors {
		manager := ad.acceptormanagers[idx]
		executor.EnqueueFuncAsync(func() (bool, error) {
			manager.SetMetrics(metrics)
			return false, nil
		})
	}
}

func (ad *AcceptorDispatcher) Status(sc *utils.StatusConsumer) {
	sc.Emit("Acceptors")
	for idx, executor := range ad.Executors {
		s := sc.Fork()
		s.Emit(fmt.Sprintf("Acceptor Manager %v", idx))
		manager := ad.acceptormanagers[idx]
		executor.EnqueueFuncAsync(func() (bool, error) {
			manager.Status(s)
			return false, nil
		})
	}
	sc.Join()
}

func (ad *AcceptorDispatcher) loadFromDisk(db *db.Databases) {
	res, err := db.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		res, _ := rtxn.WithCursor(db.BallotOutcomes, func(cursor *mdbs.Cursor) interface{} {
			// cursor.Get returns a copy of the data. So it's fine for us
			// to store and process this later - it's not about to be
			// overwritten on disk.
			acceptorStates := make(map[*common.TxnId][]byte)
			txnIdData, acceptorState, err := cursor.Get(nil, nil, mdb.FIRST)
			for ; err == nil; txnIdData, acceptorState, err = cursor.Get(nil, nil, mdb.NEXT) {
				txnId := common.MakeTxnId(txnIdData)
				acceptorStates[txnId] = acceptorState
			}
			if err == mdb.NotFound {
				// fine, we just fell off the end as expected.
				return acceptorStates
			} else {
				cursor.Error(err)
				return nil
			}
		})
		return res
	}).ResultError()
	if err != nil {
		panic(fmt.Sprintf("AcceptorDispatcher error loading from disk: %v", err))
	} else if res != nil {
		acceptorStates := res.(map[*common.TxnId][]byte)
		for txnId, acceptorState := range acceptorStates {
			acceptorStateCopy := acceptorState
			txnIdCopy := txnId
			ad.withAcceptorManager(txnIdCopy, func(am *AcceptorManager) {
				if err := am.loadFromData(txnIdCopy, acceptorStateCopy); err != nil {
					panic(fmt.Sprintf("AcceptorDispatcher error loading %v from disk: %v\n", txnIdCopy, err))
				}
			})
		}
		ad.logger.Log("msg", "Loaded acceptors from disk.", "count", len(acceptorStates))
	}
}

func (ad *AcceptorDispatcher) withAcceptorManager(txnId *common.TxnId, fun func(*AcceptorManager)) bool {
	idx := uint8(txnId[server.MostRandomByteIndex]) % ad.ExecutorCount
	executor := ad.Executors[idx]
	manager := ad.acceptormanagers[idx]
	return executor.EnqueueFuncAsync(func() (bool, error) {
		fun(manager)
		return false, nil
	})
}
