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
	"log"
)

type AcceptorDispatcher struct {
	dispatcher.Dispatcher
	connectionManager ConnectionManager
	acceptormanagers  []*AcceptorManager
}

func NewAcceptorDispatcher(cm ConnectionManager, count uint8, server *mdbs.MDBServer) *AcceptorDispatcher {
	ad := &AcceptorDispatcher{
		acceptormanagers: make([]*AcceptorManager, count),
	}
	ad.Dispatcher.Init(count)
	for idx, exe := range ad.Executors {
		ad.acceptormanagers[idx] = NewAcceptorManager(exe, cm, server)
	}
	ad.loadFromDisk(server)
	return ad
}

func (ad *AcceptorDispatcher) OneATxnVotesReceived(sender common.RMId, oneATxnVotes *msgs.OneATxnVotes) {
	txnId := common.MakeTxnId(oneATxnVotes.TxnId())
	ad.withAcceptorManager(txnId, func(am *AcceptorManager) { am.OneATxnVotesReceived(sender, txnId, oneATxnVotes) })
}

func (ad *AcceptorDispatcher) TwoATxnVotesReceived(sender common.RMId, twoATxnVotes *msgs.TwoATxnVotes) {
	txnId := common.MakeTxnId(twoATxnVotes.Txn().Id())
	ad.withAcceptorManager(txnId, func(am *AcceptorManager) { am.TwoATxnVotesReceived(sender, txnId, twoATxnVotes) })
}

func (ad *AcceptorDispatcher) TxnLocallyCompleteReceived(sender common.RMId, tlc *msgs.TxnLocallyComplete) {
	txnId := common.MakeTxnId(tlc.TxnId())
	ad.withAcceptorManager(txnId, func(am *AcceptorManager) { am.TxnLocallyCompleteReceived(sender, txnId, tlc) })
}

func (ad *AcceptorDispatcher) TxnSubmissionCompleteReceived(sender common.RMId, tsc *msgs.TxnSubmissionComplete) {
	txnId := common.MakeTxnId(tsc.TxnId())
	ad.withAcceptorManager(txnId, func(am *AcceptorManager) { am.TxnSubmissionCompleteReceived(sender, txnId, tsc) })
}

func (ad *AcceptorDispatcher) Status(sc *server.StatusConsumer) {
	sc.Emit("Acceptors")
	for idx, executor := range ad.Executors {
		s := sc.Fork()
		s.Emit(fmt.Sprintf("Acceptor Manager %v", idx))
		manager := ad.acceptormanagers[idx]
		executor.Enqueue(func() { manager.Status(s) })
	}
	sc.Join()
}

func (ad *AcceptorDispatcher) loadFromDisk(server *mdbs.MDBServer) {
	res, err := server.ReadonlyTransaction(func(rtxn *mdbs.RTxn) (interface{}, error) {
		return rtxn.WithCursor(db.DB.BallotOutcomes, func(cursor *mdb.Cursor) (interface{}, error) {
			// cursor.Get returns a copy of the data. So it's fine for us
			// to store and process this later - it's not about to be
			// overwritten on disk.
			count := 0
			txnIdData, acceptorState, err := cursor.Get(nil, nil, mdb.FIRST)
			for ; err == nil; txnIdData, acceptorState, err = cursor.Get(nil, nil, mdb.NEXT) {
				count++
				txnId := common.MakeTxnId(txnIdData)
				acceptorStateCopy := acceptorState
				ad.withAcceptorManager(txnId, func(am *AcceptorManager) {
					am.loadFromData(txnId, acceptorStateCopy)
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
		log.Printf("Loaded %v acceptors from disk\n", res.(int))
	} else {
		log.Println("AcceptorDispatcher error loading from disk:", err)
	}
}

func (ad *AcceptorDispatcher) withAcceptorManager(txnId *common.TxnId, fun func(*AcceptorManager)) bool {
	idx := uint8(txnId[server.MostRandomByteIndex]) % ad.ExecutorCount
	executor := ad.Executors[idx]
	manager := ad.acceptormanagers[idx]
	return executor.Enqueue(func() { fun(manager) })
}
