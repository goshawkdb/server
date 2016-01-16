package paxos

import (
	"encoding/binary"
	"fmt"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/db"
	eng "goshawkdb.io/server/txnengine"
)

type Dispatchers struct {
	disk               *mdbs.MDBServer
	AcceptorDispatcher *AcceptorDispatcher
	VarDispatcher      *eng.VarDispatcher
	ProposerDispatcher *ProposerDispatcher
	connectionManager  ConnectionManager
}

func NewDispatchers(cm ConnectionManager, rmId common.RMId, count uint8, disk *mdbs.MDBServer, lc eng.LocalConnection) *Dispatchers {
	// It actually doesn't matter at this point what order we start up
	// the acceptors. This is because we are called from the
	// ConnectionManager constructor, and its actor loop hasn't been
	// started at this point. Thus whilst AddSender msgs can be sent,
	// none will be processed until after we return from this. So an
	// acceptor sending 2B msgs to a proposer will not get sent until
	// after all the proposers have been loaded off disk.

	d := &Dispatchers{
		disk:               disk,
		AcceptorDispatcher: NewAcceptorDispatcher(cm, count, disk),
		VarDispatcher:      eng.NewVarDispatcher(count, disk, lc),
		connectionManager:  cm,
	}
	d.ProposerDispatcher = NewProposerDispatcher(count, rmId, d.VarDispatcher, cm, disk)

	return d
}

func (d *Dispatchers) DispatchMessage(sender common.RMId, msgType msgs.Message_Which, msg *msgs.Message) {
	switch msgType {
	case msgs.MESSAGE_TXNSUBMISSION:
		txn := msg.TxnSubmission()
		d.ProposerDispatcher.TxnReceived(sender, &txn)
	case msgs.MESSAGE_SUBMISSIONOUTCOME:
		outcome := msg.SubmissionOutcome()
		txnId := common.MakeTxnId(outcome.Txn().Id())
		connNumber := binary.BigEndian.Uint32(txnId[8:12])
		bootNumber := binary.BigEndian.Uint32(txnId[12:16])
		if conn := d.connectionManager.GetClient(bootNumber, connNumber); conn == nil {
			NewOneShotSender(MakeTxnSubmissionCompleteMsg(txnId), d.connectionManager, sender)
		} else {
			conn.SubmissionOutcomeReceived(sender, txnId, &outcome)
			return
		}
	case msgs.MESSAGE_SUBMISSIONCOMPLETE:
		tsc := msg.SubmissionComplete()
		d.AcceptorDispatcher.TxnSubmissionCompleteReceived(sender, &tsc)
	case msgs.MESSAGE_SUBMISSIONABORT:
		tsa := msg.SubmissionAbort()
		d.ProposerDispatcher.TxnSubmissionAbortReceived(sender, &tsa)
	case msgs.MESSAGE_ONEATXNVOTES:
		oneATxnVotes := msg.OneATxnVotes()
		d.AcceptorDispatcher.OneATxnVotesReceived(sender, &oneATxnVotes)
	case msgs.MESSAGE_ONEBTXNVOTES:
		oneBTxnVotes := msg.OneBTxnVotes()
		d.ProposerDispatcher.OneBTxnVotesReceived(sender, &oneBTxnVotes)
	case msgs.MESSAGE_TWOATXNVOTES:
		twoATxnVotes := msg.TwoATxnVotes()
		d.AcceptorDispatcher.TwoATxnVotesReceived(sender, &twoATxnVotes)
	case msgs.MESSAGE_TWOBTXNVOTES:
		twoBTxnVotes := msg.TwoBTxnVotes()
		d.ProposerDispatcher.TwoBTxnVotesReceived(sender, &twoBTxnVotes)
	case msgs.MESSAGE_TXNLOCALLYCOMPLETE:
		tlc := msg.TxnLocallyComplete()
		d.AcceptorDispatcher.TxnLocallyCompleteReceived(sender, &tlc)
	case msgs.MESSAGE_TXNGLOBALLYCOMPLETE:
		tgc := msg.TxnGloballyComplete()
		d.ProposerDispatcher.TxnGloballyCompleteReceived(sender, &tgc)
	default:
		panic(fmt.Sprintf("Unexpected message received from %v", sender))
	}
}

func (d *Dispatchers) IsDatabaseEmpty() (bool, error) {
	res, err := d.disk.ReadonlyTransaction(func(rtxn *mdbs.RTxn) interface{} {
		res, _ := rtxn.WithCursor(db.DB.Transactions, func(cursor *mdbs.Cursor) interface{} {
			_, _, err := cursor.Get(nil, nil, mdb.FIRST)
			return err == mdb.NotFound
		})
		return res
	}).ResultError()
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}
