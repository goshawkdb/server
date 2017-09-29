package router

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	"goshawkdb.io/server/types/connectionmanager"
	"goshawkdb.io/server/types/topology"
	"goshawkdb.io/server/utils/senders"
	"goshawkdb.io/server/utils/status"
	"goshawkdb.io/server/utils/txnreader"
)

type Router struct {
	self common.RMId
	*paxos.Dispatchers
	ConnectionManager connectionmanager.ConnectionManager
	Transmogrifier    topology.TopologyTransmogrifier
	logger            log.Logger
}

func NewRouter(rmId common.RMId, logger log.Logger) *Router {
	return &Router{
		self:   rmId,
		logger: logger,
	}
}

func (r Router) Dispatch(sender common.RMId, msgType msgs.Message_Which, msg msgs.Message) {
	switch msgType {
	case msgs.MESSAGE_TXNSUBMISSION:
		r.ProposerDispatcher.TxnReceived(sender, txnreader.TxnReaderFromData(msg.TxnSubmission()))
	case msgs.MESSAGE_SUBMISSIONOUTCOME:
		subOutcome := msg.SubmissionOutcome()
		outcome := subOutcome.Outcome()
		txn := txnreader.TxnReaderFromData(outcome.Txn())
		txnId := txn.Id
		subscribers := subOutcome.Subscribers()
		for idx, l := 0, subscribers.Len(); idx < l; idx++ {
			clientId := common.MakeClientId(subscribers.At(idx))
			connNumber := clientId.ConnectionCount()
			bootNumber := clientId.BootCount()
			if conn := r.ConnectionManager.GetClient(bootNumber, connNumber); conn == nil {
				// OSS is safe here - it's the default action on receipt of outcome for unknown client.
				senders.NewOneShotSender(r.logger, paxos.MakeTxnSubmissionCompleteMsg(txnId), r.ConnectionManager, sender)
			} else {
				conn.SubmissionOutcomeReceived(sender, txn, &outcome)
			}
		}
	case msgs.MESSAGE_SUBMISSIONCOMPLETE:
		tsc := msg.SubmissionComplete()
		r.AcceptorDispatcher.TxnSubmissionCompleteReceived(sender, tsc)
	case msgs.MESSAGE_SUBMISSIONABORT:
		tsa := msg.SubmissionAbort()
		r.ProposerDispatcher.TxnSubmissionAbortReceived(sender, tsa)
	case msgs.MESSAGE_ONEATXNVOTES:
		oneATxnVotes := msg.OneATxnVotes()
		r.AcceptorDispatcher.OneATxnVotesReceived(sender, oneATxnVotes)
	case msgs.MESSAGE_ONEBTXNVOTES:
		oneBTxnVotes := msg.OneBTxnVotes()
		r.ProposerDispatcher.OneBTxnVotesReceived(sender, oneBTxnVotes)
	case msgs.MESSAGE_TWOATXNVOTES:
		twoATxnVotes := msg.TwoATxnVotes()
		r.AcceptorDispatcher.TwoATxnVotesReceived(sender, twoATxnVotes)
	case msgs.MESSAGE_TWOBTXNVOTES:
		twoBTxnVotes := msg.TwoBTxnVotes()
		r.ProposerDispatcher.TwoBTxnVotesReceived(sender, twoBTxnVotes)
	case msgs.MESSAGE_TXNLOCALLYCOMPLETE:
		tlc := msg.TxnLocallyComplete()
		r.AcceptorDispatcher.TxnLocallyCompleteReceived(sender, tlc)
	case msgs.MESSAGE_TXNGLOBALLYCOMPLETE:
		tgc := msg.TxnGloballyComplete()
		r.ProposerDispatcher.TxnGloballyCompleteReceived(sender, tgc)
	case msgs.MESSAGE_TOPOLOGYCHANGEREQUEST:
		if sender != r.self {
			configCap := msg.TopologyChangeRequest()
			config := configuration.ConfigurationFromCap(configCap)
			r.Transmogrifier.RequestConfigurationChange(config)
		}
	case msgs.MESSAGE_MIGRATION:
		migration := msg.Migration()
		r.Transmogrifier.ImmigrationReceived(sender, migration)
	case msgs.MESSAGE_MIGRATIONCOMPLETE:
		migrationComplete := msg.MigrationComplete()
		r.Transmogrifier.ImmigrationCompleteReceived(sender, migrationComplete)
	case msgs.MESSAGE_FLUSHED: // coming from a remote - i.e. we've been flushed through the remote.
		r.ConnectionManager.ServerConnectionFlushed(sender)
	default:
		panic(fmt.Sprintf("Unexpected message received from %v (%v)", sender, msgType))
	}
}

func (r Router) Status(sc *status.StatusConsumer) {
	r.VarDispatcher.Status(sc.Fork())
	r.ProposerDispatcher.Status(sc.Fork())
	r.AcceptorDispatcher.Status(sc.Fork())
	sc.Join()
}

func (r Router) ShutdownSync() {
	r.AcceptorDispatcher.ShutdownSync()
	r.ProposerDispatcher.ShutdownSync()
	r.VarDispatcher.ShutdownSync()
}

func (r Router) Send(b []byte) {
	seg, _, err := capn.ReadFromMemoryZeroCopy(b)
	if err != nil {
		panic(fmt.Sprintf("Error in capnproto decode when sending to self! %v", err))
	}
	msg := msgs.ReadRootMessage(seg)
	r.Dispatch(r.self, msg.Which(), msg)
}
