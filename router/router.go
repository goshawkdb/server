package router

import (
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/network"
	"goshawkdb.io/server/paxos"
	"goshawkdb.io/server/topologytransmogrifier"
	"goshawkdb.io/server/utils"
)

type Router struct {
	*paxos.Dispatchers
	connectionManager *network.ConnectionManager
	transmogrifier    *topologytransmogrifier.TopologyTransmogrifier
	logger            *log.Logger
}

func NewRouter(dispatchers *paxos.Dispatchers, connectionManager *network.ConnectionManager, transmogrifier *topologytransmogrifier.TopologyTransmogrifier, logger log.Logger) *Router {
	return &Router{
		Dispatchers:       dispatchers,
		connectionManager: connectionManager,
		transmogrifier:    transmogrifier,
		logger:            logger,
	}
}

func (r Router) Dispatch(sender common.RMId, msgType msgs.Message_Which, msg msgs.Message) {
	switch msgType {
	case msgs.MESSAGE_TXNSUBMISSION:
		r.ProposerDispatcher.TxnReceived(sender, utils.TxnReaderFromData(msg.TxnSubmission()))
	case msgs.MESSAGE_SUBMISSIONOUTCOME:
		outcome := msg.SubmissionOutcome()
		txn := utils.TxnReaderFromData(outcome.Txn())
		txnId := txn.Id
		connNumber := txnId.ConnectionCount()
		bootNumber := txnId.BootCount()
		if conn := r.connectionManager.GetClient(bootNumber, connNumber); conn == nil {
			// OSS is safe here - it's the default action on receipt of outcome for unknown client.
			utils.NewOneShotSender(r.logger, paxos.MakeTxnSubmissionCompleteMsg(txnId), r.connectionManager, sender)
		} else {
			conn.SubmissionOutcomeReceived(sender, txn, &outcome)
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
		if sender != cm.RMId {
			configCap := msg.TopologyChangeRequest()
			config := configuration.ConfigurationFromCap(configCap)
			r.transmogrifier.RequestConfigurationChange(config)
		}
	case msgs.MESSAGE_MIGRATION:
		migration := msg.Migration()
		r.transmogrifier.ImmigrationReceived(sender, migration)
	case msgs.MESSAGE_MIGRATIONCOMPLETE:
		migrationComplete := msg.MigrationComplete()
		r.transmogrifier.ImmigrationCompleteReceived(sender, migrationComplete)
	case msgs.MESSAGE_FLUSHED:
		r.connectionManager.ServerConnectionFlushed(sender)
	default:
		panic(fmt.Sprintf("Unexpected message received from %v (%v)", sender, msgType))
	}
}
