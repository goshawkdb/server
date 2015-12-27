using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xb30c67519ab66651;

using PTV = import "paxostxnvote.capnp";
using Outcome = import "outcome.capnp";
using Txn = import "transaction.capnp";
using TxnCompletion = import "txncompletion.capnp";

struct Message {
  union {
    heartbeat           @0:  Void;
    txnSubmission       @1:  Txn.Txn;
    submissionOutcome   @2:  Outcome.Outcome;
    submissionComplete  @3:  TxnCompletion.TxnSubmissionComplete;
    submissionAbort     @4:  TxnCompletion.TxnSubmissionAbort;
    oneATxnVotes        @5:  PTV.OneATxnVotes;
    oneBTxnVotes        @6:  PTV.OneBTxnVotes;
    twoATxnVotes        @7:  PTV.TwoATxnVotes;
    twoBTxnVotes        @8: PTV.TwoBTxnVotes;
    txnLocallyComplete  @9: TxnCompletion.TxnLocallyComplete;
    txnGloballyComplete @10: TxnCompletion.TxnGloballyComplete;
  }
}
