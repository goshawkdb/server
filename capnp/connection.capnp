using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xb30c67519ab66651;

using PTV = import "paxostxnvote.capnp";
using Outcome = import "outcome.capnp";
using Txn = import "transaction.capnp";
using TxnCompletion = import "txncompletion.capnp";

struct HelloServerFromServer {
 localHost @0: Text;
 rmId      @1: UInt32;
 bootCount @2: UInt32;
 tieBreak  @3: UInt32;
 clusterId @4: Text;
 rootId    @5: Data;
}

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
    twoBTxnVotes        @8:  PTV.TwoBTxnVotes;
    txnLocallyComplete  @9:  TxnCompletion.TxnLocallyComplete;
    txnGloballyComplete @10: TxnCompletion.TxnGloballyComplete;
    connectionError     @11: Text;
  }
}
