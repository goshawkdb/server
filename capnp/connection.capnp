using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xb30c67519ab66651;

using PTV = import "paxostxnvote.capnp";
using Outcome = import "outcome.capnp";
using Txn = import "transaction.capnp";
using TxnCompletion = import "txncompletion.capnp";
using Config = import "configuration.capnp";
using Migration = import "migration.capnp";

struct HelloServerFromServer {
 localHost   @0: Text;
 rmId        @1: UInt32;
 bootCount   @2: UInt32;
 tieBreak    @3: UInt32;
 clusterId   @4: Text;
 clusterUUId @5: UInt64;
}

struct Message {
  union {
    heartbeat             @0:  Void;
    connectionError       @1:  Text;
    txnSubmission         @2:  Txn.Txn;
    submissionOutcome     @3:  Outcome.Outcome;
    submissionComplete    @4:  TxnCompletion.TxnSubmissionComplete;
    submissionAbort       @5:  TxnCompletion.TxnSubmissionAbort;
    oneATxnVotes          @6:  PTV.OneATxnVotes;
    oneBTxnVotes          @7:  PTV.OneBTxnVotes;
    twoATxnVotes          @8:  PTV.TwoATxnVotes;
    twoBTxnVotes          @9:  PTV.TwoBTxnVotes;
    txnLocallyComplete    @10: TxnCompletion.TxnLocallyComplete;
    txnGloballyComplete   @11: TxnCompletion.TxnGloballyComplete;
    topologyChangeRequest @12: Config.Configuration;
    migration             @13: Migration.Migration;
    migrationComplete     @14: Migration.MigrationComplete;
  }
}
