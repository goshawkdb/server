using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xb30c67519ab66651;

using PTV = import "paxostxnvote.capnp";
using Outcome = import "outcome.capnp";
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
    flushed               @1:  Void;
    connectionError       @2:  Text;
    txnSubmission         @3:  Data;
    submissionOutcome     @4:  Outcome.Outcome;
    submissionComplete    @5:  TxnCompletion.TxnSubmissionComplete;
    submissionAbort       @6:  TxnCompletion.TxnSubmissionAbort;
    oneATxnVotes          @7:  PTV.OneATxnVotes;
    oneBTxnVotes          @8:  PTV.OneBTxnVotes;
    twoATxnVotes          @9:  PTV.TwoATxnVotes;
    twoBTxnVotes          @10: PTV.TwoBTxnVotes;
    txnLocallyComplete    @11: TxnCompletion.TxnLocallyComplete;
    txnGloballyComplete   @12: TxnCompletion.TxnGloballyComplete;
    topologyChangeRequest @13: Config.Configuration;
    migration             @14: Migration.Migration;
    migrationComplete     @15: Migration.MigrationComplete;
  }
}
