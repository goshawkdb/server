using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xefa5a1e88b6da9e3;

using Ballot  = import "ballot.capnp";
using Txn     = import "transaction.capnp";
using Outcome = import "outcome.capnp";

struct AcceptorState {
  txn       @0: Txn.Txn;
  outcome   @1: Outcome.Outcome;
  sendToAll @2: Bool;
  instances @3: List(InstancesForVar);
}

struct InstancesForVar {
  varId     @0: Data;
  instances @1: List(AcceptedInstance);
  result    @2: Ballot.Ballot;
}

struct AcceptedInstance {
  rmId        @0: UInt32;
  roundNumber @1: UInt64;
  ballot      @2: Ballot.Ballot;
}
