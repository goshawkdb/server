using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xefa5a1e88b6da9e3;

using Outcome = import "outcome.capnp";

struct AcceptorState {
  outcome   @0: Outcome.Outcome;
  sendToAll @1: Bool;
  instances @2: List(InstancesForVar);
}

struct InstancesForVar {
  varId     @0: Data;
  instances @1: List(AcceptedInstance);
  result    @2: Data;
}

struct AcceptedInstance {
  rmId        @0: UInt32;
  roundNumber @1: UInt64;
  ballot      @2: Data;
}
