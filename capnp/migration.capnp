using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0x83d51cd76711395c;

using Outcome = import "outcome.capnp";

struct Migration {
  version  @0: UInt32;
  outcomes @1: List(Outcome.Outcome);
}

struct MigrationComplete {
  version  @0: UInt32;
}
