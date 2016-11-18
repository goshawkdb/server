using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0x83d51cd76711395c;

using Outcome = import "outcome.capnp";
using Var = import "var.capnp";

struct Migration {
  version @0: UInt32;
  elems   @1: List(MigrationElement);
}

struct MigrationComplete {
  version  @0: UInt32;
}

struct MigrationElement {
  txn  @0: Data;
  vars @1: List(Var.Var);
}
