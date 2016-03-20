using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0x83d51cd76711395c;

using Txn = import "transaction.capnp";
using Outcome = import "outcome.capnp";
using Var = import "var.capnp";

struct Migration {
  version @0: UInt32;
  txns    @1: List(Txn.Txn);
  vars    @2: List(Var.Var);
}

struct MigrationComplete {
  version  @0: UInt32;
}
