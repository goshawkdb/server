using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0x960e5f709149380d;

using Txn = import "transaction.capnp";

struct Ballot {
  varId @0: Data;
  clock @1: Data;
  vote  @2: Vote;
}

struct Vote {
  union {
    commit                @0: Void;
    abortBadRead :group {
      txnId      @1: Data;
      txnActions @2: List(Txn.Action);
    }
    abortDeadlock         @3: Void;
  }
}
