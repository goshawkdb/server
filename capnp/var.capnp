using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xc3ce226b914ee1eb;

using VC = import "vectorclock.capnp";

struct Var {
  id              @0: Data;
  positions       @1: List(UInt8);
  writeTxnId      @2: Data;
  writeTxnClock   @3: VC.VectorClock;
  writesClock     @4: VC.VectorClock;
}

struct VarIdPos {
  id        @0: Data;
  positions @1: List(UInt8);
  capabilities :group {
    value :group {
      read  @2: Bool;
      write @3: Bool;
    }
    references :group {
      read :union {
        all  @4: Void;
        only @5: List(UInt32);
      }
      write :union {
        all  @6: Void;
        only @7: List(UInt32);
      }
    }
  }
}
