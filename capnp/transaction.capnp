using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xbc29bdc7c3fbad47;

using Var = import "var.capnp";

struct Txn {
  id                 @0: Data;
  isTopology         @1: Bool;
  actions            @2: Data;
  allocations        @3: List(Allocation);
  twoFInc            @4: UInt16;
  topologyVersion    @5: UInt32;
}

struct ActionListWrapper {
  actions @0: List(Action);
}

struct Action {
  varId @0: Data;
  value :union {
    missing @1: Void;
    create :group {
      positions  @2: List(UInt8);
      value      @3: Data;
      references @4: List(Var.VarIdPos);
    }
    existing :group {
      read   @5: Data;
      modify :union {
        not  @6: Void;
        roll @7: Void;
        write :group {
          value      @8: Data;
          references @9: List(Var.VarIdPos);
        }
      }
    }
  }
  meta :group {
    addSub @10: Bool; # requires create or read
    delSub @11: Data; # requires read
  }
}

struct Allocation {
  rmId          @0: UInt32;
  actionIndices @1: List(UInt16);
  active        @2: UInt32;
}
