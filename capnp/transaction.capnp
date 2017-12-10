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
  varId      @0: Data;
  version    @1: Data;
  positions  @2: List(UInt8);
  union {
    unmodified   @3: Void;
    modified :group {
      value      @4: Data;
      references @5: List(Var.VarIdPos);
    }
  }
  actionType @6: ActionType;
}

enum ActionType {
  create          @0;
  readOnly        @1;
  writeOnly       @2;
  readWrite       @3;
  missing         @4;
  roll            @5;
  addSubscription @6;
  delSubscription @7;
}

struct Allocation {
  rmId          @0: UInt32;
  actionIndices @1: List(UInt16);
  active        @2: UInt32;
}
