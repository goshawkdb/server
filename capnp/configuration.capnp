using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xbbc717d787db5c5f;

using Common = import "../../common/capnp/capabilities.capnp";

struct Configuration {
  clusterId          @0: Text;
  clusterUUId        @1: UInt64;
  version            @2: UInt32;
  hosts              @3: List(Text);
  f                  @4: UInt8;
  maxRMCount         @5: UInt16;
  noSync             @6: Bool;
  rms                @7: List(UInt32);
  rmsRemoved         @8: List(UInt32);
  fingerprints       @9: List(Fingerprint);
  union {
    transitioningTo :group {
      configuration   @10: Configuration;
      allHosts        @11: List(Text);
      newRMIds        @12: List(UInt32);
      survivingRMIds  @13: List(UInt32);
      lostRMIds       @14: List(UInt32);
      rootIndices     @15: List(UInt32);
      installedOnNew  @16: Bool;
      pending         @17: List(ConditionPair);
    }
    stable           @18: Void;
  }
}

struct Fingerprint {
  sha256 @0: Data;
  roots  @1: List(Root);
}

struct Root {
  name       @0: Text;
  capability @1: Common.Capability;
}

struct ConditionPair {
  rmId      @0: UInt32;
  condition @1: Condition;
  suppliers @2: List(UInt32);
}

struct Condition {
  union {
    and       @0: Conjunction;
    or        @1: Disjunction;
    generator @2: Generator;
  }
}

struct Conjunction {
  left  @0: Condition;
  right @1: Condition;
}

struct Disjunction {
  left  @0: Condition;
  right @1: Condition;
}

struct Generator {
  rmId      @0: UInt32;
  useNext   @1: Bool;
  includes  @2: Bool;
}
