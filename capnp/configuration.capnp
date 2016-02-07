using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xbbc717d787db5c5f;

struct Configuration {
  clusterId          @0: Text;
  version            @1: UInt32;
  hosts              @2: List(Text);
  f                  @3: UInt8;
  maxRMCount         @4: UInt8;
  asyncFlush         @5: Bool;
  rms                @6: List(UInt32);
  rmsRemoved         @7: List(UInt32);
  fingerprints       @8: List(Data);
  union {
    transitioningTo :group {
      configuration  @9: Configuration;
      installedOnAll @10: Bool;
    }
    stable           @11: Void;
  }
}
