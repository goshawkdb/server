using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xe10cac715301f488;

struct Outcome {
  id  @0: List(OutcomeId);
  txn @1: Data;
  union {
    commit       @2: Data;
    abort :group {
      union {
        resubmit @3: Void;
        rerun    @4: List(Update);
      }
    }
  }
}

struct Update {
  txnId   @0: Data;
  actions @1: Data;
  clock   @2: Data;
}

struct OutcomeId {
  varId             @0: Data;
  acceptedInstances @1: List(AcceptedInstanceId);
}

struct AcceptedInstanceId {
  rmId @0: UInt32;
  vote @1: VoteEnum;
}

enum VoteEnum {
  commit        @0;
  abortBadRead  @1;
  abortDeadlock @2;
}
