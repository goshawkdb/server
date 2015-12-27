using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xd2704db828b80d1c;

struct TxnLocallyComplete {
  txnId @0: Data;
}

struct TxnGloballyComplete {
  txnId @0: Data;
}

struct TxnSubmissionComplete {
  txnId @0: Data;
}

struct TxnSubmissionAbort {
  txnId @0: Data;
}
