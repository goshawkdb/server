using Go = import "../../common/capnp/go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/server/capnp");

@0xd3af64eb7d699620;

using Outcome = import "outcome.capnp";

struct OneATxnVotes {
  txnId     @0: Data;
  rmId      @1: UInt32;
  proposals @2: List(TxnVoteProposal);
}

struct OneBTxnVotes {
  txnId     @0: Data;
  rmId      @1: UInt32;
  promises  @2: List(TxnVotePromise);
}

struct TwoATxnVotes {
  txn            @0: Data;
  rmId           @1: UInt32;
  acceptRequests @2: List(TxnVoteAcceptRequest);
}

struct TwoBTxnVotes {
  union {
    failures :group {
      txnId     @0: Data;
      rmId      @1: UInt32;
      nacks     @2: List(TxnVoteTwoBFailure);
    }
    outcome @3: Outcome.Outcome;
  }
}

struct TxnVoteProposal {
  varId       @0: Data;
  roundNumber @1: UInt64;
}

struct TxnVotePromise {
  varId       @0: Data;
  roundNumber @1: UInt64;
  union {
    freeChoice @2: Void;
    accepted :group {
      roundNumber @3: UInt64;
      ballot      @4: Data;
    }
    roundNumberTooLow @5: UInt32;
  }
}

struct TxnVoteAcceptRequest {
  ballot      @0: Data;
  roundNumber @1: UInt64;
}

struct TxnVoteTwoBFailure {
  varId             @0: Data;
  roundNumber       @1: UInt64;
  roundNumberTooLow @2: UInt32;
}
