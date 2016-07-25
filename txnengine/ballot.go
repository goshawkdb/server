package txnengine

import (
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
)

type Vote msgs.Vote_Which

const (
	Commit        = Vote(msgs.VOTE_COMMIT)
	AbortBadRead  = Vote(msgs.VOTE_ABORTBADREAD)
	AbortDeadlock = Vote(msgs.VOTE_ABORTDEADLOCK)
)

func (v Vote) ToVoteEnum() msgs.VoteEnum {
	switch v {
	case AbortBadRead:
		return msgs.VOTEENUM_ABORTBADREAD
	case AbortDeadlock:
		return msgs.VOTEENUM_ABORTDEADLOCK
	default:
		return msgs.VOTEENUM_COMMIT
	}
}

type Ballot struct {
	VarUUId   *common.VarUUId
	ClockData []byte
	Vote      Vote
	BallotCap *msgs.Ballot
	VoteCap   *msgs.Vote
}

func NewBallot(vUUId *common.VarUUId, vote Vote, clock *VectorClock) *Ballot {
	ballot := &Ballot{
		VarUUId:   vUUId,
		Vote:      vote,
		BallotCap: nil,
		VoteCap:   nil,
	}
	if clock == nil {
		ballot.ClockData = []byte{}
	} else {
		ballot.ClockData = clock.AsData()
	}
	return ballot
}

func BallotFromCap(ballotCap *msgs.Ballot) *Ballot {
	voteCap := ballotCap.Vote()
	ballot := &Ballot{
		VarUUId:   common.MakeVarUUId(ballotCap.VarId()),
		ClockData: ballotCap.Clock(),
		Vote:      Vote(voteCap.Which()),
		BallotCap: ballotCap,
		VoteCap:   &voteCap,
	}
	return ballot
}

func (ballot *Ballot) Aborted() bool {
	return ballot.Vote != Commit
}

func (ballot *Ballot) CreateBadReadCap(txnId *common.TxnId, actions *msgs.Action_List) {
	seg := capn.NewBuffer(nil)
	voteCap := msgs.NewVote(seg)
	voteCap.SetAbortBadRead()
	badReadCap := voteCap.AbortBadRead()
	badReadCap.SetTxnId(txnId[:])
	badReadCap.SetTxnActions(*actions)
	ballot.VoteCap = &voteCap
	ballot.Vote = AbortBadRead
}

func (ballot *Ballot) AddToSeg(seg *capn.Segment) msgs.Ballot {
	ballotCap := msgs.NewBallot(seg)
	ballotCap.SetVarId(ballot.VarUUId[:])
	ballotCap.SetClock(ballot.ClockData)

	if ballot.VoteCap == nil {
		voteCap := msgs.NewVote(seg)
		ballot.VoteCap = &voteCap
		switch ballot.Vote {
		case Commit:
			voteCap.SetCommit()
		case AbortDeadlock:
			voteCap.SetAbortDeadlock()
		case AbortBadRead:
			voteCap.SetAbortBadRead()
		}
	}

	ballotCap.SetVote(*ballot.VoteCap)
	ballot.BallotCap = &ballotCap
	return ballotCap
}
