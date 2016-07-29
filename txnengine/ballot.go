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
	Clock     *VectorClock
	Vote      Vote
	BallotCap *msgs.Ballot
	VoteCap   *msgs.Vote
}

type BallotBuilder struct {
	*Ballot
	Clock *VectorClockMutable
	seg   *capn.Segment
}

func BallotFromCap(ballotCap *msgs.Ballot) *Ballot {
	voteCap := ballotCap.Vote()
	return &Ballot{
		VarUUId:   common.MakeVarUUId(ballotCap.VarId()),
		Clock:     VectorClockFromData(ballotCap.Clock(), false),
		Vote:      Vote(voteCap.Which()),
		BallotCap: ballotCap,
		VoteCap:   &voteCap,
	}
}

func (ballot *Ballot) Aborted() bool {
	return ballot.Vote != Commit
}

func (ballot *Ballot) AddToSeg(seg *capn.Segment) msgs.Ballot {
	return *ballot.BallotCap
}

func NewBallotBuilder(vUUId *common.VarUUId, vote Vote, clock *VectorClockMutable) *BallotBuilder {
	ballot := &Ballot{
		VarUUId: vUUId,
		Vote:    vote,
	}
	return &BallotBuilder{
		Ballot: ballot,
		Clock:  clock,
	}
}

func (ballot *BallotBuilder) CreateBadReadCap(txnId *common.TxnId, actions *msgs.Action_List) *BallotBuilder {
	seg := capn.NewBuffer(nil)
	ballot.seg = seg
	voteCap := msgs.NewVote(seg)
	voteCap.SetAbortBadRead()
	badReadCap := voteCap.AbortBadRead()
	badReadCap.SetTxnId(txnId[:])
	badReadCap.SetTxnActions(*actions)
	ballot.VoteCap = &voteCap
	ballot.Vote = AbortBadRead
	return ballot
}

func (ballot *BallotBuilder) ToBallot() *Ballot {
	if ballot.BallotCap == nil {
		if ballot.seg == nil {
			ballot.seg = capn.NewBuffer(nil)
		}
		seg := ballot.seg
		ballotCap := msgs.NewBallot(seg)
		ballotCap.SetVarId(ballot.VarUUId[:])
		clockData := ballot.Clock.AsData()
		ballot.Ballot.Clock = VectorClockFromData(clockData, false)
		ballotCap.SetClock(clockData)

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
	}
	return ballot.Ballot
}
