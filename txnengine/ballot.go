package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
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
	VarUUId *common.VarUUId
	Data    []byte
	VoteCap *msgs.Vote
	Clock   *VectorClock
	Vote    Vote
}

type BallotBuilder struct {
	*Ballot
	Clock *VectorClockMutable
}

func BallotFromData(data []byte) *Ballot {
	seg, _, err := capn.ReadFromMemoryZeroCopy(data)
	if err != nil {
		panic(fmt.Sprintf("Error when decoding ballot: %v", err))
	}
	ballotCap := msgs.ReadRootBallot(seg)
	voteCap := ballotCap.Vote()
	vUUId := common.MakeVarUUId(ballotCap.VarId())
	return &Ballot{
		VarUUId: vUUId,
		Data:    data,
		VoteCap: &voteCap,
		Clock:   VectorClockFromData(ballotCap.Clock(), false),
		Vote:    Vote(voteCap.Which()),
	}
}

func (ballot *Ballot) Aborted() bool {
	return ballot.Vote != Commit
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

func (ballot *BallotBuilder) buildSeg() (*capn.Segment, msgs.Ballot) {
	seg := capn.NewBuffer(nil)
	ballotCap := msgs.NewRootBallot(seg)
	ballotCap.SetVarId(ballot.VarUUId[:])
	clockData := ballot.Clock.AsData()
	ballot.Ballot.Clock = VectorClockFromData(clockData, false)
	ballotCap.SetClock(clockData)
	return seg, ballotCap
}

func (ballot *BallotBuilder) CreateBadReadBallot(txnId *common.TxnId, actions *TxnActions) *Ballot {
	ballot.Vote = AbortBadRead
	seg, ballotCap := ballot.buildSeg()

	voteCap := msgs.NewVote(seg)
	ballot.VoteCap = &voteCap
	voteCap.SetAbortBadRead()
	badReadCap := voteCap.AbortBadRead()
	badReadCap.SetTxnId(txnId[:])
	badReadCap.SetTxnActions(actions.Data)
	ballotCap.SetVote(voteCap)
	ballot.Data = server.SegToBytes(seg)
	return ballot.Ballot
}

func (ballot *BallotBuilder) ToBallot() *Ballot {
	seg, ballotCap := ballot.buildSeg()

	if ballot.VoteCap == nil {
		voteCap := msgs.NewVote(seg)
		ballot.VoteCap = &voteCap
		switch ballot.Vote {
		case Commit:
			voteCap.SetCommit()
		case AbortDeadlock:
			voteCap.SetAbortDeadlock()
		default:
			panic("ToBallot called for Abort Badread vote")
		}
	}

	ballotCap.SetVote(*ballot.VoteCap)
	ballot.Data = server.SegToBytes(seg)
	return ballot.Ballot
}
