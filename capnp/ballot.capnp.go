package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type Ballot C.Struct

func NewBallot(s *C.Segment) Ballot      { return Ballot(s.NewStruct(0, 3)) }
func NewRootBallot(s *C.Segment) Ballot  { return Ballot(s.NewRootStruct(0, 3)) }
func AutoNewBallot(s *C.Segment) Ballot  { return Ballot(s.NewStructAR(0, 3)) }
func ReadRootBallot(s *C.Segment) Ballot { return Ballot(s.Root(0).ToStruct()) }
func (s Ballot) VarId() []byte           { return C.Struct(s).GetObject(0).ToData() }
func (s Ballot) SetVarId(v []byte)       { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s Ballot) Clock() []byte           { return C.Struct(s).GetObject(1).ToData() }
func (s Ballot) SetClock(v []byte)       { C.Struct(s).SetObject(1, s.Segment.NewData(v)) }
func (s Ballot) Vote() Vote              { return Vote(C.Struct(s).GetObject(2).ToStruct()) }
func (s Ballot) SetVote(v Vote)          { C.Struct(s).SetObject(2, C.Object(v)) }
func (s Ballot) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"varId\":")
	if err != nil {
		return err
	}
	{
		s := s.VarId()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"clock\":")
	if err != nil {
		return err
	}
	{
		s := s.Clock()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"vote\":")
	if err != nil {
		return err
	}
	{
		s := s.Vote()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Ballot) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Ballot) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("varId = ")
	if err != nil {
		return err
	}
	{
		s := s.VarId()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("clock = ")
	if err != nil {
		return err
	}
	{
		s := s.Clock()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("vote = ")
	if err != nil {
		return err
	}
	{
		s := s.Vote()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Ballot) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Ballot_List C.PointerList

func NewBallotList(s *C.Segment, sz int) Ballot_List { return Ballot_List(s.NewCompositeList(0, 3, sz)) }
func (s Ballot_List) Len() int                       { return C.PointerList(s).Len() }
func (s Ballot_List) At(i int) Ballot                { return Ballot(C.PointerList(s).At(i).ToStruct()) }
func (s Ballot_List) ToArray() []Ballot {
	n := s.Len()
	a := make([]Ballot, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Ballot_List) Set(i int, item Ballot) { C.PointerList(s).Set(i, C.Object(item)) }

type Vote C.Struct
type VoteAbortBadRead Vote
type Vote_Which uint16

const (
	VOTE_COMMIT        Vote_Which = 0
	VOTE_ABORTBADREAD  Vote_Which = 1
	VOTE_ABORTDEADLOCK Vote_Which = 2
)

func NewVote(s *C.Segment) Vote                   { return Vote(s.NewStruct(8, 2)) }
func NewRootVote(s *C.Segment) Vote               { return Vote(s.NewRootStruct(8, 2)) }
func AutoNewVote(s *C.Segment) Vote               { return Vote(s.NewStructAR(8, 2)) }
func ReadRootVote(s *C.Segment) Vote              { return Vote(s.Root(0).ToStruct()) }
func (s Vote) Which() Vote_Which                  { return Vote_Which(C.Struct(s).Get16(0)) }
func (s Vote) SetCommit()                         { C.Struct(s).Set16(0, 0) }
func (s Vote) AbortBadRead() VoteAbortBadRead     { return VoteAbortBadRead(s) }
func (s Vote) SetAbortBadRead()                   { C.Struct(s).Set16(0, 1) }
func (s VoteAbortBadRead) TxnId() []byte          { return C.Struct(s).GetObject(0).ToData() }
func (s VoteAbortBadRead) SetTxnId(v []byte)      { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s VoteAbortBadRead) TxnActions() []byte     { return C.Struct(s).GetObject(1).ToData() }
func (s VoteAbortBadRead) SetTxnActions(v []byte) { C.Struct(s).SetObject(1, s.Segment.NewData(v)) }
func (s Vote) SetAbortDeadlock()                  { C.Struct(s).Set16(0, 2) }
func (s Vote) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	if s.Which() == VOTE_COMMIT {
		_, err = b.WriteString("\"commit\":")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	if s.Which() == VOTE_ABORTBADREAD {
		_, err = b.WriteString("\"abortBadRead\":")
		if err != nil {
			return err
		}
		{
			s := s.AbortBadRead()
			err = b.WriteByte('{')
			if err != nil {
				return err
			}
			_, err = b.WriteString("\"txnId\":")
			if err != nil {
				return err
			}
			{
				s := s.TxnId()
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(',')
			if err != nil {
				return err
			}
			_, err = b.WriteString("\"txnActions\":")
			if err != nil {
				return err
			}
			{
				s := s.TxnActions()
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte('}')
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == VOTE_ABORTDEADLOCK {
		_, err = b.WriteString("\"abortDeadlock\":")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Vote) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Vote) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	if s.Which() == VOTE_COMMIT {
		_, err = b.WriteString("commit = ")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	if s.Which() == VOTE_ABORTBADREAD {
		_, err = b.WriteString("abortBadRead = ")
		if err != nil {
			return err
		}
		{
			s := s.AbortBadRead()
			err = b.WriteByte('(')
			if err != nil {
				return err
			}
			_, err = b.WriteString("txnId = ")
			if err != nil {
				return err
			}
			{
				s := s.TxnId()
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			_, err = b.WriteString(", ")
			if err != nil {
				return err
			}
			_, err = b.WriteString("txnActions = ")
			if err != nil {
				return err
			}
			{
				s := s.TxnActions()
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(')')
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == VOTE_ABORTDEADLOCK {
		_, err = b.WriteString("abortDeadlock = ")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Vote) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Vote_List C.PointerList

func NewVoteList(s *C.Segment, sz int) Vote_List { return Vote_List(s.NewCompositeList(8, 2, sz)) }
func (s Vote_List) Len() int                     { return C.PointerList(s).Len() }
func (s Vote_List) At(i int) Vote                { return Vote(C.PointerList(s).At(i).ToStruct()) }
func (s Vote_List) ToArray() []Vote {
	n := s.Len()
	a := make([]Vote, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Vote_List) Set(i int, item Vote) { C.PointerList(s).Set(i, C.Object(item)) }
