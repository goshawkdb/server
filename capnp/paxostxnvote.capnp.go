package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type OneATxnVotes C.Struct

func NewOneATxnVotes(s *C.Segment) OneATxnVotes      { return OneATxnVotes(s.NewStruct(8, 2)) }
func NewRootOneATxnVotes(s *C.Segment) OneATxnVotes  { return OneATxnVotes(s.NewRootStruct(8, 2)) }
func AutoNewOneATxnVotes(s *C.Segment) OneATxnVotes  { return OneATxnVotes(s.NewStructAR(8, 2)) }
func ReadRootOneATxnVotes(s *C.Segment) OneATxnVotes { return OneATxnVotes(s.Root(0).ToStruct()) }
func (s OneATxnVotes) TxnId() []byte                 { return C.Struct(s).GetObject(0).ToData() }
func (s OneATxnVotes) SetTxnId(v []byte)             { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s OneATxnVotes) RmId() uint32                  { return C.Struct(s).Get32(0) }
func (s OneATxnVotes) SetRmId(v uint32)              { C.Struct(s).Set32(0, v) }
func (s OneATxnVotes) Proposals() TxnVoteProposal_List {
	return TxnVoteProposal_List(C.Struct(s).GetObject(1))
}
func (s OneATxnVotes) SetProposals(v TxnVoteProposal_List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s OneATxnVotes) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
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
	_, err = b.WriteString("\"rmId\":")
	if err != nil {
		return err
	}
	{
		s := s.RmId()
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
	_, err = b.WriteString("\"proposals\":")
	if err != nil {
		return err
	}
	{
		s := s.Proposals()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
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
func (s OneATxnVotes) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s OneATxnVotes) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
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
	_, err = b.WriteString("rmId = ")
	if err != nil {
		return err
	}
	{
		s := s.RmId()
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
	_, err = b.WriteString("proposals = ")
	if err != nil {
		return err
	}
	{
		s := s.Proposals()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
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
func (s OneATxnVotes) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type OneATxnVotes_List C.PointerList

func NewOneATxnVotesList(s *C.Segment, sz int) OneATxnVotes_List {
	return OneATxnVotes_List(s.NewCompositeList(8, 2, sz))
}
func (s OneATxnVotes_List) Len() int { return C.PointerList(s).Len() }
func (s OneATxnVotes_List) At(i int) OneATxnVotes {
	return OneATxnVotes(C.PointerList(s).At(i).ToStruct())
}
func (s OneATxnVotes_List) ToArray() []OneATxnVotes {
	n := s.Len()
	a := make([]OneATxnVotes, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s OneATxnVotes_List) Set(i int, item OneATxnVotes) { C.PointerList(s).Set(i, C.Object(item)) }

type OneBTxnVotes C.Struct

func NewOneBTxnVotes(s *C.Segment) OneBTxnVotes      { return OneBTxnVotes(s.NewStruct(8, 2)) }
func NewRootOneBTxnVotes(s *C.Segment) OneBTxnVotes  { return OneBTxnVotes(s.NewRootStruct(8, 2)) }
func AutoNewOneBTxnVotes(s *C.Segment) OneBTxnVotes  { return OneBTxnVotes(s.NewStructAR(8, 2)) }
func ReadRootOneBTxnVotes(s *C.Segment) OneBTxnVotes { return OneBTxnVotes(s.Root(0).ToStruct()) }
func (s OneBTxnVotes) TxnId() []byte                 { return C.Struct(s).GetObject(0).ToData() }
func (s OneBTxnVotes) SetTxnId(v []byte)             { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s OneBTxnVotes) RmId() uint32                  { return C.Struct(s).Get32(0) }
func (s OneBTxnVotes) SetRmId(v uint32)              { C.Struct(s).Set32(0, v) }
func (s OneBTxnVotes) Promises() TxnVotePromise_List {
	return TxnVotePromise_List(C.Struct(s).GetObject(1))
}
func (s OneBTxnVotes) SetPromises(v TxnVotePromise_List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s OneBTxnVotes) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
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
	_, err = b.WriteString("\"rmId\":")
	if err != nil {
		return err
	}
	{
		s := s.RmId()
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
	_, err = b.WriteString("\"promises\":")
	if err != nil {
		return err
	}
	{
		s := s.Promises()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
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
func (s OneBTxnVotes) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s OneBTxnVotes) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
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
	_, err = b.WriteString("rmId = ")
	if err != nil {
		return err
	}
	{
		s := s.RmId()
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
	_, err = b.WriteString("promises = ")
	if err != nil {
		return err
	}
	{
		s := s.Promises()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
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
func (s OneBTxnVotes) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type OneBTxnVotes_List C.PointerList

func NewOneBTxnVotesList(s *C.Segment, sz int) OneBTxnVotes_List {
	return OneBTxnVotes_List(s.NewCompositeList(8, 2, sz))
}
func (s OneBTxnVotes_List) Len() int { return C.PointerList(s).Len() }
func (s OneBTxnVotes_List) At(i int) OneBTxnVotes {
	return OneBTxnVotes(C.PointerList(s).At(i).ToStruct())
}
func (s OneBTxnVotes_List) ToArray() []OneBTxnVotes {
	n := s.Len()
	a := make([]OneBTxnVotes, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s OneBTxnVotes_List) Set(i int, item OneBTxnVotes) { C.PointerList(s).Set(i, C.Object(item)) }

type TwoATxnVotes C.Struct

func NewTwoATxnVotes(s *C.Segment) TwoATxnVotes      { return TwoATxnVotes(s.NewStruct(8, 2)) }
func NewRootTwoATxnVotes(s *C.Segment) TwoATxnVotes  { return TwoATxnVotes(s.NewRootStruct(8, 2)) }
func AutoNewTwoATxnVotes(s *C.Segment) TwoATxnVotes  { return TwoATxnVotes(s.NewStructAR(8, 2)) }
func ReadRootTwoATxnVotes(s *C.Segment) TwoATxnVotes { return TwoATxnVotes(s.Root(0).ToStruct()) }
func (s TwoATxnVotes) Txn() Txn                      { return Txn(C.Struct(s).GetObject(0).ToStruct()) }
func (s TwoATxnVotes) SetTxn(v Txn)                  { C.Struct(s).SetObject(0, C.Object(v)) }
func (s TwoATxnVotes) RmId() uint32                  { return C.Struct(s).Get32(0) }
func (s TwoATxnVotes) SetRmId(v uint32)              { C.Struct(s).Set32(0, v) }
func (s TwoATxnVotes) AcceptRequests() TxnVoteAcceptRequest_List {
	return TxnVoteAcceptRequest_List(C.Struct(s).GetObject(1))
}
func (s TwoATxnVotes) SetAcceptRequests(v TxnVoteAcceptRequest_List) {
	C.Struct(s).SetObject(1, C.Object(v))
}
func (s TwoATxnVotes) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"txn\":")
	if err != nil {
		return err
	}
	{
		s := s.Txn()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"rmId\":")
	if err != nil {
		return err
	}
	{
		s := s.RmId()
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
	_, err = b.WriteString("\"acceptRequests\":")
	if err != nil {
		return err
	}
	{
		s := s.AcceptRequests()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
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
func (s TwoATxnVotes) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TwoATxnVotes) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("txn = ")
	if err != nil {
		return err
	}
	{
		s := s.Txn()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("rmId = ")
	if err != nil {
		return err
	}
	{
		s := s.RmId()
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
	_, err = b.WriteString("acceptRequests = ")
	if err != nil {
		return err
	}
	{
		s := s.AcceptRequests()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
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
func (s TwoATxnVotes) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TwoATxnVotes_List C.PointerList

func NewTwoATxnVotesList(s *C.Segment, sz int) TwoATxnVotes_List {
	return TwoATxnVotes_List(s.NewCompositeList(8, 2, sz))
}
func (s TwoATxnVotes_List) Len() int { return C.PointerList(s).Len() }
func (s TwoATxnVotes_List) At(i int) TwoATxnVotes {
	return TwoATxnVotes(C.PointerList(s).At(i).ToStruct())
}
func (s TwoATxnVotes_List) ToArray() []TwoATxnVotes {
	n := s.Len()
	a := make([]TwoATxnVotes, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TwoATxnVotes_List) Set(i int, item TwoATxnVotes) { C.PointerList(s).Set(i, C.Object(item)) }

type TwoBTxnVotes C.Struct
type TwoBTxnVotesFailures TwoBTxnVotes
type TwoBTxnVotes_Which uint16

const (
	TWOBTXNVOTES_FAILURES TwoBTxnVotes_Which = 0
	TWOBTXNVOTES_OUTCOME  TwoBTxnVotes_Which = 1
)

func NewTwoBTxnVotes(s *C.Segment) TwoBTxnVotes       { return TwoBTxnVotes(s.NewStruct(8, 2)) }
func NewRootTwoBTxnVotes(s *C.Segment) TwoBTxnVotes   { return TwoBTxnVotes(s.NewRootStruct(8, 2)) }
func AutoNewTwoBTxnVotes(s *C.Segment) TwoBTxnVotes   { return TwoBTxnVotes(s.NewStructAR(8, 2)) }
func ReadRootTwoBTxnVotes(s *C.Segment) TwoBTxnVotes  { return TwoBTxnVotes(s.Root(0).ToStruct()) }
func (s TwoBTxnVotes) Which() TwoBTxnVotes_Which      { return TwoBTxnVotes_Which(C.Struct(s).Get16(4)) }
func (s TwoBTxnVotes) Failures() TwoBTxnVotesFailures { return TwoBTxnVotesFailures(s) }
func (s TwoBTxnVotes) SetFailures()                   { C.Struct(s).Set16(4, 0) }
func (s TwoBTxnVotesFailures) TxnId() []byte          { return C.Struct(s).GetObject(0).ToData() }
func (s TwoBTxnVotesFailures) SetTxnId(v []byte)      { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s TwoBTxnVotesFailures) RmId() uint32           { return C.Struct(s).Get32(0) }
func (s TwoBTxnVotesFailures) SetRmId(v uint32)       { C.Struct(s).Set32(0, v) }
func (s TwoBTxnVotesFailures) Nacks() TxnVoteTwoBFailure_List {
	return TxnVoteTwoBFailure_List(C.Struct(s).GetObject(1))
}
func (s TwoBTxnVotesFailures) SetNacks(v TxnVoteTwoBFailure_List) {
	C.Struct(s).SetObject(1, C.Object(v))
}
func (s TwoBTxnVotes) Outcome() Outcome { return Outcome(C.Struct(s).GetObject(0).ToStruct()) }
func (s TwoBTxnVotes) SetOutcome(v Outcome) {
	C.Struct(s).Set16(4, 1)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s TwoBTxnVotes) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	if s.Which() == TWOBTXNVOTES_FAILURES {
		_, err = b.WriteString("\"failures\":")
		if err != nil {
			return err
		}
		{
			s := s.Failures()
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
			_, err = b.WriteString("\"rmId\":")
			if err != nil {
				return err
			}
			{
				s := s.RmId()
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
			_, err = b.WriteString("\"nacks\":")
			if err != nil {
				return err
			}
			{
				s := s.Nacks()
				{
					err = b.WriteByte('[')
					if err != nil {
						return err
					}
					for i, s := range s.ToArray() {
						if i != 0 {
							_, err = b.WriteString(", ")
						}
						if err != nil {
							return err
						}
						err = s.WriteJSON(b)
						if err != nil {
							return err
						}
					}
					err = b.WriteByte(']')
				}
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
	if s.Which() == TWOBTXNVOTES_OUTCOME {
		_, err = b.WriteString("\"outcome\":")
		if err != nil {
			return err
		}
		{
			s := s.Outcome()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TwoBTxnVotes) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TwoBTxnVotes) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	if s.Which() == TWOBTXNVOTES_FAILURES {
		_, err = b.WriteString("failures = ")
		if err != nil {
			return err
		}
		{
			s := s.Failures()
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
			_, err = b.WriteString("rmId = ")
			if err != nil {
				return err
			}
			{
				s := s.RmId()
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
			_, err = b.WriteString("nacks = ")
			if err != nil {
				return err
			}
			{
				s := s.Nacks()
				{
					err = b.WriteByte('[')
					if err != nil {
						return err
					}
					for i, s := range s.ToArray() {
						if i != 0 {
							_, err = b.WriteString(", ")
						}
						if err != nil {
							return err
						}
						err = s.WriteCapLit(b)
						if err != nil {
							return err
						}
					}
					err = b.WriteByte(']')
				}
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
	if s.Which() == TWOBTXNVOTES_OUTCOME {
		_, err = b.WriteString("outcome = ")
		if err != nil {
			return err
		}
		{
			s := s.Outcome()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TwoBTxnVotes) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TwoBTxnVotes_List C.PointerList

func NewTwoBTxnVotesList(s *C.Segment, sz int) TwoBTxnVotes_List {
	return TwoBTxnVotes_List(s.NewCompositeList(8, 2, sz))
}
func (s TwoBTxnVotes_List) Len() int { return C.PointerList(s).Len() }
func (s TwoBTxnVotes_List) At(i int) TwoBTxnVotes {
	return TwoBTxnVotes(C.PointerList(s).At(i).ToStruct())
}
func (s TwoBTxnVotes_List) ToArray() []TwoBTxnVotes {
	n := s.Len()
	a := make([]TwoBTxnVotes, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TwoBTxnVotes_List) Set(i int, item TwoBTxnVotes) { C.PointerList(s).Set(i, C.Object(item)) }

type TxnVoteProposal C.Struct

func NewTxnVoteProposal(s *C.Segment) TxnVoteProposal { return TxnVoteProposal(s.NewStruct(8, 1)) }
func NewRootTxnVoteProposal(s *C.Segment) TxnVoteProposal {
	return TxnVoteProposal(s.NewRootStruct(8, 1))
}
func AutoNewTxnVoteProposal(s *C.Segment) TxnVoteProposal { return TxnVoteProposal(s.NewStructAR(8, 1)) }
func ReadRootTxnVoteProposal(s *C.Segment) TxnVoteProposal {
	return TxnVoteProposal(s.Root(0).ToStruct())
}
func (s TxnVoteProposal) VarId() []byte           { return C.Struct(s).GetObject(0).ToData() }
func (s TxnVoteProposal) SetVarId(v []byte)       { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s TxnVoteProposal) RoundNumber() uint64     { return C.Struct(s).Get64(0) }
func (s TxnVoteProposal) SetRoundNumber(v uint64) { C.Struct(s).Set64(0, v) }
func (s TxnVoteProposal) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"roundNumber\":")
	if err != nil {
		return err
	}
	{
		s := s.RoundNumber()
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
	err = b.Flush()
	return err
}
func (s TxnVoteProposal) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TxnVoteProposal) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("roundNumber = ")
	if err != nil {
		return err
	}
	{
		s := s.RoundNumber()
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
	err = b.Flush()
	return err
}
func (s TxnVoteProposal) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TxnVoteProposal_List C.PointerList

func NewTxnVoteProposalList(s *C.Segment, sz int) TxnVoteProposal_List {
	return TxnVoteProposal_List(s.NewCompositeList(8, 1, sz))
}
func (s TxnVoteProposal_List) Len() int { return C.PointerList(s).Len() }
func (s TxnVoteProposal_List) At(i int) TxnVoteProposal {
	return TxnVoteProposal(C.PointerList(s).At(i).ToStruct())
}
func (s TxnVoteProposal_List) ToArray() []TxnVoteProposal {
	n := s.Len()
	a := make([]TxnVoteProposal, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TxnVoteProposal_List) Set(i int, item TxnVoteProposal) {
	C.PointerList(s).Set(i, C.Object(item))
}

type TxnVotePromise C.Struct
type TxnVotePromiseAccepted TxnVotePromise
type TxnVotePromise_Which uint16

const (
	TXNVOTEPROMISE_FREECHOICE        TxnVotePromise_Which = 0
	TXNVOTEPROMISE_ACCEPTED          TxnVotePromise_Which = 1
	TXNVOTEPROMISE_ROUNDNUMBERTOOLOW TxnVotePromise_Which = 2
)

func NewTxnVotePromise(s *C.Segment) TxnVotePromise      { return TxnVotePromise(s.NewStruct(24, 2)) }
func NewRootTxnVotePromise(s *C.Segment) TxnVotePromise  { return TxnVotePromise(s.NewRootStruct(24, 2)) }
func AutoNewTxnVotePromise(s *C.Segment) TxnVotePromise  { return TxnVotePromise(s.NewStructAR(24, 2)) }
func ReadRootTxnVotePromise(s *C.Segment) TxnVotePromise { return TxnVotePromise(s.Root(0).ToStruct()) }
func (s TxnVotePromise) Which() TxnVotePromise_Which {
	return TxnVotePromise_Which(C.Struct(s).Get16(8))
}
func (s TxnVotePromise) VarId() []byte                    { return C.Struct(s).GetObject(0).ToData() }
func (s TxnVotePromise) SetVarId(v []byte)                { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s TxnVotePromise) RoundNumber() uint64              { return C.Struct(s).Get64(0) }
func (s TxnVotePromise) SetRoundNumber(v uint64)          { C.Struct(s).Set64(0, v) }
func (s TxnVotePromise) SetFreeChoice()                   { C.Struct(s).Set16(8, 0) }
func (s TxnVotePromise) Accepted() TxnVotePromiseAccepted { return TxnVotePromiseAccepted(s) }
func (s TxnVotePromise) SetAccepted()                     { C.Struct(s).Set16(8, 1) }
func (s TxnVotePromiseAccepted) RoundNumber() uint64      { return C.Struct(s).Get64(16) }
func (s TxnVotePromiseAccepted) SetRoundNumber(v uint64)  { C.Struct(s).Set64(16, v) }
func (s TxnVotePromiseAccepted) Ballot() []byte           { return C.Struct(s).GetObject(1).ToData() }
func (s TxnVotePromiseAccepted) SetBallot(v []byte)       { C.Struct(s).SetObject(1, s.Segment.NewData(v)) }
func (s TxnVotePromise) RoundNumberTooLow() uint32        { return C.Struct(s).Get32(16) }
func (s TxnVotePromise) SetRoundNumberTooLow(v uint32) {
	C.Struct(s).Set16(8, 2)
	C.Struct(s).Set32(16, v)
}
func (s TxnVotePromise) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"roundNumber\":")
	if err != nil {
		return err
	}
	{
		s := s.RoundNumber()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	if s.Which() == TXNVOTEPROMISE_FREECHOICE {
		_, err = b.WriteString("\"freeChoice\":")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	if s.Which() == TXNVOTEPROMISE_ACCEPTED {
		_, err = b.WriteString("\"accepted\":")
		if err != nil {
			return err
		}
		{
			s := s.Accepted()
			err = b.WriteByte('{')
			if err != nil {
				return err
			}
			_, err = b.WriteString("\"roundNumber\":")
			if err != nil {
				return err
			}
			{
				s := s.RoundNumber()
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
			_, err = b.WriteString("\"ballot\":")
			if err != nil {
				return err
			}
			{
				s := s.Ballot()
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
	if s.Which() == TXNVOTEPROMISE_ROUNDNUMBERTOOLOW {
		_, err = b.WriteString("\"roundNumberTooLow\":")
		if err != nil {
			return err
		}
		{
			s := s.RoundNumberTooLow()
			buf, err = json.Marshal(s)
			if err != nil {
				return err
			}
			_, err = b.Write(buf)
			if err != nil {
				return err
			}
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TxnVotePromise) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TxnVotePromise) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("roundNumber = ")
	if err != nil {
		return err
	}
	{
		s := s.RoundNumber()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	if s.Which() == TXNVOTEPROMISE_FREECHOICE {
		_, err = b.WriteString("freeChoice = ")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	if s.Which() == TXNVOTEPROMISE_ACCEPTED {
		_, err = b.WriteString("accepted = ")
		if err != nil {
			return err
		}
		{
			s := s.Accepted()
			err = b.WriteByte('(')
			if err != nil {
				return err
			}
			_, err = b.WriteString("roundNumber = ")
			if err != nil {
				return err
			}
			{
				s := s.RoundNumber()
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
			_, err = b.WriteString("ballot = ")
			if err != nil {
				return err
			}
			{
				s := s.Ballot()
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
	if s.Which() == TXNVOTEPROMISE_ROUNDNUMBERTOOLOW {
		_, err = b.WriteString("roundNumberTooLow = ")
		if err != nil {
			return err
		}
		{
			s := s.RoundNumberTooLow()
			buf, err = json.Marshal(s)
			if err != nil {
				return err
			}
			_, err = b.Write(buf)
			if err != nil {
				return err
			}
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TxnVotePromise) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TxnVotePromise_List C.PointerList

func NewTxnVotePromiseList(s *C.Segment, sz int) TxnVotePromise_List {
	return TxnVotePromise_List(s.NewCompositeList(24, 2, sz))
}
func (s TxnVotePromise_List) Len() int { return C.PointerList(s).Len() }
func (s TxnVotePromise_List) At(i int) TxnVotePromise {
	return TxnVotePromise(C.PointerList(s).At(i).ToStruct())
}
func (s TxnVotePromise_List) ToArray() []TxnVotePromise {
	n := s.Len()
	a := make([]TxnVotePromise, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TxnVotePromise_List) Set(i int, item TxnVotePromise) { C.PointerList(s).Set(i, C.Object(item)) }

type TxnVoteAcceptRequest C.Struct

func NewTxnVoteAcceptRequest(s *C.Segment) TxnVoteAcceptRequest {
	return TxnVoteAcceptRequest(s.NewStruct(8, 1))
}
func NewRootTxnVoteAcceptRequest(s *C.Segment) TxnVoteAcceptRequest {
	return TxnVoteAcceptRequest(s.NewRootStruct(8, 1))
}
func AutoNewTxnVoteAcceptRequest(s *C.Segment) TxnVoteAcceptRequest {
	return TxnVoteAcceptRequest(s.NewStructAR(8, 1))
}
func ReadRootTxnVoteAcceptRequest(s *C.Segment) TxnVoteAcceptRequest {
	return TxnVoteAcceptRequest(s.Root(0).ToStruct())
}
func (s TxnVoteAcceptRequest) Ballot() []byte          { return C.Struct(s).GetObject(0).ToData() }
func (s TxnVoteAcceptRequest) SetBallot(v []byte)      { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s TxnVoteAcceptRequest) RoundNumber() uint64     { return C.Struct(s).Get64(0) }
func (s TxnVoteAcceptRequest) SetRoundNumber(v uint64) { C.Struct(s).Set64(0, v) }
func (s TxnVoteAcceptRequest) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"ballot\":")
	if err != nil {
		return err
	}
	{
		s := s.Ballot()
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
	_, err = b.WriteString("\"roundNumber\":")
	if err != nil {
		return err
	}
	{
		s := s.RoundNumber()
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
	err = b.Flush()
	return err
}
func (s TxnVoteAcceptRequest) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TxnVoteAcceptRequest) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("ballot = ")
	if err != nil {
		return err
	}
	{
		s := s.Ballot()
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
	_, err = b.WriteString("roundNumber = ")
	if err != nil {
		return err
	}
	{
		s := s.RoundNumber()
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
	err = b.Flush()
	return err
}
func (s TxnVoteAcceptRequest) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TxnVoteAcceptRequest_List C.PointerList

func NewTxnVoteAcceptRequestList(s *C.Segment, sz int) TxnVoteAcceptRequest_List {
	return TxnVoteAcceptRequest_List(s.NewCompositeList(8, 1, sz))
}
func (s TxnVoteAcceptRequest_List) Len() int { return C.PointerList(s).Len() }
func (s TxnVoteAcceptRequest_List) At(i int) TxnVoteAcceptRequest {
	return TxnVoteAcceptRequest(C.PointerList(s).At(i).ToStruct())
}
func (s TxnVoteAcceptRequest_List) ToArray() []TxnVoteAcceptRequest {
	n := s.Len()
	a := make([]TxnVoteAcceptRequest, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TxnVoteAcceptRequest_List) Set(i int, item TxnVoteAcceptRequest) {
	C.PointerList(s).Set(i, C.Object(item))
}

type TxnVoteTwoBFailure C.Struct

func NewTxnVoteTwoBFailure(s *C.Segment) TxnVoteTwoBFailure {
	return TxnVoteTwoBFailure(s.NewStruct(16, 1))
}
func NewRootTxnVoteTwoBFailure(s *C.Segment) TxnVoteTwoBFailure {
	return TxnVoteTwoBFailure(s.NewRootStruct(16, 1))
}
func AutoNewTxnVoteTwoBFailure(s *C.Segment) TxnVoteTwoBFailure {
	return TxnVoteTwoBFailure(s.NewStructAR(16, 1))
}
func ReadRootTxnVoteTwoBFailure(s *C.Segment) TxnVoteTwoBFailure {
	return TxnVoteTwoBFailure(s.Root(0).ToStruct())
}
func (s TxnVoteTwoBFailure) VarId() []byte                 { return C.Struct(s).GetObject(0).ToData() }
func (s TxnVoteTwoBFailure) SetVarId(v []byte)             { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s TxnVoteTwoBFailure) RoundNumber() uint64           { return C.Struct(s).Get64(0) }
func (s TxnVoteTwoBFailure) SetRoundNumber(v uint64)       { C.Struct(s).Set64(0, v) }
func (s TxnVoteTwoBFailure) RoundNumberTooLow() uint32     { return C.Struct(s).Get32(8) }
func (s TxnVoteTwoBFailure) SetRoundNumberTooLow(v uint32) { C.Struct(s).Set32(8, v) }
func (s TxnVoteTwoBFailure) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"roundNumber\":")
	if err != nil {
		return err
	}
	{
		s := s.RoundNumber()
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
	_, err = b.WriteString("\"roundNumberTooLow\":")
	if err != nil {
		return err
	}
	{
		s := s.RoundNumberTooLow()
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
	err = b.Flush()
	return err
}
func (s TxnVoteTwoBFailure) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TxnVoteTwoBFailure) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("roundNumber = ")
	if err != nil {
		return err
	}
	{
		s := s.RoundNumber()
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
	_, err = b.WriteString("roundNumberTooLow = ")
	if err != nil {
		return err
	}
	{
		s := s.RoundNumberTooLow()
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
	err = b.Flush()
	return err
}
func (s TxnVoteTwoBFailure) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TxnVoteTwoBFailure_List C.PointerList

func NewTxnVoteTwoBFailureList(s *C.Segment, sz int) TxnVoteTwoBFailure_List {
	return TxnVoteTwoBFailure_List(s.NewCompositeList(16, 1, sz))
}
func (s TxnVoteTwoBFailure_List) Len() int { return C.PointerList(s).Len() }
func (s TxnVoteTwoBFailure_List) At(i int) TxnVoteTwoBFailure {
	return TxnVoteTwoBFailure(C.PointerList(s).At(i).ToStruct())
}
func (s TxnVoteTwoBFailure_List) ToArray() []TxnVoteTwoBFailure {
	n := s.Len()
	a := make([]TxnVoteTwoBFailure, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TxnVoteTwoBFailure_List) Set(i int, item TxnVoteTwoBFailure) {
	C.PointerList(s).Set(i, C.Object(item))
}
