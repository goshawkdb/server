package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type TxnSubmissionOutcome C.Struct

func NewTxnSubmissionOutcome(s *C.Segment) TxnSubmissionOutcome {
	return TxnSubmissionOutcome(s.NewStruct(0, 2))
}
func NewRootTxnSubmissionOutcome(s *C.Segment) TxnSubmissionOutcome {
	return TxnSubmissionOutcome(s.NewRootStruct(0, 2))
}
func AutoNewTxnSubmissionOutcome(s *C.Segment) TxnSubmissionOutcome {
	return TxnSubmissionOutcome(s.NewStructAR(0, 2))
}
func ReadRootTxnSubmissionOutcome(s *C.Segment) TxnSubmissionOutcome {
	return TxnSubmissionOutcome(s.Root(0).ToStruct())
}
func (s TxnSubmissionOutcome) Outcome() Outcome            { return Outcome(C.Struct(s).GetObject(0).ToStruct()) }
func (s TxnSubmissionOutcome) SetOutcome(v Outcome)        { C.Struct(s).SetObject(0, C.Object(v)) }
func (s TxnSubmissionOutcome) Subscribers() C.DataList     { return C.DataList(C.Struct(s).GetObject(1)) }
func (s TxnSubmissionOutcome) SetSubscribers(v C.DataList) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s TxnSubmissionOutcome) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
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
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"subscribers\":")
	if err != nil {
		return err
	}
	{
		s := s.Subscribers()
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
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
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
func (s TxnSubmissionOutcome) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TxnSubmissionOutcome) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
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
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("subscribers = ")
	if err != nil {
		return err
	}
	{
		s := s.Subscribers()
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
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
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
func (s TxnSubmissionOutcome) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TxnSubmissionOutcome_List C.PointerList

func NewTxnSubmissionOutcomeList(s *C.Segment, sz int) TxnSubmissionOutcome_List {
	return TxnSubmissionOutcome_List(s.NewCompositeList(0, 2, sz))
}
func (s TxnSubmissionOutcome_List) Len() int { return C.PointerList(s).Len() }
func (s TxnSubmissionOutcome_List) At(i int) TxnSubmissionOutcome {
	return TxnSubmissionOutcome(C.PointerList(s).At(i).ToStruct())
}
func (s TxnSubmissionOutcome_List) ToArray() []TxnSubmissionOutcome {
	n := s.Len()
	a := make([]TxnSubmissionOutcome, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TxnSubmissionOutcome_List) Set(i int, item TxnSubmissionOutcome) {
	C.PointerList(s).Set(i, C.Object(item))
}

type Outcome C.Struct
type OutcomeAbort Outcome
type Outcome_Which uint16

const (
	OUTCOME_COMMIT Outcome_Which = 0
	OUTCOME_ABORT  Outcome_Which = 1
)

type OutcomeAbort_Which uint16

const (
	OUTCOMEABORT_RESUBMIT OutcomeAbort_Which = 0
	OUTCOMEABORT_RERUN    OutcomeAbort_Which = 1
)

func NewOutcome(s *C.Segment) Outcome      { return Outcome(s.NewStruct(8, 3)) }
func NewRootOutcome(s *C.Segment) Outcome  { return Outcome(s.NewRootStruct(8, 3)) }
func AutoNewOutcome(s *C.Segment) Outcome  { return Outcome(s.NewStructAR(8, 3)) }
func ReadRootOutcome(s *C.Segment) Outcome { return Outcome(s.Root(0).ToStruct()) }
func (s Outcome) Which() Outcome_Which     { return Outcome_Which(C.Struct(s).Get16(0)) }
func (s Outcome) Id() OutcomeId_List       { return OutcomeId_List(C.Struct(s).GetObject(0)) }
func (s Outcome) SetId(v OutcomeId_List)   { C.Struct(s).SetObject(0, C.Object(v)) }
func (s Outcome) Txn() []byte              { return C.Struct(s).GetObject(1).ToData() }
func (s Outcome) SetTxn(v []byte)          { C.Struct(s).SetObject(1, s.Segment.NewData(v)) }
func (s Outcome) Commit() []byte           { return C.Struct(s).GetObject(2).ToData() }
func (s Outcome) SetCommit(v []byte) {
	C.Struct(s).Set16(0, 0)
	C.Struct(s).SetObject(2, s.Segment.NewData(v))
}
func (s Outcome) Abort() OutcomeAbort            { return OutcomeAbort(s) }
func (s Outcome) SetAbort()                      { C.Struct(s).Set16(0, 1) }
func (s OutcomeAbort) Which() OutcomeAbort_Which { return OutcomeAbort_Which(C.Struct(s).Get16(2)) }
func (s OutcomeAbort) SetResubmit()              { C.Struct(s).Set16(2, 0) }
func (s OutcomeAbort) Rerun() Update_List        { return Update_List(C.Struct(s).GetObject(2)) }
func (s OutcomeAbort) SetRerun(v Update_List) {
	C.Struct(s).Set16(2, 1)
	C.Struct(s).SetObject(2, C.Object(v))
}
func (s Outcome) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"id\":")
	if err != nil {
		return err
	}
	{
		s := s.Id()
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
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"txn\":")
	if err != nil {
		return err
	}
	{
		s := s.Txn()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	if s.Which() == OUTCOME_COMMIT {
		_, err = b.WriteString("\"commit\":")
		if err != nil {
			return err
		}
		{
			s := s.Commit()
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
	if s.Which() == OUTCOME_ABORT {
		_, err = b.WriteString("\"abort\":")
		if err != nil {
			return err
		}
		{
			s := s.Abort()
			err = b.WriteByte('{')
			if err != nil {
				return err
			}
			if s.Which() == OUTCOMEABORT_RESUBMIT {
				_, err = b.WriteString("\"resubmit\":")
				if err != nil {
					return err
				}
				_ = s
				_, err = b.WriteString("null")
				if err != nil {
					return err
				}
			}
			if s.Which() == OUTCOMEABORT_RERUN {
				_, err = b.WriteString("\"rerun\":")
				if err != nil {
					return err
				}
				{
					s := s.Rerun()
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
			}
			err = b.WriteByte('}')
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
func (s Outcome) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Outcome) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("id = ")
	if err != nil {
		return err
	}
	{
		s := s.Id()
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
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("txn = ")
	if err != nil {
		return err
	}
	{
		s := s.Txn()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	if s.Which() == OUTCOME_COMMIT {
		_, err = b.WriteString("commit = ")
		if err != nil {
			return err
		}
		{
			s := s.Commit()
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
	if s.Which() == OUTCOME_ABORT {
		_, err = b.WriteString("abort = ")
		if err != nil {
			return err
		}
		{
			s := s.Abort()
			err = b.WriteByte('(')
			if err != nil {
				return err
			}
			if s.Which() == OUTCOMEABORT_RESUBMIT {
				_, err = b.WriteString("resubmit = ")
				if err != nil {
					return err
				}
				_ = s
				_, err = b.WriteString("null")
				if err != nil {
					return err
				}
			}
			if s.Which() == OUTCOMEABORT_RERUN {
				_, err = b.WriteString("rerun = ")
				if err != nil {
					return err
				}
				{
					s := s.Rerun()
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
			}
			err = b.WriteByte(')')
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
func (s Outcome) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Outcome_List C.PointerList

func NewOutcomeList(s *C.Segment, sz int) Outcome_List {
	return Outcome_List(s.NewCompositeList(8, 3, sz))
}
func (s Outcome_List) Len() int         { return C.PointerList(s).Len() }
func (s Outcome_List) At(i int) Outcome { return Outcome(C.PointerList(s).At(i).ToStruct()) }
func (s Outcome_List) ToArray() []Outcome {
	n := s.Len()
	a := make([]Outcome, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Outcome_List) Set(i int, item Outcome) { C.PointerList(s).Set(i, C.Object(item)) }

type Update C.Struct

func NewUpdate(s *C.Segment) Update      { return Update(s.NewStruct(0, 3)) }
func NewRootUpdate(s *C.Segment) Update  { return Update(s.NewRootStruct(0, 3)) }
func AutoNewUpdate(s *C.Segment) Update  { return Update(s.NewStructAR(0, 3)) }
func ReadRootUpdate(s *C.Segment) Update { return Update(s.Root(0).ToStruct()) }
func (s Update) TxnId() []byte           { return C.Struct(s).GetObject(0).ToData() }
func (s Update) SetTxnId(v []byte)       { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s Update) Actions() []byte         { return C.Struct(s).GetObject(1).ToData() }
func (s Update) SetActions(v []byte)     { C.Struct(s).SetObject(1, s.Segment.NewData(v)) }
func (s Update) Clock() []byte           { return C.Struct(s).GetObject(2).ToData() }
func (s Update) SetClock(v []byte)       { C.Struct(s).SetObject(2, s.Segment.NewData(v)) }
func (s Update) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"actions\":")
	if err != nil {
		return err
	}
	{
		s := s.Actions()
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
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Update) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Update) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("actions = ")
	if err != nil {
		return err
	}
	{
		s := s.Actions()
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
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Update) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Update_List C.PointerList

func NewUpdateList(s *C.Segment, sz int) Update_List { return Update_List(s.NewCompositeList(0, 3, sz)) }
func (s Update_List) Len() int                       { return C.PointerList(s).Len() }
func (s Update_List) At(i int) Update                { return Update(C.PointerList(s).At(i).ToStruct()) }
func (s Update_List) ToArray() []Update {
	n := s.Len()
	a := make([]Update, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Update_List) Set(i int, item Update) { C.PointerList(s).Set(i, C.Object(item)) }

type OutcomeId C.Struct

func NewOutcomeId(s *C.Segment) OutcomeId      { return OutcomeId(s.NewStruct(0, 2)) }
func NewRootOutcomeId(s *C.Segment) OutcomeId  { return OutcomeId(s.NewRootStruct(0, 2)) }
func AutoNewOutcomeId(s *C.Segment) OutcomeId  { return OutcomeId(s.NewStructAR(0, 2)) }
func ReadRootOutcomeId(s *C.Segment) OutcomeId { return OutcomeId(s.Root(0).ToStruct()) }
func (s OutcomeId) VarId() []byte              { return C.Struct(s).GetObject(0).ToData() }
func (s OutcomeId) SetVarId(v []byte)          { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s OutcomeId) AcceptedInstances() AcceptedInstanceId_List {
	return AcceptedInstanceId_List(C.Struct(s).GetObject(1))
}
func (s OutcomeId) SetAcceptedInstances(v AcceptedInstanceId_List) {
	C.Struct(s).SetObject(1, C.Object(v))
}
func (s OutcomeId) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"acceptedInstances\":")
	if err != nil {
		return err
	}
	{
		s := s.AcceptedInstances()
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
func (s OutcomeId) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s OutcomeId) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("acceptedInstances = ")
	if err != nil {
		return err
	}
	{
		s := s.AcceptedInstances()
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
func (s OutcomeId) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type OutcomeId_List C.PointerList

func NewOutcomeIdList(s *C.Segment, sz int) OutcomeId_List {
	return OutcomeId_List(s.NewCompositeList(0, 2, sz))
}
func (s OutcomeId_List) Len() int           { return C.PointerList(s).Len() }
func (s OutcomeId_List) At(i int) OutcomeId { return OutcomeId(C.PointerList(s).At(i).ToStruct()) }
func (s OutcomeId_List) ToArray() []OutcomeId {
	n := s.Len()
	a := make([]OutcomeId, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s OutcomeId_List) Set(i int, item OutcomeId) { C.PointerList(s).Set(i, C.Object(item)) }

type AcceptedInstanceId C.Struct

func NewAcceptedInstanceId(s *C.Segment) AcceptedInstanceId {
	return AcceptedInstanceId(s.NewStruct(8, 0))
}
func NewRootAcceptedInstanceId(s *C.Segment) AcceptedInstanceId {
	return AcceptedInstanceId(s.NewRootStruct(8, 0))
}
func AutoNewAcceptedInstanceId(s *C.Segment) AcceptedInstanceId {
	return AcceptedInstanceId(s.NewStructAR(8, 0))
}
func ReadRootAcceptedInstanceId(s *C.Segment) AcceptedInstanceId {
	return AcceptedInstanceId(s.Root(0).ToStruct())
}
func (s AcceptedInstanceId) RmId() uint32       { return C.Struct(s).Get32(0) }
func (s AcceptedInstanceId) SetRmId(v uint32)   { C.Struct(s).Set32(0, v) }
func (s AcceptedInstanceId) Vote() VoteEnum     { return VoteEnum(C.Struct(s).Get16(4)) }
func (s AcceptedInstanceId) SetVote(v VoteEnum) { C.Struct(s).Set16(4, uint16(v)) }
func (s AcceptedInstanceId) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
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
func (s AcceptedInstanceId) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s AcceptedInstanceId) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
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
func (s AcceptedInstanceId) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type AcceptedInstanceId_List C.PointerList

func NewAcceptedInstanceIdList(s *C.Segment, sz int) AcceptedInstanceId_List {
	return AcceptedInstanceId_List(s.NewCompositeList(8, 0, sz))
}
func (s AcceptedInstanceId_List) Len() int { return C.PointerList(s).Len() }
func (s AcceptedInstanceId_List) At(i int) AcceptedInstanceId {
	return AcceptedInstanceId(C.PointerList(s).At(i).ToStruct())
}
func (s AcceptedInstanceId_List) ToArray() []AcceptedInstanceId {
	n := s.Len()
	a := make([]AcceptedInstanceId, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s AcceptedInstanceId_List) Set(i int, item AcceptedInstanceId) {
	C.PointerList(s).Set(i, C.Object(item))
}

type VoteEnum uint16

const (
	VOTEENUM_COMMIT        VoteEnum = 0
	VOTEENUM_ABORTBADREAD  VoteEnum = 1
	VOTEENUM_ABORTDEADLOCK VoteEnum = 2
)

func (c VoteEnum) String() string {
	switch c {
	case VOTEENUM_COMMIT:
		return "commit"
	case VOTEENUM_ABORTBADREAD:
		return "abortBadRead"
	case VOTEENUM_ABORTDEADLOCK:
		return "abortDeadlock"
	default:
		return ""
	}
}

func VoteEnumFromString(c string) VoteEnum {
	switch c {
	case "commit":
		return VOTEENUM_COMMIT
	case "abortBadRead":
		return VOTEENUM_ABORTBADREAD
	case "abortDeadlock":
		return VOTEENUM_ABORTDEADLOCK
	default:
		return 0
	}
}

type VoteEnum_List C.PointerList

func NewVoteEnumList(s *C.Segment, sz int) VoteEnum_List { return VoteEnum_List(s.NewUInt16List(sz)) }
func (s VoteEnum_List) Len() int                         { return C.UInt16List(s).Len() }
func (s VoteEnum_List) At(i int) VoteEnum                { return VoteEnum(C.UInt16List(s).At(i)) }
func (s VoteEnum_List) ToArray() []VoteEnum {
	n := s.Len()
	a := make([]VoteEnum, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s VoteEnum_List) Set(i int, item VoteEnum) { C.UInt16List(s).Set(i, uint16(item)) }
func (s VoteEnum) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	buf, err = json.Marshal(s.String())
	if err != nil {
		return err
	}
	_, err = b.Write(buf)
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s VoteEnum) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s VoteEnum) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	_, err = b.WriteString(s.String())
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s VoteEnum) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}
