package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type AcceptorState C.Struct

func NewAcceptorState(s *C.Segment) AcceptorState      { return AcceptorState(s.NewStruct(8, 3)) }
func NewRootAcceptorState(s *C.Segment) AcceptorState  { return AcceptorState(s.NewRootStruct(8, 3)) }
func AutoNewAcceptorState(s *C.Segment) AcceptorState  { return AcceptorState(s.NewStructAR(8, 3)) }
func ReadRootAcceptorState(s *C.Segment) AcceptorState { return AcceptorState(s.Root(0).ToStruct()) }
func (s AcceptorState) Outcome() Outcome               { return Outcome(C.Struct(s).GetObject(0).ToStruct()) }
func (s AcceptorState) SetOutcome(v Outcome)           { C.Struct(s).SetObject(0, C.Object(v)) }
func (s AcceptorState) SendToAll() bool                { return C.Struct(s).Get1(0) }
func (s AcceptorState) SetSendToAll(v bool)            { C.Struct(s).Set1(0, v) }
func (s AcceptorState) Instances() InstancesForVar_List {
	return InstancesForVar_List(C.Struct(s).GetObject(1))
}
func (s AcceptorState) SetInstances(v InstancesForVar_List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s AcceptorState) Subscribers() C.DataList             { return C.DataList(C.Struct(s).GetObject(2)) }
func (s AcceptorState) SetSubscribers(v C.DataList)         { C.Struct(s).SetObject(2, C.Object(v)) }
func (s AcceptorState) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"sendToAll\":")
	if err != nil {
		return err
	}
	{
		s := s.SendToAll()
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
	_, err = b.WriteString("\"instances\":")
	if err != nil {
		return err
	}
	{
		s := s.Instances()
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
func (s AcceptorState) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s AcceptorState) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("sendToAll = ")
	if err != nil {
		return err
	}
	{
		s := s.SendToAll()
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
	_, err = b.WriteString("instances = ")
	if err != nil {
		return err
	}
	{
		s := s.Instances()
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
func (s AcceptorState) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type AcceptorState_List C.PointerList

func NewAcceptorStateList(s *C.Segment, sz int) AcceptorState_List {
	return AcceptorState_List(s.NewCompositeList(8, 3, sz))
}
func (s AcceptorState_List) Len() int { return C.PointerList(s).Len() }
func (s AcceptorState_List) At(i int) AcceptorState {
	return AcceptorState(C.PointerList(s).At(i).ToStruct())
}
func (s AcceptorState_List) ToArray() []AcceptorState {
	n := s.Len()
	a := make([]AcceptorState, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s AcceptorState_List) Set(i int, item AcceptorState) { C.PointerList(s).Set(i, C.Object(item)) }

type InstancesForVar C.Struct

func NewInstancesForVar(s *C.Segment) InstancesForVar { return InstancesForVar(s.NewStruct(0, 3)) }
func NewRootInstancesForVar(s *C.Segment) InstancesForVar {
	return InstancesForVar(s.NewRootStruct(0, 3))
}
func AutoNewInstancesForVar(s *C.Segment) InstancesForVar { return InstancesForVar(s.NewStructAR(0, 3)) }
func ReadRootInstancesForVar(s *C.Segment) InstancesForVar {
	return InstancesForVar(s.Root(0).ToStruct())
}
func (s InstancesForVar) VarId() []byte     { return C.Struct(s).GetObject(0).ToData() }
func (s InstancesForVar) SetVarId(v []byte) { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s InstancesForVar) Instances() AcceptedInstance_List {
	return AcceptedInstance_List(C.Struct(s).GetObject(1))
}
func (s InstancesForVar) SetInstances(v AcceptedInstance_List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s InstancesForVar) Result() []byte                       { return C.Struct(s).GetObject(2).ToData() }
func (s InstancesForVar) SetResult(v []byte)                   { C.Struct(s).SetObject(2, s.Segment.NewData(v)) }
func (s InstancesForVar) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"instances\":")
	if err != nil {
		return err
	}
	{
		s := s.Instances()
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
	_, err = b.WriteString("\"result\":")
	if err != nil {
		return err
	}
	{
		s := s.Result()
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
func (s InstancesForVar) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s InstancesForVar) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("instances = ")
	if err != nil {
		return err
	}
	{
		s := s.Instances()
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
	_, err = b.WriteString("result = ")
	if err != nil {
		return err
	}
	{
		s := s.Result()
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
func (s InstancesForVar) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type InstancesForVar_List C.PointerList

func NewInstancesForVarList(s *C.Segment, sz int) InstancesForVar_List {
	return InstancesForVar_List(s.NewCompositeList(0, 3, sz))
}
func (s InstancesForVar_List) Len() int { return C.PointerList(s).Len() }
func (s InstancesForVar_List) At(i int) InstancesForVar {
	return InstancesForVar(C.PointerList(s).At(i).ToStruct())
}
func (s InstancesForVar_List) ToArray() []InstancesForVar {
	n := s.Len()
	a := make([]InstancesForVar, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s InstancesForVar_List) Set(i int, item InstancesForVar) {
	C.PointerList(s).Set(i, C.Object(item))
}

type AcceptedInstance C.Struct

func NewAcceptedInstance(s *C.Segment) AcceptedInstance { return AcceptedInstance(s.NewStruct(16, 1)) }
func NewRootAcceptedInstance(s *C.Segment) AcceptedInstance {
	return AcceptedInstance(s.NewRootStruct(16, 1))
}
func AutoNewAcceptedInstance(s *C.Segment) AcceptedInstance {
	return AcceptedInstance(s.NewStructAR(16, 1))
}
func ReadRootAcceptedInstance(s *C.Segment) AcceptedInstance {
	return AcceptedInstance(s.Root(0).ToStruct())
}
func (s AcceptedInstance) RmId() uint32            { return C.Struct(s).Get32(0) }
func (s AcceptedInstance) SetRmId(v uint32)        { C.Struct(s).Set32(0, v) }
func (s AcceptedInstance) RoundNumber() uint64     { return C.Struct(s).Get64(8) }
func (s AcceptedInstance) SetRoundNumber(v uint64) { C.Struct(s).Set64(8, v) }
func (s AcceptedInstance) Ballot() []byte          { return C.Struct(s).GetObject(0).ToData() }
func (s AcceptedInstance) SetBallot(v []byte)      { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s AcceptedInstance) WriteJSON(w io.Writer) error {
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
	err = b.Flush()
	return err
}
func (s AcceptedInstance) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s AcceptedInstance) WriteCapLit(w io.Writer) error {
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
	err = b.Flush()
	return err
}
func (s AcceptedInstance) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type AcceptedInstance_List C.PointerList

func NewAcceptedInstanceList(s *C.Segment, sz int) AcceptedInstance_List {
	return AcceptedInstance_List(s.NewCompositeList(16, 1, sz))
}
func (s AcceptedInstance_List) Len() int { return C.PointerList(s).Len() }
func (s AcceptedInstance_List) At(i int) AcceptedInstance {
	return AcceptedInstance(C.PointerList(s).At(i).ToStruct())
}
func (s AcceptedInstance_List) ToArray() []AcceptedInstance {
	n := s.Len()
	a := make([]AcceptedInstance, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s AcceptedInstance_List) Set(i int, item AcceptedInstance) {
	C.PointerList(s).Set(i, C.Object(item))
}
