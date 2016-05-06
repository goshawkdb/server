package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type ProposerState C.Struct

func NewProposerState(s *C.Segment) ProposerState      { return ProposerState(s.NewStruct(0, 1)) }
func NewRootProposerState(s *C.Segment) ProposerState  { return ProposerState(s.NewRootStruct(0, 1)) }
func AutoNewProposerState(s *C.Segment) ProposerState  { return ProposerState(s.NewStructAR(0, 1)) }
func ReadRootProposerState(s *C.Segment) ProposerState { return ProposerState(s.Root(0).ToStruct()) }
func (s ProposerState) Acceptors() C.UInt32List        { return C.UInt32List(C.Struct(s).GetObject(0)) }
func (s ProposerState) SetAcceptors(v C.UInt32List)    { C.Struct(s).SetObject(0, C.Object(v)) }
func (s ProposerState) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"acceptors\":")
	if err != nil {
		return err
	}
	{
		s := s.Acceptors()
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
func (s ProposerState) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s ProposerState) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("acceptors = ")
	if err != nil {
		return err
	}
	{
		s := s.Acceptors()
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
func (s ProposerState) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type ProposerState_List C.PointerList

func NewProposerStateList(s *C.Segment, sz int) ProposerState_List {
	return ProposerState_List(s.NewCompositeList(0, 1, sz))
}
func (s ProposerState_List) Len() int { return C.PointerList(s).Len() }
func (s ProposerState_List) At(i int) ProposerState {
	return ProposerState(C.PointerList(s).At(i).ToStruct())
}
func (s ProposerState_List) ToArray() []ProposerState {
	n := s.Len()
	a := make([]ProposerState, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s ProposerState_List) Set(i int, item ProposerState) { C.PointerList(s).Set(i, C.Object(item)) }
