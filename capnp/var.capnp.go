package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type Var C.Struct

func NewVar(s *C.Segment) Var                { return Var(s.NewStruct(0, 5)) }
func NewRootVar(s *C.Segment) Var            { return Var(s.NewRootStruct(0, 5)) }
func AutoNewVar(s *C.Segment) Var            { return Var(s.NewStructAR(0, 5)) }
func ReadRootVar(s *C.Segment) Var           { return Var(s.Root(0).ToStruct()) }
func (s Var) Id() []byte                     { return C.Struct(s).GetObject(0).ToData() }
func (s Var) SetId(v []byte)                 { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s Var) Positions() C.UInt8List         { return C.UInt8List(C.Struct(s).GetObject(1)) }
func (s Var) SetPositions(v C.UInt8List)     { C.Struct(s).SetObject(1, C.Object(v)) }
func (s Var) WriteTxnId() []byte             { return C.Struct(s).GetObject(2).ToData() }
func (s Var) SetWriteTxnId(v []byte)         { C.Struct(s).SetObject(2, s.Segment.NewData(v)) }
func (s Var) WriteTxnClock() VectorClock     { return VectorClock(C.Struct(s).GetObject(3).ToStruct()) }
func (s Var) SetWriteTxnClock(v VectorClock) { C.Struct(s).SetObject(3, C.Object(v)) }
func (s Var) WritesClock() VectorClock       { return VectorClock(C.Struct(s).GetObject(4).ToStruct()) }
func (s Var) SetWritesClock(v VectorClock)   { C.Struct(s).SetObject(4, C.Object(v)) }
func (s Var) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"positions\":")
	if err != nil {
		return err
	}
	{
		s := s.Positions()
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
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"writeTxnId\":")
	if err != nil {
		return err
	}
	{
		s := s.WriteTxnId()
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
	_, err = b.WriteString("\"writeTxnClock\":")
	if err != nil {
		return err
	}
	{
		s := s.WriteTxnClock()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"writesClock\":")
	if err != nil {
		return err
	}
	{
		s := s.WritesClock()
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
func (s Var) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Var) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("positions = ")
	if err != nil {
		return err
	}
	{
		s := s.Positions()
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
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("writeTxnId = ")
	if err != nil {
		return err
	}
	{
		s := s.WriteTxnId()
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
	_, err = b.WriteString("writeTxnClock = ")
	if err != nil {
		return err
	}
	{
		s := s.WriteTxnClock()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("writesClock = ")
	if err != nil {
		return err
	}
	{
		s := s.WritesClock()
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
func (s Var) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Var_List C.PointerList

func NewVarList(s *C.Segment, sz int) Var_List { return Var_List(s.NewCompositeList(0, 5, sz)) }
func (s Var_List) Len() int                    { return C.PointerList(s).Len() }
func (s Var_List) At(i int) Var                { return Var(C.PointerList(s).At(i).ToStruct()) }
func (s Var_List) ToArray() []Var {
	n := s.Len()
	a := make([]Var, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Var_List) Set(i int, item Var) { C.PointerList(s).Set(i, C.Object(item)) }
