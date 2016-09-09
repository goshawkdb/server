package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common/capnp"
	"io"
)

type Var C.Struct

func NewVar(s *C.Segment) Var            { return Var(s.NewStruct(0, 5)) }
func NewRootVar(s *C.Segment) Var        { return Var(s.NewRootStruct(0, 5)) }
func AutoNewVar(s *C.Segment) Var        { return Var(s.NewStructAR(0, 5)) }
func ReadRootVar(s *C.Segment) Var       { return Var(s.Root(0).ToStruct()) }
func (s Var) Id() []byte                 { return C.Struct(s).GetObject(0).ToData() }
func (s Var) SetId(v []byte)             { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s Var) Positions() C.UInt8List     { return C.UInt8List(C.Struct(s).GetObject(1)) }
func (s Var) SetPositions(v C.UInt8List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s Var) WriteTxnId() []byte         { return C.Struct(s).GetObject(2).ToData() }
func (s Var) SetWriteTxnId(v []byte)     { C.Struct(s).SetObject(2, s.Segment.NewData(v)) }
func (s Var) WriteTxnClock() []byte      { return C.Struct(s).GetObject(3).ToData() }
func (s Var) SetWriteTxnClock(v []byte)  { C.Struct(s).SetObject(3, s.Segment.NewData(v)) }
func (s Var) WritesClock() []byte        { return C.Struct(s).GetObject(4).ToData() }
func (s Var) SetWritesClock(v []byte)    { C.Struct(s).SetObject(4, s.Segment.NewData(v)) }
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
	_, err = b.WriteString("\"writesClock\":")
	if err != nil {
		return err
	}
	{
		s := s.WritesClock()
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
	_, err = b.WriteString("writesClock = ")
	if err != nil {
		return err
	}
	{
		s := s.WritesClock()
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

type VarIdPos C.Struct

func NewVarIdPos(s *C.Segment) VarIdPos       { return VarIdPos(s.NewStruct(0, 3)) }
func NewRootVarIdPos(s *C.Segment) VarIdPos   { return VarIdPos(s.NewRootStruct(0, 3)) }
func AutoNewVarIdPos(s *C.Segment) VarIdPos   { return VarIdPos(s.NewStructAR(0, 3)) }
func ReadRootVarIdPos(s *C.Segment) VarIdPos  { return VarIdPos(s.Root(0).ToStruct()) }
func (s VarIdPos) Id() []byte                 { return C.Struct(s).GetObject(0).ToData() }
func (s VarIdPos) SetId(v []byte)             { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s VarIdPos) Positions() C.UInt8List     { return C.UInt8List(C.Struct(s).GetObject(1)) }
func (s VarIdPos) SetPositions(v C.UInt8List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s VarIdPos) Capability() capnp.Capability {
	return capnp.Capability(C.Struct(s).GetObject(2).ToStruct())
}
func (s VarIdPos) SetCapability(v capnp.Capability) { C.Struct(s).SetObject(2, C.Object(v)) }
func (s VarIdPos) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"capability\":")
	if err != nil {
		return err
	}
	{
		s := s.Capability()
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
func (s VarIdPos) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s VarIdPos) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("capability = ")
	if err != nil {
		return err
	}
	{
		s := s.Capability()
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
func (s VarIdPos) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type VarIdPos_List C.PointerList

func NewVarIdPosList(s *C.Segment, sz int) VarIdPos_List {
	return VarIdPos_List(s.NewCompositeList(0, 3, sz))
}
func (s VarIdPos_List) Len() int          { return C.PointerList(s).Len() }
func (s VarIdPos_List) At(i int) VarIdPos { return VarIdPos(C.PointerList(s).At(i).ToStruct()) }
func (s VarIdPos_List) ToArray() []VarIdPos {
	n := s.Len()
	a := make([]VarIdPos, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s VarIdPos_List) Set(i int, item VarIdPos) { C.PointerList(s).Set(i, C.Object(item)) }
