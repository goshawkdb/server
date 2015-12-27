package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type VectorClock C.Struct

func NewVectorClock(s *C.Segment) VectorClock      { return VectorClock(s.NewStruct(0, 2)) }
func NewRootVectorClock(s *C.Segment) VectorClock  { return VectorClock(s.NewRootStruct(0, 2)) }
func AutoNewVectorClock(s *C.Segment) VectorClock  { return VectorClock(s.NewStructAR(0, 2)) }
func ReadRootVectorClock(s *C.Segment) VectorClock { return VectorClock(s.Root(0).ToStruct()) }
func (s VectorClock) VarUuids() C.DataList         { return C.DataList(C.Struct(s).GetObject(0)) }
func (s VectorClock) SetVarUuids(v C.DataList)     { C.Struct(s).SetObject(0, C.Object(v)) }
func (s VectorClock) Values() C.UInt64List         { return C.UInt64List(C.Struct(s).GetObject(1)) }
func (s VectorClock) SetValues(v C.UInt64List)     { C.Struct(s).SetObject(1, C.Object(v)) }
func (s VectorClock) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"varUuids\":")
	if err != nil {
		return err
	}
	{
		s := s.VarUuids()
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
	_, err = b.WriteString("\"values\":")
	if err != nil {
		return err
	}
	{
		s := s.Values()
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
func (s VectorClock) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s VectorClock) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("varUuids = ")
	if err != nil {
		return err
	}
	{
		s := s.VarUuids()
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
	_, err = b.WriteString("values = ")
	if err != nil {
		return err
	}
	{
		s := s.Values()
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
func (s VectorClock) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type VectorClock_List C.PointerList

func NewVectorClockList(s *C.Segment, sz int) VectorClock_List {
	return VectorClock_List(s.NewCompositeList(0, 2, sz))
}
func (s VectorClock_List) Len() int             { return C.PointerList(s).Len() }
func (s VectorClock_List) At(i int) VectorClock { return VectorClock(C.PointerList(s).At(i).ToStruct()) }
func (s VectorClock_List) ToArray() []VectorClock {
	n := s.Len()
	a := make([]VectorClock, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s VectorClock_List) Set(i int, item VectorClock) { C.PointerList(s).Set(i, C.Object(item)) }
