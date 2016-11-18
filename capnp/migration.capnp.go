package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type Migration C.Struct

func NewMigration(s *C.Segment) Migration      { return Migration(s.NewStruct(8, 1)) }
func NewRootMigration(s *C.Segment) Migration  { return Migration(s.NewRootStruct(8, 1)) }
func AutoNewMigration(s *C.Segment) Migration  { return Migration(s.NewStructAR(8, 1)) }
func ReadRootMigration(s *C.Segment) Migration { return Migration(s.Root(0).ToStruct()) }
func (s Migration) Version() uint32            { return C.Struct(s).Get32(0) }
func (s Migration) SetVersion(v uint32)        { C.Struct(s).Set32(0, v) }
func (s Migration) Elems() MigrationElement_List {
	return MigrationElement_List(C.Struct(s).GetObject(0))
}
func (s Migration) SetElems(v MigrationElement_List) { C.Struct(s).SetObject(0, C.Object(v)) }
func (s Migration) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"version\":")
	if err != nil {
		return err
	}
	{
		s := s.Version()
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
	_, err = b.WriteString("\"elems\":")
	if err != nil {
		return err
	}
	{
		s := s.Elems()
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
func (s Migration) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Migration) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("version = ")
	if err != nil {
		return err
	}
	{
		s := s.Version()
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
	_, err = b.WriteString("elems = ")
	if err != nil {
		return err
	}
	{
		s := s.Elems()
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
func (s Migration) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Migration_List C.PointerList

func NewMigrationList(s *C.Segment, sz int) Migration_List {
	return Migration_List(s.NewCompositeList(8, 1, sz))
}
func (s Migration_List) Len() int           { return C.PointerList(s).Len() }
func (s Migration_List) At(i int) Migration { return Migration(C.PointerList(s).At(i).ToStruct()) }
func (s Migration_List) ToArray() []Migration {
	n := s.Len()
	a := make([]Migration, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Migration_List) Set(i int, item Migration) { C.PointerList(s).Set(i, C.Object(item)) }

type MigrationComplete C.Struct

func NewMigrationComplete(s *C.Segment) MigrationComplete { return MigrationComplete(s.NewStruct(8, 0)) }
func NewRootMigrationComplete(s *C.Segment) MigrationComplete {
	return MigrationComplete(s.NewRootStruct(8, 0))
}
func AutoNewMigrationComplete(s *C.Segment) MigrationComplete {
	return MigrationComplete(s.NewStructAR(8, 0))
}
func ReadRootMigrationComplete(s *C.Segment) MigrationComplete {
	return MigrationComplete(s.Root(0).ToStruct())
}
func (s MigrationComplete) Version() uint32     { return C.Struct(s).Get32(0) }
func (s MigrationComplete) SetVersion(v uint32) { C.Struct(s).Set32(0, v) }
func (s MigrationComplete) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"version\":")
	if err != nil {
		return err
	}
	{
		s := s.Version()
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
func (s MigrationComplete) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s MigrationComplete) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("version = ")
	if err != nil {
		return err
	}
	{
		s := s.Version()
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
func (s MigrationComplete) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type MigrationComplete_List C.PointerList

func NewMigrationCompleteList(s *C.Segment, sz int) MigrationComplete_List {
	return MigrationComplete_List(s.NewCompositeList(8, 0, sz))
}
func (s MigrationComplete_List) Len() int { return C.PointerList(s).Len() }
func (s MigrationComplete_List) At(i int) MigrationComplete {
	return MigrationComplete(C.PointerList(s).At(i).ToStruct())
}
func (s MigrationComplete_List) ToArray() []MigrationComplete {
	n := s.Len()
	a := make([]MigrationComplete, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s MigrationComplete_List) Set(i int, item MigrationComplete) {
	C.PointerList(s).Set(i, C.Object(item))
}

type MigrationElement C.Struct

func NewMigrationElement(s *C.Segment) MigrationElement { return MigrationElement(s.NewStruct(0, 2)) }
func NewRootMigrationElement(s *C.Segment) MigrationElement {
	return MigrationElement(s.NewRootStruct(0, 2))
}
func AutoNewMigrationElement(s *C.Segment) MigrationElement {
	return MigrationElement(s.NewStructAR(0, 2))
}
func ReadRootMigrationElement(s *C.Segment) MigrationElement {
	return MigrationElement(s.Root(0).ToStruct())
}
func (s MigrationElement) Txn() []byte        { return C.Struct(s).GetObject(0).ToData() }
func (s MigrationElement) SetTxn(v []byte)    { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s MigrationElement) Vars() Var_List     { return Var_List(C.Struct(s).GetObject(1)) }
func (s MigrationElement) SetVars(v Var_List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s MigrationElement) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"vars\":")
	if err != nil {
		return err
	}
	{
		s := s.Vars()
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
func (s MigrationElement) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s MigrationElement) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("vars = ")
	if err != nil {
		return err
	}
	{
		s := s.Vars()
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
func (s MigrationElement) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type MigrationElement_List C.PointerList

func NewMigrationElementList(s *C.Segment, sz int) MigrationElement_List {
	return MigrationElement_List(s.NewCompositeList(0, 2, sz))
}
func (s MigrationElement_List) Len() int { return C.PointerList(s).Len() }
func (s MigrationElement_List) At(i int) MigrationElement {
	return MigrationElement(C.PointerList(s).At(i).ToStruct())
}
func (s MigrationElement_List) ToArray() []MigrationElement {
	n := s.Len()
	a := make([]MigrationElement, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s MigrationElement_List) Set(i int, item MigrationElement) {
	C.PointerList(s).Set(i, C.Object(item))
}
