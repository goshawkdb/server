package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type TxnLocallyComplete C.Struct

func NewTxnLocallyComplete(s *C.Segment) TxnLocallyComplete {
	return TxnLocallyComplete(s.NewStruct(0, 1))
}
func NewRootTxnLocallyComplete(s *C.Segment) TxnLocallyComplete {
	return TxnLocallyComplete(s.NewRootStruct(0, 1))
}
func AutoNewTxnLocallyComplete(s *C.Segment) TxnLocallyComplete {
	return TxnLocallyComplete(s.NewStructAR(0, 1))
}
func ReadRootTxnLocallyComplete(s *C.Segment) TxnLocallyComplete {
	return TxnLocallyComplete(s.Root(0).ToStruct())
}
func (s TxnLocallyComplete) TxnId() []byte     { return C.Struct(s).GetObject(0).ToData() }
func (s TxnLocallyComplete) SetTxnId(v []byte) { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s TxnLocallyComplete) WriteJSON(w io.Writer) error {
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
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TxnLocallyComplete) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TxnLocallyComplete) WriteCapLit(w io.Writer) error {
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
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TxnLocallyComplete) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TxnLocallyComplete_List C.PointerList

func NewTxnLocallyCompleteList(s *C.Segment, sz int) TxnLocallyComplete_List {
	return TxnLocallyComplete_List(s.NewCompositeList(0, 1, sz))
}
func (s TxnLocallyComplete_List) Len() int { return C.PointerList(s).Len() }
func (s TxnLocallyComplete_List) At(i int) TxnLocallyComplete {
	return TxnLocallyComplete(C.PointerList(s).At(i).ToStruct())
}
func (s TxnLocallyComplete_List) ToArray() []TxnLocallyComplete {
	n := s.Len()
	a := make([]TxnLocallyComplete, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TxnLocallyComplete_List) Set(i int, item TxnLocallyComplete) {
	C.PointerList(s).Set(i, C.Object(item))
}

type TxnGloballyComplete C.Struct

func NewTxnGloballyComplete(s *C.Segment) TxnGloballyComplete {
	return TxnGloballyComplete(s.NewStruct(0, 1))
}
func NewRootTxnGloballyComplete(s *C.Segment) TxnGloballyComplete {
	return TxnGloballyComplete(s.NewRootStruct(0, 1))
}
func AutoNewTxnGloballyComplete(s *C.Segment) TxnGloballyComplete {
	return TxnGloballyComplete(s.NewStructAR(0, 1))
}
func ReadRootTxnGloballyComplete(s *C.Segment) TxnGloballyComplete {
	return TxnGloballyComplete(s.Root(0).ToStruct())
}
func (s TxnGloballyComplete) TxnId() []byte     { return C.Struct(s).GetObject(0).ToData() }
func (s TxnGloballyComplete) SetTxnId(v []byte) { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s TxnGloballyComplete) WriteJSON(w io.Writer) error {
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
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TxnGloballyComplete) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TxnGloballyComplete) WriteCapLit(w io.Writer) error {
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
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TxnGloballyComplete) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TxnGloballyComplete_List C.PointerList

func NewTxnGloballyCompleteList(s *C.Segment, sz int) TxnGloballyComplete_List {
	return TxnGloballyComplete_List(s.NewCompositeList(0, 1, sz))
}
func (s TxnGloballyComplete_List) Len() int { return C.PointerList(s).Len() }
func (s TxnGloballyComplete_List) At(i int) TxnGloballyComplete {
	return TxnGloballyComplete(C.PointerList(s).At(i).ToStruct())
}
func (s TxnGloballyComplete_List) ToArray() []TxnGloballyComplete {
	n := s.Len()
	a := make([]TxnGloballyComplete, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TxnGloballyComplete_List) Set(i int, item TxnGloballyComplete) {
	C.PointerList(s).Set(i, C.Object(item))
}

type TxnSubmissionComplete C.Struct

func NewTxnSubmissionComplete(s *C.Segment) TxnSubmissionComplete {
	return TxnSubmissionComplete(s.NewStruct(0, 1))
}
func NewRootTxnSubmissionComplete(s *C.Segment) TxnSubmissionComplete {
	return TxnSubmissionComplete(s.NewRootStruct(0, 1))
}
func AutoNewTxnSubmissionComplete(s *C.Segment) TxnSubmissionComplete {
	return TxnSubmissionComplete(s.NewStructAR(0, 1))
}
func ReadRootTxnSubmissionComplete(s *C.Segment) TxnSubmissionComplete {
	return TxnSubmissionComplete(s.Root(0).ToStruct())
}
func (s TxnSubmissionComplete) TxnId() []byte     { return C.Struct(s).GetObject(0).ToData() }
func (s TxnSubmissionComplete) SetTxnId(v []byte) { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s TxnSubmissionComplete) WriteJSON(w io.Writer) error {
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
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TxnSubmissionComplete) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TxnSubmissionComplete) WriteCapLit(w io.Writer) error {
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
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TxnSubmissionComplete) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TxnSubmissionComplete_List C.PointerList

func NewTxnSubmissionCompleteList(s *C.Segment, sz int) TxnSubmissionComplete_List {
	return TxnSubmissionComplete_List(s.NewCompositeList(0, 1, sz))
}
func (s TxnSubmissionComplete_List) Len() int { return C.PointerList(s).Len() }
func (s TxnSubmissionComplete_List) At(i int) TxnSubmissionComplete {
	return TxnSubmissionComplete(C.PointerList(s).At(i).ToStruct())
}
func (s TxnSubmissionComplete_List) ToArray() []TxnSubmissionComplete {
	n := s.Len()
	a := make([]TxnSubmissionComplete, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TxnSubmissionComplete_List) Set(i int, item TxnSubmissionComplete) {
	C.PointerList(s).Set(i, C.Object(item))
}

type TxnSubmissionAbort C.Struct

func NewTxnSubmissionAbort(s *C.Segment) TxnSubmissionAbort {
	return TxnSubmissionAbort(s.NewStruct(0, 1))
}
func NewRootTxnSubmissionAbort(s *C.Segment) TxnSubmissionAbort {
	return TxnSubmissionAbort(s.NewRootStruct(0, 1))
}
func AutoNewTxnSubmissionAbort(s *C.Segment) TxnSubmissionAbort {
	return TxnSubmissionAbort(s.NewStructAR(0, 1))
}
func ReadRootTxnSubmissionAbort(s *C.Segment) TxnSubmissionAbort {
	return TxnSubmissionAbort(s.Root(0).ToStruct())
}
func (s TxnSubmissionAbort) TxnId() []byte     { return C.Struct(s).GetObject(0).ToData() }
func (s TxnSubmissionAbort) SetTxnId(v []byte) { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s TxnSubmissionAbort) WriteJSON(w io.Writer) error {
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
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TxnSubmissionAbort) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s TxnSubmissionAbort) WriteCapLit(w io.Writer) error {
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
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s TxnSubmissionAbort) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type TxnSubmissionAbort_List C.PointerList

func NewTxnSubmissionAbortList(s *C.Segment, sz int) TxnSubmissionAbort_List {
	return TxnSubmissionAbort_List(s.NewCompositeList(0, 1, sz))
}
func (s TxnSubmissionAbort_List) Len() int { return C.PointerList(s).Len() }
func (s TxnSubmissionAbort_List) At(i int) TxnSubmissionAbort {
	return TxnSubmissionAbort(C.PointerList(s).At(i).ToStruct())
}
func (s TxnSubmissionAbort_List) ToArray() []TxnSubmissionAbort {
	n := s.Len()
	a := make([]TxnSubmissionAbort, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s TxnSubmissionAbort_List) Set(i int, item TxnSubmissionAbort) {
	C.PointerList(s).Set(i, C.Object(item))
}
