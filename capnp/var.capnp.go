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

func NewVar(s *C.Segment) Var            { return Var(s.NewStruct(0, 6)) }
func NewRootVar(s *C.Segment) Var        { return Var(s.NewRootStruct(0, 6)) }
func AutoNewVar(s *C.Segment) Var        { return Var(s.NewStructAR(0, 6)) }
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
func (s Var) Subscriptions() []byte      { return C.Struct(s).GetObject(5).ToData() }
func (s Var) SetSubscriptions(v []byte)  { C.Struct(s).SetObject(5, s.Segment.NewData(v)) }
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
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"subscriptions\":")
	if err != nil {
		return err
	}
	{
		s := s.Subscriptions()
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
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("subscriptions = ")
	if err != nil {
		return err
	}
	{
		s := s.Subscriptions()
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

func NewVarList(s *C.Segment, sz int) Var_List { return Var_List(s.NewCompositeList(0, 6, sz)) }
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

type SubscriptionListWrapper C.Struct

func NewSubscriptionListWrapper(s *C.Segment) SubscriptionListWrapper {
	return SubscriptionListWrapper(s.NewStruct(0, 1))
}
func NewRootSubscriptionListWrapper(s *C.Segment) SubscriptionListWrapper {
	return SubscriptionListWrapper(s.NewRootStruct(0, 1))
}
func AutoNewSubscriptionListWrapper(s *C.Segment) SubscriptionListWrapper {
	return SubscriptionListWrapper(s.NewStructAR(0, 1))
}
func ReadRootSubscriptionListWrapper(s *C.Segment) SubscriptionListWrapper {
	return SubscriptionListWrapper(s.Root(0).ToStruct())
}
func (s SubscriptionListWrapper) Subscriptions() Subscription_List {
	return Subscription_List(C.Struct(s).GetObject(0))
}
func (s SubscriptionListWrapper) SetSubscriptions(v Subscription_List) {
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s SubscriptionListWrapper) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"subscriptions\":")
	if err != nil {
		return err
	}
	{
		s := s.Subscriptions()
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
func (s SubscriptionListWrapper) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s SubscriptionListWrapper) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("subscriptions = ")
	if err != nil {
		return err
	}
	{
		s := s.Subscriptions()
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
func (s SubscriptionListWrapper) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type SubscriptionListWrapper_List C.PointerList

func NewSubscriptionListWrapperList(s *C.Segment, sz int) SubscriptionListWrapper_List {
	return SubscriptionListWrapper_List(s.NewCompositeList(0, 1, sz))
}
func (s SubscriptionListWrapper_List) Len() int { return C.PointerList(s).Len() }
func (s SubscriptionListWrapper_List) At(i int) SubscriptionListWrapper {
	return SubscriptionListWrapper(C.PointerList(s).At(i).ToStruct())
}
func (s SubscriptionListWrapper_List) ToArray() []SubscriptionListWrapper {
	n := s.Len()
	a := make([]SubscriptionListWrapper, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s SubscriptionListWrapper_List) Set(i int, item SubscriptionListWrapper) {
	C.PointerList(s).Set(i, C.Object(item))
}

type Subscription C.Struct

func NewSubscription(s *C.Segment) Subscription      { return Subscription(s.NewStruct(8, 1)) }
func NewRootSubscription(s *C.Segment) Subscription  { return Subscription(s.NewRootStruct(8, 1)) }
func AutoNewSubscription(s *C.Segment) Subscription  { return Subscription(s.NewStructAR(8, 1)) }
func ReadRootSubscription(s *C.Segment) Subscription { return Subscription(s.Root(0).ToStruct()) }
func (s Subscription) TxnId() []byte                 { return C.Struct(s).GetObject(0).ToData() }
func (s Subscription) SetTxnId(v []byte)             { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s Subscription) Added() bool                   { return C.Struct(s).Get1(0) }
func (s Subscription) SetAdded(v bool)               { C.Struct(s).Set1(0, v) }
func (s Subscription) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"added\":")
	if err != nil {
		return err
	}
	{
		s := s.Added()
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
func (s Subscription) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Subscription) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("added = ")
	if err != nil {
		return err
	}
	{
		s := s.Added()
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
func (s Subscription) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Subscription_List C.PointerList

func NewSubscriptionList(s *C.Segment, sz int) Subscription_List {
	return Subscription_List(s.NewCompositeList(8, 1, sz))
}
func (s Subscription_List) Len() int { return C.PointerList(s).Len() }
func (s Subscription_List) At(i int) Subscription {
	return Subscription(C.PointerList(s).At(i).ToStruct())
}
func (s Subscription_List) ToArray() []Subscription {
	n := s.Len()
	a := make([]Subscription, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Subscription_List) Set(i int, item Subscription) { C.PointerList(s).Set(i, C.Object(item)) }
