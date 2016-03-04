package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type Configuration C.Struct
type ConfigurationTransitioningTo Configuration
type Configuration_Which uint16

const (
	CONFIGURATION_TRANSITIONINGTO Configuration_Which = 0
	CONFIGURATION_STABLE          Configuration_Which = 1
)

func NewConfiguration(s *C.Segment) Configuration      { return Configuration(s.NewStruct(16, 7)) }
func NewRootConfiguration(s *C.Segment) Configuration  { return Configuration(s.NewRootStruct(16, 7)) }
func AutoNewConfiguration(s *C.Segment) Configuration  { return Configuration(s.NewStructAR(16, 7)) }
func ReadRootConfiguration(s *C.Segment) Configuration { return Configuration(s.Root(0).ToStruct()) }
func (s Configuration) Which() Configuration_Which     { return Configuration_Which(C.Struct(s).Get16(8)) }
func (s Configuration) ClusterId() string              { return C.Struct(s).GetObject(0).ToText() }
func (s Configuration) ClusterIdBytes() []byte         { return C.Struct(s).GetObject(0).ToData() }
func (s Configuration) SetClusterId(v string)          { C.Struct(s).SetObject(0, s.Segment.NewText(v)) }
func (s Configuration) Version() uint32                { return C.Struct(s).Get32(0) }
func (s Configuration) SetVersion(v uint32)            { C.Struct(s).Set32(0, v) }
func (s Configuration) Hosts() C.TextList              { return C.TextList(C.Struct(s).GetObject(1)) }
func (s Configuration) SetHosts(v C.TextList)          { C.Struct(s).SetObject(1, C.Object(v)) }
func (s Configuration) F() uint8                       { return C.Struct(s).Get8(4) }
func (s Configuration) SetF(v uint8)                   { C.Struct(s).Set8(4, v) }
func (s Configuration) MaxRMCount() uint8              { return C.Struct(s).Get8(5) }
func (s Configuration) SetMaxRMCount(v uint8)          { C.Struct(s).Set8(5, v) }
func (s Configuration) AsyncFlush() bool               { return C.Struct(s).Get1(48) }
func (s Configuration) SetAsyncFlush(v bool)           { C.Struct(s).Set1(48, v) }
func (s Configuration) Rms() C.UInt32List              { return C.UInt32List(C.Struct(s).GetObject(2)) }
func (s Configuration) SetRms(v C.UInt32List)          { C.Struct(s).SetObject(2, C.Object(v)) }
func (s Configuration) RmsRemoved() C.UInt32List       { return C.UInt32List(C.Struct(s).GetObject(3)) }
func (s Configuration) SetRmsRemoved(v C.UInt32List)   { C.Struct(s).SetObject(3, C.Object(v)) }
func (s Configuration) Fingerprints() C.DataList       { return C.DataList(C.Struct(s).GetObject(4)) }
func (s Configuration) SetFingerprints(v C.DataList)   { C.Struct(s).SetObject(4, C.Object(v)) }
func (s Configuration) TransitioningTo() ConfigurationTransitioningTo {
	return ConfigurationTransitioningTo(s)
}
func (s Configuration) SetTransitioningTo() { C.Struct(s).Set16(8, 0) }
func (s ConfigurationTransitioningTo) Configuration() Configuration {
	return Configuration(C.Struct(s).GetObject(5).ToStruct())
}
func (s ConfigurationTransitioningTo) SetConfiguration(v Configuration) {
	C.Struct(s).SetObject(5, C.Object(v))
}
func (s ConfigurationTransitioningTo) InstalledOnAll() bool     { return C.Struct(s).Get1(49) }
func (s ConfigurationTransitioningTo) SetInstalledOnAll(v bool) { C.Struct(s).Set1(49, v) }
func (s ConfigurationTransitioningTo) Pending() Condition_List {
	return Condition_List(C.Struct(s).GetObject(6))
}
func (s ConfigurationTransitioningTo) SetPending(v Condition_List) {
	C.Struct(s).SetObject(6, C.Object(v))
}
func (s Configuration) SetStable() { C.Struct(s).Set16(8, 1) }
func (s Configuration) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"clusterId\":")
	if err != nil {
		return err
	}
	{
		s := s.ClusterId()
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
	_, err = b.WriteString("\"hosts\":")
	if err != nil {
		return err
	}
	{
		s := s.Hosts()
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
	_, err = b.WriteString("\"f\":")
	if err != nil {
		return err
	}
	{
		s := s.F()
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
	_, err = b.WriteString("\"maxRMCount\":")
	if err != nil {
		return err
	}
	{
		s := s.MaxRMCount()
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
	_, err = b.WriteString("\"asyncFlush\":")
	if err != nil {
		return err
	}
	{
		s := s.AsyncFlush()
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
	_, err = b.WriteString("\"rms\":")
	if err != nil {
		return err
	}
	{
		s := s.Rms()
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
	_, err = b.WriteString("\"rmsRemoved\":")
	if err != nil {
		return err
	}
	{
		s := s.RmsRemoved()
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
	_, err = b.WriteString("\"fingerprints\":")
	if err != nil {
		return err
	}
	{
		s := s.Fingerprints()
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
	if s.Which() == CONFIGURATION_TRANSITIONINGTO {
		_, err = b.WriteString("\"transitioningTo\":")
		if err != nil {
			return err
		}
		{
			s := s.TransitioningTo()
			err = b.WriteByte('{')
			if err != nil {
				return err
			}
			_, err = b.WriteString("\"configuration\":")
			if err != nil {
				return err
			}
			{
				s := s.Configuration()
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(',')
			if err != nil {
				return err
			}
			_, err = b.WriteString("\"installedOnAll\":")
			if err != nil {
				return err
			}
			{
				s := s.InstalledOnAll()
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
			_, err = b.WriteString("\"pending\":")
			if err != nil {
				return err
			}
			{
				s := s.Pending()
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
	if s.Which() == CONFIGURATION_STABLE {
		_, err = b.WriteString("\"stable\":")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
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
func (s Configuration) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Configuration) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("clusterId = ")
	if err != nil {
		return err
	}
	{
		s := s.ClusterId()
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
	_, err = b.WriteString("hosts = ")
	if err != nil {
		return err
	}
	{
		s := s.Hosts()
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
	_, err = b.WriteString("f = ")
	if err != nil {
		return err
	}
	{
		s := s.F()
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
	_, err = b.WriteString("maxRMCount = ")
	if err != nil {
		return err
	}
	{
		s := s.MaxRMCount()
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
	_, err = b.WriteString("asyncFlush = ")
	if err != nil {
		return err
	}
	{
		s := s.AsyncFlush()
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
	_, err = b.WriteString("rms = ")
	if err != nil {
		return err
	}
	{
		s := s.Rms()
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
	_, err = b.WriteString("rmsRemoved = ")
	if err != nil {
		return err
	}
	{
		s := s.RmsRemoved()
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
	_, err = b.WriteString("fingerprints = ")
	if err != nil {
		return err
	}
	{
		s := s.Fingerprints()
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
	if s.Which() == CONFIGURATION_TRANSITIONINGTO {
		_, err = b.WriteString("transitioningTo = ")
		if err != nil {
			return err
		}
		{
			s := s.TransitioningTo()
			err = b.WriteByte('(')
			if err != nil {
				return err
			}
			_, err = b.WriteString("configuration = ")
			if err != nil {
				return err
			}
			{
				s := s.Configuration()
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			_, err = b.WriteString(", ")
			if err != nil {
				return err
			}
			_, err = b.WriteString("installedOnAll = ")
			if err != nil {
				return err
			}
			{
				s := s.InstalledOnAll()
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
			_, err = b.WriteString("pending = ")
			if err != nil {
				return err
			}
			{
				s := s.Pending()
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
	if s.Which() == CONFIGURATION_STABLE {
		_, err = b.WriteString("stable = ")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
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
func (s Configuration) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Configuration_List C.PointerList

func NewConfigurationList(s *C.Segment, sz int) Configuration_List {
	return Configuration_List(s.NewCompositeList(16, 7, sz))
}
func (s Configuration_List) Len() int { return C.PointerList(s).Len() }
func (s Configuration_List) At(i int) Configuration {
	return Configuration(C.PointerList(s).At(i).ToStruct())
}
func (s Configuration_List) ToArray() []Configuration {
	n := s.Len()
	a := make([]Configuration, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Configuration_List) Set(i int, item Configuration) { C.PointerList(s).Set(i, C.Object(item)) }

type Condition C.Struct
type Condition_Which uint16

const (
	CONDITION_AND       Condition_Which = 0
	CONDITION_OR        Condition_Which = 1
	CONDITION_GENERATOR Condition_Which = 2
)

func NewCondition(s *C.Segment) Condition      { return Condition(s.NewStruct(8, 1)) }
func NewRootCondition(s *C.Segment) Condition  { return Condition(s.NewRootStruct(8, 1)) }
func AutoNewCondition(s *C.Segment) Condition  { return Condition(s.NewStructAR(8, 1)) }
func ReadRootCondition(s *C.Segment) Condition { return Condition(s.Root(0).ToStruct()) }
func (s Condition) Which() Condition_Which     { return Condition_Which(C.Struct(s).Get16(0)) }
func (s Condition) And() Conjunction           { return Conjunction(C.Struct(s).GetObject(0).ToStruct()) }
func (s Condition) SetAnd(v Conjunction) {
	C.Struct(s).Set16(0, 0)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Condition) Or() Disjunction { return Disjunction(C.Struct(s).GetObject(0).ToStruct()) }
func (s Condition) SetOr(v Disjunction) {
	C.Struct(s).Set16(0, 1)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Condition) Generator() Generator { return Generator(C.Struct(s).GetObject(0).ToStruct()) }
func (s Condition) SetGenerator(v Generator) {
	C.Struct(s).Set16(0, 2)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Condition) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	if s.Which() == CONDITION_AND {
		_, err = b.WriteString("\"and\":")
		if err != nil {
			return err
		}
		{
			s := s.And()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == CONDITION_OR {
		_, err = b.WriteString("\"or\":")
		if err != nil {
			return err
		}
		{
			s := s.Or()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == CONDITION_GENERATOR {
		_, err = b.WriteString("\"generator\":")
		if err != nil {
			return err
		}
		{
			s := s.Generator()
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
func (s Condition) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Condition) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	if s.Which() == CONDITION_AND {
		_, err = b.WriteString("and = ")
		if err != nil {
			return err
		}
		{
			s := s.And()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == CONDITION_OR {
		_, err = b.WriteString("or = ")
		if err != nil {
			return err
		}
		{
			s := s.Or()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == CONDITION_GENERATOR {
		_, err = b.WriteString("generator = ")
		if err != nil {
			return err
		}
		{
			s := s.Generator()
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
func (s Condition) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Condition_List C.PointerList

func NewConditionList(s *C.Segment, sz int) Condition_List {
	return Condition_List(s.NewCompositeList(8, 1, sz))
}
func (s Condition_List) Len() int           { return C.PointerList(s).Len() }
func (s Condition_List) At(i int) Condition { return Condition(C.PointerList(s).At(i).ToStruct()) }
func (s Condition_List) ToArray() []Condition {
	n := s.Len()
	a := make([]Condition, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Condition_List) Set(i int, item Condition) { C.PointerList(s).Set(i, C.Object(item)) }

type Conjunction C.Struct

func NewConjunction(s *C.Segment) Conjunction      { return Conjunction(s.NewStruct(0, 2)) }
func NewRootConjunction(s *C.Segment) Conjunction  { return Conjunction(s.NewRootStruct(0, 2)) }
func AutoNewConjunction(s *C.Segment) Conjunction  { return Conjunction(s.NewStructAR(0, 2)) }
func ReadRootConjunction(s *C.Segment) Conjunction { return Conjunction(s.Root(0).ToStruct()) }
func (s Conjunction) Left() Condition              { return Condition(C.Struct(s).GetObject(0).ToStruct()) }
func (s Conjunction) SetLeft(v Condition)          { C.Struct(s).SetObject(0, C.Object(v)) }
func (s Conjunction) Right() Condition             { return Condition(C.Struct(s).GetObject(1).ToStruct()) }
func (s Conjunction) SetRight(v Condition)         { C.Struct(s).SetObject(1, C.Object(v)) }
func (s Conjunction) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"left\":")
	if err != nil {
		return err
	}
	{
		s := s.Left()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"right\":")
	if err != nil {
		return err
	}
	{
		s := s.Right()
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
func (s Conjunction) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Conjunction) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("left = ")
	if err != nil {
		return err
	}
	{
		s := s.Left()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("right = ")
	if err != nil {
		return err
	}
	{
		s := s.Right()
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
func (s Conjunction) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Conjunction_List C.PointerList

func NewConjunctionList(s *C.Segment, sz int) Conjunction_List {
	return Conjunction_List(s.NewCompositeList(0, 2, sz))
}
func (s Conjunction_List) Len() int             { return C.PointerList(s).Len() }
func (s Conjunction_List) At(i int) Conjunction { return Conjunction(C.PointerList(s).At(i).ToStruct()) }
func (s Conjunction_List) ToArray() []Conjunction {
	n := s.Len()
	a := make([]Conjunction, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Conjunction_List) Set(i int, item Conjunction) { C.PointerList(s).Set(i, C.Object(item)) }

type Disjunction C.Struct

func NewDisjunction(s *C.Segment) Disjunction      { return Disjunction(s.NewStruct(0, 2)) }
func NewRootDisjunction(s *C.Segment) Disjunction  { return Disjunction(s.NewRootStruct(0, 2)) }
func AutoNewDisjunction(s *C.Segment) Disjunction  { return Disjunction(s.NewStructAR(0, 2)) }
func ReadRootDisjunction(s *C.Segment) Disjunction { return Disjunction(s.Root(0).ToStruct()) }
func (s Disjunction) Left() Condition              { return Condition(C.Struct(s).GetObject(0).ToStruct()) }
func (s Disjunction) SetLeft(v Condition)          { C.Struct(s).SetObject(0, C.Object(v)) }
func (s Disjunction) Right() Condition             { return Condition(C.Struct(s).GetObject(1).ToStruct()) }
func (s Disjunction) SetRight(v Condition)         { C.Struct(s).SetObject(1, C.Object(v)) }
func (s Disjunction) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"left\":")
	if err != nil {
		return err
	}
	{
		s := s.Left()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"right\":")
	if err != nil {
		return err
	}
	{
		s := s.Right()
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
func (s Disjunction) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Disjunction) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("left = ")
	if err != nil {
		return err
	}
	{
		s := s.Left()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("right = ")
	if err != nil {
		return err
	}
	{
		s := s.Right()
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
func (s Disjunction) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Disjunction_List C.PointerList

func NewDisjunctionList(s *C.Segment, sz int) Disjunction_List {
	return Disjunction_List(s.NewCompositeList(0, 2, sz))
}
func (s Disjunction_List) Len() int             { return C.PointerList(s).Len() }
func (s Disjunction_List) At(i int) Disjunction { return Disjunction(C.PointerList(s).At(i).ToStruct()) }
func (s Disjunction_List) ToArray() []Disjunction {
	n := s.Len()
	a := make([]Disjunction, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Disjunction_List) Set(i int, item Disjunction) { C.PointerList(s).Set(i, C.Object(item)) }

type Generator C.Struct
type Generator_Which uint16

const (
	GENERATOR_LENSIMPLE          Generator_Which = 0
	GENERATOR_LENADJUSTINTERSECT Generator_Which = 1
)

func NewGenerator(s *C.Segment) Generator            { return Generator(s.NewStruct(16, 1)) }
func NewRootGenerator(s *C.Segment) Generator        { return Generator(s.NewRootStruct(16, 1)) }
func AutoNewGenerator(s *C.Segment) Generator        { return Generator(s.NewStructAR(16, 1)) }
func ReadRootGenerator(s *C.Segment) Generator       { return Generator(s.Root(0).ToStruct()) }
func (s Generator) Which() Generator_Which           { return Generator_Which(C.Struct(s).Get16(8)) }
func (s Generator) RmId() uint32                     { return C.Struct(s).Get32(0) }
func (s Generator) SetRmId(v uint32)                 { C.Struct(s).Set32(0, v) }
func (s Generator) PermLen() uint8                   { return C.Struct(s).Get8(4) }
func (s Generator) SetPermLen(v uint8)               { C.Struct(s).Set8(4, v) }
func (s Generator) Start() uint8                     { return C.Struct(s).Get8(5) }
func (s Generator) SetStart(v uint8)                 { C.Struct(s).Set8(5, v) }
func (s Generator) LenSimple() uint8                 { return C.Struct(s).Get8(6) }
func (s Generator) SetLenSimple(v uint8)             { C.Struct(s).Set16(8, 0); C.Struct(s).Set8(6, v) }
func (s Generator) LenAdjustIntersect() C.UInt32List { return C.UInt32List(C.Struct(s).GetObject(0)) }
func (s Generator) SetLenAdjustIntersect(v C.UInt32List) {
	C.Struct(s).Set16(8, 1)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Generator) Includes() bool     { return C.Struct(s).Get1(56) }
func (s Generator) SetIncludes(v bool) { C.Struct(s).Set1(56, v) }
func (s Generator) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"permLen\":")
	if err != nil {
		return err
	}
	{
		s := s.PermLen()
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
	_, err = b.WriteString("\"start\":")
	if err != nil {
		return err
	}
	{
		s := s.Start()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	if s.Which() == GENERATOR_LENSIMPLE {
		_, err = b.WriteString("\"lenSimple\":")
		if err != nil {
			return err
		}
		{
			s := s.LenSimple()
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
	if s.Which() == GENERATOR_LENADJUSTINTERSECT {
		_, err = b.WriteString("\"lenAdjustIntersect\":")
		if err != nil {
			return err
		}
		{
			s := s.LenAdjustIntersect()
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
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"includes\":")
	if err != nil {
		return err
	}
	{
		s := s.Includes()
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
func (s Generator) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Generator) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("permLen = ")
	if err != nil {
		return err
	}
	{
		s := s.PermLen()
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
	_, err = b.WriteString("start = ")
	if err != nil {
		return err
	}
	{
		s := s.Start()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	if s.Which() == GENERATOR_LENSIMPLE {
		_, err = b.WriteString("lenSimple = ")
		if err != nil {
			return err
		}
		{
			s := s.LenSimple()
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
	if s.Which() == GENERATOR_LENADJUSTINTERSECT {
		_, err = b.WriteString("lenAdjustIntersect = ")
		if err != nil {
			return err
		}
		{
			s := s.LenAdjustIntersect()
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
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("includes = ")
	if err != nil {
		return err
	}
	{
		s := s.Includes()
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
func (s Generator) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Generator_List C.PointerList

func NewGeneratorList(s *C.Segment, sz int) Generator_List {
	return Generator_List(s.NewCompositeList(16, 1, sz))
}
func (s Generator_List) Len() int           { return C.PointerList(s).Len() }
func (s Generator_List) At(i int) Generator { return Generator(C.PointerList(s).At(i).ToStruct()) }
func (s Generator_List) ToArray() []Generator {
	n := s.Len()
	a := make([]Generator, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Generator_List) Set(i int, item Generator) { C.PointerList(s).Set(i, C.Object(item)) }
