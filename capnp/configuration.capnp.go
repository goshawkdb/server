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
type Configuration_Which uint16

const (
	CONFIGURATION_TRANSITIONINGTO Configuration_Which = 0
	CONFIGURATION_STABLE          Configuration_Which = 1
)

func NewConfiguration(s *C.Segment) Configuration      { return Configuration(s.NewStruct(16, 6)) }
func NewRootConfiguration(s *C.Segment) Configuration  { return Configuration(s.NewRootStruct(16, 6)) }
func AutoNewConfiguration(s *C.Segment) Configuration  { return Configuration(s.NewStructAR(16, 6)) }
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
func (s Configuration) TransitioningTo() Configuration {
	return Configuration(C.Struct(s).GetObject(5).ToStruct())
}
func (s Configuration) SetTransitioningTo(v Configuration) {
	C.Struct(s).Set16(8, 0)
	C.Struct(s).SetObject(5, C.Object(v))
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
			err = s.WriteJSON(b)
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
			err = s.WriteCapLit(b)
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
	return Configuration_List(s.NewCompositeList(16, 6, sz))
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
