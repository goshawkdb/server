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

type Configuration C.Struct
type ConfigurationTransitioningTo Configuration
type Configuration_Which uint16

const (
	CONFIGURATION_TRANSITIONINGTO Configuration_Which = 0
	CONFIGURATION_STABLE          Configuration_Which = 1
)

func NewConfiguration(s *C.Segment) Configuration      { return Configuration(s.NewStruct(24, 13)) }
func NewRootConfiguration(s *C.Segment) Configuration  { return Configuration(s.NewRootStruct(24, 13)) }
func AutoNewConfiguration(s *C.Segment) Configuration  { return Configuration(s.NewStructAR(24, 13)) }
func ReadRootConfiguration(s *C.Segment) Configuration { return Configuration(s.Root(0).ToStruct()) }
func (s Configuration) Which() Configuration_Which     { return Configuration_Which(C.Struct(s).Get16(16)) }
func (s Configuration) ClusterId() string              { return C.Struct(s).GetObject(0).ToText() }
func (s Configuration) ClusterIdBytes() []byte         { return C.Struct(s).GetObject(0).ToDataTrimLastByte() }
func (s Configuration) SetClusterId(v string)          { C.Struct(s).SetObject(0, s.Segment.NewText(v)) }
func (s Configuration) ClusterUUId() uint64            { return C.Struct(s).Get64(0) }
func (s Configuration) SetClusterUUId(v uint64)        { C.Struct(s).Set64(0, v) }
func (s Configuration) Version() uint32                { return C.Struct(s).Get32(8) }
func (s Configuration) SetVersion(v uint32)            { C.Struct(s).Set32(8, v) }
func (s Configuration) Hosts() C.TextList              { return C.TextList(C.Struct(s).GetObject(1)) }
func (s Configuration) SetHosts(v C.TextList)          { C.Struct(s).SetObject(1, C.Object(v)) }
func (s Configuration) F() uint8                       { return C.Struct(s).Get8(12) }
func (s Configuration) SetF(v uint8)                   { C.Struct(s).Set8(12, v) }
func (s Configuration) MaxRMCount() uint16             { return C.Struct(s).Get16(14) }
func (s Configuration) SetMaxRMCount(v uint16)         { C.Struct(s).Set16(14, v) }
func (s Configuration) NoSync() bool                   { return C.Struct(s).Get1(104) }
func (s Configuration) SetNoSync(v bool)               { C.Struct(s).Set1(104, v) }
func (s Configuration) Rms() C.UInt32List              { return C.UInt32List(C.Struct(s).GetObject(2)) }
func (s Configuration) SetRms(v C.UInt32List)          { C.Struct(s).SetObject(2, C.Object(v)) }
func (s Configuration) RmsRemoved() C.UInt32List       { return C.UInt32List(C.Struct(s).GetObject(3)) }
func (s Configuration) SetRmsRemoved(v C.UInt32List)   { C.Struct(s).SetObject(3, C.Object(v)) }
func (s Configuration) Fingerprints() Fingerprint_List {
	return Fingerprint_List(C.Struct(s).GetObject(4))
}
func (s Configuration) SetFingerprints(v Fingerprint_List) { C.Struct(s).SetObject(4, C.Object(v)) }
func (s Configuration) TransitioningTo() ConfigurationTransitioningTo {
	return ConfigurationTransitioningTo(s)
}
func (s Configuration) SetTransitioningTo() { C.Struct(s).Set16(16, 0) }
func (s ConfigurationTransitioningTo) Configuration() Configuration {
	return Configuration(C.Struct(s).GetObject(5).ToStruct())
}
func (s ConfigurationTransitioningTo) SetConfiguration(v Configuration) {
	C.Struct(s).SetObject(5, C.Object(v))
}
func (s ConfigurationTransitioningTo) AllHosts() C.TextList {
	return C.TextList(C.Struct(s).GetObject(6))
}
func (s ConfigurationTransitioningTo) SetAllHosts(v C.TextList) { C.Struct(s).SetObject(6, C.Object(v)) }
func (s ConfigurationTransitioningTo) NewRMIds() C.UInt32List {
	return C.UInt32List(C.Struct(s).GetObject(7))
}
func (s ConfigurationTransitioningTo) SetNewRMIds(v C.UInt32List) {
	C.Struct(s).SetObject(7, C.Object(v))
}
func (s ConfigurationTransitioningTo) SurvivingRMIds() C.UInt32List {
	return C.UInt32List(C.Struct(s).GetObject(8))
}
func (s ConfigurationTransitioningTo) SetSurvivingRMIds(v C.UInt32List) {
	C.Struct(s).SetObject(8, C.Object(v))
}
func (s ConfigurationTransitioningTo) LostRMIds() C.UInt32List {
	return C.UInt32List(C.Struct(s).GetObject(9))
}
func (s ConfigurationTransitioningTo) SetLostRMIds(v C.UInt32List) {
	C.Struct(s).SetObject(9, C.Object(v))
}
func (s ConfigurationTransitioningTo) InstalledOnNew() bool     { return C.Struct(s).Get1(105) }
func (s ConfigurationTransitioningTo) SetInstalledOnNew(v bool) { C.Struct(s).Set1(105, v) }
func (s ConfigurationTransitioningTo) BarrierReached1() C.UInt32List {
	return C.UInt32List(C.Struct(s).GetObject(10))
}
func (s ConfigurationTransitioningTo) SetBarrierReached1(v C.UInt32List) {
	C.Struct(s).SetObject(10, C.Object(v))
}
func (s ConfigurationTransitioningTo) BarrierReached2() C.UInt32List {
	return C.UInt32List(C.Struct(s).GetObject(11))
}
func (s ConfigurationTransitioningTo) SetBarrierReached2(v C.UInt32List) {
	C.Struct(s).SetObject(11, C.Object(v))
}
func (s ConfigurationTransitioningTo) Pending() ConditionPair_List {
	return ConditionPair_List(C.Struct(s).GetObject(12))
}
func (s ConfigurationTransitioningTo) SetPending(v ConditionPair_List) {
	C.Struct(s).SetObject(12, C.Object(v))
}
func (s Configuration) SetStable() { C.Struct(s).Set16(16, 1) }
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
	_, err = b.WriteString("\"clusterUUId\":")
	if err != nil {
		return err
	}
	{
		s := s.ClusterUUId()
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
	_, err = b.WriteString("\"noSync\":")
	if err != nil {
		return err
	}
	{
		s := s.NoSync()
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
			_, err = b.WriteString("\"allHosts\":")
			if err != nil {
				return err
			}
			{
				s := s.AllHosts()
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
			_, err = b.WriteString("\"newRMIds\":")
			if err != nil {
				return err
			}
			{
				s := s.NewRMIds()
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
			_, err = b.WriteString("\"survivingRMIds\":")
			if err != nil {
				return err
			}
			{
				s := s.SurvivingRMIds()
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
			_, err = b.WriteString("\"lostRMIds\":")
			if err != nil {
				return err
			}
			{
				s := s.LostRMIds()
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
			_, err = b.WriteString("\"installedOnNew\":")
			if err != nil {
				return err
			}
			{
				s := s.InstalledOnNew()
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
			_, err = b.WriteString("\"barrierReached1\":")
			if err != nil {
				return err
			}
			{
				s := s.BarrierReached1()
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
			_, err = b.WriteString("\"barrierReached2\":")
			if err != nil {
				return err
			}
			{
				s := s.BarrierReached2()
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
	_, err = b.WriteString("clusterUUId = ")
	if err != nil {
		return err
	}
	{
		s := s.ClusterUUId()
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
	_, err = b.WriteString("noSync = ")
	if err != nil {
		return err
	}
	{
		s := s.NoSync()
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
			_, err = b.WriteString("allHosts = ")
			if err != nil {
				return err
			}
			{
				s := s.AllHosts()
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
			_, err = b.WriteString("newRMIds = ")
			if err != nil {
				return err
			}
			{
				s := s.NewRMIds()
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
			_, err = b.WriteString("survivingRMIds = ")
			if err != nil {
				return err
			}
			{
				s := s.SurvivingRMIds()
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
			_, err = b.WriteString("lostRMIds = ")
			if err != nil {
				return err
			}
			{
				s := s.LostRMIds()
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
			_, err = b.WriteString("installedOnNew = ")
			if err != nil {
				return err
			}
			{
				s := s.InstalledOnNew()
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
			_, err = b.WriteString("barrierReached1 = ")
			if err != nil {
				return err
			}
			{
				s := s.BarrierReached1()
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
			_, err = b.WriteString("barrierReached2 = ")
			if err != nil {
				return err
			}
			{
				s := s.BarrierReached2()
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
	return Configuration_List(s.NewCompositeList(24, 13, sz))
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

type Fingerprint C.Struct

func NewFingerprint(s *C.Segment) Fingerprint      { return Fingerprint(s.NewStruct(0, 2)) }
func NewRootFingerprint(s *C.Segment) Fingerprint  { return Fingerprint(s.NewRootStruct(0, 2)) }
func AutoNewFingerprint(s *C.Segment) Fingerprint  { return Fingerprint(s.NewStructAR(0, 2)) }
func ReadRootFingerprint(s *C.Segment) Fingerprint { return Fingerprint(s.Root(0).ToStruct()) }
func (s Fingerprint) Sha256() []byte               { return C.Struct(s).GetObject(0).ToData() }
func (s Fingerprint) SetSha256(v []byte)           { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s Fingerprint) Roots() Root_List             { return Root_List(C.Struct(s).GetObject(1)) }
func (s Fingerprint) SetRoots(v Root_List)         { C.Struct(s).SetObject(1, C.Object(v)) }
func (s Fingerprint) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"sha256\":")
	if err != nil {
		return err
	}
	{
		s := s.Sha256()
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
	_, err = b.WriteString("\"roots\":")
	if err != nil {
		return err
	}
	{
		s := s.Roots()
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
func (s Fingerprint) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Fingerprint) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("sha256 = ")
	if err != nil {
		return err
	}
	{
		s := s.Sha256()
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
	_, err = b.WriteString("roots = ")
	if err != nil {
		return err
	}
	{
		s := s.Roots()
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
func (s Fingerprint) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Fingerprint_List C.PointerList

func NewFingerprintList(s *C.Segment, sz int) Fingerprint_List {
	return Fingerprint_List(s.NewCompositeList(0, 2, sz))
}
func (s Fingerprint_List) Len() int             { return C.PointerList(s).Len() }
func (s Fingerprint_List) At(i int) Fingerprint { return Fingerprint(C.PointerList(s).At(i).ToStruct()) }
func (s Fingerprint_List) ToArray() []Fingerprint {
	n := s.Len()
	a := make([]Fingerprint, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Fingerprint_List) Set(i int, item Fingerprint) { C.PointerList(s).Set(i, C.Object(item)) }

type Root C.Struct

func NewRoot(s *C.Segment) Root      { return Root(s.NewStruct(0, 2)) }
func NewRootRoot(s *C.Segment) Root  { return Root(s.NewRootStruct(0, 2)) }
func AutoNewRoot(s *C.Segment) Root  { return Root(s.NewStructAR(0, 2)) }
func ReadRootRoot(s *C.Segment) Root { return Root(s.Root(0).ToStruct()) }
func (s Root) Name() string          { return C.Struct(s).GetObject(0).ToText() }
func (s Root) NameBytes() []byte     { return C.Struct(s).GetObject(0).ToDataTrimLastByte() }
func (s Root) SetName(v string)      { C.Struct(s).SetObject(0, s.Segment.NewText(v)) }
func (s Root) Capabilities() capnp.Capabilities {
	return capnp.Capabilities(C.Struct(s).GetObject(1).ToStruct())
}
func (s Root) SetCapabilities(v capnp.Capabilities) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s Root) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"name\":")
	if err != nil {
		return err
	}
	{
		s := s.Name()
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
	_, err = b.WriteString("\"capabilities\":")
	if err != nil {
		return err
	}
	{
		s := s.Capabilities()
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
func (s Root) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Root) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("name = ")
	if err != nil {
		return err
	}
	{
		s := s.Name()
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
	_, err = b.WriteString("capabilities = ")
	if err != nil {
		return err
	}
	{
		s := s.Capabilities()
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
func (s Root) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Root_List C.PointerList

func NewRootList(s *C.Segment, sz int) Root_List { return Root_List(s.NewCompositeList(0, 2, sz)) }
func (s Root_List) Len() int                     { return C.PointerList(s).Len() }
func (s Root_List) At(i int) Root                { return Root(C.PointerList(s).At(i).ToStruct()) }
func (s Root_List) ToArray() []Root {
	n := s.Len()
	a := make([]Root, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Root_List) Set(i int, item Root) { C.PointerList(s).Set(i, C.Object(item)) }

type ConditionPair C.Struct

func NewConditionPair(s *C.Segment) ConditionPair      { return ConditionPair(s.NewStruct(8, 2)) }
func NewRootConditionPair(s *C.Segment) ConditionPair  { return ConditionPair(s.NewRootStruct(8, 2)) }
func AutoNewConditionPair(s *C.Segment) ConditionPair  { return ConditionPair(s.NewStructAR(8, 2)) }
func ReadRootConditionPair(s *C.Segment) ConditionPair { return ConditionPair(s.Root(0).ToStruct()) }
func (s ConditionPair) RmId() uint32                   { return C.Struct(s).Get32(0) }
func (s ConditionPair) SetRmId(v uint32)               { C.Struct(s).Set32(0, v) }
func (s ConditionPair) Condition() Condition           { return Condition(C.Struct(s).GetObject(0).ToStruct()) }
func (s ConditionPair) SetCondition(v Condition)       { C.Struct(s).SetObject(0, C.Object(v)) }
func (s ConditionPair) Suppliers() C.UInt32List        { return C.UInt32List(C.Struct(s).GetObject(1)) }
func (s ConditionPair) SetSuppliers(v C.UInt32List)    { C.Struct(s).SetObject(1, C.Object(v)) }
func (s ConditionPair) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"condition\":")
	if err != nil {
		return err
	}
	{
		s := s.Condition()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"suppliers\":")
	if err != nil {
		return err
	}
	{
		s := s.Suppliers()
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
func (s ConditionPair) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s ConditionPair) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("condition = ")
	if err != nil {
		return err
	}
	{
		s := s.Condition()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("suppliers = ")
	if err != nil {
		return err
	}
	{
		s := s.Suppliers()
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
func (s ConditionPair) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type ConditionPair_List C.PointerList

func NewConditionPairList(s *C.Segment, sz int) ConditionPair_List {
	return ConditionPair_List(s.NewCompositeList(8, 2, sz))
}
func (s ConditionPair_List) Len() int { return C.PointerList(s).Len() }
func (s ConditionPair_List) At(i int) ConditionPair {
	return ConditionPair(C.PointerList(s).At(i).ToStruct())
}
func (s ConditionPair_List) ToArray() []ConditionPair {
	n := s.Len()
	a := make([]ConditionPair, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s ConditionPair_List) Set(i int, item ConditionPair) { C.PointerList(s).Set(i, C.Object(item)) }

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

func NewGenerator(s *C.Segment) Generator      { return Generator(s.NewStruct(8, 0)) }
func NewRootGenerator(s *C.Segment) Generator  { return Generator(s.NewRootStruct(8, 0)) }
func AutoNewGenerator(s *C.Segment) Generator  { return Generator(s.NewStructAR(8, 0)) }
func ReadRootGenerator(s *C.Segment) Generator { return Generator(s.Root(0).ToStruct()) }
func (s Generator) RmId() uint32               { return C.Struct(s).Get32(0) }
func (s Generator) SetRmId(v uint32)           { C.Struct(s).Set32(0, v) }
func (s Generator) UseNext() bool              { return C.Struct(s).Get1(32) }
func (s Generator) SetUseNext(v bool)          { C.Struct(s).Set1(32, v) }
func (s Generator) Includes() bool             { return C.Struct(s).Get1(33) }
func (s Generator) SetIncludes(v bool)         { C.Struct(s).Set1(33, v) }
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
	_, err = b.WriteString("\"useNext\":")
	if err != nil {
		return err
	}
	{
		s := s.UseNext()
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
	_, err = b.WriteString("useNext = ")
	if err != nil {
		return err
	}
	{
		s := s.UseNext()
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
	return Generator_List(s.NewCompositeList(8, 0, sz))
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
