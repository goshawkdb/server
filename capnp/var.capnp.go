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

type VarIdPos C.Struct
type VarIdPosCapabilities VarIdPos
type VarIdPosCapabilitiesValue VarIdPos
type VarIdPosCapabilitiesReferences VarIdPos
type VarIdPosCapabilitiesReferencesRead VarIdPos
type VarIdPosCapabilitiesReferencesWrite VarIdPos
type VarIdPosCapabilitiesReferencesRead_Which uint16

const (
	VARIDPOSCAPABILITIESREFERENCESREAD_ALL  VarIdPosCapabilitiesReferencesRead_Which = 0
	VARIDPOSCAPABILITIESREFERENCESREAD_ONLY VarIdPosCapabilitiesReferencesRead_Which = 1
)

type VarIdPosCapabilitiesReferencesWrite_Which uint16

const (
	VARIDPOSCAPABILITIESREFERENCESWRITE_ALL  VarIdPosCapabilitiesReferencesWrite_Which = 0
	VARIDPOSCAPABILITIESREFERENCESWRITE_ONLY VarIdPosCapabilitiesReferencesWrite_Which = 1
)

func NewVarIdPos(s *C.Segment) VarIdPos                         { return VarIdPos(s.NewStruct(8, 4)) }
func NewRootVarIdPos(s *C.Segment) VarIdPos                     { return VarIdPos(s.NewRootStruct(8, 4)) }
func AutoNewVarIdPos(s *C.Segment) VarIdPos                     { return VarIdPos(s.NewStructAR(8, 4)) }
func ReadRootVarIdPos(s *C.Segment) VarIdPos                    { return VarIdPos(s.Root(0).ToStruct()) }
func (s VarIdPos) Id() []byte                                   { return C.Struct(s).GetObject(0).ToData() }
func (s VarIdPos) SetId(v []byte)                               { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s VarIdPos) Positions() C.UInt8List                       { return C.UInt8List(C.Struct(s).GetObject(1)) }
func (s VarIdPos) SetPositions(v C.UInt8List)                   { C.Struct(s).SetObject(1, C.Object(v)) }
func (s VarIdPos) Capabilities() VarIdPosCapabilities           { return VarIdPosCapabilities(s) }
func (s VarIdPosCapabilities) Value() VarIdPosCapabilitiesValue { return VarIdPosCapabilitiesValue(s) }
func (s VarIdPosCapabilitiesValue) Read() bool                  { return C.Struct(s).Get1(0) }
func (s VarIdPosCapabilitiesValue) SetRead(v bool)              { C.Struct(s).Set1(0, v) }
func (s VarIdPosCapabilitiesValue) Write() bool                 { return C.Struct(s).Get1(1) }
func (s VarIdPosCapabilitiesValue) SetWrite(v bool)             { C.Struct(s).Set1(1, v) }
func (s VarIdPosCapabilities) References() VarIdPosCapabilitiesReferences {
	return VarIdPosCapabilitiesReferences(s)
}
func (s VarIdPosCapabilitiesReferences) Read() VarIdPosCapabilitiesReferencesRead {
	return VarIdPosCapabilitiesReferencesRead(s)
}
func (s VarIdPosCapabilitiesReferencesRead) Which() VarIdPosCapabilitiesReferencesRead_Which {
	return VarIdPosCapabilitiesReferencesRead_Which(C.Struct(s).Get16(2))
}
func (s VarIdPosCapabilitiesReferencesRead) SetAll() { C.Struct(s).Set16(2, 0) }
func (s VarIdPosCapabilitiesReferencesRead) Only() C.UInt32List {
	return C.UInt32List(C.Struct(s).GetObject(2))
}
func (s VarIdPosCapabilitiesReferencesRead) SetOnly(v C.UInt32List) {
	C.Struct(s).Set16(2, 1)
	C.Struct(s).SetObject(2, C.Object(v))
}
func (s VarIdPosCapabilitiesReferences) Write() VarIdPosCapabilitiesReferencesWrite {
	return VarIdPosCapabilitiesReferencesWrite(s)
}
func (s VarIdPosCapabilitiesReferencesWrite) Which() VarIdPosCapabilitiesReferencesWrite_Which {
	return VarIdPosCapabilitiesReferencesWrite_Which(C.Struct(s).Get16(4))
}
func (s VarIdPosCapabilitiesReferencesWrite) SetAll() { C.Struct(s).Set16(4, 0) }
func (s VarIdPosCapabilitiesReferencesWrite) Only() C.UInt32List {
	return C.UInt32List(C.Struct(s).GetObject(3))
}
func (s VarIdPosCapabilitiesReferencesWrite) SetOnly(v C.UInt32List) {
	C.Struct(s).Set16(4, 1)
	C.Struct(s).SetObject(3, C.Object(v))
}
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
	_, err = b.WriteString("\"capabilities\":")
	if err != nil {
		return err
	}
	{
		s := s.Capabilities()
		err = b.WriteByte('{')
		if err != nil {
			return err
		}
		_, err = b.WriteString("\"value\":")
		if err != nil {
			return err
		}
		{
			s := s.Value()
			err = b.WriteByte('{')
			if err != nil {
				return err
			}
			_, err = b.WriteString("\"read\":")
			if err != nil {
				return err
			}
			{
				s := s.Read()
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
			_, err = b.WriteString("\"write\":")
			if err != nil {
				return err
			}
			{
				s := s.Write()
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
		}
		err = b.WriteByte(',')
		if err != nil {
			return err
		}
		_, err = b.WriteString("\"references\":")
		if err != nil {
			return err
		}
		{
			s := s.References()
			err = b.WriteByte('{')
			if err != nil {
				return err
			}
			_, err = b.WriteString("\"read\":")
			if err != nil {
				return err
			}
			{
				s := s.Read()
				err = b.WriteByte('{')
				if err != nil {
					return err
				}
				if s.Which() == VARIDPOSCAPABILITIESREFERENCESREAD_ALL {
					_, err = b.WriteString("\"all\":")
					if err != nil {
						return err
					}
					_ = s
					_, err = b.WriteString("null")
					if err != nil {
						return err
					}
				}
				if s.Which() == VARIDPOSCAPABILITIESREFERENCESREAD_ONLY {
					_, err = b.WriteString("\"only\":")
					if err != nil {
						return err
					}
					{
						s := s.Only()
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
				err = b.WriteByte('}')
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(',')
			if err != nil {
				return err
			}
			_, err = b.WriteString("\"write\":")
			if err != nil {
				return err
			}
			{
				s := s.Write()
				err = b.WriteByte('{')
				if err != nil {
					return err
				}
				if s.Which() == VARIDPOSCAPABILITIESREFERENCESWRITE_ALL {
					_, err = b.WriteString("\"all\":")
					if err != nil {
						return err
					}
					_ = s
					_, err = b.WriteString("null")
					if err != nil {
						return err
					}
				}
				if s.Which() == VARIDPOSCAPABILITIESREFERENCESWRITE_ONLY {
					_, err = b.WriteString("\"only\":")
					if err != nil {
						return err
					}
					{
						s := s.Only()
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
				err = b.WriteByte('}')
				if err != nil {
					return err
				}
			}
			err = b.WriteByte('}')
			if err != nil {
				return err
			}
		}
		err = b.WriteByte('}')
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
	_, err = b.WriteString("capabilities = ")
	if err != nil {
		return err
	}
	{
		s := s.Capabilities()
		err = b.WriteByte('(')
		if err != nil {
			return err
		}
		_, err = b.WriteString("value = ")
		if err != nil {
			return err
		}
		{
			s := s.Value()
			err = b.WriteByte('(')
			if err != nil {
				return err
			}
			_, err = b.WriteString("read = ")
			if err != nil {
				return err
			}
			{
				s := s.Read()
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
			_, err = b.WriteString("write = ")
			if err != nil {
				return err
			}
			{
				s := s.Write()
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
		}
		_, err = b.WriteString(", ")
		if err != nil {
			return err
		}
		_, err = b.WriteString("references = ")
		if err != nil {
			return err
		}
		{
			s := s.References()
			err = b.WriteByte('(')
			if err != nil {
				return err
			}
			_, err = b.WriteString("read = ")
			if err != nil {
				return err
			}
			{
				s := s.Read()
				err = b.WriteByte('(')
				if err != nil {
					return err
				}
				if s.Which() == VARIDPOSCAPABILITIESREFERENCESREAD_ALL {
					_, err = b.WriteString("all = ")
					if err != nil {
						return err
					}
					_ = s
					_, err = b.WriteString("null")
					if err != nil {
						return err
					}
				}
				if s.Which() == VARIDPOSCAPABILITIESREFERENCESREAD_ONLY {
					_, err = b.WriteString("only = ")
					if err != nil {
						return err
					}
					{
						s := s.Only()
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
				err = b.WriteByte(')')
				if err != nil {
					return err
				}
			}
			_, err = b.WriteString(", ")
			if err != nil {
				return err
			}
			_, err = b.WriteString("write = ")
			if err != nil {
				return err
			}
			{
				s := s.Write()
				err = b.WriteByte('(')
				if err != nil {
					return err
				}
				if s.Which() == VARIDPOSCAPABILITIESREFERENCESWRITE_ALL {
					_, err = b.WriteString("all = ")
					if err != nil {
						return err
					}
					_ = s
					_, err = b.WriteString("null")
					if err != nil {
						return err
					}
				}
				if s.Which() == VARIDPOSCAPABILITIESREFERENCESWRITE_ONLY {
					_, err = b.WriteString("only = ")
					if err != nil {
						return err
					}
					{
						s := s.Only()
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
				err = b.WriteByte(')')
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(')')
			if err != nil {
				return err
			}
		}
		err = b.WriteByte(')')
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
	return VarIdPos_List(s.NewCompositeList(8, 4, sz))
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
