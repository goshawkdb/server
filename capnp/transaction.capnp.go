package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type Txn C.Struct

func NewTxn(s *C.Segment) Txn                  { return Txn(s.NewStruct(8, 3)) }
func NewRootTxn(s *C.Segment) Txn              { return Txn(s.NewRootStruct(8, 3)) }
func AutoNewTxn(s *C.Segment) Txn              { return Txn(s.NewStructAR(8, 3)) }
func ReadRootTxn(s *C.Segment) Txn             { return Txn(s.Root(0).ToStruct()) }
func (s Txn) Id() []byte                       { return C.Struct(s).GetObject(0).ToData() }
func (s Txn) SetId(v []byte)                   { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s Txn) IsTopology() bool                 { return C.Struct(s).Get1(0) }
func (s Txn) SetIsTopology(v bool)             { C.Struct(s).Set1(0, v) }
func (s Txn) Actions() []byte                  { return C.Struct(s).GetObject(1).ToData() }
func (s Txn) SetActions(v []byte)              { C.Struct(s).SetObject(1, s.Segment.NewData(v)) }
func (s Txn) Allocations() Allocation_List     { return Allocation_List(C.Struct(s).GetObject(2)) }
func (s Txn) SetAllocations(v Allocation_List) { C.Struct(s).SetObject(2, C.Object(v)) }
func (s Txn) TwoFInc() uint16                  { return C.Struct(s).Get16(2) }
func (s Txn) SetTwoFInc(v uint16)              { C.Struct(s).Set16(2, v) }
func (s Txn) TopologyVersion() uint32          { return C.Struct(s).Get32(4) }
func (s Txn) SetTopologyVersion(v uint32)      { C.Struct(s).Set32(4, v) }
func (s Txn) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"isTopology\":")
	if err != nil {
		return err
	}
	{
		s := s.IsTopology()
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
	_, err = b.WriteString("\"actions\":")
	if err != nil {
		return err
	}
	{
		s := s.Actions()
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
	_, err = b.WriteString("\"allocations\":")
	if err != nil {
		return err
	}
	{
		s := s.Allocations()
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
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"twoFInc\":")
	if err != nil {
		return err
	}
	{
		s := s.TwoFInc()
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
	_, err = b.WriteString("\"topologyVersion\":")
	if err != nil {
		return err
	}
	{
		s := s.TopologyVersion()
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
func (s Txn) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Txn) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("isTopology = ")
	if err != nil {
		return err
	}
	{
		s := s.IsTopology()
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
	_, err = b.WriteString("actions = ")
	if err != nil {
		return err
	}
	{
		s := s.Actions()
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
	_, err = b.WriteString("allocations = ")
	if err != nil {
		return err
	}
	{
		s := s.Allocations()
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
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("twoFInc = ")
	if err != nil {
		return err
	}
	{
		s := s.TwoFInc()
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
	_, err = b.WriteString("topologyVersion = ")
	if err != nil {
		return err
	}
	{
		s := s.TopologyVersion()
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
func (s Txn) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Txn_List C.PointerList

func NewTxnList(s *C.Segment, sz int) Txn_List { return Txn_List(s.NewCompositeList(8, 3, sz)) }
func (s Txn_List) Len() int                    { return C.PointerList(s).Len() }
func (s Txn_List) At(i int) Txn                { return Txn(C.PointerList(s).At(i).ToStruct()) }
func (s Txn_List) ToArray() []Txn {
	n := s.Len()
	a := make([]Txn, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Txn_List) Set(i int, item Txn) { C.PointerList(s).Set(i, C.Object(item)) }

type ActionListWrapper C.Struct

func NewActionListWrapper(s *C.Segment) ActionListWrapper { return ActionListWrapper(s.NewStruct(0, 1)) }
func NewRootActionListWrapper(s *C.Segment) ActionListWrapper {
	return ActionListWrapper(s.NewRootStruct(0, 1))
}
func AutoNewActionListWrapper(s *C.Segment) ActionListWrapper {
	return ActionListWrapper(s.NewStructAR(0, 1))
}
func ReadRootActionListWrapper(s *C.Segment) ActionListWrapper {
	return ActionListWrapper(s.Root(0).ToStruct())
}
func (s ActionListWrapper) Actions() Action_List     { return Action_List(C.Struct(s).GetObject(0)) }
func (s ActionListWrapper) SetActions(v Action_List) { C.Struct(s).SetObject(0, C.Object(v)) }
func (s ActionListWrapper) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"actions\":")
	if err != nil {
		return err
	}
	{
		s := s.Actions()
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
func (s ActionListWrapper) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s ActionListWrapper) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("actions = ")
	if err != nil {
		return err
	}
	{
		s := s.Actions()
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
func (s ActionListWrapper) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type ActionListWrapper_List C.PointerList

func NewActionListWrapperList(s *C.Segment, sz int) ActionListWrapper_List {
	return ActionListWrapper_List(s.NewCompositeList(0, 1, sz))
}
func (s ActionListWrapper_List) Len() int { return C.PointerList(s).Len() }
func (s ActionListWrapper_List) At(i int) ActionListWrapper {
	return ActionListWrapper(C.PointerList(s).At(i).ToStruct())
}
func (s ActionListWrapper_List) ToArray() []ActionListWrapper {
	n := s.Len()
	a := make([]ActionListWrapper, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s ActionListWrapper_List) Set(i int, item ActionListWrapper) {
	C.PointerList(s).Set(i, C.Object(item))
}

type Action C.Struct
type ActionValue Action
type ActionValueCreate Action
type ActionValueExisting Action
type ActionValueExistingModify Action
type ActionValueExistingModifyWrite Action
type ActionMeta Action
type ActionValue_Which uint16

const (
	ACTIONVALUE_MISSING  ActionValue_Which = 0
	ACTIONVALUE_CREATE   ActionValue_Which = 1
	ACTIONVALUE_EXISTING ActionValue_Which = 2
)

type ActionValueExistingModify_Which uint16

const (
	ACTIONVALUEEXISTINGMODIFY_NOT   ActionValueExistingModify_Which = 0
	ACTIONVALUEEXISTINGMODIFY_ROLL  ActionValueExistingModify_Which = 1
	ACTIONVALUEEXISTINGMODIFY_WRITE ActionValueExistingModify_Which = 2
)

func NewAction(s *C.Segment) Action                             { return Action(s.NewStruct(8, 5)) }
func NewRootAction(s *C.Segment) Action                         { return Action(s.NewRootStruct(8, 5)) }
func AutoNewAction(s *C.Segment) Action                         { return Action(s.NewStructAR(8, 5)) }
func ReadRootAction(s *C.Segment) Action                        { return Action(s.Root(0).ToStruct()) }
func (s Action) VarId() []byte                                  { return C.Struct(s).GetObject(0).ToData() }
func (s Action) SetVarId(v []byte)                              { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s Action) Value() ActionValue                             { return ActionValue(s) }
func (s ActionValue) Which() ActionValue_Which                  { return ActionValue_Which(C.Struct(s).Get16(0)) }
func (s ActionValue) SetMissing()                               { C.Struct(s).Set16(0, 0) }
func (s ActionValue) Create() ActionValueCreate                 { return ActionValueCreate(s) }
func (s ActionValue) SetCreate()                                { C.Struct(s).Set16(0, 1) }
func (s ActionValueCreate) Positions() C.UInt8List              { return C.UInt8List(C.Struct(s).GetObject(1)) }
func (s ActionValueCreate) SetPositions(v C.UInt8List)          { C.Struct(s).SetObject(1, C.Object(v)) }
func (s ActionValueCreate) Value() []byte                       { return C.Struct(s).GetObject(2).ToData() }
func (s ActionValueCreate) SetValue(v []byte)                   { C.Struct(s).SetObject(2, s.Segment.NewData(v)) }
func (s ActionValueCreate) References() VarIdPos_List           { return VarIdPos_List(C.Struct(s).GetObject(3)) }
func (s ActionValueCreate) SetReferences(v VarIdPos_List)       { C.Struct(s).SetObject(3, C.Object(v)) }
func (s ActionValue) Existing() ActionValueExisting             { return ActionValueExisting(s) }
func (s ActionValue) SetExisting()                              { C.Struct(s).Set16(0, 2) }
func (s ActionValueExisting) Read() []byte                      { return C.Struct(s).GetObject(1).ToData() }
func (s ActionValueExisting) SetRead(v []byte)                  { C.Struct(s).SetObject(1, s.Segment.NewData(v)) }
func (s ActionValueExisting) Modify() ActionValueExistingModify { return ActionValueExistingModify(s) }
func (s ActionValueExistingModify) Which() ActionValueExistingModify_Which {
	return ActionValueExistingModify_Which(C.Struct(s).Get16(2))
}
func (s ActionValueExistingModify) SetNot()  { C.Struct(s).Set16(2, 0) }
func (s ActionValueExistingModify) SetRoll() { C.Struct(s).Set16(2, 1) }
func (s ActionValueExistingModify) Write() ActionValueExistingModifyWrite {
	return ActionValueExistingModifyWrite(s)
}
func (s ActionValueExistingModify) SetWrite()          { C.Struct(s).Set16(2, 2) }
func (s ActionValueExistingModifyWrite) Value() []byte { return C.Struct(s).GetObject(2).ToData() }
func (s ActionValueExistingModifyWrite) SetValue(v []byte) {
	C.Struct(s).SetObject(2, s.Segment.NewData(v))
}
func (s ActionValueExistingModifyWrite) References() VarIdPos_List {
	return VarIdPos_List(C.Struct(s).GetObject(3))
}
func (s ActionValueExistingModifyWrite) SetReferences(v VarIdPos_List) {
	C.Struct(s).SetObject(3, C.Object(v))
}
func (s Action) Meta() ActionMeta       { return ActionMeta(s) }
func (s ActionMeta) AddSub() bool       { return C.Struct(s).Get1(32) }
func (s ActionMeta) SetAddSub(v bool)   { C.Struct(s).Set1(32, v) }
func (s ActionMeta) DelSub() []byte     { return C.Struct(s).GetObject(4).ToData() }
func (s ActionMeta) SetDelSub(v []byte) { C.Struct(s).SetObject(4, s.Segment.NewData(v)) }
func (s Action) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"varId\":")
	if err != nil {
		return err
	}
	{
		s := s.VarId()
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
		if s.Which() == ACTIONVALUE_MISSING {
			_, err = b.WriteString("\"missing\":")
			if err != nil {
				return err
			}
			_ = s
			_, err = b.WriteString("null")
			if err != nil {
				return err
			}
		}
		if s.Which() == ACTIONVALUE_CREATE {
			_, err = b.WriteString("\"create\":")
			if err != nil {
				return err
			}
			{
				s := s.Create()
				err = b.WriteByte('{')
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
				_, err = b.WriteString("\"value\":")
				if err != nil {
					return err
				}
				{
					s := s.Value()
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
				_, err = b.WriteString("\"references\":")
				if err != nil {
					return err
				}
				{
					s := s.References()
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
		if s.Which() == ACTIONVALUE_EXISTING {
			_, err = b.WriteString("\"existing\":")
			if err != nil {
				return err
			}
			{
				s := s.Existing()
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
				_, err = b.WriteString("\"modify\":")
				if err != nil {
					return err
				}
				{
					s := s.Modify()
					err = b.WriteByte('{')
					if err != nil {
						return err
					}
					if s.Which() == ACTIONVALUEEXISTINGMODIFY_NOT {
						_, err = b.WriteString("\"not\":")
						if err != nil {
							return err
						}
						_ = s
						_, err = b.WriteString("null")
						if err != nil {
							return err
						}
					}
					if s.Which() == ACTIONVALUEEXISTINGMODIFY_ROLL {
						_, err = b.WriteString("\"roll\":")
						if err != nil {
							return err
						}
						_ = s
						_, err = b.WriteString("null")
						if err != nil {
							return err
						}
					}
					if s.Which() == ACTIONVALUEEXISTINGMODIFY_WRITE {
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
							_, err = b.WriteString("\"value\":")
							if err != nil {
								return err
							}
							{
								s := s.Value()
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
							_, err = b.WriteString("\"references\":")
							if err != nil {
								return err
							}
							{
								s := s.References()
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
	_, err = b.WriteString("\"meta\":")
	if err != nil {
		return err
	}
	{
		s := s.Meta()
		err = b.WriteByte('{')
		if err != nil {
			return err
		}
		_, err = b.WriteString("\"addSub\":")
		if err != nil {
			return err
		}
		{
			s := s.AddSub()
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
		_, err = b.WriteString("\"delSub\":")
		if err != nil {
			return err
		}
		{
			s := s.DelSub()
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
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Action) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Action) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("varId = ")
	if err != nil {
		return err
	}
	{
		s := s.VarId()
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
		if s.Which() == ACTIONVALUE_MISSING {
			_, err = b.WriteString("missing = ")
			if err != nil {
				return err
			}
			_ = s
			_, err = b.WriteString("null")
			if err != nil {
				return err
			}
		}
		if s.Which() == ACTIONVALUE_CREATE {
			_, err = b.WriteString("create = ")
			if err != nil {
				return err
			}
			{
				s := s.Create()
				err = b.WriteByte('(')
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
				_, err = b.WriteString("value = ")
				if err != nil {
					return err
				}
				{
					s := s.Value()
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
				_, err = b.WriteString("references = ")
				if err != nil {
					return err
				}
				{
					s := s.References()
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
		if s.Which() == ACTIONVALUE_EXISTING {
			_, err = b.WriteString("existing = ")
			if err != nil {
				return err
			}
			{
				s := s.Existing()
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
				_, err = b.WriteString("modify = ")
				if err != nil {
					return err
				}
				{
					s := s.Modify()
					err = b.WriteByte('(')
					if err != nil {
						return err
					}
					if s.Which() == ACTIONVALUEEXISTINGMODIFY_NOT {
						_, err = b.WriteString("not = ")
						if err != nil {
							return err
						}
						_ = s
						_, err = b.WriteString("null")
						if err != nil {
							return err
						}
					}
					if s.Which() == ACTIONVALUEEXISTINGMODIFY_ROLL {
						_, err = b.WriteString("roll = ")
						if err != nil {
							return err
						}
						_ = s
						_, err = b.WriteString("null")
						if err != nil {
							return err
						}
					}
					if s.Which() == ACTIONVALUEEXISTINGMODIFY_WRITE {
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
							_, err = b.WriteString("value = ")
							if err != nil {
								return err
							}
							{
								s := s.Value()
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
							_, err = b.WriteString("references = ")
							if err != nil {
								return err
							}
							{
								s := s.References()
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
	_, err = b.WriteString("meta = ")
	if err != nil {
		return err
	}
	{
		s := s.Meta()
		err = b.WriteByte('(')
		if err != nil {
			return err
		}
		_, err = b.WriteString("addSub = ")
		if err != nil {
			return err
		}
		{
			s := s.AddSub()
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
		_, err = b.WriteString("delSub = ")
		if err != nil {
			return err
		}
		{
			s := s.DelSub()
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
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Action) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Action_List C.PointerList

func NewActionList(s *C.Segment, sz int) Action_List { return Action_List(s.NewCompositeList(8, 5, sz)) }
func (s Action_List) Len() int                       { return C.PointerList(s).Len() }
func (s Action_List) At(i int) Action                { return Action(C.PointerList(s).At(i).ToStruct()) }
func (s Action_List) ToArray() []Action {
	n := s.Len()
	a := make([]Action, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Action_List) Set(i int, item Action) { C.PointerList(s).Set(i, C.Object(item)) }

type Allocation C.Struct

func NewAllocation(s *C.Segment) Allocation          { return Allocation(s.NewStruct(8, 1)) }
func NewRootAllocation(s *C.Segment) Allocation      { return Allocation(s.NewRootStruct(8, 1)) }
func AutoNewAllocation(s *C.Segment) Allocation      { return Allocation(s.NewStructAR(8, 1)) }
func ReadRootAllocation(s *C.Segment) Allocation     { return Allocation(s.Root(0).ToStruct()) }
func (s Allocation) RmId() uint32                    { return C.Struct(s).Get32(0) }
func (s Allocation) SetRmId(v uint32)                { C.Struct(s).Set32(0, v) }
func (s Allocation) ActionIndices() C.UInt16List     { return C.UInt16List(C.Struct(s).GetObject(0)) }
func (s Allocation) SetActionIndices(v C.UInt16List) { C.Struct(s).SetObject(0, C.Object(v)) }
func (s Allocation) Active() uint32                  { return C.Struct(s).Get32(4) }
func (s Allocation) SetActive(v uint32)              { C.Struct(s).Set32(4, v) }
func (s Allocation) WriteJSON(w io.Writer) error {
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
	_, err = b.WriteString("\"actionIndices\":")
	if err != nil {
		return err
	}
	{
		s := s.ActionIndices()
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
	_, err = b.WriteString("\"active\":")
	if err != nil {
		return err
	}
	{
		s := s.Active()
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
func (s Allocation) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Allocation) WriteCapLit(w io.Writer) error {
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
	_, err = b.WriteString("actionIndices = ")
	if err != nil {
		return err
	}
	{
		s := s.ActionIndices()
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
	_, err = b.WriteString("active = ")
	if err != nil {
		return err
	}
	{
		s := s.Active()
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
func (s Allocation) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Allocation_List C.PointerList

func NewAllocationList(s *C.Segment, sz int) Allocation_List {
	return Allocation_List(s.NewCompositeList(8, 1, sz))
}
func (s Allocation_List) Len() int            { return C.PointerList(s).Len() }
func (s Allocation_List) At(i int) Allocation { return Allocation(C.PointerList(s).At(i).ToStruct()) }
func (s Allocation_List) ToArray() []Allocation {
	n := s.Len()
	a := make([]Allocation, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Allocation_List) Set(i int, item Allocation) { C.PointerList(s).Set(i, C.Object(item)) }
