package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

type HelloServerFromServer C.Struct

func NewHelloServerFromServer(s *C.Segment) HelloServerFromServer {
	return HelloServerFromServer(s.NewStruct(16, 3))
}
func NewRootHelloServerFromServer(s *C.Segment) HelloServerFromServer {
	return HelloServerFromServer(s.NewRootStruct(16, 3))
}
func AutoNewHelloServerFromServer(s *C.Segment) HelloServerFromServer {
	return HelloServerFromServer(s.NewStructAR(16, 3))
}
func ReadRootHelloServerFromServer(s *C.Segment) HelloServerFromServer {
	return HelloServerFromServer(s.Root(0).ToStruct())
}
func (s HelloServerFromServer) LocalHost() string      { return C.Struct(s).GetObject(0).ToText() }
func (s HelloServerFromServer) LocalHostBytes() []byte { return C.Struct(s).GetObject(0).ToData() }
func (s HelloServerFromServer) SetLocalHost(v string)  { C.Struct(s).SetObject(0, s.Segment.NewText(v)) }
func (s HelloServerFromServer) RmId() uint32           { return C.Struct(s).Get32(0) }
func (s HelloServerFromServer) SetRmId(v uint32)       { C.Struct(s).Set32(0, v) }
func (s HelloServerFromServer) BootCount() uint32      { return C.Struct(s).Get32(4) }
func (s HelloServerFromServer) SetBootCount(v uint32)  { C.Struct(s).Set32(4, v) }
func (s HelloServerFromServer) TieBreak() uint32       { return C.Struct(s).Get32(8) }
func (s HelloServerFromServer) SetTieBreak(v uint32)   { C.Struct(s).Set32(8, v) }
func (s HelloServerFromServer) ClusterId() string      { return C.Struct(s).GetObject(1).ToText() }
func (s HelloServerFromServer) ClusterIdBytes() []byte { return C.Struct(s).GetObject(1).ToData() }
func (s HelloServerFromServer) SetClusterId(v string)  { C.Struct(s).SetObject(1, s.Segment.NewText(v)) }
func (s HelloServerFromServer) RootId() []byte         { return C.Struct(s).GetObject(2).ToData() }
func (s HelloServerFromServer) SetRootId(v []byte)     { C.Struct(s).SetObject(2, s.Segment.NewData(v)) }
func (s HelloServerFromServer) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"localHost\":")
	if err != nil {
		return err
	}
	{
		s := s.LocalHost()
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
	_, err = b.WriteString("\"bootCount\":")
	if err != nil {
		return err
	}
	{
		s := s.BootCount()
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
	_, err = b.WriteString("\"tieBreak\":")
	if err != nil {
		return err
	}
	{
		s := s.TieBreak()
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
	_, err = b.WriteString("\"rootId\":")
	if err != nil {
		return err
	}
	{
		s := s.RootId()
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
func (s HelloServerFromServer) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s HelloServerFromServer) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("localHost = ")
	if err != nil {
		return err
	}
	{
		s := s.LocalHost()
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
	_, err = b.WriteString("bootCount = ")
	if err != nil {
		return err
	}
	{
		s := s.BootCount()
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
	_, err = b.WriteString("tieBreak = ")
	if err != nil {
		return err
	}
	{
		s := s.TieBreak()
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
	_, err = b.WriteString("rootId = ")
	if err != nil {
		return err
	}
	{
		s := s.RootId()
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
func (s HelloServerFromServer) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type HelloServerFromServer_List C.PointerList

func NewHelloServerFromServerList(s *C.Segment, sz int) HelloServerFromServer_List {
	return HelloServerFromServer_List(s.NewCompositeList(16, 3, sz))
}
func (s HelloServerFromServer_List) Len() int { return C.PointerList(s).Len() }
func (s HelloServerFromServer_List) At(i int) HelloServerFromServer {
	return HelloServerFromServer(C.PointerList(s).At(i).ToStruct())
}
func (s HelloServerFromServer_List) ToArray() []HelloServerFromServer {
	n := s.Len()
	a := make([]HelloServerFromServer, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s HelloServerFromServer_List) Set(i int, item HelloServerFromServer) {
	C.PointerList(s).Set(i, C.Object(item))
}

type Message C.Struct
type Message_Which uint16

const (
	MESSAGE_HEARTBEAT           Message_Which = 0
	MESSAGE_TXNSUBMISSION       Message_Which = 1
	MESSAGE_SUBMISSIONOUTCOME   Message_Which = 2
	MESSAGE_SUBMISSIONCOMPLETE  Message_Which = 3
	MESSAGE_SUBMISSIONABORT     Message_Which = 4
	MESSAGE_ONEATXNVOTES        Message_Which = 5
	MESSAGE_ONEBTXNVOTES        Message_Which = 6
	MESSAGE_TWOATXNVOTES        Message_Which = 7
	MESSAGE_TWOBTXNVOTES        Message_Which = 8
	MESSAGE_TXNLOCALLYCOMPLETE  Message_Which = 9
	MESSAGE_TXNGLOBALLYCOMPLETE Message_Which = 10
	MESSAGE_CONNECTIONERROR     Message_Which = 11
)

func NewMessage(s *C.Segment) Message      { return Message(s.NewStruct(8, 1)) }
func NewRootMessage(s *C.Segment) Message  { return Message(s.NewRootStruct(8, 1)) }
func AutoNewMessage(s *C.Segment) Message  { return Message(s.NewStructAR(8, 1)) }
func ReadRootMessage(s *C.Segment) Message { return Message(s.Root(0).ToStruct()) }
func (s Message) Which() Message_Which     { return Message_Which(C.Struct(s).Get16(0)) }
func (s Message) SetHeartbeat()            { C.Struct(s).Set16(0, 0) }
func (s Message) TxnSubmission() Txn       { return Txn(C.Struct(s).GetObject(0).ToStruct()) }
func (s Message) SetTxnSubmission(v Txn) {
	C.Struct(s).Set16(0, 1)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Message) SubmissionOutcome() Outcome { return Outcome(C.Struct(s).GetObject(0).ToStruct()) }
func (s Message) SetSubmissionOutcome(v Outcome) {
	C.Struct(s).Set16(0, 2)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Message) SubmissionComplete() TxnSubmissionComplete {
	return TxnSubmissionComplete(C.Struct(s).GetObject(0).ToStruct())
}
func (s Message) SetSubmissionComplete(v TxnSubmissionComplete) {
	C.Struct(s).Set16(0, 3)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Message) SubmissionAbort() TxnSubmissionAbort {
	return TxnSubmissionAbort(C.Struct(s).GetObject(0).ToStruct())
}
func (s Message) SetSubmissionAbort(v TxnSubmissionAbort) {
	C.Struct(s).Set16(0, 4)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Message) OneATxnVotes() OneATxnVotes { return OneATxnVotes(C.Struct(s).GetObject(0).ToStruct()) }
func (s Message) SetOneATxnVotes(v OneATxnVotes) {
	C.Struct(s).Set16(0, 5)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Message) OneBTxnVotes() OneBTxnVotes { return OneBTxnVotes(C.Struct(s).GetObject(0).ToStruct()) }
func (s Message) SetOneBTxnVotes(v OneBTxnVotes) {
	C.Struct(s).Set16(0, 6)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Message) TwoATxnVotes() TwoATxnVotes { return TwoATxnVotes(C.Struct(s).GetObject(0).ToStruct()) }
func (s Message) SetTwoATxnVotes(v TwoATxnVotes) {
	C.Struct(s).Set16(0, 7)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Message) TwoBTxnVotes() TwoBTxnVotes { return TwoBTxnVotes(C.Struct(s).GetObject(0).ToStruct()) }
func (s Message) SetTwoBTxnVotes(v TwoBTxnVotes) {
	C.Struct(s).Set16(0, 8)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Message) TxnLocallyComplete() TxnLocallyComplete {
	return TxnLocallyComplete(C.Struct(s).GetObject(0).ToStruct())
}
func (s Message) SetTxnLocallyComplete(v TxnLocallyComplete) {
	C.Struct(s).Set16(0, 9)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Message) TxnGloballyComplete() TxnGloballyComplete {
	return TxnGloballyComplete(C.Struct(s).GetObject(0).ToStruct())
}
func (s Message) SetTxnGloballyComplete(v TxnGloballyComplete) {
	C.Struct(s).Set16(0, 10)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Message) ConnectionError() string      { return C.Struct(s).GetObject(0).ToText() }
func (s Message) ConnectionErrorBytes() []byte { return C.Struct(s).GetObject(0).ToData() }
func (s Message) SetConnectionError(v string) {
	C.Struct(s).Set16(0, 11)
	C.Struct(s).SetObject(0, s.Segment.NewText(v))
}
func (s Message) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	if s.Which() == MESSAGE_HEARTBEAT {
		_, err = b.WriteString("\"heartbeat\":")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	if s.Which() == MESSAGE_TXNSUBMISSION {
		_, err = b.WriteString("\"txnSubmission\":")
		if err != nil {
			return err
		}
		{
			s := s.TxnSubmission()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_SUBMISSIONOUTCOME {
		_, err = b.WriteString("\"submissionOutcome\":")
		if err != nil {
			return err
		}
		{
			s := s.SubmissionOutcome()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_SUBMISSIONCOMPLETE {
		_, err = b.WriteString("\"submissionComplete\":")
		if err != nil {
			return err
		}
		{
			s := s.SubmissionComplete()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_SUBMISSIONABORT {
		_, err = b.WriteString("\"submissionAbort\":")
		if err != nil {
			return err
		}
		{
			s := s.SubmissionAbort()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_ONEATXNVOTES {
		_, err = b.WriteString("\"oneATxnVotes\":")
		if err != nil {
			return err
		}
		{
			s := s.OneATxnVotes()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_ONEBTXNVOTES {
		_, err = b.WriteString("\"oneBTxnVotes\":")
		if err != nil {
			return err
		}
		{
			s := s.OneBTxnVotes()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_TWOATXNVOTES {
		_, err = b.WriteString("\"twoATxnVotes\":")
		if err != nil {
			return err
		}
		{
			s := s.TwoATxnVotes()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_TWOBTXNVOTES {
		_, err = b.WriteString("\"twoBTxnVotes\":")
		if err != nil {
			return err
		}
		{
			s := s.TwoBTxnVotes()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_TXNLOCALLYCOMPLETE {
		_, err = b.WriteString("\"txnLocallyComplete\":")
		if err != nil {
			return err
		}
		{
			s := s.TxnLocallyComplete()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_TXNGLOBALLYCOMPLETE {
		_, err = b.WriteString("\"txnGloballyComplete\":")
		if err != nil {
			return err
		}
		{
			s := s.TxnGloballyComplete()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_CONNECTIONERROR {
		_, err = b.WriteString("\"connectionError\":")
		if err != nil {
			return err
		}
		{
			s := s.ConnectionError()
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
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Message) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Message) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	if s.Which() == MESSAGE_HEARTBEAT {
		_, err = b.WriteString("heartbeat = ")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	if s.Which() == MESSAGE_TXNSUBMISSION {
		_, err = b.WriteString("txnSubmission = ")
		if err != nil {
			return err
		}
		{
			s := s.TxnSubmission()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_SUBMISSIONOUTCOME {
		_, err = b.WriteString("submissionOutcome = ")
		if err != nil {
			return err
		}
		{
			s := s.SubmissionOutcome()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_SUBMISSIONCOMPLETE {
		_, err = b.WriteString("submissionComplete = ")
		if err != nil {
			return err
		}
		{
			s := s.SubmissionComplete()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_SUBMISSIONABORT {
		_, err = b.WriteString("submissionAbort = ")
		if err != nil {
			return err
		}
		{
			s := s.SubmissionAbort()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_ONEATXNVOTES {
		_, err = b.WriteString("oneATxnVotes = ")
		if err != nil {
			return err
		}
		{
			s := s.OneATxnVotes()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_ONEBTXNVOTES {
		_, err = b.WriteString("oneBTxnVotes = ")
		if err != nil {
			return err
		}
		{
			s := s.OneBTxnVotes()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_TWOATXNVOTES {
		_, err = b.WriteString("twoATxnVotes = ")
		if err != nil {
			return err
		}
		{
			s := s.TwoATxnVotes()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_TWOBTXNVOTES {
		_, err = b.WriteString("twoBTxnVotes = ")
		if err != nil {
			return err
		}
		{
			s := s.TwoBTxnVotes()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_TXNLOCALLYCOMPLETE {
		_, err = b.WriteString("txnLocallyComplete = ")
		if err != nil {
			return err
		}
		{
			s := s.TxnLocallyComplete()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_TXNGLOBALLYCOMPLETE {
		_, err = b.WriteString("txnGloballyComplete = ")
		if err != nil {
			return err
		}
		{
			s := s.TxnGloballyComplete()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == MESSAGE_CONNECTIONERROR {
		_, err = b.WriteString("connectionError = ")
		if err != nil {
			return err
		}
		{
			s := s.ConnectionError()
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
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Message) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Message_List C.PointerList

func NewMessageList(s *C.Segment, sz int) Message_List {
	return Message_List(s.NewCompositeList(8, 1, sz))
}
func (s Message_List) Len() int         { return C.PointerList(s).Len() }
func (s Message_List) At(i int) Message { return Message(C.PointerList(s).At(i).ToStruct()) }
func (s Message_List) ToArray() []Message {
	n := s.Len()
	a := make([]Message, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Message_List) Set(i int, item Message) { C.PointerList(s).Set(i, C.Object(item)) }
