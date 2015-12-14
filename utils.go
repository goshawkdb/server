package server

import (
	"bytes"
	capn "github.com/glycerine/go-capnproto"
	"log"
)

func CheckFatal(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func CheckWarn(e error) bool {
	if e != nil {
		log.Println("Warning:", e)
		return true
	}
	return false
}

type LogFunc func(...interface{})

var Log LogFunc = LogFunc(func(elems ...interface{}) {})

func SegToBytes(seg *capn.Segment) []byte {
	if seg == nil {
		log.Fatal("SegToBytes called with nil segment!")
	}
	buf := new(bytes.Buffer)
	if _, err := seg.WriteTo(buf); err != nil {
		log.Fatal("Error when writing segment to bytes:", err)
	}
	return buf.Bytes()
}

type EmptyStruct struct{}

var EmptyStructVal = EmptyStruct{}

func (es EmptyStruct) String() string { return "" }
