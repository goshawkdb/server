package server

import (
	"bytes"
	capn "github.com/glycerine/go-capnproto"
	"log"
	"math/rand"
	"time"
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

type BinaryBackoffEngine struct {
	rng *rand.Rand
	min time.Duration
	max time.Duration
	Cur time.Duration
}

func NewBinaryBackoffEngine(rng *rand.Rand, min, max time.Duration) *BinaryBackoffEngine {
	cur := time.Duration(0)
	if min > 0 {
		cur = min + time.Duration(rng.Intn(int(min)))
	}
	return &BinaryBackoffEngine{
		rng: rng,
		min: min,
		max: max,
		Cur: cur,
	}
}

// returns the old delay, prior to change
func (bbe *BinaryBackoffEngine) Advance() time.Duration {
	return bbe.AdvanceBy(bbe.Cur)
}

// returns the old delay, prior to change
func (bbe *BinaryBackoffEngine) AdvanceBy(d time.Duration) time.Duration {
	oldCur := bbe.Cur
	bbe.Cur += time.Duration(bbe.rng.Intn(int(d)))
	for bbe.max > bbe.min && bbe.Cur > bbe.max {
		bbe.Cur = bbe.Cur / 2
	}
	return oldCur
}

func (bbe *BinaryBackoffEngine) After(fun func()) {
	duration := bbe.Cur
	go func() {
		if duration > 0 {
			time.Sleep(duration)
		}
		fun()
	}()
}

func (bbe *BinaryBackoffEngine) Shrink(roundToMin time.Duration) {
	bbe.Cur = bbe.Cur / 2
	if bbe.Cur < bbe.min {
		bbe.Cur = bbe.min + time.Duration(bbe.rng.Intn(int(bbe.min)))
	} else if bbe.Cur < bbe.min+roundToMin {
		bbe.Cur = bbe.min
	}
}
