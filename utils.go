package server

import (
	"bytes"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	"math/rand"
	"time"
)

func CheckWarn(e error, logger log.Logger) bool {
	if e != nil {
		logger.Log("msg", "Warning", "error", e)
		return true
	}
	return false
}

type LogFunc func(...interface{})

var Log LogFunc = LogFunc(func(elems ...interface{}) {})

func SegToBytes(seg *capn.Segment) []byte {
	if seg == nil {
		panic("SegToBytes called with nil segment!")
	}
	buf := new(bytes.Buffer)
	if _, err := seg.WriteTo(buf); err != nil {
		panic(fmt.Sprintf("Error when writing segment to bytes: %v", err))
	}
	return buf.Bytes()
}

type EmptyStruct struct{}

var EmptyStructVal = EmptyStruct{}

func (es EmptyStruct) String() string { return "" }

type BinaryBackoffEngine struct {
	rng    *rand.Rand
	min    time.Duration
	max    time.Duration
	period time.Duration
	Cur    time.Duration
}

func NewBinaryBackoffEngine(rng *rand.Rand, min, max time.Duration) *BinaryBackoffEngine {
	if min <= 0 {
		return nil
	}
	return &BinaryBackoffEngine{
		rng:    rng,
		min:    min,
		max:    max,
		period: min,
		Cur:    0,
	}
}

func (bbe *BinaryBackoffEngine) Advance() time.Duration {
	oldCur := bbe.Cur
	bbe.period *= 2
	if bbe.period > bbe.max {
		bbe.period = bbe.max
	}
	bbe.Cur = time.Duration(bbe.rng.Intn(int(bbe.period)))
	return oldCur
}

func (bbe *BinaryBackoffEngine) After(fun func()) {
	if duration := bbe.Cur; duration == 0 {
		fun()
	} else {
		go func() {
			time.Sleep(duration)
			fun()
		}()
	}
}

func (bbe *BinaryBackoffEngine) Shrink(roundToZero time.Duration) {
	bbe.period /= 2
	if bbe.period < bbe.min {
		bbe.period = bbe.min
	}
	bbe.Cur = time.Duration(bbe.rng.Intn(int(bbe.period)))
	if bbe.Cur <= roundToZero {
		bbe.Cur = 0
	}
}
