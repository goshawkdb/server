package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
)

type VectorClock struct {
	Clock map[common.VarUUId]uint64
	cap   *msgs.VectorClock
}

func VectorClockFromCap(vcCap msgs.VectorClock) *VectorClock {
	vUUIds := vcCap.VarUuids()
	values := vcCap.Values()
	vc := &VectorClock{
		Clock: make(map[common.VarUUId]uint64, vUUIds.Len()),
		cap:   &vcCap,
	}
	for idx, l := 0, vUUIds.Len(); idx < l; idx++ {
		vUUId := common.MakeVarUUId(vUUIds.At(idx))
		vc.Clock[*vUUId] = values.At(idx)
	}
	return vc
}

func NewVectorClock() *VectorClock {
	return &VectorClock{
		Clock: make(map[common.VarUUId]uint64, 32),
		cap:   nil,
	}
}

func (vcA *VectorClock) Clone() *VectorClock {
	vcB := NewVectorClock()
	vcB.MergeInMax(vcA)
	vcB.cap = vcA.cap
	return vcB
}

func (vc *VectorClock) String() string {
	return fmt.Sprintf("VC:%v (cached cap? %v)", vc.Clock, vc.cap != nil)
}

func (vc *VectorClock) Bump(oid common.VarUUId, inc uint64) *VectorClock {
	vc.Clock[oid] = inc + vc.Clock[oid]
	vc.cap = nil
	return vc
}

func (vc *VectorClock) SetVarIdMax(oid common.VarUUId, v uint64) bool {
	if old, found := vc.Clock[oid]; found && old >= v {
		return false
	} else {
		vc.Clock[oid] = v
		vc.cap = nil
		return true
	}
}

func (vcA *VectorClock) MergeInMax(vcB *VectorClock) bool {
	changed := false
	for k, v := range vcB.Clock {
		// put "|| changed" last to avoid short-circuit
		changed = vcA.SetVarIdMax(k, v) || changed
	}
	return changed
}

func (vcA *VectorClock) MergeInMissing(vcB *VectorClock) bool {
	changed := false
	for k, v := range vcB.Clock {
		if _, found := vcA.Clock[k]; !found {
			vcA.Clock[k] = v
			changed = true
		}
	}
	if changed {
		vcA.cap = nil
	}
	return changed
}

func (vc *VectorClock) SubtractIfMatch(oid common.VarUUId, v uint64) bool {
	if old, found := vc.Clock[oid]; found && old <= v {
		delete(vc.Clock, oid)
		vc.cap = nil
		return true
	}
	return false
}

func (vcA *VectorClock) Equal(vcB *VectorClock) bool {
	if len(vcA.Clock) != len(vcB.Clock) {
		return false
	} else {
		for k, vA := range vcA.Clock {
			if vB, found := vcB.Clock[k]; !(found && vA == vB) {
				return false
			}
		}
		return true
	}
}

func (vcA *VectorClock) EqualOnIntersection(vcB *VectorClock) bool {
	smaller, larger := vcA, vcB
	if len(vcB.Clock) < len(vcA.Clock) {
		smaller, larger = vcB, vcA
	}
	for k, vS := range smaller.Clock {
		if vL, found := larger.Clock[k]; found && vS != vL {
			return false
		}
	}
	return true
}

func (vcA *VectorClock) LessThan(vcB *VectorClock) bool {
	// 1. If A has more elems than B then A cannot be < B
	if len(vcA.Clock) > len(vcB.Clock) {
		return false
	}
	ltFound := false
	// 2. For every elem in A, B[e] must be >= A[e]
	for k, valA := range vcA.Clock {
		valB, found := vcB.Clock[k]
		if !found || valB < valA {
			return false
		}
		// Have we found anything for which A[e] < B[e]?
		ltFound = ltFound || (found && valA < valB)
	}
	// 3. Everything in A is also in B and <= B. If A == B for
	// everything in A, then B must be > A if len(B) > len(A)
	return ltFound || len(vcB.Clock) > len(vcA.Clock)
}

func (vcA *VectorClock) LessThanOnIntersection(vcB *VectorClock) bool {
	smallerFound := false
	for k, vA := range vcA.Clock {
		if vB, found := vcB.Clock[k]; found {
			switch {
			case vA > vB:
				return false
			case vA < vB:
				smallerFound = true
			}
		}
	}
	return smallerFound
}

func (vc *VectorClock) AddToSeg(seg *capn.Segment) msgs.VectorClock {
	if vc == nil {
		vcCap := msgs.NewVectorClock(seg)
		vcCap.SetVarUuids(seg.NewDataList(0))
		vcCap.SetValues(seg.NewUInt64List(0))
		return vcCap

	} else if vc.cap == nil {
		vcCap := msgs.NewVectorClock(seg)
		vc.cap = &vcCap
		vUUIds := seg.NewDataList(len(vc.Clock))
		values := seg.NewUInt64List(len(vc.Clock))
		vcCap.SetVarUuids(vUUIds)
		vcCap.SetValues(values)
		idx := 0
		for vUUId, ver := range vc.Clock {
			vUUIds.Set(idx, vUUId[:])
			values.Set(idx, ver)
			idx++
		}
		return vcCap

	} else {
		return *vc.cap
	}
}
