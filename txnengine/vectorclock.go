package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
)

const (
	Deleted uint64 = 0
)

type VectorClock struct {
	cap     *msgs.VectorClock
	initial map[common.VarUUId]uint64
	adds    map[common.VarUUId]uint64
	changes map[common.VarUUId]uint64
	deletes map[common.VarUUId]server.EmptyStruct
	Len     int
}

func VectorClockFromCap(vcCap msgs.VectorClock) *VectorClock {
	l := vcCap.VarUuids().Len()
	vc := &VectorClock{
		cap:     &vcCap,
		initial: make(map[common.VarUUId]uint64, l),
		Len:     l,
	}
	keys := vcCap.VarUuids()
	values := vcCap.Values()
	for idx, l := 0, keys.Len(); idx < l; idx++ {
		k := common.MakeVarUUId(keys.At(idx))
		vc.initial[*k] = values.At(idx)
	}
	return vc
}

func NewVectorClock() *VectorClock {
	return &VectorClock{}
}

func (vcA *VectorClock) Clone() *VectorClock {
	adds, changes, deletes := vcA.adds, vcA.changes, vcA.deletes
	if len(adds) > 0 {
		adds := make(map[common.VarUUId]uint64, len(adds))
		for k, v := range vcA.adds {
			adds[k] = v
		}
	}
	if len(changes) > 0 {
		changes := make(map[common.VarUUId]uint64, len(changes))
		for k, v := range vcA.changes {
			changes[k] = v
		}
	}
	if len(deletes) > 0 {
		deletes := make(map[common.VarUUId]server.EmptyStruct, len(deletes))
		for k := range vcA.deletes {
			deletes[k] = server.EmptyStructVal
		}
	}
	return &VectorClock{
		cap:     vcA.cap,
		initial: vcA.initial,
		adds:    adds,
		changes: changes,
		deletes: deletes,
		Len:     vcA.Len,
	}
}

func (vc *VectorClock) ForEach(it func(*common.VarUUId, uint64) bool) bool {
	for k, v := range vc.adds {
		if !it(&k, v) {
			return false
		}
	}
	for k, v := range vc.initial {
		if ch, found := vc.changes[k]; found {
			if !it(&k, ch) {
				return false
			}
		}
		if _, found := vc.deletes[k]; found {
			continue
		}
		if !it(&k, v) {
			return false
		}
	}

	return true
}

func (vc *VectorClock) String() string {
	str := fmt.Sprintf("VC:(%v)", vc.Len)
	vc.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		str += fmt.Sprintf(" %v:%v", vUUId, v)
		return true
	})
	return str
}

func (vc *VectorClock) At(vUUId *common.VarUUId) uint64 {
	if value, found := vc.adds[*vUUId]; found {
		return value
	}
	if value, found := vc.changes[*vUUId]; found {
		return value
	}
	if _, found := vc.deletes[*vUUId]; found {
		return Deleted
	}
	if value, found := vc.initial[*vUUId]; found {
		return value
	}
	return Deleted
}

func (vc *VectorClock) Delete(vUUId *common.VarUUId) *VectorClock {
	if _, found := vc.deletes[*vUUId]; found {
		return vc
	}
	if _, found := vc.adds[*vUUId]; found {
		delete(vc.adds, *vUUId)
		vc.Len--
		return vc
	}
	if _, found := vc.initial[*vUUId]; found {
		delete(vc.changes, *vUUId)
		if vc.deletes == nil {
			vc.deletes = make(map[common.VarUUId]server.EmptyStruct)
		}
		vc.deletes[*vUUId] = server.EmptyStructVal
		vc.Len--
	}
	return vc
}

func (vc *VectorClock) Bump(vUUId *common.VarUUId, inc uint64) *VectorClock {
	if old, found := vc.adds[*vUUId]; found {
		vc.adds[*vUUId] = old + inc
		return vc
	}
	if old, found := vc.changes[*vUUId]; found {
		vc.changes[*vUUId] = old + inc
		return vc
	}
	if _, found := vc.deletes[*vUUId]; found {
		delete(vc.deletes, *vUUId)
		vc.Len++
		if vc.changes == nil {
			vc.changes = make(map[common.VarUUId]uint64)
		}
		vc.changes[*vUUId] = inc
		return vc
	}
	if old, found := vc.initial[*vUUId]; found {
		if vc.changes == nil {
			vc.changes = make(map[common.VarUUId]uint64)
		}
		vc.changes[*vUUId] = old + inc
		return vc
	}
	if vc.adds == nil {
		vc.adds = make(map[common.VarUUId]uint64)
	}
	vc.adds[*vUUId] = inc
	vc.Len++
	return vc
}

func (vc *VectorClock) SetVarIdMax(vUUId *common.VarUUId, v uint64) bool {
	if old, found := vc.adds[*vUUId]; found {
		if v > old {
			vc.adds[*vUUId] = v
			return true
		}
		return false
	}
	if old, found := vc.changes[*vUUId]; found {
		if v > old {
			vc.changes[*vUUId] = v
			return true
		}
		return false
	}
	if old, found := vc.initial[*vUUId]; found {
		if v > old {
			if _, found := vc.deletes[*vUUId]; found {
				delete(vc.deletes, *vUUId)
				vc.Len++
			}
			if vc.changes == nil {
				vc.changes = make(map[common.VarUUId]uint64)
			}
			vc.changes[*vUUId] = v
			return true
		}
		return false
	}
	if vc.adds == nil {
		vc.adds = make(map[common.VarUUId]uint64)
	}
	vc.adds[*vUUId] = v
	vc.Len++
	return true
}

func (vcA *VectorClock) MergeInMax(vcB *VectorClock) bool {
	changed := false
	vcB.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		// put "|| changed" last to avoid short-circuit
		changed = vcA.SetVarIdMax(vUUId, v) || changed
		return true
	})
	return changed
}

func (vcA *VectorClock) MergeInMissing(vcB *VectorClock) bool {
	changed := false
	vcB.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		if _, found := vcA.deletes[*vUUId]; found {
			delete(vcA.deletes, *vUUId)
			vcA.Len++
			if vcA.changes == nil {
				vcA.changes = make(map[common.VarUUId]uint64)
			}
			vcA.changes[*vUUId] = v
		} else if _, found := vcA.adds[*vUUId]; found {
			return false
		} else if _, found := vcA.initial[*vUUId]; !found {
			vcA.Len++
			if vcA.adds == nil {
				vcA.adds = make(map[common.VarUUId]uint64)
			}
			vcA.adds[*vUUId] = v
		}
		return true
	})
	return changed
}

func (vc *VectorClock) SubtractIfMatch(vUUId *common.VarUUId, v uint64) bool {
	if _, found := vc.deletes[*vUUId]; found {
		return false
	}
	if old, found := vc.adds[*vUUId]; found {
		if old <= v {
			delete(vc.adds, *vUUId)
			vc.Len--
			return true
		}
		return false
	}
	if old, found := vc.changes[*vUUId]; found {
		if old <= v {
			if vc.deletes == nil {
				vc.deletes = make(map[common.VarUUId]server.EmptyStruct)
			}
			vc.deletes[*vUUId] = server.EmptyStructVal
			delete(vc.changes, *vUUId)
			vc.Len--
			return true
		}
		return false
	}
	if old, found := vc.initial[*vUUId]; found {
		if old <= v {
			if vc.deletes == nil {
				vc.deletes = make(map[common.VarUUId]server.EmptyStruct)
			}
			vc.deletes[*vUUId] = server.EmptyStructVal
			delete(vc.changes, *vUUId)
			vc.Len--
			return true
		}
		return false
	}
	return false
}

func (vcA *VectorClock) LessThan(vcB *VectorClock) bool {
	// 1. If A has more elems than B then A cannot be < B
	if vcA.Len > vcB.Len {
		return false
	}
	ltFound := false
	// 2. For every elem e in A, B[e] must be >= A[e]
	completed := vcA.ForEach(func(vUUId *common.VarUUId, valA uint64) bool {
		valB := vcB.At(vUUId)
		if valA > valB {
			return false
		}
		ltFound = ltFound || valA < valB
		return true
	})
	if !completed {
		return false
	}
	// 3. Everything in A is also in B and <= B. If A == B for
	// everything in A, then B must be > A if len(B) > len(A)
	return ltFound || vcB.Len > vcA.Len
}

func (vc *VectorClock) AddToSeg(seg *capn.Segment) msgs.VectorClock {
	if vc == nil {
		vcCap := msgs.NewVectorClock(seg)
		vcCap.SetVarUuids(seg.NewDataList(0))
		vcCap.SetValues(seg.NewUInt64List(0))
		return vcCap
	}

	if vc.cap != nil && vc.adds == nil && vc.changes == nil && vc.deletes == nil {
		return *vc.cap
	}

	vcCap := msgs.NewVectorClock(seg)
	vUUIds := seg.NewDataList(vc.Len)
	values := seg.NewUInt64List(vc.Len)
	vcCap.SetVarUuids(vUUIds)
	vcCap.SetValues(values)
	idx := 0
	vc.initial = make(map[common.VarUUId]uint64, vc.Len)
	vc.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		vc.initial[*vUUId] = v
		vUUIds.Set(idx, vUUId[:])
		values.Set(idx, v)
		idx++
		return true
	})

	vc.adds = nil
	vc.changes = nil
	vc.deletes = nil
	vc.cap = &vcCap

	return vcCap
}
