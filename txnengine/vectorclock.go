package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
)

const (
	deleted uint64 = 0
)

type VectorClock struct {
	cap     *msgs.VectorClock
	initial map[common.VarUUId]uint64
	adds    map[common.VarUUId]uint64
	changes map[common.VarUUId]uint64
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
	adds, changes := vcA.adds, vcA.changes
	if len(adds) > 0 {
		adds = make(map[common.VarUUId]uint64, len(adds))
		for k, v := range vcA.adds {
			adds[k] = v
		}
	}
	if len(changes) > 0 {
		changes = make(map[common.VarUUId]uint64, len(changes))
		for k, v := range vcA.changes {
			changes[k] = v
		}
	}
	return &VectorClock{
		cap:     vcA.cap,
		initial: vcA.initial,
		adds:    adds,
		changes: changes,
		Len:     vcA.Len,
	}
}

func (vc *VectorClock) ForEach(it func(*common.VarUUId, uint64) bool) bool {
	for k, v := range vc.adds {
		if !it(&k, v) {
			return false
		}
	}
	chCount := len(vc.changes)
	for k, v := range vc.initial {
		if chCount == 0 {
			if !it(&k, v) {
				return false
			}
		} else if ch, found := vc.changes[k]; found {
			chCount--
			if ch != deleted {
				if !it(&k, ch) {
					return false
				}
			}
		} else if !it(&k, v) {
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
	} else if value, found := vc.changes[*vUUId]; found {
		return value
	} else if value, found := vc.initial[*vUUId]; found {
		return value
	}
	return deleted
}

func (vc *VectorClock) Delete(vUUId *common.VarUUId) *VectorClock {
	if _, found := vc.adds[*vUUId]; found {
		delete(vc.adds, *vUUId)
		vc.Len--
		return vc
	} else if ch, found := vc.changes[*vUUId]; found {
		if ch != deleted {
			vc.Len--
			vc.changes[*vUUId] = deleted
		}
		return vc
	} else if _, found := vc.initial[*vUUId]; found {
		if vc.changes == nil {
			vc.changes = make(map[common.VarUUId]uint64)
		}
		vc.changes[*vUUId] = deleted
		vc.Len--
	}
	return vc
}

func (vc *VectorClock) Bump(vUUId *common.VarUUId, inc uint64) *VectorClock {
	if old, found := vc.adds[*vUUId]; found {
		vc.adds[*vUUId] = old + inc
		return vc
	} else if old, found := vc.changes[*vUUId]; found {
		if old == deleted {
			vc.changes[*vUUId] = inc
			vc.Len++
		} else {
			vc.changes[*vUUId] = old + inc
		}
		return vc
	} else if old, found := vc.initial[*vUUId]; found {
		if vc.changes == nil {
			vc.changes = make(map[common.VarUUId]uint64)
		}
		vc.changes[*vUUId] = old + inc
		return vc
	} else {
		if vc.adds == nil {
			vc.adds = make(map[common.VarUUId]uint64)
		}
		vc.adds[*vUUId] = inc
		vc.Len++
		return vc
	}
}

func (vc *VectorClock) SetVarIdMax(vUUId *common.VarUUId, v uint64) bool {
	if old, found := vc.adds[*vUUId]; found {
		if v > old {
			vc.adds[*vUUId] = v
			return true
		}
		return false
	} else if old, found := vc.changes[*vUUId]; found {
		if v > old {
			vc.changes[*vUUId] = v
			if old == deleted {
				vc.Len++
			}
			return true
		}
		return false
	} else if old, found := vc.initial[*vUUId]; found {
		if v > old {
			if vc.changes == nil {
				vc.changes = make(map[common.VarUUId]uint64)
			}
			vc.changes[*vUUId] = v
			return true
		}
		return false
	} else {
		if vc.adds == nil {
			vc.adds = make(map[common.VarUUId]uint64)
		}
		vc.adds[*vUUId] = v
		vc.Len++
		return true
	}
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
		if _, found := vcA.adds[*vUUId]; found {
			return true
		} else if ch, found := vcA.changes[*vUUId]; found {
			if ch == deleted {
				vcA.Len++
				vcA.changes[*vUUId] = v
				changed = true
			}
			return true
		} else if _, found := vcA.initial[*vUUId]; found {
			return true
		} else {
			vcA.Len++
			if vcA.adds == nil {
				vcA.adds = make(map[common.VarUUId]uint64)
			}
			vcA.adds[*vUUId] = v
			changed = true
			return true
		}
	})
	return changed
}

func (vc *VectorClock) SubtractIfMatch(vUUId *common.VarUUId, v uint64) bool {
	if old, found := vc.adds[*vUUId]; found {
		if old <= v {
			delete(vc.adds, *vUUId)
			vc.Len--
			return true
		}
		return false
	} else if old, found := vc.changes[*vUUId]; found {
		if old != deleted && old <= v {
			vc.changes[*vUUId] = deleted
			vc.Len--
			return true
		}
		return false
	} else if old, found := vc.initial[*vUUId]; found {
		if old <= v {
			if vc.changes == nil {
				vc.changes = make(map[common.VarUUId]uint64)
			}
			vc.changes[*vUUId] = deleted
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

	if vc.cap != nil && len(vc.adds) == 0 && len(vc.changes) == 0 {
		return *vc.cap
	}

	vcCap := msgs.NewVectorClock(seg)
	vUUIds := seg.NewDataList(vc.Len)
	values := seg.NewUInt64List(vc.Len)
	vcCap.SetVarUuids(vUUIds)
	vcCap.SetValues(values)
	idx := 0
	initial := make(map[common.VarUUId]uint64, vc.Len)
	vc.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		initial[*vUUId] = v
		vUUIds.Set(idx, vUUId[:])
		values.Set(idx, v)
		idx++
		return true
	})

	vc.initial = initial
	vc.adds = nil
	vc.changes = nil
	vc.cap = &vcCap

	return vcCap
}
