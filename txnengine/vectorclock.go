package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
)

type VectorClock struct {
	cap     *msgs.VectorClock
	initial map[common.VarUUId]uint64
	deltas  map[common.VarUUId]uint64
	Len     int
}

const (
	Deleted uint64 = 0
)

func VectorClockFromCap(vcCap msgs.VectorClock) *VectorClock {
	l := vcCap.VarUuids().Len()
	vc := &VectorClock{
		cap:     &vcCap,
		initial: make(map[common.VarUUId]uint64, l),
		deltas:  make(map[common.VarUUId]uint64),
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
	return &VectorClock{deltas: make(map[common.VarUUId]uint64)}
}

func (vcA *VectorClock) Clone() *VectorClock {
	deltas := make(map[common.VarUUId]uint64, len(vcA.deltas))
	for k, v := range vcA.deltas {
		deltas[k] = v
	}
	return &VectorClock{
		cap:     vcA.cap,
		initial: vcA.initial,
		deltas:  deltas,
		Len:     vcA.Len,
	}
}

func (vc *VectorClock) ForEach(it func(*common.VarUUId, uint64) bool) bool {
	deltaKeys := common.VarUUIds(make([]*common.VarUUId, 0, len(vc.deltas)))
	for k := range vc.deltas {
		kCopy := k
		deltaKeys = append(deltaKeys, &kCopy)
	}
	deltaKeys.Sort()
	if vc.cap != nil {
		keys := vc.cap.VarUuids()
		if l := keys.Len(); l > 0 {
			values := vc.cap.Values()
			idx, key := 0, common.MakeVarUUId(keys.At(0))
			nextMain := func() {
				idx++
				if idx < l {
					key = common.MakeVarUUId(keys.At(idx))
				}
			}
			if len(deltaKeys) > 0 {
				dk := deltaKeys[0]
				dv := vc.deltas[*dk]
				nextDelta := func() {
					deltaKeys = deltaKeys[1:]
					if len(deltaKeys) > 0 {
						dk = deltaKeys[0]
						dv = vc.deltas[*dk]
					}
				}
				for len(deltaKeys) > 0 && idx < l {
					switch dk.Compare(key) {
					case common.LT:
						if dv != Deleted {
							if !it(dk, dv) {
								return false
							}
						}
						nextDelta()
					case common.EQ:
						if dv != Deleted {
							if !it(dk, dv) {
								return false
							}
						}
						nextDelta()
						nextMain()
					default:
						if !it(key, values.At(idx)) {
							return false
						}
						nextMain()
					}
				}
			}
			for idx < l {
				if !it(key, values.At(idx)) {
					return false
				}
				nextMain()
			}
		}
	}
	for _, dk := range deltaKeys {
		if value := vc.deltas[*dk]; value != Deleted {
			if !it(dk, value) {
				return false
			}
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
	if value, found := vc.deltas[*vUUId]; found {
		return value
	}
	if vc.cap == nil {
		return Deleted
	}
	if value, found := vc.initial[*vUUId]; found {
		return value
	}
	return Deleted
}

func (vc *VectorClock) Delete(vUUId *common.VarUUId) *VectorClock {
	if Deleted != vc.At(vUUId) {
		vc.deltas[*vUUId] = Deleted
		vc.Len--
	}
	return vc
}

func (vc *VectorClock) Bump(vUUId *common.VarUUId, inc uint64) *VectorClock {
	old := vc.At(vUUId)
	if old == Deleted {
		vc.deltas[*vUUId] = inc
		vc.Len++
	} else {
		vc.deltas[*vUUId] = old + inc
	}
	return vc
}

func (vc *VectorClock) SetVarIdMax(vUUId *common.VarUUId, v uint64) bool {
	old := vc.At(vUUId)
	if v > old {
		vc.deltas[*vUUId] = v
		if old == Deleted {
			vc.Len++
		}
		return true
	}
	return false
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
		if vcA.At(vUUId) == Deleted {
			vcA.deltas[*vUUId] = v
			changed = true
			vcA.Len++
		}
		return true
	})
	return changed
}

func (vc *VectorClock) SubtractIfMatch(vUUId *common.VarUUId, v uint64) bool {
	if old := vc.At(vUUId); old <= v {
		if old != Deleted {
			vc.deltas[*vUUId] = Deleted
			vc.Len--
		}
		return true
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

	if len(vc.deltas) == 0 && vc.cap != nil {
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

	vc.deltas = make(map[common.VarUUId]uint64)
	vc.cap = &vcCap

	return vcCap
}
