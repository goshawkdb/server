package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
)

const (
	deleted uint64 = 0
)

type VectorClock struct {
	data    []byte
	initial map[common.VarUUId]uint64
	adds    map[common.VarUUId]uint64
	changes map[common.VarUUId]uint64
	length  int
	inited  bool
}

func VectorClockFromData(vcData []byte) *VectorClock {
	return &VectorClock{data: vcData}
}

func NewVectorClock() *VectorClock {
	return &VectorClock{
		data:   []byte{},
		inited: true,
	}
}

func (vc *VectorClock) init() {
	if vc == nil || vc.inited {
		return
	}
	vc.inited = true
	if len(vc.data) == 0 {
		return
	}
	seg, _, err := capn.ReadFromMemoryZeroCopy(vc.data)
	if err != nil {
		panic(fmt.Sprintf("Error when decoding vector clock: %v", err))
	}
	vcCap := msgs.ReadRootVectorClock(seg)
	l := vcCap.VarUuids().Len()
	vc.length = l
	vc.initial = make(map[common.VarUUId]uint64, l)
	keys := vcCap.VarUuids()
	values := vcCap.Values()
	for idx, l := 0, keys.Len(); idx < l; idx++ {
		k := common.MakeVarUUId(keys.At(idx))
		vc.initial[*k] = values.At(idx)
	}
}

func (vcA *VectorClock) Clone() *VectorClock {
	if vcA == nil {
		return nil
	}
	if !vcA.inited {
		return VectorClockFromData(vcA.data)
	}
	vcB := &VectorClock{
		data:    vcA.data,
		initial: vcA.initial,
		length:  vcA.length,
		inited:  true,
	}
	if len(vcA.adds) > 0 {
		adds := make(map[common.VarUUId]uint64, len(vcA.adds))
		for k, v := range vcA.adds {
			adds[k] = v
		}
		vcB.adds = adds
	}
	if len(vcA.changes) > 0 {
		changes := make(map[common.VarUUId]uint64, len(vcA.changes))
		for k, v := range vcA.changes {
			changes[k] = v
		}
		vcB.changes = changes
	}
	return vcB
}

func (vc *VectorClock) Len() int {
	vc.init()
	return vc.length
}

func (vc *VectorClock) ForEach(it func(*common.VarUUId, uint64) bool) bool {
	vc.init()
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
	if !vc.inited {
		return "VC:(undecoded)"
	}
	str := fmt.Sprintf("VC:(%v)", vc.Len())
	vc.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		str += fmt.Sprintf(" %v:%v", vUUId, v)
		return true
	})
	return str
}

func (vc *VectorClock) At(vUUId *common.VarUUId) uint64 {
	vc.init()
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
	vc.init()
	if _, found := vc.adds[*vUUId]; found {
		delete(vc.adds, *vUUId)
		vc.length--
		return vc
	} else if ch, found := vc.changes[*vUUId]; found {
		if ch != deleted {
			vc.length--
			vc.changes[*vUUId] = deleted
		}
		return vc
	} else if _, found := vc.initial[*vUUId]; found {
		if vc.changes == nil {
			vc.changes = make(map[common.VarUUId]uint64)
		}
		vc.changes[*vUUId] = deleted
		vc.length--
	}
	return vc
}

func (vc *VectorClock) Bump(vUUId *common.VarUUId, inc uint64) *VectorClock {
	vc.init()
	if old, found := vc.adds[*vUUId]; found {
		vc.adds[*vUUId] = old + inc
		return vc
	} else if old, found := vc.changes[*vUUId]; found {
		if old == deleted {
			vc.changes[*vUUId] = inc
			vc.length++
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
		vc.length++
		return vc
	}
}

func (vc *VectorClock) SetVarIdMax(vUUId *common.VarUUId, v uint64) bool {
	vc.init()
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
				vc.length++
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
		vc.length++
		return true
	}
}

func (vcA *VectorClock) MergeInMax(vcB *VectorClock) bool {
	vcA.init()
	vcB.init()
	changed := false
	vcB.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		// put "|| changed" last to avoid short-circuit
		changed = vcA.SetVarIdMax(vUUId, v) || changed
		return true
	})
	return changed
}

func (vcA *VectorClock) MergeInMissing(vcB *VectorClock) bool {
	vcA.init()
	vcB.init()
	changed := false
	vcB.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		if _, found := vcA.adds[*vUUId]; found {
			return true
		} else if ch, found := vcA.changes[*vUUId]; found {
			if ch == deleted {
				vcA.length++
				vcA.changes[*vUUId] = v
				changed = true
			}
			return true
		} else if _, found := vcA.initial[*vUUId]; found {
			return true
		} else {
			vcA.length++
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
	vc.init()
	if old, found := vc.adds[*vUUId]; found {
		if old <= v {
			delete(vc.adds, *vUUId)
			vc.length--
			return true
		}
		return false
	} else if old, found := vc.changes[*vUUId]; found {
		if old != deleted && old <= v {
			vc.changes[*vUUId] = deleted
			vc.length--
			return true
		}
		return false
	} else if old, found := vc.initial[*vUUId]; found {
		if old <= v {
			if vc.changes == nil {
				vc.changes = make(map[common.VarUUId]uint64)
			}
			vc.changes[*vUUId] = deleted
			vc.length--
			return true
		}
		return false
	}
	return false
}

func (vcA *VectorClock) LessThan(vcB *VectorClock) bool {
	vcA.init()
	vcB.init()
	// 1. If A has more elems than B then A cannot be < B
	if vcA.length > vcB.length {
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
	return ltFound || vcB.length > vcA.length
}

func (vc *VectorClock) AsData() []byte {
	if vc == nil {
		return []byte{}
	}

	if len(vc.adds) == 0 && len(vc.changes) == 0 {
		return vc.data
	}

	seg := capn.NewBuffer(nil)
	vcCap := msgs.NewRootVectorClock(seg)
	vUUIds := seg.NewDataList(vc.length)
	values := seg.NewUInt64List(vc.length)
	vcCap.SetVarUuids(vUUIds)
	vcCap.SetValues(values)
	idx := 0
	initial := make(map[common.VarUUId]uint64, vc.length)
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
	vc.data = server.SegToBytes(seg)

	return vc.data
}
