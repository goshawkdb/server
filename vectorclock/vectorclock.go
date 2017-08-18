package vectorclock

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
)

// NB this implementation can not tell the difference between 0 and a
// deleted key. I.e. you should consider that all keys always exist in
// every vectorclock with a value of at least 0.

const (
	deleted uint64 = 0
)

type VectorClock interface {
	Len() int // given the above, Len() is really the count of non-0 values
	ForEach(func(*common.VarUUId, uint64) bool) bool
	At(*common.VarUUId) uint64
	LessThan(VectorClock) bool
	AsMutable() *VectorClockMutable
	AsData() []byte
}

func lessThan(a, b VectorClock) bool {
	// 1. If A has more elems than B then A cannot be < B
	aLen, bLen := a.Len(), b.Len()
	if aLen > bLen {
		return false
	}
	ltFound := false
	// 2. For every elem e in A, B[e] must be >= A[e]
	completed := a.ForEach(func(vUUId *common.VarUUId, valA uint64) bool {
		valB := b.At(vUUId)
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
	return ltFound || bLen > aLen
}

type VectorClockImmutable struct {
	data    []byte
	initial map[common.VarUUId]uint64
	decoded bool
}

type VectorClockMutable struct {
	*VectorClockImmutable
	data    []byte
	adds    map[common.VarUUId]uint64
	changes map[common.VarUUId]uint64
	length  int
}

func VectorClockFromData(vcData []byte, forceDecode bool) *VectorClockImmutable {
	vc := &VectorClockImmutable{
		data:    vcData,
		decoded: false,
	}
	if forceDecode {
		vc.decode()
	}
	return vc
}

func NewVectorClock() *VectorClockImmutable {
	return &VectorClockImmutable{
		data:    []byte{},
		decoded: true,
	}
}

func (vc *VectorClockImmutable) decode() {
	if vc == nil || vc.decoded {
		return
	}
	vc.decoded = true
	if len(vc.data) == 0 {
		return
	}
	seg, _, err := capn.ReadFromMemoryZeroCopy(vc.data)
	if err != nil {
		panic(fmt.Sprintf("Error when decoding vector clock: %v", err))
	}
	vcCap := msgs.ReadRootVectorClock(seg)
	l := vcCap.VarUuids().Len()
	vc.initial = make(map[common.VarUUId]uint64, l)
	keys := vcCap.VarUuids()
	values := vcCap.Values()
	for idx, l := 0, keys.Len(); idx < l; idx++ {
		k := common.MakeVarUUId(keys.At(idx))
		vc.initial[*k] = values.At(idx)
	}
}

func (vc *VectorClockImmutable) Len() int {
	vc.decode()
	return len(vc.initial)
}

func (vc *VectorClockImmutable) At(vUUId *common.VarUUId) uint64 {
	vc.decode()
	if value, found := vc.initial[*vUUId]; found {
		return value
	}
	return deleted
}

func (vc *VectorClockImmutable) ForEach(it func(*common.VarUUId, uint64) bool) bool {
	vc.decode()
	for k, v := range vc.initial {
		if !it(&k, v) {
			return false
		}
	}
	return true
}

func (vcA *VectorClockImmutable) LessThan(vcB VectorClock) bool {
	return lessThan(vcA, vcB)
}

func (vc *VectorClockImmutable) AsData() []byte {
	if vc == nil {
		return []byte{}
	}
	return vc.data
}

func (vc *VectorClockImmutable) AsMutable() *VectorClockMutable {
	return &VectorClockMutable{
		VectorClockImmutable: vc,
		length:               vc.Len(), // forces decode
		data:                 vc.data,
	}
}

func (vc *VectorClockImmutable) String() string {
	if !vc.decoded {
		return "VCI:(undecoded)"
	}
	str := fmt.Sprintf("VCI:(%v)", vc.Len())
	vc.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		str += fmt.Sprintf(" %v:%v", vUUId, v)
		return true
	})
	return str
}

func (vc *VectorClockMutable) ensureChanges() {
	if vc.changes == nil {
		vc.changes = make(map[common.VarUUId]uint64)
	}
}

func (vc *VectorClockMutable) ensureAdds() {
	if vc.adds == nil {
		vc.adds = make(map[common.VarUUId]uint64)
	}
}

func (vc *VectorClockMutable) AsMutable() *VectorClockMutable {
	return vc
}

func (vcA *VectorClockMutable) Clone() *VectorClockMutable {
	if vcA == nil {
		return nil
	}
	vcB := &VectorClockMutable{
		VectorClockImmutable: vcA.VectorClockImmutable,
		data:                 vcA.data,
		length:               vcA.Len(),
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

func (vc *VectorClockMutable) Len() int {
	return vc.length
}

func (vc *VectorClockMutable) At(vUUId *common.VarUUId) uint64 {
	if value, found := vc.adds[*vUUId]; found {
		return value
	} else if value, found := vc.changes[*vUUId]; found {
		return value
	} else {
		return vc.VectorClockImmutable.At(vUUId)
	}
}

func (vc *VectorClockMutable) ForEach(it func(*common.VarUUId, uint64) bool) bool {
	for k, v := range vc.adds {
		if !it(&k, v) {
			return false
		}
	}
	chCount := len(vc.changes)
	return vc.VectorClockImmutable.ForEach(func(k *common.VarUUId, v uint64) bool {
		if chCount == 0 {
			return it(k, v)
		} else if ch, found := vc.changes[*k]; found {
			chCount--
			if ch == deleted {
				return true
			} else {
				return it(k, ch)
			}
		} else {
			return it(k, v)
		}
	})
}

func (vc *VectorClockMutable) Delete(vUUId *common.VarUUId) *VectorClockMutable {
	if _, found := vc.adds[*vUUId]; found {
		delete(vc.adds, *vUUId)
		vc.length--
		vc.data = nil
	} else if ch, found := vc.changes[*vUUId]; found {
		if ch != deleted {
			vc.length--
			vc.changes[*vUUId] = deleted
			vc.data = nil
		}
	} else if _, found := vc.initial[*vUUId]; found {
		vc.ensureChanges()
		vc.changes[*vUUId] = deleted
		vc.length--
		vc.data = nil
	}
	return vc
}

func (vc *VectorClockMutable) Bump(vUUId *common.VarUUId, inc uint64) *VectorClockMutable {
	if old, found := vc.adds[*vUUId]; found {
		vc.adds[*vUUId] = old + inc
	} else if old, found := vc.changes[*vUUId]; found {
		if old == deleted {
			vc.changes[*vUUId] = inc
			vc.length++
		} else {
			vc.changes[*vUUId] = old + inc
		}
	} else if old, found := vc.initial[*vUUId]; found {
		vc.ensureChanges()
		vc.changes[*vUUId] = inc + old
	} else {
		vc.ensureAdds()
		vc.adds[*vUUId] = inc
		vc.length++
	}
	vc.data = nil
	return vc
}

func (vc *VectorClockMutable) SetVarIdMax(vUUId *common.VarUUId, v uint64) (changed bool) {
	if old, found := vc.adds[*vUUId]; found {
		if v > old {
			vc.adds[*vUUId] = v
			vc.data = nil
			changed = true
		}
	} else if old, found := vc.changes[*vUUId]; found {
		if v > old {
			if old == deleted {
				vc.length++
			}
			vc.changes[*vUUId] = v
			vc.data = nil
			changed = true
		}
	} else if old, found := vc.initial[*vUUId]; found {
		if v > old {
			vc.ensureChanges()
			vc.changes[*vUUId] = v
			vc.data = nil
			changed = true
		}
	} else {
		vc.ensureAdds()
		vc.adds[*vUUId] = v
		vc.length++
		vc.data = nil
		changed = true
	}
	return
}

func (vc *VectorClockMutable) DeleteIfMatch(vUUId *common.VarUUId, v uint64) (changed bool) {
	if old, found := vc.adds[*vUUId]; found {
		if old <= v {
			delete(vc.adds, *vUUId)
			vc.length--
			vc.data = nil
			changed = true
		}
	} else if old, found := vc.changes[*vUUId]; found {
		if old != deleted && old <= v {
			vc.changes[*vUUId] = deleted
			vc.length--
			vc.data = nil
			changed = true
		}
	} else if old, found := vc.initial[*vUUId]; found {
		if old <= v {
			vc.ensureChanges()
			vc.changes[*vUUId] = deleted
			vc.length--
			vc.data = nil
			changed = true
		}
	}
	return
}

func (vcA *VectorClockMutable) LessThan(vcB VectorClock) bool {
	return lessThan(vcA, vcB)
}

func (vcA *VectorClockMutable) MergeInMax(vcB VectorClock) bool {
	if vcB == nil || vcB.Len() == 0 {
		return false
	}
	changed := false
	vcB.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		changed = vcA.SetVarIdMax(vUUId, v) || changed
		return true
	})
	return changed
}

func (vcA *VectorClockMutable) MergeInMissing(vcB VectorClock) (changed bool) {
	vcB.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		if _, found := vcA.adds[*vUUId]; found {
		} else if ch, found := vcA.changes[*vUUId]; found {
			if ch == deleted {
				vcA.length++
				vcA.changes[*vUUId] = v
				changed = true
			}
		} else if _, found := vcA.initial[*vUUId]; found {
		} else {
			vcA.length++
			vcA.ensureAdds()
			vcA.adds[*vUUId] = v
			changed = true
		}
		return true
	})
	if changed {
		vcA.data = nil
	}
	return
}

func (vc *VectorClockMutable) AsData() []byte {
	if vc == nil {
		return []byte{}
	}

	if vc.data == nil {
		if vc.length == 0 {
			vc.data = []byte{}

		} else if len(vc.adds) == 0 && len(vc.changes) == 0 {
			vc.data = vc.VectorClockImmutable.data

		} else {
			// for each pair, we need KeyLen bytes for the vUUIds, and 8
			// bytes for value. Then double it to be safe.
			seg := capn.NewBuffer(make([]byte, 0, vc.length*(common.KeyLen+8)*2))
			vcCap := msgs.NewRootVectorClock(seg)
			vUUIds := seg.NewDataList(vc.length)
			values := seg.NewUInt64List(vc.length)
			vcCap.SetVarUuids(vUUIds)
			vcCap.SetValues(values)
			idx := 0
			vc.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
				vUUIds.Set(idx, vUUId[:])
				values.Set(idx, v)
				idx++
				return true
			})
			vc.data = common.SegToBytes(seg)
		}
	}

	return vc.data
}

func (vc *VectorClockMutable) String() string {
	str := fmt.Sprintf("VCM:(%v)", vc.Len())
	vc.ForEach(func(vUUId *common.VarUUId, v uint64) bool {
		str += fmt.Sprintf(" %v:%v", vUUId, v)
		return true
	})
	return str
}
