package client

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	eng "goshawkdb.io/server/txnengine"
)

type versionCache map[common.VarUUId]*cached

type cached struct {
	txnId     *common.TxnId
	clockElem uint64
	caps      *cmsgs.Capabilities
}

var maxCapsCap *cmsgs.Capabilities

func init() {
	seg := capn.NewBuffer(nil)
	cap := cmsgs.NewCapabilities(seg)
	cap.SetValue(cmsgs.VALUECAPABILITY_READWRITE)
	ref := cap.References()
	ref.Read().SetAll()
	ref.Write().SetAll()
	maxCapsCap = &cap
}

func NewVersionCache(roots map[common.VarUUId]*cmsgs.Capabilities) versionCache {
	cache := make(map[common.VarUUId]*cached)
	for vUUId, caps := range roots {
		cache[vUUId] = &cached{caps: caps}
	}
	return cache
}

func (vc versionCache) UpdateFromCommit(txnId *common.TxnId, outcome *msgs.Outcome) {
	clock := eng.VectorClockFromCap(outcome.Commit())
	actions := outcome.Txn().Actions()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if act := action.Which(); act != msgs.ACTION_READ {
			vUUId := common.MakeVarUUId(action.VarId())
			if c, found := vc[*vUUId]; found {
				c.txnId = txnId
				c.clockElem = clock.At(vUUId)
			} else if act == msgs.ACTION_CREATE {
				vc[*vUUId] = &cached{
					txnId:     txnId,
					clockElem: clock.At(vUUId),
					caps:      maxCapsCap,
				}
			} else {
				panic(fmt.Sprintf("%v contained action (%v) for unknown %v", txnId, act, vUUId))
			}
		}
	}
}

type unreached struct {
	cached  *cached
	action  *msgs.Action
	actions *[]*msgs.Action
}

func (vc versionCache) UpdateFromAbort(updates *msgs.Update_List) map[*msgs.Update]*[]*msgs.Action {
	l := updates.Len()
	validUpdates := make(map[*msgs.Update]*[]*msgs.Action, l)
	unreachedMap := make(map[common.VarUUId]unreached, l)

	for idx := 0; idx < l; idx++ {
		update := updates.At(idx)
		txnId := common.MakeTxnId(update.TxnId())
		clock := eng.VectorClockFromCap(update.Clock())
		actions := update.Actions()
		validActionsList := make([]*msgs.Action, 0, actions.Len())
		validActions := &validActionsList
		validUpdates[&update] = validActions

		for idy, m := 0, actions.Len(); idy < m; idy++ {
			action := actions.At(idy)
			vUUId := common.MakeVarUUId(action.VarId())
			clockElem := clock.At(vUUId)

			switch action.Which() {
			case msgs.ACTION_MISSING:
				// In this context, ACTION_MISSING means we know there was
				// a write of vUUId by txnId, but we have no idea what the
				// value written was.
				//log.Printf("%v contains missing write action of %v\n", txnId, vUUId)
				if c, found := vc[*vUUId]; found && c.txnId != nil {
					cmp := c.txnId.Compare(txnId)
					if cmp == common.EQ && clockElem != c.clockElem {
						panic(fmt.Sprintf("Clock version changed on missing for %v@%v (new:%v != old:%v)", vUUId, txnId, clockElem, c.clockElem))
					}
					if clockElem > c.clockElem || (clockElem == c.clockElem && cmp == common.LT) {
						c.txnId = nil
						c.clockElem = 0
						*validActions = append(*validActions, &action)
					}
				}

			case msgs.ACTION_WRITE:
				if c, found := vc[*vUUId]; found {
					cmp := c.txnId.Compare(txnId)
					if cmp == common.EQ && clockElem != c.clockElem {
						panic(fmt.Sprintf("Clock version changed on write for %v@%v (new:%v != old:%v)", vUUId, txnId, clockElem, c.clockElem))
					}
					if c.txnId == nil || clockElem > c.clockElem || (clockElem == c.clockElem && cmp == common.LT) {
						c.txnId = txnId
						c.clockElem = clockElem
						*validActions = append(*validActions, &action)
						refs := action.Write().References()
						worklist := []*msgs.VarIdPos_List{&refs}
						for len(worklist) > 0 {
							refs, worklist = *worklist[0], worklist[1:]
							for idz, n := 0, refs.Len(); idz < n; idz++ {
								ref := refs.At(idz)
								caps := ref.Capabilities()
								vUUId := common.MakeVarUUId(ref.Id())
								if c, found := vc[*vUUId]; found {
									c.caps = mergeCaps(c.caps, &caps)
								} else if ur, found := unreachedMap[*vUUId]; found {
									delete(unreachedMap, *vUUId)
									c := ur.cached
									c.caps = &caps
									vc[*vUUId] = c
									*ur.actions = append(*ur.actions, ur.action)
									refs1 := ur.action.Write().References()
									worklist = append(worklist, &refs1)
								} else {
									vc[*vUUId] = &cached{caps: &caps}
								}
							}
						}
					}
				} else if _, found := unreachedMap[*vUUId]; found {
					panic(fmt.Sprintf("%v reported twice in same update (and appeared in unreachedMap twice!)", vUUId))
				} else {
					//log.Printf("%v contains write action of %v\n", txnId, vUUId)
					unreachedMap[*vUUId] = unreached{
						cached: &cached{
							txnId:     txnId,
							clockElem: clockElem,
						},
						action:  &action,
						actions: validActions,
					}
				}

			default:
				panic(fmt.Sprintf("Unexpected action for %v on %v: %v", txnId, vUUId, action.Which()))
			}
		}
	}

	for update, actions := range validUpdates {
		if len(*actions) == 0 {
			delete(validUpdates, update)
		}
	}
	return validUpdates
}

func mergeCaps(a, b *cmsgs.Capabilities) *cmsgs.Capabilities {
	if a == maxCapsCap || b == maxCapsCap {
		return maxCapsCap
	}

	aValue := a.Value()
	aRefsRead := a.References().Read()
	aRefsWrite := a.References().Write()
	if aValue == cmsgs.VALUECAPABILITY_READWRITE &&
		aRefsRead.Which() == cmsgs.CAPABILITIESREFERENCESREAD_ALL &&
		aRefsWrite.Which() == cmsgs.CAPABILITIESREFERENCESWRITE_ALL {
		return a
	}

	seg := capn.NewBuffer(nil)
	cap := cmsgs.NewCapabilities(seg)

	bValue := b.Value()
	valueRead := aValue == cmsgs.VALUECAPABILITY_READWRITE || aValue == cmsgs.VALUECAPABILITY_READ ||
		bValue == cmsgs.VALUECAPABILITY_READWRITE || bValue == cmsgs.VALUECAPABILITY_READ
	valueWrite := aValue == cmsgs.VALUECAPABILITY_READWRITE || aValue == cmsgs.VALUECAPABILITY_WRITE ||
		bValue == cmsgs.VALUECAPABILITY_READWRITE || bValue == cmsgs.VALUECAPABILITY_WRITE
	switch {
	case valueRead && valueWrite:
		cap.SetValue(cmsgs.VALUECAPABILITY_READWRITE)
	case valueWrite:
		cap.SetValue(cmsgs.VALUECAPABILITY_WRITE)
	case valueRead:
		cap.SetValue(cmsgs.VALUECAPABILITY_WRITE)
	default:
		cap.SetValue(cmsgs.VALUECAPABILITY_NONE)
	}

	isMax := valueRead && valueWrite

	bRefsRead := b.References().Read()
	readAll := aRefsRead.Which() == cmsgs.CAPABILITIESREFERENCESREAD_ALL ||
		aRefsRead.Which() == cmsgs.CAPABILITIESREFERENCESREAD_ALL
	if readAll {
		cap.References().Read().SetAll()
	} else {
		isMax = false
		aOnly, bOnly := aRefsRead.Only().ToArray(), bRefsRead.Only().ToArray()
		cap.References().Read().SetOnly(mergeOnlies(seg, aOnly, bOnly))
	}

	bRefsWrite := b.References().Write()
	writeAll := aRefsWrite.Which() == cmsgs.CAPABILITIESREFERENCESWRITE_ALL ||
		aRefsWrite.Which() == cmsgs.CAPABILITIESREFERENCESWRITE_ALL
	if writeAll {
		cap.References().Write().SetAll()
	} else {
		isMax = false
		aOnly, bOnly := aRefsWrite.Only().ToArray(), bRefsWrite.Only().ToArray()
		cap.References().Write().SetOnly(mergeOnlies(seg, aOnly, bOnly))
	}

	if isMax {
		return maxCapsCap
	} else {
		return &cap
	}
}

func mergeOnlies(seg *capn.Segment, a, b []uint32) capn.UInt32List {
	only := make([]uint32, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		aIndex, bIndex := a[0], b[0]
		switch {
		case aIndex < bIndex:
			only = append(only, aIndex)
			a = a[1:]
		case aIndex > bIndex:
			only = append(only, bIndex)
			b = b[1:]
		default:
			only = append(only, bIndex)
			a = a[1:]
			b = b[1:]
		}
	}
	if len(a) > 0 {
		only = append(only, a...)
	} else {
		only = append(only, b...)
	}

	cap := seg.NewUInt32List(len(only))
	for idx, index := range only {
		cap.Set(idx, index)
	}
	return cap
}
