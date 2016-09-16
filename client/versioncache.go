package client

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	ch "goshawkdb.io/server/consistenthash"
	eng "goshawkdb.io/server/txnengine"
)

type versionCache map[common.VarUUId]*cached

type cached struct {
	txnId      *common.TxnId
	clockElem  uint64
	caps       *common.Capability
	value      []byte
	references []msgs.VarIdPos
}

type update struct {
	*cached
	varUUId *common.VarUUId
}

type cacheOverlay struct {
	*cached
	// we only duplicate the txnId here for the MISSING case
	txnId  *common.TxnId
	stored bool
}

func NewVersionCache(roots map[common.VarUUId]*common.Capability) versionCache {
	cache := make(map[common.VarUUId]*cached)
	for vUUId, caps := range roots {
		cache[vUUId] = &cached{caps: caps}
	}
	return cache
}

func (vc versionCache) ValidateTransaction(cTxn *cmsgs.ClientTxn) error {
	actions := cTxn.Actions()
	if cTxn.Retry() {
		for idx, l := 0, actions.Len(); idx < l; idx++ {
			action := actions.At(idx)
			vUUId := common.MakeVarUUId(action.VarId())
			if which := action.Which(); which != cmsgs.CLIENTACTION_READ {
				return fmt.Errorf("Retry transaction should only include reads. Found %v", which)
			} else if vc, found := vc[*vUUId]; !found {
				return fmt.Errorf("Retry transaction has attempted to read from unknown object: %v", vUUId)
			} else if cap := vc.caps.Which(); !(cap == cmsgs.CAPABILITY_READ || cap == cmsgs.CAPABILITY_READWRITE) {
				return fmt.Errorf("Retry transaction has attempted illegal read from object: %v", vUUId)
			}
		}

	} else {
		for idx, l := 0, actions.Len(); idx < l; idx++ {
			action := actions.At(idx)
			vUUId := common.MakeVarUUId(action.VarId())
			vc, found := vc[*vUUId]
			switch act := action.Which(); act {
			case cmsgs.CLIENTACTION_READ, cmsgs.CLIENTACTION_WRITE, cmsgs.CLIENTACTION_READWRITE:
				if !found {
					return fmt.Errorf("Transaction manipulates unknown object: %v", vUUId)
				} else {
					cap := vc.caps.Which()
					canRead := cap == cmsgs.CAPABILITY_READ || cap == cmsgs.CAPABILITY_READWRITE
					canWrite := cap == cmsgs.CAPABILITY_WRITE || cap == cmsgs.CAPABILITY_READWRITE
					switch {
					case act == cmsgs.CLIENTACTION_READ && !canRead:
						return fmt.Errorf("Transaction has illegal read action on object: %v", vUUId)
					case act == cmsgs.CLIENTACTION_WRITE && !canWrite:
						return fmt.Errorf("Transaction has illegal write action on object: %v", vUUId)
					case act == cmsgs.CLIENTACTION_READWRITE && cap != cmsgs.CAPABILITY_READWRITE:
						return fmt.Errorf("Transaction has illegal readwrite action on object: %v", vUUId)
					}
				}

			case cmsgs.CLIENTACTION_CREATE:
				if found {
					return fmt.Errorf("Transaction tries to create existing object %v", vUUId)
				}

			default:
				return fmt.Errorf("Only read, write, readwrite or create actions allowed in client transaction, found %v", action.Which())
			}
		}
	}
	return nil
}

func (vc versionCache) EnsureSubset(vUUId *common.VarUUId, cap cmsgs.Capability) bool {
	if vc == nil {
		return true
	}
	if c, found := vc[*vUUId]; found {
		if c.caps == common.MaxCapability {
			return true
		}
		capNew, capOld := cap.Which(), c.caps.Which()
		switch {
		case capNew == capOld:
		case capNew == cmsgs.CAPABILITY_NONE: // new is bottom, always fine
		case capOld == cmsgs.CAPABILITY_READWRITE: // old is top, always fine
		default:
			return false
		}
	}
	return true
}

func (vc versionCache) UpdateFromCommit(txn *eng.TxnReader, outcome *msgs.Outcome) {
	txnId := txn.Id
	clock := eng.VectorClockFromData(outcome.Commit(), false)
	actions := txn.Actions(true).Actions()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if act := action.Which(); act != msgs.ACTION_READ {
			vUUId := common.MakeVarUUId(action.VarId())
			c, found := vc[*vUUId]
			switch {
			case !found && act == msgs.ACTION_CREATE:
				create := action.Create()
				c = &cached{
					txnId:      txnId,
					clockElem:  clock.At(vUUId),
					caps:       common.MaxCapability,
					value:      create.Value(),
					references: create.References().ToArray(),
				}
				vc[*vUUId] = c
			case !found, act == msgs.ACTION_CREATE:
				panic(fmt.Sprintf("%v contained illegal action (%v) for %v", txnId, act, vUUId))
			}

			c.txnId = txnId
			c.clockElem = clock.At(vUUId)

			switch act {
			case msgs.ACTION_WRITE:
				write := action.Write()
				c.value = write.Value()
				c.references = write.References().ToArray()
			case msgs.ACTION_READWRITE:
				rw := action.Readwrite()
				c.value = rw.Value()
				c.references = rw.References().ToArray()
			case msgs.ACTION_CREATE:
			default:
				panic(fmt.Sprintf("Unexpected action type on txn commit! %v %v", txnId, act))
			}
		}
	}
}

func (vc versionCache) UpdateFromAbort(updatesCap *msgs.Update_List) map[common.TxnId]*[]*update {
	updateGraph := make(map[common.VarUUId]*cacheOverlay)

	// 1. update everything we know we can already reach, and filter out erroneous updates
	vc.updateExisting(updatesCap, updateGraph)

	// 2. figure out what we can now reach, and propagate through extended caps
	vc.updateReachable(updateGraph)

	// 3. populate results
	updates := make([]update, len(updateGraph))
	validUpdates := make(map[common.TxnId]*[]*update, len(updateGraph))
	for vUUId, overlay := range updateGraph {
		if !overlay.stored {
			continue
		}
		updateListPtr, found := validUpdates[*overlay.txnId]
		if !found {
			updateList := []*update{}
			updateListPtr = &updateList
			validUpdates[*overlay.txnId] = updateListPtr
		}
		vUUIdCopy := vUUId
		update := &updates[0]
		updates = updates[1:]
		update.cached = overlay.cached
		update.varUUId = &vUUIdCopy
		*updateListPtr = append(*updateListPtr, update)
	}

	return validUpdates
}

func (vc versionCache) updateExisting(updatesCap *msgs.Update_List, updateGraph map[common.VarUUId]*cacheOverlay) {
	for idx, l := 0, updatesCap.Len(); idx < l; idx++ {
		updateCap := updatesCap.At(idx)
		txnId := common.MakeTxnId(updateCap.TxnId())
		clock := eng.VectorClockFromData(updateCap.Clock(), true)
		actionsCap := eng.TxnActionsFromData(updateCap.Actions(), true).Actions()

		for idy, m := 0, actionsCap.Len(); idy < m; idy++ {
			actionCap := actionsCap.At(idy)
			vUUId := common.MakeVarUUId(actionCap.VarId())
			clockElem := clock.At(vUUId)

			switch actionCap.Which() {
			case msgs.ACTION_MISSING:
				// In this context, ACTION_MISSING means we know there was
				// a write of vUUId by txnId, but we have no idea what the
				// value written was. The only safe thing we can do is
				// remove it from the client.
				// log.Printf("%v contains missing write action of %v\n", txnId, vUUId)
				if c, found := vc[*vUUId]; found && c.txnId != nil {
					cmp := c.txnId.Compare(txnId)
					if cmp == common.EQ && clockElem != c.clockElem {
						panic(fmt.Sprintf("Clock version changed on missing for %v@%v (new:%v != old:%v)", vUUId, txnId, clockElem, c.clockElem))
					}
					if clockElem > c.clockElem || (clockElem == c.clockElem && cmp == common.LT) {
						// do not blank out c.caps here
						c.txnId = nil
						c.clockElem = 0
						c.value = nil
						c.references = nil
						updateGraph[*vUUId] = &cacheOverlay{
							cached: c,
							txnId:  txnId,
							stored: true,
						}
					}
				}

			case msgs.ACTION_WRITE:
				write := actionCap.Write()
				if c, found := vc[*vUUId]; found {
					// If it's in vc then we can either reach it currently
					// or we have been able to in the past.
					updating := c.txnId == nil
					if !updating {
						cmp := c.txnId.Compare(txnId)
						if cmp == common.EQ && clockElem != c.clockElem {
							panic(fmt.Sprintf("Clock version changed on write for %v@%v (new:%v != old:%v)", vUUId, txnId, clockElem, c.clockElem))
						}
						updating = clockElem > c.clockElem || (clockElem == c.clockElem && cmp == common.LT)
					}
					// If we're not updating then the update must pre-date
					// our current knowledge of vUUId. So we're not going
					// to send it to the client in which case the
					// capabilities vUUId grants via its own refs can't
					// widen: we already know everything the client knows
					// and we're not extending that. So it's safe to
					// totally ignore it.
					if updating {
						c.txnId = txnId
						c.clockElem = clockElem
						c.value = write.Value()
						c.references = write.References().ToArray()
						updateGraph[*vUUId] = &cacheOverlay{
							cached: c,
							txnId:  txnId,
							stored: true,
						}
					}

				} else {
					//log.Printf("%v contains write action of %v\n", txnId, vUUId)
					updateGraph[*vUUId] = &cacheOverlay{
						cached: &cached{
							txnId:      txnId,
							clockElem:  clockElem,
							value:      write.Value(),
							references: write.References().ToArray(),
						},
						txnId:  txnId,
						stored: false,
					}
				}

			default:
				panic(fmt.Sprintf("Unexpected action for %v on %v: %v", txnId, vUUId, actionCap.Which()))
			}
		}
	}
}

func (vc versionCache) updateReachable(updateGraph map[common.VarUUId]*cacheOverlay) {
	reaches := make(map[common.VarUUId][]msgs.VarIdPos)
	worklist := make([]common.VarUUId, 0, len(updateGraph))

	for vUUId, overlay := range updateGraph {
		if overlay.stored {
			reaches[vUUId] = overlay.reachableReferences()
			worklist = append(worklist, vUUId)
		}
	}

	for len(worklist) > 0 {
		vUUId := worklist[0]
		worklist = worklist[1:]
		for _, ref := range reaches[vUUId] {
			// Given the current vUUId.caps, we're looking at what we
			// can reach from there.
			vUUIdRef := common.MakeVarUUId(ref.Id())
			caps := common.NewCapability(ref.Capability())
			var c *cached
			overlay, found := updateGraph[*vUUIdRef]
			if found {
				if !overlay.stored {
					overlay.stored = true
					vc[*vUUIdRef] = overlay.cached
				}
				c = overlay.cached
			} else {
				// There's no update for vUUIdRef, but it's possible we're
				// adding to the capabilities the client now has on
				// vUUIdRef so we need to record that. That in turn can
				// mean we now have access to extra vars.
				c, found = vc[*vUUIdRef]
				if !found {
					// We have no idea though what this var (vUUIdRef)
					// actually points to. caps is just our capabilities to
					// act on this var, so there's no extra work to do
					// (c.reachableReferences will return []).
					c = &cached{caps: caps}
					vc[*vUUIdRef] = c
				}
			}
			// We have two questions to answer: 1. Have we already
			// processed vUUIdRef?  2. If we have, do we have wider caps
			// now than before?
			before, found := reaches[*vUUIdRef]
			if !found {
				before = c.reachableReferences()
				reaches[*vUUIdRef] = before
			}
			ensureUpdate := c.mergeCaps(caps)
			after := c.reachableReferences()
			if len(after) > len(before) {
				reaches[*vUUIdRef] = after
				worklist = append(worklist, *vUUIdRef)
				ensureUpdate = true
			}
			if ensureUpdate && overlay == nil && c.txnId != nil {
				// Our access to vUUIdRef has expanded to the extent that
				// we can now see more of the refs from vUUIdRef, or we
				// can now see the value of vUUIdRef. So even though there
				// wasn't an actual update for vUUIdRef, we need to create
				// one.
				updateGraph[*vUUIdRef] = &cacheOverlay{
					cached: c,
					txnId:  c.txnId,
					stored: true,
				}
			}
		}
	}
}

// returns true iff we couldn't read the value before merge, but we
// can after
func (c *cached) mergeCaps(b *common.Capability) (gainedRead bool) {
	a := c.caps
	c.caps = a.Union(b)
	if a != c.caps { // change has happened
		nCap := c.caps.Which()
		nRead := nCap == cmsgs.CAPABILITY_READ || nCap == cmsgs.CAPABILITY_READWRITE
		if a == nil {
			return nRead
		} else {
			aCap := a.Which()
			return nRead && aCap != cmsgs.CAPABILITY_READ && aCap != cmsgs.CAPABILITY_READWRITE
		}
	}
	return false
}

func (c *cached) reachableReferences() []msgs.VarIdPos {
	if c.caps == nil || len(c.references) == 0 {
		return nil
	}

	switch c.caps.Which() {
	case cmsgs.CAPABILITY_READ, cmsgs.CAPABILITY_READWRITE:
		return c.references
	default:
		return nil
	}
}

func (u *update) AddToClientAction(hashCache *ch.ConsistentHashCache, seg *capn.Segment, clientAction *cmsgs.ClientAction) {
	clientAction.SetVarId(u.varUUId[:])
	c := u.cached
	if c.txnId == nil {
		clientAction.SetDelete()
	} else {
		clientAction.SetWrite()
		clientWrite := clientAction.Write()

		switch c.caps.Which() {
		case cmsgs.CAPABILITY_READ, cmsgs.CAPABILITY_READWRITE:
			clientWrite.SetValue(c.value)
			clientReferences := cmsgs.NewClientVarIdPosList(seg, len(c.references))
			for idx, ref := range c.references {
				varIdPos := clientReferences.At(idx)
				varIdPos.SetVarId(ref.Id())
				varIdPos.SetCapability(ref.Capability())
				positions := common.Positions(ref.Positions())
				hashCache.AddPosition(common.MakeVarUUId(ref.Id()), &positions)
			}
			clientWrite.SetReferences(clientReferences)
		default:
			clientWrite.SetValue([]byte{})
			clientWrite.SetReferences(cmsgs.NewClientVarIdPosList(seg, 0))
		}
	}
}
