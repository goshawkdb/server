package client

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	loco "goshawkdb.io/server/types/localconnection"
	ch "goshawkdb.io/server/utils/consistenthash"
)

func (tr *TransactionRecord) formServerTxn(translationCallback loco.TranslationCallback, isTopologyTxn bool) error {
	outgoingSeg := capn.NewBuffer(nil)
	txnCap := msgs.NewRootTxn(outgoingSeg)

	txnCap.SetId(tr.client.Id())
	txnCap.SetRetry(tr.client.Retry())
	txnCap.SetIsTopology(isTopologyTxn)
	txnCap.SetTwoFInc(tr.topology.TwoFInc)
	txnCap.SetTopologyVersion(tr.topology.Version)

	clientActions := tr.client.Actions()
	tr.objs = make(map[common.VarUUId]*Cached, clientActions.Len())

	actionsListSeg := capn.NewBuffer(nil)
	actionsWrapper := msgs.NewRootActionListWrapper(actionsListSeg)
	actions := msgs.NewActionList(actionsListSeg, clientActions.Len())
	actionsWrapper.SetActions(actions)

	picker := ch.NewCombinationPicker(int(tr.topology.FInc), tr.disabledHashCodes)

	rmIdToActionIndices, err := tr.formServerActions(translationCallback, picker, &actions, &clientActions)
	if err != nil {
		return err
	}

	txnCap.SetActions(common.SegToBytes(actionsListSeg))

	// NB: we're guaranteed that activeRMs and passiveRMs are
	// disjoint. Thus there is no RM that has some active and some
	// passive actions.
	activeRMs, passiveRMs, err := picker.Choose()
	if err != nil {
		return err
	}
	allocations := msgs.NewAllocationList(outgoingSeg, len(activeRMs)+len(passiveRMs))
	txnCap.SetAllocations(allocations)
	tr.setAllocations(outgoingSeg, activeRMs, true, 0, rmIdToActionIndices, &allocations)
	tr.setAllocations(outgoingSeg, passiveRMs, false, len(activeRMs), rmIdToActionIndices, &allocations)

	tr.server = &txnCap
	tr.active = activeRMs

	return nil
}

func (tr *TransactionRecord) setAllocations(seg *capn.Segment, rmIds common.RMIds, active bool, allocIdx int, rmIdToActionIndices map[common.RMId]*[]int, allocations *msgs.Allocation_List) {
	for _, rmId := range rmIds {
		// by construction, these are already sorted ascending
		actionIndices := *(rmIdToActionIndices[rmId])
		allocation := allocations.At(allocIdx)
		allocIdx++
		allocation.SetRmId(uint32(rmId))
		actionIndicesCap := seg.NewUInt16List(len(actionIndices))
		allocation.SetActionIndices(actionIndicesCap)
		for k, v := range actionIndices {
			actionIndicesCap.Set(k, uint16(v))
		}
		if active {
			allocation.SetActive(tr.connections[rmId].BootCount)
		} else {
			allocation.SetActive(0)
		}
	}
}

func (tr *TransactionRecord) formServerActions(translationCallback loco.TranslationCallback, picker *ch.CombinationPicker, actions *msgs.Action_List, clientActions *cmsgs.ClientAction_List) (map[common.RMId]*[]int, error) {
	// if a.refs[n] --pointsTo-> b, and b is new, then we will need to
	// create positions for b, and write those into the pointer too.
	// referencesInNeedOfPositions keeps track of all such pointers.
	referencesInNeedOfPositions := []*msgs.VarIdPos{}
	rmIdToActionIndices := make(map[common.RMId]*[]int)

	for idx, l := 0, clientActions.Len(); idx < l; idx++ {
		clientAction := clientActions.At(idx)
		action := actions.At(idx)
		action.SetVarId(clientAction.VarId())
		vUUId := common.MakeVarUUId(clientAction.VarId())

		clientActionType := clientAction.ActionType()
		create := clientActionType == cmsgs.CLIENTACTIONTYPE_CREATE
		c, err := tr.cache.Find(vUUId, !create)
		if err != nil {
			// either we found it and we're creating, or we didn't find it and we're not creating
			return nil, err
		}

		switch clientActionType {
		case cmsgs.CLIENTACTIONTYPE_CREATE:
			// but, don't add it to the cache yet
			c = &Cached{
				Cache: tr.cache,
				caps:  common.ReadWriteCapability,
			}
			c.CreatePositions(int(tr.topology.MaxRMCount))
			action.SetActionType(msgs.ACTIONTYPE_CREATE)
			action.SetPositions((capn.UInt8List)(*c.positions))

		case cmsgs.CLIENTACTIONTYPE_READONLY:
			action.SetActionType(msgs.ACTIONTYPE_READONLY)
			action.SetVersion(c.version[:])
			action.SetUnmodified()

		case cmsgs.CLIENTACTIONTYPE_WRITEONLY:
			action.SetActionType(msgs.ACTIONTYPE_WRITEONLY)

		case cmsgs.CLIENTACTIONTYPE_READWRITE:
			action.SetActionType(msgs.ACTIONTYPE_READWRITE)
			action.SetVersion(c.version[:])

		case cmsgs.CLIENTACTIONTYPE_ROLL:
			action.SetActionType(msgs.ACTIONTYPE_ROLL)
			action.SetVersion(c.version[:])

		default:
			panic(fmt.Sprintf("%v: %v: Unexpected action type: %v", tr.origId, vUUId, clientAction.Which()))
		}

		if err = c.EnsureHashCodes(); err != nil {
			return nil, err
		}

		if clientActionType != cmsgs.CLIENTACTIONTYPE_READONLY {
			action.SetModified()
			actionMod := action.Modified()
			clientActionMod := clientAction.Modified()
			actionMod.SetValue(clientActionMod.Value())
			if actionRefs, err := c.copyReferences(action.Segment, &referencesInNeedOfPositions, clientActionMod.References()); err == nil {
				actionMod.SetReferences(*actionRefs)
			} else {
				return nil, err
			}
		}

		tr.objs[*vUUId] = c

		if translationCallback != nil {
			if err = translationCallback(&clientAction, &action, c.hashCodes, tr.connections); err != nil {
				return nil, err
			}
		}

		picker.AddPermutation(c.hashCodes)
		for _, rmId := range c.hashCodes {
			if listPtr, found := rmIdToActionIndices[rmId]; found {
				*listPtr = append(*listPtr, idx)
			} else {
				// Use of l for capacity guarantees an upper bound: there
				// are only l actions in total, so each RM can't possibly
				// be involved in > l actions. May waste a tiny amount of
				// memory, but minimises mallocs and copying.
				list := make([]int, 1, l)
				list[0] = idx
				rmIdToActionIndices[rmId] = &list
			}
		}
	}

	// Some of the references may be to vars that are being
	// created. Consequently, we must process all of the actions first
	// to make sure all the positions actually exist before adding the
	// positions into the references.
	for _, dstRef := range referencesInNeedOfPositions {
		target := common.MakeVarUUId(dstRef.Id())
		c, found := tr.objs[*target]
		if !found {
			var err error
			c, err = tr.cache.Find(target, true)
			if err != nil {
				return nil, err
			}
		}
		dstRef.SetPositions((capn.UInt8List)(*c.positions))
		if !c.caps.IsSubset(common.NewCapability(dstRef.Capability())) {
			return nil, fmt.Errorf("Attempt made to widen received capability to %v.", target)
		}
	}

	return rmIdToActionIndices, nil
}
