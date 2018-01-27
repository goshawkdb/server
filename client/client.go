package client

import (
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	loco "goshawkdb.io/server/types/localconnection"
	ch "goshawkdb.io/server/utils/consistenthash"
)

func (tr *TransactionRecord) formServerTxn(translationCallback loco.TranslationCallback, isTopologyTxn bool) (bool, error) {
	outgoingSeg := capn.NewBuffer(nil)
	txnCap := msgs.NewRootTxn(outgoingSeg)

	txnCap.SetId(tr.client.Id())
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

	rmIdToActionIndices, addsSubs, err := tr.formServerActions(tr.client.Counter(), translationCallback, picker, &actions, &clientActions)
	if err != nil {
		return false, err
	}

	txnCap.SetActions(common.SegToBytes(actionsListSeg))

	// NB: we're guaranteed that activeRMs and passiveRMs are
	// disjoint. Thus there is no RM that has some active and some
	// passive actions.
	activeRMs, passiveRMs, err := picker.Choose()
	if err != nil {
		return false, err
	}
	allocations := msgs.NewAllocationList(outgoingSeg, len(activeRMs)+len(passiveRMs))
	txnCap.SetAllocations(allocations)
	tr.setAllocations(outgoingSeg, activeRMs, true, 0, rmIdToActionIndices, &allocations)
	tr.setAllocations(outgoingSeg, passiveRMs, false, len(activeRMs), rmIdToActionIndices, &allocations)

	tr.server = &txnCap
	tr.active = activeRMs

	return addsSubs, nil
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

var badCounter = errors.New("Counter too low; rerun needed.")

func (tr *TransactionRecord) formServerActions(counter uint32, translationCallback loco.TranslationCallback, picker *ch.CombinationPicker, actions *msgs.Action_List, clientActions *cmsgs.ClientAction_List) (map[common.RMId]*[]int, bool, error) {
	// if a.refs[n] --pointsTo-> b, and b is new, then we will need to
	// create positions for b, and write those into the pointer too.
	// referencesInNeedOfPositions keeps track of all such pointers.
	referencesInNeedOfPositions := []*msgs.VarIdPos{}
	rmIdToActionIndices := make(map[common.RMId]*[]int)
	addsSubs := false

	for idx, l := 0, clientActions.Len(); idx < l; idx++ {
		clientAction := clientActions.At(idx)
		action := actions.At(idx)
		vUUId := common.MakeVarUUId(clientAction.VarId())
		action.SetVarId(vUUId[:])

		clientValue := clientAction.Value()
		create := clientValue.Which() == cmsgs.CLIENTACTIONVALUE_CREATE
		c, err := tr.cache.Find(vUUId, !create)
		if err != nil {
			// either we found it and we're creating, or we didn't find it and we're not creating
			return nil, false, err
		}

		clientMeta, actionMeta := clientAction.Meta(), action.Meta()
		readRequired := false
		readOrCreateRequired := false

		if clientMeta.AddSub() {
			if !c.caps.CanRead() {
				return nil, false, fmt.Errorf("Illegal addSub of %v", vUUId)
			}
			readOrCreateRequired = true
			actionMeta.SetAddSub(true)
		}
		if delSub := clientMeta.DelSub(); len(delSub) != 0 {
			if !c.caps.CanRead() {
				return nil, false, fmt.Errorf("Illegal delSub of %v", vUUId)
			}
			readRequired = true
			actionMeta.SetDelSub(delSub)
		}

		switch actionValue := action.Value(); clientValue.Which() {
		case cmsgs.CLIENTACTIONVALUE_CREATE:
			clientCreate := clientValue.Create()
			// but, don't add it to the cache yet
			c = &Cached{
				Cache:   tr.cache,
				counter: counter,
				caps:    common.ReadWriteCapability,
			}
			c.CreatePositions(int(tr.topology.MaxRMCount))
			actionValue.SetCreate()
			actionCreate := actionValue.Create()
			actionCreate.SetPositions((capn.UInt8List)(*c.positions))
			actionCreate.SetValue(clientCreate.Value())
			if actionRefs, err := c.copyReferences(action.Segment, &referencesInNeedOfPositions, clientCreate.References()); err != nil {
				return nil, false, err
			} else {
				actionCreate.SetReferences(*actionRefs)
			}
			readOrCreateRequired = false

		case cmsgs.CLIENTACTIONVALUE_EXISTING:
			clientExisting := clientValue.Existing()
			actionValue.SetExisting()
			actionExisting := actionValue.Existing()

			switch clientModify, actionModify := clientExisting.Modify(), actionExisting.Modify(); clientModify.Which() {
			case cmsgs.CLIENTACTIONVALUEEXISTINGMODIFY_NOT:
				actionModify.SetNot()

			case cmsgs.CLIENTACTIONVALUEEXISTINGMODIFY_ROLL:
				if !c.caps.CanRead() {
					return nil, false, fmt.Errorf("Illegal roll of %v (insufficient capabilities)", vUUId)
				} else if counter < c.counter { // for completeness. In reality, not possible to fail
					return nil, false, badCounter
				}
				actionModify.SetRoll()
				readRequired = true

			case cmsgs.CLIENTACTIONVALUEEXISTINGMODIFY_WRITE:
				clientWrite := clientModify.Write()
				actionModify.SetWrite()
				actionWrite := actionModify.Write()
				actionWrite.SetValue(clientWrite.Value())
				if actionRefs, err := c.copyReferences(action.Segment, &referencesInNeedOfPositions, clientWrite.References()); err != nil {
					return nil, false, err
				} else {
					actionWrite.SetReferences(*actionRefs)
				}

			default:
				panic(fmt.Sprintf("%v: %v: Unexpected action value existing modify: %v", tr.origId, vUUId, clientModify.Which()))
			}

			if clientExisting.Read() {
				if !c.caps.CanRead() {
					return nil, false, fmt.Errorf("Illegal read of %v", vUUId)
				} else if counter < c.counter {
					return nil, false, badCounter
				}
				actionExisting.SetRead(c.version[:])
				readRequired = false
				readOrCreateRequired = false
			}
		default:
			panic(fmt.Sprintf("%v: %v: Unexpected action value: %v", tr.origId, vUUId, clientValue.Which()))
		}

		if readRequired {
			return nil, false, fmt.Errorf("Illegal action in %v (read required and not provided)", vUUId)
		} else if readOrCreateRequired {
			return nil, false, fmt.Errorf("Illegal action in %v (read or create required and not provided)", vUUId)
		}

		if err = c.EnsureHashCodes(); err != nil {
			return nil, false, err
		}

		tr.objs[*vUUId] = c

		if translationCallback != nil {
			if err = translationCallback(&clientAction, &action, c.hashCodes, tr.connections); err != nil {
				return nil, false, err
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
				return nil, false, err
			}
		}
		dstRef.SetPositions((capn.UInt8List)(*c.positions))
		if !c.caps.IsSubset(common.NewCapability(dstRef.Capability())) {
			return nil, false, fmt.Errorf("Attempt made to widen received capability to %v.", target)
		}
	}

	return rmIdToActionIndices, addsSubs, nil
}
