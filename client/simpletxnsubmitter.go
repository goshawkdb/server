package client

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	ch "goshawkdb.io/server/consistenthash"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"math/rand"
	"sort"
	"time"
)

type SimpleTxnSubmitter struct {
	rmId                common.RMId
	bootCount           uint32
	disabledHashCodes   map[common.RMId]server.EmptyStruct
	connections         map[common.RMId]paxos.Connection
	connPub             paxos.ServerConnectionPublisher
	outcomeConsumers    map[common.TxnId]txnOutcomeConsumer
	onShutdown          map[*func(bool) error]server.EmptyStruct
	resolver            *ch.Resolver
	hashCache           *ch.ConsistentHashCache
	topology            *configuration.Topology
	rng                 *rand.Rand
	bufferedSubmissions []func() error
}

type txnOutcomeConsumer func(common.RMId, *eng.TxnReader, *msgs.Outcome) error
type TxnCompletionConsumer func(*eng.TxnReader, *msgs.Outcome, error) error

func NewSimpleTxnSubmitter(rmId common.RMId, bootCount uint32, connPub paxos.ServerConnectionPublisher) *SimpleTxnSubmitter {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	cache := ch.NewCache(nil, rng)

	sts := &SimpleTxnSubmitter{
		rmId:             rmId,
		bootCount:        bootCount,
		connections:      nil,
		connPub:          connPub,
		outcomeConsumers: make(map[common.TxnId]txnOutcomeConsumer),
		onShutdown:       make(map[*func(bool) error]server.EmptyStruct),
		hashCache:        cache,
		rng:              rng,
	}
	return sts
}

func (sts *SimpleTxnSubmitter) Status(sc *server.StatusConsumer) {
	txnIds := make([]common.TxnId, 0, len(sts.outcomeConsumers))
	for txnId := range sts.outcomeConsumers {
		txnIds = append(txnIds, txnId)
	}
	sc.Emit(fmt.Sprintf("SimpleTxnSubmitter: live TxnIds: %v", txnIds))
	sc.Emit(fmt.Sprintf("SimpleTxnSubmitter: buffered Txns: %v", len(sts.bufferedSubmissions)))
	sc.Join()
}

func (sts *SimpleTxnSubmitter) IsIdle() bool {
	return len(sts.outcomeConsumers) == 0
}

func (sts *SimpleTxnSubmitter) EnsurePositions(varPosMap map[common.VarUUId]*common.Positions) {
	for vUUId, pos := range varPosMap {
		sts.hashCache.AddPosition(&vUUId, pos)
	}
}

func (sts *SimpleTxnSubmitter) SubmissionOutcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) error {
	txnId := txn.Id
	if consumer, found := sts.outcomeConsumers[*txnId]; found {
		return consumer(sender, txn, outcome)
	} else {
		// OSS is safe here - it's the default action on receipt of an unknown txnid
		paxos.NewOneShotSender(paxos.MakeTxnSubmissionCompleteMsg(txnId), sts.connPub, sender)
		return nil
	}
}

// txnCap must be a root
func (sts *SimpleTxnSubmitter) SubmitTransaction(txnCap *msgs.Txn, txnId *common.TxnId, activeRMs []common.RMId, continuation TxnCompletionConsumer, delay time.Duration) {
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	msg.SetTxnSubmission(server.SegToBytes(txnCap.Segment))

	server.Log(txnId, "Submitting txn")
	txnSender := paxos.NewRepeatingSender(server.SegToBytes(seg), activeRMs...)
	var removeSenderCh chan chan server.EmptyStruct
	if delay == 0 {
		sts.connPub.AddServerConnectionSubscriber(txnSender)
	} else {
		removeSenderCh = make(chan chan server.EmptyStruct)
		go func() {
			// fmt.Printf("%v ", delay)
			time.Sleep(delay)
			sts.connPub.AddServerConnectionSubscriber(txnSender)
			doneChan := <-removeSenderCh
			sts.connPub.RemoveServerConnectionSubscriber(txnSender)
			close(doneChan)
		}()
	}
	acceptors := paxos.GetAcceptorsFromTxn(*txnCap)

	shutdownFun := func(shutdown bool) error {
		delete(sts.outcomeConsumers, *txnId)
		// fmt.Printf("sts%v ", len(sts.outcomeConsumers))
		if delay == 0 {
			sts.connPub.RemoveServerConnectionSubscriber(txnSender)
		} else {
			txnSenderRemovedChan := make(chan server.EmptyStruct)
			removeSenderCh <- txnSenderRemovedChan
			<-txnSenderRemovedChan
		}
		// OSS is safe here - see above.
		paxos.NewOneShotSender(paxos.MakeTxnSubmissionCompleteMsg(txnId), sts.connPub, acceptors...)
		if shutdown {
			if txnCap.Retry() {
				// If this msg doesn't make it then proposers should
				// observe our death and tidy up anyway. If it's just this
				// connection shutting down then there should be no
				// problem with these msgs getting to the propposers.
				paxos.NewOneShotSender(paxos.MakeTxnSubmissionAbortMsg(txnId), sts.connPub, activeRMs...)
			}
			return continuation(nil, nil, nil)
		} else {
			return nil
		}
	}
	shutdownFunPtr := &shutdownFun
	sts.onShutdown[shutdownFunPtr] = server.EmptyStructVal

	outcomeAccumulator := paxos.NewOutcomeAccumulator(int(txnCap.FInc()), acceptors)
	consumer := func(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) error {
		if outcome, _ = outcomeAccumulator.BallotOutcomeReceived(sender, outcome); outcome != nil {
			delete(sts.onShutdown, shutdownFunPtr)
			if err := shutdownFun(false); err != nil {
				return err
			} else {
				return continuation(txn, outcome, nil)
			}
		}
		return nil
	}
	sts.outcomeConsumers[*txnId] = consumer
	// fmt.Printf("sts%v ", len(sts.outcomeConsumers))
}

func (sts *SimpleTxnSubmitter) SubmitClientTransaction(ctxnCap *cmsgs.ClientTxn, txnId *common.TxnId, continuation TxnCompletionConsumer, delay time.Duration, useNextVersion bool, vc versionCache) error {
	// Frames could attempt rolls before we have a topology.
	if sts.topology.IsBlank() || (sts.topology.Next() != nil && (!useNextVersion || !sts.topology.NextBarrierReached1(sts.rmId))) {
		fun := func() error {
			return sts.SubmitClientTransaction(ctxnCap, txnId, continuation, delay, useNextVersion, vc)
		}
		if sts.bufferedSubmissions == nil {
			sts.bufferedSubmissions = []func() error{fun}
		} else {
			sts.bufferedSubmissions = append(sts.bufferedSubmissions, fun)
		}
		return nil
	}
	version := sts.topology.Version
	if next := sts.topology.Next(); next != nil && useNextVersion {
		version = next.Version
	}
	txnCap, activeRMs, _, err := sts.clientToServerTxn(ctxnCap, version, vc)
	if err != nil {
		return continuation(nil, nil, err)
	}
	sts.SubmitTransaction(txnCap, txnId, activeRMs, continuation, delay)
	return nil
}

func (sts *SimpleTxnSubmitter) TopologyChanged(topology *configuration.Topology) error {
	server.Log("STS Topology Changed", topology)
	if topology.IsBlank() {
		// topology is needed for client txns. As we're booting up, we
		// just don't care.
		return nil
	}
	sts.topology = topology
	sts.resolver = ch.NewResolver(topology.RMs(), topology.TwoFInc)
	sts.hashCache.SetResolver(sts.resolver)
	if topology.Roots != nil {
		for _, root := range topology.Roots {
			sts.hashCache.AddPosition(root.VarUUId, root.Positions)
		}
	}
	return sts.calculateDisabledHashcodes()
}

func (sts *SimpleTxnSubmitter) ServerConnectionsChanged(servers map[common.RMId]paxos.Connection) error {
	server.Log("STS ServerConnectionsChanged", servers)
	sts.connections = servers
	return sts.calculateDisabledHashcodes()
}

func (sts *SimpleTxnSubmitter) calculateDisabledHashcodes() error {
	if sts.topology == nil || sts.connections == nil {
		return nil
	}
	sts.disabledHashCodes = make(map[common.RMId]server.EmptyStruct, len(sts.topology.RMs()))
	for _, rmId := range sts.topology.RMs() {
		if rmId == common.RMIdEmpty {
			continue
		} else if _, found := sts.connections[rmId]; !found {
			sts.disabledHashCodes[rmId] = server.EmptyStructVal
		}
	}
	server.Log("STS disabled hash codes", sts.disabledHashCodes)
	// need to wait until we've updated disabledHashCodes before
	// starting up any buffered txns.
	if !sts.topology.IsBlank() && sts.bufferedSubmissions != nil {
		funcs := sts.bufferedSubmissions
		sts.bufferedSubmissions = nil
		for _, fun := range funcs {
			if err := fun(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sts *SimpleTxnSubmitter) Shutdown() {
	for fun := range sts.onShutdown {
		(*fun)(true)
	}
}

func (sts *SimpleTxnSubmitter) clientToServerTxn(clientTxnCap *cmsgs.ClientTxn, topologyVersion uint32, vc versionCache) (*msgs.Txn, []common.RMId, []common.RMId, error) {
	outgoingSeg := capn.NewBuffer(nil)
	txnCap := msgs.NewRootTxn(outgoingSeg)

	txnCap.SetId(clientTxnCap.Id())
	txnCap.SetRetry(clientTxnCap.Retry())
	txnCap.SetSubmitter(uint32(sts.rmId))
	txnCap.SetSubmitterBootCount(sts.bootCount)
	txnCap.SetFInc(sts.topology.FInc)
	txnCap.SetTopologyVersion(topologyVersion)

	clientActions := clientTxnCap.Actions()
	actionsListSeg := capn.NewBuffer(nil)
	actionsWrapper := msgs.NewRootActionListWrapper(actionsListSeg)
	actions := msgs.NewActionList(actionsListSeg, clientActions.Len())
	actionsWrapper.SetActions(actions)
	picker := ch.NewCombinationPicker(int(sts.topology.FInc), sts.disabledHashCodes)

	rmIdToActionIndices, err := sts.translateActions(actionsListSeg, picker, &actions, &clientActions, vc)
	if err != nil {
		return nil, nil, nil, err
	}

	txnCap.SetActions(server.SegToBytes(actionsListSeg))
	// NB: we're guaranteed that activeRMs and passiveRMs are
	// disjoint. Thus there is no RM that has some active and some
	// passive actions.
	activeRMs, passiveRMs, err := picker.Choose()
	if err != nil {
		return nil, nil, nil, err
	}
	allocations := msgs.NewAllocationList(outgoingSeg, len(activeRMs)+len(passiveRMs))
	txnCap.SetAllocations(allocations)
	sts.setAllocations(0, rmIdToActionIndices, &allocations, outgoingSeg, true, activeRMs)
	sts.setAllocations(len(activeRMs), rmIdToActionIndices, &allocations, outgoingSeg, false, passiveRMs)
	return &txnCap, activeRMs, passiveRMs, nil
}

func (sts *SimpleTxnSubmitter) setAllocations(allocIdx int, rmIdToActionIndices map[common.RMId]*[]int, allocations *msgs.Allocation_List, seg *capn.Segment, active bool, rmIds []common.RMId) {
	for _, rmId := range rmIds {
		actionIndices := *(rmIdToActionIndices[rmId])
		sort.Ints(actionIndices)
		allocation := allocations.At(allocIdx)
		allocation.SetRmId(uint32(rmId))
		actionIndicesCap := seg.NewUInt16List(len(actionIndices))
		allocation.SetActionIndices(actionIndicesCap)
		for k, v := range actionIndices {
			actionIndicesCap.Set(k, uint16(v))
		}
		if active {
			allocation.SetActive(sts.connections[rmId].BootCount())
		} else {
			allocation.SetActive(0)
		}
		allocIdx++
	}
}

func (sts *SimpleTxnSubmitter) translateActions(outgoingSeg *capn.Segment, picker *ch.CombinationPicker, actions *msgs.Action_List, clientActions *cmsgs.ClientAction_List, vc versionCache) (map[common.RMId]*[]int, error) {

	referencesInNeedOfPositions := []*msgs.VarIdPos{}
	rmIdToActionIndices := make(map[common.RMId]*[]int)
	createdPositions := make(map[common.VarUUId]*common.Positions)

	for idx, l := 0, clientActions.Len(); idx < l; idx++ {
		clientAction := clientActions.At(idx)
		action := actions.At(idx)
		action.SetVarId(clientAction.VarId())
		vUUId := common.MakeVarUUId(clientAction.VarId())

		var err error
		var hashCodes []common.RMId

		switch clientAction.Which() {
		case cmsgs.CLIENTACTION_READ:
			sts.translateRead(&action, &clientAction)

		case cmsgs.CLIENTACTION_WRITE:
			sts.translateWrite(outgoingSeg, &referencesInNeedOfPositions, &action, &clientAction, vUUId, vc)

		case cmsgs.CLIENTACTION_READWRITE:
			sts.translateReadWrite(outgoingSeg, &referencesInNeedOfPositions, &action, &clientAction, vUUId, vc)

		case cmsgs.CLIENTACTION_CREATE:
			var positions *common.Positions
			positions, hashCodes, err = sts.translateCreate(outgoingSeg, &referencesInNeedOfPositions, &action, &clientAction, vUUId, vc)
			if err != nil {
				return nil, err
			}
			createdPositions[*vUUId] = positions

		case cmsgs.CLIENTACTION_ROLL:
			sts.translateRoll(outgoingSeg, &referencesInNeedOfPositions, &action, &clientAction)

		default:
			panic(fmt.Sprintf("Unexpected action type: %v", clientAction.Which()))
		}

		if hashCodes == nil {
			hashCodes, err = sts.hashCache.GetHashCodes(common.MakeVarUUId(action.VarId()))
			if err != nil {
				return nil, err
			}
		}
		if clientAction.Which() == cmsgs.CLIENTACTION_ROLL {
			// We cannot roll for anyone else. This could try to happen
			// during immigration.
			found := false
			for _, rmId := range hashCodes {
				if found = rmId == sts.rmId; found {
					break
				}
			}
			if !found {
				return nil, eng.AbortRollNotInPermutation
			}

			// If we're not first then first must not be active
			if hashCodes[0] != sts.rmId {
				if _, found := sts.connections[hashCodes[0]]; found {
					return nil, eng.AbortRollNotFirst
				}
			}
		}

		picker.AddPermutation(hashCodes)
		for _, rmId := range hashCodes {
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
	for _, vUUIdPos := range referencesInNeedOfPositions {
		vUUId := common.MakeVarUUId(vUUIdPos.Id())
		positions, found := createdPositions[*vUUId]
		if !found {
			positions = sts.hashCache.GetPositions(vUUId)
			if !vc.EnsureSubset(vUUId, vUUIdPos.Capabilities()) {
				return nil, fmt.Errorf("Reference created to %v attempts to extend known capabilities.", vUUId)
			}
		}
		if positions == nil {
			return nil, fmt.Errorf("Txn contains reference to unknown var %v", vUUId)
		}
		vUUIdPos.SetPositions((capn.UInt8List)(*positions))
	}
	return rmIdToActionIndices, nil
}

func (sts *SimpleTxnSubmitter) translateRead(action *msgs.Action, clientAction *cmsgs.ClientAction) {
	action.SetRead()
	clientRead := clientAction.Read()
	read := action.Read()
	read.SetVersion(clientRead.Version())
}

func (sts *SimpleTxnSubmitter) translateWrite(outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, action *msgs.Action, clientAction *cmsgs.ClientAction, vUUId *common.VarUUId, vc versionCache) {
	action.SetWrite()
	clientWrite := clientAction.Write()
	write := action.Write()
	write.SetValue(vc.ValueForWrite(vUUId, clientWrite.Value()))
	clientReferences := clientWrite.References()
	write.SetReferences(copyReferences(&clientReferences, outgoingSeg, referencesInNeedOfPositions, vUUId, vc))
}

func (sts *SimpleTxnSubmitter) translateReadWrite(outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, action *msgs.Action, clientAction *cmsgs.ClientAction, vUUId *common.VarUUId, vc versionCache) {
	action.SetReadwrite()
	clientReadWrite := clientAction.Readwrite()
	readWrite := action.Readwrite()
	readWrite.SetVersion(clientReadWrite.Version())
	readWrite.SetValue(vc.ValueForWrite(vUUId, clientReadWrite.Value()))
	clientReferences := clientReadWrite.References()
	readWrite.SetReferences(copyReferences(&clientReferences, outgoingSeg, referencesInNeedOfPositions, vUUId, vc))
}

func (sts *SimpleTxnSubmitter) translateCreate(outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, action *msgs.Action, clientAction *cmsgs.ClientAction, vUUId *common.VarUUId, vc versionCache) (*common.Positions, []common.RMId, error) {
	action.SetCreate()
	clientCreate := clientAction.Create()
	create := action.Create()
	create.SetValue(clientCreate.Value())
	positions, hashCodes, err := sts.hashCache.CreatePositions(vUUId, int(sts.topology.MaxRMCount))
	if err != nil {
		return nil, nil, err
	}
	create.SetPositions((capn.UInt8List)(*positions))
	clientReferences := clientCreate.References()
	create.SetReferences(copyReferences(&clientReferences, outgoingSeg, referencesInNeedOfPositions, nil, vc))
	return positions, hashCodes, nil
}

func (sts *SimpleTxnSubmitter) translateRoll(outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, action *msgs.Action, clientAction *cmsgs.ClientAction) {
	action.SetRoll()
	clientRoll := clientAction.Roll()
	roll := action.Roll()
	roll.SetVersion(clientRoll.Version())
	roll.SetValue(clientRoll.Value())
	clientReferences := clientRoll.References()
	roll.SetReferences(copyReferences(&clientReferences, outgoingSeg, referencesInNeedOfPositions, nil, nil))
}

func copyReferences(clientReferences *cmsgs.ClientVarIdPos_List, seg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, vUUId *common.VarUUId, vc versionCache) msgs.VarIdPos_List {
	all, mask, existingRefs := vc.ReferencesWriteMask(vUUId)
	if all {
		refs := msgs.NewVarIdPosList(seg, clientReferences.Len())
		for idx, l := 0, clientReferences.Len(); idx < l; idx++ {
			clientRef := clientReferences.At(idx)
			vUUIdPos := refs.At(idx)
			vUUIdPos.SetId(clientRef.VarId())
			vUUIdPos.SetCapabilities(translateCapabilities(seg, clientRef.Capabilities()))
			*referencesInNeedOfPositions = append(*referencesInNeedOfPositions, &vUUIdPos)
		}
		return refs
	} else {
		refs := msgs.NewVarIdPosList(seg, len(existingRefs))
		clientRefLen := clientReferences.Len()
		if clientRefLen > len(existingRefs) {
			clientRefLen = len(existingRefs)
		}
		idx := 0
		for ; idx < clientRefLen; idx++ {
			vUUIdPos := refs.At(idx)
			if len(mask) > 0 && mask[0] == uint32(idx) {
				mask = mask[1:]
				clientRef := clientReferences.At(idx)
				vUUIdPos.SetId(clientRef.VarId())
				vUUIdPos.SetCapabilities(translateCapabilities(seg, clientRef.Capabilities()))
			} else {
				existing := existingRefs[idx]
				vUUIdPos.SetId(existing.Id())
				vUUIdPos.SetCapabilities(existing.Capabilities())
			}
			*referencesInNeedOfPositions = append(*referencesInNeedOfPositions, &vUUIdPos)
		}
		for ; idx < len(existingRefs); idx++ {
			vUUIdPos := refs.At(idx)
			existing := existingRefs[idx]
			vUUIdPos.SetId(existing.Id())
			vUUIdPos.SetCapabilities(existing.Capabilities())
		}
		return refs
	}
}

func translateCapabilities(seg *capn.Segment, cap cmsgs.Capabilities) cmsgs.Capabilities {
	readWhich, writeWhich := cap.References().Read().Which(), cap.References().Write().Which()
	if readWhich == cmsgs.CAPABILITIESREFERENCESREAD_ALL &&
		writeWhich == cmsgs.CAPABILITIESREFERENCESWRITE_ALL {
		return cap
	}
	rebuild := false
	if readWhich == cmsgs.CAPABILITIESREFERENCESREAD_ONLY {
		only := cap.References().Read().Only().ToArray()
		if len(only) > 1 {
			old := only[0]
			for _, index := range only[1:] {
				if old >= index {
					rebuild = true
					break
				}
				old = index
			}
		}
	}
	if !rebuild && writeWhich == cmsgs.CAPABILITIESREFERENCESWRITE_ONLY {
		only := cap.References().Write().Only().ToArray()
		if len(only) > 1 {
			old := only[0]
			for _, index := range only[1:] {
				if old >= index {
					rebuild = true
					break
				}
				old = index
			}
		}
	}
	if !rebuild {
		return cap
	}
	capNew := cmsgs.NewCapabilities(seg)
	capNew.SetValue(cap.Value())
	readNew := capNew.References().Read()
	if readWhich == cmsgs.CAPABILITIESREFERENCESREAD_ALL {
		readNew.SetAll()
	} else {
		only := cap.References().Read().Only().ToArray()
		common.SortUInt32(only).Sort()
		if len(only) > 1 {
			old := only[0]
			for idx := 1; idx < len(only); idx++ {
				cur := only[idx]
				if cur == old {
					only = append(only[:idx], only[idx+1:]...)
					idx--
				}
				old = cur
			}
		}
		onlyNew := seg.NewUInt32List(len(only))
		for idx, index := range only {
			onlyNew.Set(idx, index)
		}
		readNew.SetOnly(onlyNew)
	}
	writeNew := capNew.References().Write()
	if writeWhich == cmsgs.CAPABILITIESREFERENCESWRITE_ALL {
		writeNew.SetAll()
	} else {
		only := cap.References().Write().Only().ToArray()
		common.SortUInt32(only).Sort()
		if len(only) > 1 {
			old := only[0]
			for idx := 1; idx < len(only); idx++ {
				cur := only[idx]
				if cur == old {
					only = append(only[:idx], only[idx+1:]...)
					idx--
				}
				old = cur
			}
		}
		onlyNew := seg.NewUInt32List(len(only))
		for idx, index := range only {
			onlyNew.Set(idx, index)
		}
		writeNew.SetOnly(onlyNew)
	}
	return capNew
}
