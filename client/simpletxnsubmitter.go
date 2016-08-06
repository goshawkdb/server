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
	onShutdown          map[*func(bool)]server.EmptyStruct
	resolver            *ch.Resolver
	hashCache           *ch.ConsistentHashCache
	topology            *configuration.Topology
	rng                 *rand.Rand
	bufferedSubmissions []func()
}

type txnOutcomeConsumer func(common.RMId, *eng.TxnReader, *msgs.Outcome)
type TxnCompletionConsumer func(*eng.TxnReader, *msgs.Outcome, error)

func NewSimpleTxnSubmitter(rmId common.RMId, bootCount uint32, connPub paxos.ServerConnectionPublisher) *SimpleTxnSubmitter {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	cache := ch.NewCache(nil, rng)

	sts := &SimpleTxnSubmitter{
		rmId:             rmId,
		bootCount:        bootCount,
		connections:      nil,
		connPub:          connPub,
		outcomeConsumers: make(map[common.TxnId]txnOutcomeConsumer),
		onShutdown:       make(map[*func(bool)]server.EmptyStruct),
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

func (sts *SimpleTxnSubmitter) SubmissionOutcomeReceived(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) {
	txnId := txn.Id
	if consumer, found := sts.outcomeConsumers[*txnId]; found {
		consumer(sender, txn, outcome)
	} else {
		// OSS is safe here - it's the default action on receipt of an unknown txnid
		paxos.NewOneShotSender(paxos.MakeTxnSubmissionCompleteMsg(txnId), sts.connPub, sender)
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

	shutdownFun := func(shutdown bool) {
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
			continuation(nil, nil, nil)
		}
	}
	shutdownFunPtr := &shutdownFun
	sts.onShutdown[shutdownFunPtr] = server.EmptyStructVal

	outcomeAccumulator := paxos.NewOutcomeAccumulator(int(txnCap.FInc()), acceptors)
	consumer := func(sender common.RMId, txn *eng.TxnReader, outcome *msgs.Outcome) {
		if outcome, _ = outcomeAccumulator.BallotOutcomeReceived(sender, outcome); outcome != nil {
			delete(sts.onShutdown, shutdownFunPtr)
			shutdownFun(false)
			continuation(txn, outcome, nil)
		}
	}
	sts.outcomeConsumers[*txnId] = consumer
	// fmt.Printf("sts%v ", len(sts.outcomeConsumers))
}

func (sts *SimpleTxnSubmitter) SubmitClientTransaction(ctxnCap *cmsgs.ClientTxn, txnId *common.TxnId, continuation TxnCompletionConsumer, delay time.Duration, useNextVersion bool) {
	// Frames could attempt rolls before we have a topology.
	if sts.topology.IsBlank() || (sts.topology.Next() != nil && (!useNextVersion || !sts.topology.NextBarrierReached1(sts.rmId))) {
		fun := func() { sts.SubmitClientTransaction(ctxnCap, txnId, continuation, delay, useNextVersion) }
		if sts.bufferedSubmissions == nil {
			sts.bufferedSubmissions = []func(){fun}
		} else {
			sts.bufferedSubmissions = append(sts.bufferedSubmissions, fun)
		}
		return
	}
	version := sts.topology.Version
	if next := sts.topology.Next(); next != nil && useNextVersion {
		version = next.Version
	}
	txnCap, activeRMs, _, err := sts.clientToServerTxn(ctxnCap, version)
	if err != nil {
		continuation(nil, nil, err)
		return
	}
	sts.SubmitTransaction(txnCap, txnId, activeRMs, continuation, delay)
}

func (sts *SimpleTxnSubmitter) TopologyChanged(topology *configuration.Topology) {
	server.Log("STS Topology Changed", topology)
	if topology.IsBlank() {
		// topology is needed for client txns. As we're booting up, we
		// just don't care.
		return
	}
	sts.topology = topology
	sts.resolver = ch.NewResolver(topology.RMs(), topology.TwoFInc)
	sts.hashCache.SetResolver(sts.resolver)
	if topology.Roots != nil {
		for _, root := range topology.Roots {
			sts.hashCache.AddPosition(root.VarUUId, root.Positions)
		}
	}
	sts.calculateDisabledHashcodes()
}

func (sts *SimpleTxnSubmitter) ServerConnectionsChanged(servers map[common.RMId]paxos.Connection) {
	server.Log("STS ServerConnectionsChanged", servers)
	sts.connections = servers
	sts.calculateDisabledHashcodes()
}

func (sts *SimpleTxnSubmitter) calculateDisabledHashcodes() {
	if sts.topology == nil || sts.connections == nil {
		return
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
			fun()
		}
	}
}

func (sts *SimpleTxnSubmitter) Shutdown() {
	for fun := range sts.onShutdown {
		(*fun)(true)
	}
}

func (sts *SimpleTxnSubmitter) clientToServerTxn(clientTxnCap *cmsgs.ClientTxn, topologyVersion uint32) (*msgs.Txn, []common.RMId, []common.RMId, error) {
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

	rmIdToActionIndices, err := sts.translateActions(actionsListSeg, picker, &actions, &clientActions)
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

func (sts *SimpleTxnSubmitter) translateActions(outgoingSeg *capn.Segment, picker *ch.CombinationPicker, actions *msgs.Action_List, clientActions *cmsgs.ClientAction_List) (map[common.RMId]*[]int, error) {

	referencesInNeedOfPositions := []*msgs.VarIdPos{}
	rmIdToActionIndices := make(map[common.RMId]*[]int)
	createdPositions := make(map[common.VarUUId]*common.Positions)

	for idx, l := 0, clientActions.Len(); idx < l; idx++ {
		clientAction := clientActions.At(idx)
		action := actions.At(idx)
		action.SetVarId(clientAction.VarId())

		var err error
		var hashCodes []common.RMId

		switch clientAction.Which() {
		case cmsgs.CLIENTACTION_READ:
			sts.translateRead(&action, &clientAction)

		case cmsgs.CLIENTACTION_WRITE:
			sts.translateWrite(outgoingSeg, &referencesInNeedOfPositions, &action, &clientAction)

		case cmsgs.CLIENTACTION_READWRITE:
			sts.translateReadWrite(outgoingSeg, &referencesInNeedOfPositions, &action, &clientAction)

		case cmsgs.CLIENTACTION_CREATE:
			var positions *common.Positions
			positions, hashCodes, err = sts.translateCreate(outgoingSeg, &referencesInNeedOfPositions, &action, &clientAction)
			if err != nil {
				return nil, err
			}
			vUUId := common.MakeVarUUId(clientAction.VarId())
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

func (sts *SimpleTxnSubmitter) translateWrite(outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, action *msgs.Action, clientAction *cmsgs.ClientAction) {
	action.SetWrite()
	clientWrite := clientAction.Write()
	write := action.Write()
	write.SetValue(clientWrite.Value())
	clientReferences := clientWrite.References()
	references := msgs.NewVarIdPosList(outgoingSeg, clientReferences.Len())
	write.SetReferences(references)
	copyReferences(&clientReferences, &references, referencesInNeedOfPositions)
}

func (sts *SimpleTxnSubmitter) translateReadWrite(outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, action *msgs.Action, clientAction *cmsgs.ClientAction) {
	action.SetReadwrite()
	clientReadWrite := clientAction.Readwrite()
	readWrite := action.Readwrite()
	readWrite.SetVersion(clientReadWrite.Version())
	readWrite.SetValue(clientReadWrite.Value())
	clientReferences := clientReadWrite.References()
	references := msgs.NewVarIdPosList(outgoingSeg, clientReferences.Len())
	readWrite.SetReferences(references)
	copyReferences(&clientReferences, &references, referencesInNeedOfPositions)
}

func (sts *SimpleTxnSubmitter) translateCreate(outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, action *msgs.Action, clientAction *cmsgs.ClientAction) (*common.Positions, []common.RMId, error) {
	action.SetCreate()
	clientCreate := clientAction.Create()
	create := action.Create()
	create.SetValue(clientCreate.Value())
	vUUId := common.MakeVarUUId(clientAction.VarId())
	positions, hashCodes, err := sts.hashCache.CreatePositions(vUUId, int(sts.topology.MaxRMCount))
	if err != nil {
		return nil, nil, err
	}
	create.SetPositions((capn.UInt8List)(*positions))
	clientReferences := clientCreate.References()
	references := msgs.NewVarIdPosList(outgoingSeg, clientReferences.Len())
	create.SetReferences(references)
	copyReferences(&clientReferences, &references, referencesInNeedOfPositions)
	return positions, hashCodes, nil
}

func (sts *SimpleTxnSubmitter) translateRoll(outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, action *msgs.Action, clientAction *cmsgs.ClientAction) {
	action.SetRoll()
	clientRoll := clientAction.Roll()
	roll := action.Roll()
	roll.SetVersion(clientRoll.Version())
	roll.SetValue(clientRoll.Value())
	clientReferences := clientRoll.References()
	references := msgs.NewVarIdPosList(outgoingSeg, clientReferences.Len())
	roll.SetReferences(references)
	copyReferences(&clientReferences, &references, referencesInNeedOfPositions)
}

func copyReferences(clientReferences *capn.DataList, references *msgs.VarIdPos_List, referencesInNeedOfPositions *[]*msgs.VarIdPos) {
	for idx, l := 0, clientReferences.Len(); idx < l; idx++ {
		vUUIdPos := references.At(idx)
		vUUId := clientReferences.At(idx)
		vUUIdPos.SetId(vUUId)
		*referencesInNeedOfPositions = append(*referencesInNeedOfPositions, &vUUIdPos)
	}
}
