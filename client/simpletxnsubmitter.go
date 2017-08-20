package client

import (
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/paxos"
	eng "goshawkdb.io/server/txnengine"
	"goshawkdb.io/server/types"
	"goshawkdb.io/server/types/actor"
	sconn "goshawkdb.io/server/types/connections/server"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/binarybackoff"
	ch "goshawkdb.io/server/utils/consistenthash"
	"goshawkdb.io/server/utils/senders"
	"goshawkdb.io/server/utils/status"
	"goshawkdb.io/server/utils/txnreader"
	"math/rand"
	"sort"
	"sync/atomic"
)

type SimpleTxnSubmitter struct {
	logger              log.Logger
	rmId                common.RMId
	bootCount           uint32
	disabledHashCodes   map[common.RMId]types.EmptyStruct
	connections         map[common.RMId]*sconn.ServerConnection
	connectionsBool     map[common.RMId]bool
	connPub             sconn.ServerConnectionPublisher
	outcomeConsumers    map[common.TxnId]txnOutcomeConsumer
	onShutdown          map[*func(bool)]types.EmptyStruct
	shutdownRequested   func()
	hashCache           *ch.ConsistentHashCache
	topology            *configuration.Topology
	rng                 *rand.Rand
	bufferedSubmissions []func() error
	actor               actor.EnqueueActor
}

type txnOutcomeConsumer func(common.RMId, *txnreader.TxnReader, *msgs.Outcome) error
type TxnCompletionConsumer func(*txnreader.TxnReader, *msgs.Outcome, error) error

func NewSimpleTxnSubmitter(rmId common.RMId, bootCount uint32, connPub sconn.ServerConnectionPublisher, actor actor.EnqueueActor, rng *rand.Rand, logger log.Logger) *SimpleTxnSubmitter {
	cache := ch.NewCache(nil, rng)

	sts := &SimpleTxnSubmitter{
		logger:            logger,
		rmId:              rmId,
		bootCount:         bootCount,
		connections:       nil,
		connPub:           connPub,
		outcomeConsumers:  make(map[common.TxnId]txnOutcomeConsumer),
		onShutdown:        make(map[*func(bool)]types.EmptyStruct),
		shutdownRequested: nil,
		hashCache:         cache,
		rng:               rng,
		actor:             actor,
	}
	return sts
}

func (sts *SimpleTxnSubmitter) Status(sc *status.StatusConsumer) {
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

func (sts *SimpleTxnSubmitter) SubmissionOutcomeReceived(sender common.RMId, txn *txnreader.TxnReader, outcome *msgs.Outcome) error {
	txnId := txn.Id
	if consumer, found := sts.outcomeConsumers[*txnId]; found {
		return consumer(sender, txn, outcome)
	} else {
		// OSS is safe here - it's the default action on receipt of an unknown txnid
		senders.NewOneShotSender(sts.logger, paxos.MakeTxnSubmissionCompleteMsg(txnId), sts.connPub, sender)
		return nil
	}
}

// txnCap must be a root
func (sts *SimpleTxnSubmitter) SubmitTransaction(txnCap *msgs.Txn, txnId *common.TxnId, activeRMs []common.RMId, continuation TxnCompletionConsumer, delay *binarybackoff.BinaryBackoffEngine) {
	if sts.shutdownRequested != nil {
		continuation(nil, nil, errors.New("Shutdown in progress"))
		return
	}
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(seg)
	msg.SetTxnSubmission(common.SegToBytes(txnCap.Segment))

	utils.DebugLog(sts.logger, "debug", "Submitting txn.", "TxnId", txnId, "active", activeRMs)
	txnSender := senders.NewRepeatingSender(common.SegToBytes(seg), activeRMs...)
	removeNeeded := uint32(0)
	sleeping := delay != nil && delay.Cur > 0
	if sleeping {
		// fmt.Printf("%v ", delay.Cur)
		delay.After(func() {
			sts.actor.EnqueueFuncAsync(func() (bool, error) {
				if atomic.CompareAndSwapUint32(&removeNeeded, 0, 1) {
					// we swapped ok, so we got here first, so we do work
					sts.connPub.AddServerConnectionSubscriber(txnSender)
				}
				return false, nil
			})
		})
	} else {
		sts.connPub.AddServerConnectionSubscriber(txnSender)
	}
	acceptors := paxos.GetAcceptorsFromTxn(*txnCap)

	shutdownFun := func(completed bool) {
		// If we're a retry then we have a specific Abort message to
		// send which helps tidy up garbage in the case that this node
		// *isn't* going down (connection dying only, and !completed).
		//
		// But, if we're not a retry, then we must *not* cancel the
		// sender (unless completed!): in the case that this node
		// *isn't* going down then no one else will react to the loss of
		// this connection and so we could end up with a dangling
		// transaction if the txnSender only manages to send to some but
		// not all active voters.

		// OSS is safe here - see above. In the case that the node's
		// exiting, this informs acceptors that we don't care about the
		// answer so they shouldn't hang around waiting for the node to
		// return. Of course, it's best effort.
		senders.NewOneShotSender(sts.logger, paxos.MakeTxnSubmissionCompleteMsg(txnId), sts.connPub, acceptors...)
		retry := txnCap.Retry()
		if completed || retry { // need to stop the txnSender
			if sleeping {
				if !atomic.CompareAndSwapUint32(&removeNeeded, 0, 1) {
					// for this to fail, we must have added the subscriber, so
					// we must remove it
					sts.connPub.RemoveServerConnectionSubscriber(txnSender)
				}
			} else {
				sts.connPub.RemoveServerConnectionSubscriber(txnSender)
			}
		}
		if !completed && retry {
			// If this msg doesn't make it then proposers should
			// observe our death and tidy up anyway. If it's just this
			// connection shutting down then there should be no
			// problem with these msgs getting to the proposers.
			senders.NewOneShotSender(sts.logger, paxos.MakeTxnSubmissionAbortMsg(txnId), sts.connPub, activeRMs...)
		}
		if !completed {
			// We're shutting down, there's really nothing sensible we can
			// do with any error from the continuation, so let's just log it
			if err := continuation(nil, nil, nil); err != nil {
				sts.logger.Log("msg", "Error during shutdown.", "error", err)
			}
		}
	}
	shutdownFunPtr := &shutdownFun
	sts.onShutdown[shutdownFunPtr] = types.EmptyStructVal

	outcomeAccumulator := paxos.NewOutcomeAccumulator(int(txnCap.TwoFInc()), acceptors, sts.logger)
	consumer := func(sender common.RMId, txn *txnreader.TxnReader, outcome *msgs.Outcome) error {
		if outcome, _ = outcomeAccumulator.BallotOutcomeReceived(sender, outcome); outcome != nil {
			delete(sts.onShutdown, shutdownFunPtr)
			shutdownFun(true)
			delete(sts.outcomeConsumers, *txnId)
			if onIdle := sts.shutdownRequested; onIdle != nil && sts.IsIdle() {
				sts.shutdownRequested = nil
				onIdle()
			}
			return continuation(txn, outcome, nil)
		}
		return nil
	}
	sts.outcomeConsumers[*txnId] = consumer
	// fmt.Printf("sts%v ", len(sts.outcomeConsumers))
}

func (sts *SimpleTxnSubmitter) SubmitClientTransaction(translationCallback eng.TranslationCallback, ctxnCap *cmsgs.ClientTxn, txnId *common.TxnId, continuation TxnCompletionConsumer, delay *binarybackoff.BinaryBackoffEngine, isTopologyTxn bool, vc *versionCache) error {
	// Frames could attempt rolls before we have a topology.
	if sts.topology.IsBlank() {
		fun := func() error {
			return sts.SubmitClientTransaction(translationCallback, ctxnCap, txnId, continuation, delay, isTopologyTxn, vc)
		}
		if sts.bufferedSubmissions == nil {
			sts.bufferedSubmissions = []func() error{fun}
		} else {
			sts.bufferedSubmissions = append(sts.bufferedSubmissions, fun)
		}
		return nil
	}
	txnCap, activeRMs, _, err := sts.clientToServerTxn(translationCallback, ctxnCap, sts.topology.Version, isTopologyTxn, vc)
	if err != nil {
		return continuation(nil, nil, err)
	}
	sts.SubmitTransaction(txnCap, txnId, activeRMs, continuation, delay)
	return nil
}

func (sts *SimpleTxnSubmitter) TopologyChanged(topology *configuration.Topology) error {
	utils.DebugLog(sts.logger, "debug", "STS Topology Changed.", "topology", topology)
	if topology.IsBlank() {
		// topology is needed for client txns. As we're booting up, we
		// just don't care.
		return nil
	}
	sts.topology = topology
	sts.hashCache.SetResolver(ch.NewResolver(topology.RMs, topology.TwoFInc))
	if topology.Roots != nil {
		for _, root := range topology.RootVarUUIds {
			sts.hashCache.AddPosition(root.VarUUId, root.Positions)
		}
	}
	// TODO push this topology through to the OutcomeAccumulators
	return sts.calculateDisabledHashcodes()
}

func (sts *SimpleTxnSubmitter) ServerConnectionsChanged(servers map[common.RMId]*sconn.ServerConnection) error {
	utils.DebugLog(sts.logger, "debug", "STS ServerConnectionsChanged.", "servers", servers)
	sts.connections = servers
	sts.connectionsBool = make(map[common.RMId]bool, len(servers))
	for k := range servers {
		sts.connectionsBool[k] = true
	}
	return sts.calculateDisabledHashcodes()
}

func (sts *SimpleTxnSubmitter) calculateDisabledHashcodes() error {
	if sts.topology == nil || sts.connections == nil {
		return nil
	}
	sts.disabledHashCodes = make(map[common.RMId]types.EmptyStruct, len(sts.topology.RMs))
	for _, rmId := range sts.topology.RMs {
		if rmId == common.RMIdEmpty {
			continue
		} else if _, found := sts.connections[rmId]; !found {
			sts.disabledHashCodes[rmId] = types.EmptyStructVal
		}
	}
	utils.DebugLog(sts.logger, "debug", "STS disabled hash codes.", "disabledHashCodes", sts.disabledHashCodes)
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

func (sts *SimpleTxnSubmitter) Shutdown(onIdle func()) {
	for fun := range sts.onShutdown {
		(*fun)(false)
	}
	sts.onShutdown = make(map[*func(bool)]types.EmptyStruct)
	if sts.IsIdle() && onIdle != nil {
		onIdle()
	} else {
		sts.shutdownRequested = onIdle
	}
}

func (sts *SimpleTxnSubmitter) clientToServerTxn(translationCallback eng.TranslationCallback, clientTxnCap *cmsgs.ClientTxn, topologyVersion uint32, isTopologyTxn bool, vc *versionCache) (*msgs.Txn, []common.RMId, []common.RMId, error) {
	outgoingSeg := capn.NewBuffer(nil)
	txnCap := msgs.NewRootTxn(outgoingSeg)

	txnCap.SetId(clientTxnCap.Id())
	txnCap.SetRetry(clientTxnCap.Retry())
	txnCap.SetIsTopology(isTopologyTxn)
	txnCap.SetTwoFInc(sts.topology.TwoFInc)
	txnCap.SetTopologyVersion(topologyVersion)

	clientActions := clientTxnCap.Actions()
	actionsListSeg := capn.NewBuffer(nil)
	actionsWrapper := msgs.NewRootActionListWrapper(actionsListSeg)
	actions := msgs.NewActionList(actionsListSeg, clientActions.Len())
	actionsWrapper.SetActions(actions)
	picker := ch.NewCombinationPicker(int(sts.topology.FInc), sts.disabledHashCodes)

	rmIdToActionIndices, err := sts.translateActions(translationCallback, actionsListSeg, picker, &actions, &clientActions, vc)
	if err != nil {
		return nil, nil, nil, err
	}

	txnCap.SetActions(common.SegToBytes(actionsListSeg))
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
			allocation.SetActive(sts.connections[rmId].BootCount)
		} else {
			allocation.SetActive(0)
		}
		allocIdx++
	}
}

// translate from client representation to server representation
func (sts *SimpleTxnSubmitter) translateActions(translationCallback eng.TranslationCallback, outgoingSeg *capn.Segment, picker *ch.CombinationPicker, actions *msgs.Action_List, clientActions *cmsgs.ClientAction_List, vc *versionCache) (map[common.RMId]*[]int, error) {

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
			err = sts.translateRead(&action, clientAction.Read())

		case cmsgs.CLIENTACTION_WRITE:
			err = sts.translateWrite(vc, outgoingSeg, &referencesInNeedOfPositions, vUUId, &action, clientAction.Write())

		case cmsgs.CLIENTACTION_READWRITE:
			err = sts.translateReadWrite(vc, outgoingSeg, &referencesInNeedOfPositions, vUUId, &action, clientAction.Readwrite())

		case cmsgs.CLIENTACTION_CREATE:
			var positions *common.Positions
			positions, hashCodes, err = sts.translateCreate(vc, outgoingSeg, &referencesInNeedOfPositions, vUUId, &action, clientAction.Create())
			createdPositions[*vUUId] = positions

		case cmsgs.CLIENTACTION_ROLL:
			err = sts.translateRoll(vc, outgoingSeg, &referencesInNeedOfPositions, &action, clientAction.Roll())

		default:
			panic(fmt.Sprintf("Unexpected action type: %v", clientAction.Which()))
		}

		if err != nil {
			return nil, err
		}

		if hashCodes == nil {
			hashCodes, err = sts.hashCache.GetHashCodes(common.MakeVarUUId(action.VarId()))
			if err != nil {
				return nil, err
			}
		}

		if translationCallback != nil {
			if err = translationCallback(&clientAction, &action, hashCodes, sts.connectionsBool); err != nil {
				return nil, err
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

func (sts *SimpleTxnSubmitter) translateRead(action *msgs.Action, clientRead cmsgs.ClientActionRead) error {
	action.SetRead()
	read := action.Read()
	read.SetVersion(clientRead.Version())
	return nil
}

func (sts *SimpleTxnSubmitter) translateWrite(vc *versionCache, outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, vUUId *common.VarUUId, action *msgs.Action, clientWrite cmsgs.ClientActionWrite) error {
	action.SetWrite()
	write := action.Write()
	write.SetValue(clientWrite.Value())
	clientReferences := clientWrite.References()
	refs, err := copyReferences(vc, outgoingSeg, referencesInNeedOfPositions, &clientReferences)
	if err != nil {
		return err
	}
	write.SetReferences(*refs)
	return nil
}

func (sts *SimpleTxnSubmitter) translateReadWrite(vc *versionCache, outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, vUUId *common.VarUUId, action *msgs.Action, clientReadWrite cmsgs.ClientActionReadwrite) error {
	clientReferences := clientReadWrite.References()
	action.SetReadwrite()
	readWrite := action.Readwrite()
	readWrite.SetVersion(clientReadWrite.Version())
	readWrite.SetValue(clientReadWrite.Value())
	refs, err := copyReferences(vc, outgoingSeg, referencesInNeedOfPositions, &clientReferences)
	if err != nil {
		return err
	}
	readWrite.SetReferences(*refs)
	return nil
}

func (sts *SimpleTxnSubmitter) translateCreate(vc *versionCache, outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, vUUId *common.VarUUId, action *msgs.Action, clientCreate cmsgs.ClientActionCreate) (*common.Positions, []common.RMId, error) {
	action.SetCreate()
	create := action.Create()
	create.SetValue(clientCreate.Value())
	positions, hashCodes, err := sts.hashCache.CreatePositions(vUUId, int(sts.topology.MaxRMCount))
	if err != nil {
		return nil, nil, err
	}
	create.SetPositions((capn.UInt8List)(*positions))
	clientReferences := clientCreate.References()
	refs, err := copyReferences(vc, outgoingSeg, referencesInNeedOfPositions, &clientReferences)
	if err != nil {
		return nil, nil, err
	}
	create.SetReferences(*refs)
	return positions, hashCodes, nil
}

func (sts *SimpleTxnSubmitter) translateRoll(vc *versionCache, outgoingSeg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, action *msgs.Action, clientRoll cmsgs.ClientActionRoll) error {
	action.SetRoll()
	roll := action.Roll()
	roll.SetVersion(clientRoll.Version())
	roll.SetValue(clientRoll.Value())
	clientReferences := clientRoll.References()
	refs, err := copyReferences(vc, outgoingSeg, referencesInNeedOfPositions, &clientReferences)
	if err != nil {
		return err
	}
	roll.SetReferences(*refs)
	return nil
}

func copyReferences(vc *versionCache, seg *capn.Segment, referencesInNeedOfPositions *[]*msgs.VarIdPos, clientReferences *cmsgs.ClientVarIdPos_List) (*msgs.VarIdPos_List, error) {
	refs := msgs.NewVarIdPosList(seg, clientReferences.Len())
	for idx, l := 0, clientReferences.Len(); idx < l; idx++ {
		clientRef := clientReferences.At(idx)
		vUUIdPos := refs.At(idx)
		target := common.MakeVarUUId(clientRef.VarId())
		vUUIdPos.SetId(target[:])
		caps := clientRef.Capability()
		if err := validateCapability(vc, target, caps); err != nil {
			return nil, err
		}
		vUUIdPos.SetCapability(caps)
		*referencesInNeedOfPositions = append(*referencesInNeedOfPositions, &vUUIdPos)
	}
	return &refs, nil
}

func validateCapability(vc *versionCache, target *common.VarUUId, cap cmsgs.Capability) error {
	if !vc.EnsureSubset(target, cap) {
		return fmt.Errorf("Attempt made to grant wider capabilities on %v than acceptable", target)
	}
	return nil
}
