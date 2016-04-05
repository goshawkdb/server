package paxos

import (
	"bytes"
	"encoding/binary"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/dispatcher"
)

func init() {
	db.DB.BallotOutcomes = &mdbs.DBISettings{Flags: mdb.CREATE}
}

type AcceptorManager struct {
	RMId              common.RMId
	DB                *db.Databases
	ConnectionManager ConnectionManager
	Exe               *dispatcher.Executor
	instances         map[instanceId]*instance
	acceptors         map[common.TxnId]*acceptorInstances
	Topology          *configuration.Topology
}

func NewAcceptorManager(rmId common.RMId, exe *dispatcher.Executor, cm ConnectionManager, db *db.Databases) *AcceptorManager {
	return &AcceptorManager{
		RMId:              rmId,
		DB:                db,
		ConnectionManager: cm,
		Exe:               exe,
		instances:         make(map[instanceId]*instance),
		acceptors:         make(map[common.TxnId]*acceptorInstances),
	}
}

func (am *AcceptorManager) ensureInstance(txnId *common.TxnId, instId *instanceId, vUUId *common.VarUUId) *instance {
	if inst, found := am.instances[*instId]; found {
		return inst
	} else {
		aInst, found := am.acceptors[*txnId]
		if !found {
			aInst = new(acceptorInstances)
			am.acceptors[*txnId] = aInst
		}
		aInst.addInstance(instId)
		inst = &instance{
			manager: am,
			vUUId:   vUUId,
		}
		am.instances[*instId] = inst
		return inst
	}
}

func (am *AcceptorManager) ensureAcceptor(txnId *common.TxnId, txnCap *msgs.Txn) *Acceptor {
	aInst, found := am.acceptors[*txnId]
	switch {
	case found && aInst.acceptor != nil:
		return aInst.acceptor
	case found:
		a := NewAcceptor(txnId, txnCap, am)
		aInst.acceptor = a
		a.Start()
		return a
	default:
		a := NewAcceptor(txnId, txnCap, am)
		aInst = &acceptorInstances{acceptor: a}
		am.acceptors[*txnId] = aInst
		a.Start()
		return a
	}
}

/*
  The paxos instance id is the triple of {txnId, rmId, varId}.
  The paxos round number is the pair of {num, rmId}.

  When a RM failure occurs, from the txn allocations, you can easily
  construct all the necessary paxos instance ids.

  NB, the rmId in the round number is not necessarily the same rmId as
  in the instance id.
*/

func (am *AcceptorManager) loadFromData(txnId *common.TxnId, data []byte) error {
	seg, _, err := capn.ReadFromMemoryZeroCopy(data)
	if err != nil {
		return err
	}
	state := msgs.ReadRootAcceptorState(seg)
	txn := state.Txn()

	instId := instanceId([instanceIdLen]byte{})
	instIdSlice := instId[:]

	outcome := state.Outcome()
	copy(instIdSlice, txnId[:])

	instances := state.Instances()
	acc := AcceptorFromData(txnId, &txn, &outcome, state.SendToAll(), &instances, am)
	aInst := &acceptorInstances{acceptor: acc}
	am.acceptors[*txnId] = aInst

	for idx, l := 0, instances.Len(); idx < l; idx++ {
		instancesForVar := instances.At(idx)
		vUUId := common.MakeVarUUId(instancesForVar.VarId())
		acceptedInstances := instancesForVar.Instances()
		for idy, m := 0, acceptedInstances.Len(); idy < m; idy++ {
			acceptedInstance := acceptedInstances.At(idy)
			roundNumber := acceptedInstance.RoundNumber()
			ballot := acceptedInstance.Ballot()
			instance := &instance{
				manager:     am,
				vUUId:       vUUId,
				promiseNum:  paxosNumber(roundNumber),
				acceptedNum: paxosNumber(roundNumber),
				accepted:    &ballot,
			}
			binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], acceptedInstance.RmId())
			copy(instIdSlice[common.KeyLen+4:], vUUId[:])
			am.instances[instId] = instance
			aInst.addInstance(&instId)
		}
	}

	acc.Start()
	return nil
}

func (am *AcceptorManager) SetTopology(topology *configuration.Topology) {
	am.Topology = topology
	for _, ai := range am.acceptors {
		ai.acceptor.TopologyChange(topology)
	}
}

func (am *AcceptorManager) OneATxnVotesReceived(sender common.RMId, txnId *common.TxnId, oneATxnVotes *msgs.OneATxnVotes) {
	instanceRMId := common.RMId(oneATxnVotes.RmId())
	server.Log(txnId, "1A received from", sender, "; instance:", instanceRMId)
	instId := instanceId([instanceIdLen]byte{})
	instIdSlice := instId[:]
	copy(instIdSlice, txnId[:])
	binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], uint32(instanceRMId))

	replySeg := capn.NewBuffer(nil)
	msg := msgs.NewRootMessage(replySeg)
	oneBTxnVotes := msgs.NewOneBTxnVotes(replySeg)
	msg.SetOneBTxnVotes(oneBTxnVotes)
	oneBTxnVotes.SetRmId(oneATxnVotes.RmId())
	oneBTxnVotes.SetTxnId(oneATxnVotes.TxnId())

	proposals := oneATxnVotes.Proposals()
	promises := msgs.NewTxnVotePromiseList(replySeg, proposals.Len())
	oneBTxnVotes.SetPromises(promises)
	for idx, l := 0, proposals.Len(); idx < l; idx++ {
		proposal := proposals.At(idx)
		vUUId := common.MakeVarUUId(proposal.VarId())
		copy(instIdSlice[common.KeyLen+4:], vUUId[:])
		promise := promises.At(idx)
		promise.SetVarId(vUUId[:])
		am.ensureInstance(txnId, &instId, vUUId).OneATxnVotesReceived(&proposal, &promise)
	}

	// The proposal senders are repeating, so this use of OSS is fine.
	NewOneShotSender(server.SegToBytes(replySeg), am.ConnectionManager, sender)
}

func (am *AcceptorManager) TwoATxnVotesReceived(sender common.RMId, txnId *common.TxnId, twoATxnVotes *msgs.TwoATxnVotes) {
	instanceRMId := common.RMId(twoATxnVotes.RmId())
	server.Log(txnId, "2A received from", sender, "; instance:", instanceRMId)
	instId := instanceId([instanceIdLen]byte{})
	instIdSlice := instId[:]
	copy(instIdSlice, txnId[:])
	binary.BigEndian.PutUint32(instIdSlice[common.KeyLen:], uint32(instanceRMId))

	txnCap := twoATxnVotes.Txn()
	a := am.ensureAcceptor(txnId, &txnCap)
	requests := twoATxnVotes.AcceptRequests()
	failureInstances := make([]*instance, 0, requests.Len())
	failureRequests := make([]*msgs.TxnVoteAcceptRequest, 0, requests.Len())

	for idx, l := 0, requests.Len(); idx < l; idx++ {
		request := requests.At(idx)
		vUUId := common.MakeVarUUId(request.Ballot().VarId())
		copy(instIdSlice[common.KeyLen+4:], vUUId[:])
		inst := am.ensureInstance(txnId, &instId, vUUId)
		accepted, rejected := inst.TwoATxnVotesReceived(&request)
		if accepted {
			a.BallotAccepted(instanceRMId, inst, vUUId, &txnCap)
		} else if rejected {
			failureInstances = append(failureInstances, inst)
			failureRequests = append(failureRequests, &request)
		}
	}

	if len(failureInstances) != 0 {
		replySeg := capn.NewBuffer(nil)
		msg := msgs.NewRootMessage(replySeg)
		twoBTxnVotes := msgs.NewTwoBTxnVotes(replySeg)
		msg.SetTwoBTxnVotes(twoBTxnVotes)
		twoBTxnVotes.SetFailures()
		failuresCap := twoBTxnVotes.Failures()
		failuresCap.SetTxnId(txnId[:])
		failuresCap.SetRmId(uint32(instanceRMId))
		nacks := msgs.NewTxnVoteTwoBFailureList(replySeg, len(failureInstances))
		failuresCap.SetNacks(nacks)
		for idx, inst := range failureInstances {
			failure := nacks.At(idx)
			failure.SetVarId(inst.vUUId[:])
			failure.SetRoundNumber(failureRequests[idx].RoundNumber())
			failure.SetRoundNumberTooLow(uint32(inst.promiseNum >> 32))
		}
		server.Log(txnId, "Sending 2B failures to", sender, "; instance:", instanceRMId)
		// The proposal senders are repeating, so this use of OSS is fine.
		NewOneShotSender(server.SegToBytes(replySeg), am.ConnectionManager, sender)
	}
}

func (am *AcceptorManager) TxnLocallyCompleteReceived(sender common.RMId, txnId *common.TxnId, tlc *msgs.TxnLocallyComplete) {
	if aInst, found := am.acceptors[*txnId]; found && aInst.acceptor != nil {
		server.Log(txnId, "TLC received from", sender, "(acceptor found)")
		aInst.acceptor.TxnLocallyCompleteReceived(sender)

	} else {
		// We must have deleted the acceptor state from disk,
		// immediately prior to sending TGC, and then died. Now we're
		// back up, the proposers have sent us more TLCs, and we should
		// just reply with TGCs.
		server.Log(txnId, "TLC received from", sender, "(acceptor not found)")
		seg := capn.NewBuffer(nil)
		msg := msgs.NewRootMessage(seg)
		tgc := msgs.NewTxnGloballyComplete(seg)
		msg.SetTxnGloballyComplete(tgc)
		tgc.SetTxnId(txnId[:])
		server.Log(txnId, "Sending single TGC to", sender)
		// Use of OSS here is ok because this is the default action on
		// not finding state.
		NewOneShotSender(server.SegToBytes(seg), am.ConnectionManager, sender)
	}
}

func (am *AcceptorManager) TxnSubmissionCompleteReceived(sender common.RMId, txnId *common.TxnId, tsc *msgs.TxnSubmissionComplete) {
	if aInst, found := am.acceptors[*txnId]; found && aInst.acceptor != nil {
		server.Log(txnId, "TSC received from", sender, "(acceptor found)")
		aInst.acceptor.TxnSubmissionCompleteReceived(sender)
	}
}

func (am *AcceptorManager) AcceptorFinished(txnId *common.TxnId) {
	server.Log(txnId, "Acceptor finished")
	if aInst, found := am.acceptors[*txnId]; found {
		delete(am.acceptors, *txnId)
		for _, instId := range aInst.instances {
			delete(am.instances, *instId)
		}
	}
}

func (am *AcceptorManager) Status(sc *server.StatusConsumer) {
	s := sc.Fork()
	s.Emit(fmt.Sprintf("- Live Instances: %v", len(am.instances)))
	for instId, inst := range am.instances {
		inst.status(instId, s.Fork())
	}
	s.Join()
	s = sc.Fork()
	s.Emit(fmt.Sprintf("- Acceptors: %v", len(am.acceptors)))
	for _, aInst := range am.acceptors {
		if acc := aInst.acceptor; acc != nil {
			acc.Status(s.Fork())
		}
	}
	s.Join()
	sc.Join()
}

type acceptorInstances struct {
	acceptor  *Acceptor
	instances []*instanceId
}

func (ai *acceptorInstances) addInstance(instId *instanceId) {
	if ai.instances == nil {
		ai.instances = []*instanceId{instId.Clone()}

	} else {
		for _, id := range ai.instances {
			if id.Equal(instId) {
				return
			}
		}
		ai.instances = append(ai.instances, instId.Clone())
	}
}

const ( //            txnId  rmId  vUUId
	instanceIdLen = common.KeyLen + 4 + common.KeyLen
)

type instanceId [instanceIdLen]byte

func (instId instanceId) String() string {
	txnId := common.MakeTxnId(instId[0:common.KeyLen])
	rmId := common.RMId(binary.BigEndian.Uint32(instId[common.KeyLen:]))
	vUUId := common.MakeVarUUId(instId[common.KeyLen+4:])
	return fmt.Sprintf("PaxosInstanceId:%v:%v:%v", txnId, rmId, vUUId)
}

func (instId *instanceId) Clone() *instanceId {
	cpy := instanceId([instanceIdLen]byte{})
	copy(cpy[:], instId[:])
	return &cpy
}

func (a *instanceId) Equal(b *instanceId) bool {
	return bytes.Equal(a[:], b[:])
}

type instance struct {
	manager     *AcceptorManager
	vUUId       *common.VarUUId
	promiseNum  paxosNumber
	acceptedNum paxosNumber
	accepted    *msgs.Ballot
}

func (i *instance) OneATxnVotesReceived(proposal *msgs.TxnVoteProposal, promise *msgs.TxnVotePromise) {
	promise.SetRoundNumber(proposal.RoundNumber())
	roundNumber := paxosNumber(proposal.RoundNumber())
	if roundNumber >= i.promiseNum {
		i.promiseNum = roundNumber
		if i.accepted == nil {
			promise.SetFreeChoice()
		} else {
			promise.SetAccepted()
			accepted := promise.Accepted()
			accepted.SetRoundNumber(uint64(i.acceptedNum))
			accepted.SetBallot(*i.accepted)
		}
	} else {
		promise.SetRoundNumberTooLow(uint32(i.promiseNum >> 32))
	}
}

func (i *instance) TwoATxnVotesReceived(request *msgs.TxnVoteAcceptRequest) (accepted bool, rejected bool) {
	roundNumber := paxosNumber(request.RoundNumber())
	if roundNumber == i.acceptedNum && i.accepted != nil {
		// duplicate 2a. Don't issue any response.
		return
	} else if roundNumber >= i.promiseNum || i.promiseNum == 0 {
		i.promiseNum = roundNumber
		i.acceptedNum = roundNumber
		ballot := request.Ballot()
		i.accepted = &ballot
		accepted = true
		return
	} else {
		rejected = true
		return
	}
}

func (i *instance) status(instId instanceId, sc *server.StatusConsumer) {
	sc.Emit(instId.String())
	sc.Emit(fmt.Sprintf("- Promise Number: %v", i.promiseNum))
	sc.Emit(fmt.Sprintf("- Accepted Number: %v", i.acceptedNum))
	sc.Emit(fmt.Sprintf("- Accepted Ballot: %v", i.accepted))
	sc.Join()
}

type paxosNumber uint64

func (rn paxosNumber) String() string {
	top := uint32(rn >> 32)
	rmId := common.RMId(uint32(rn))
	return fmt.Sprintf("%v|%v", top, rmId)
}
