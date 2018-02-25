package topologytransmogrifier

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/utils/binarybackoff"
	"time"
)

type subscriber struct {
	*TopologyTransmogrifier
	subscribed bool
	readVsn    *common.TxnId
	rms        common.RMIds
	runTxnMsg  actor.MsgExec
}

func (s *subscriber) Subscribe() (bool, error) {
	// If we are subscribed already, we want to resubscribe only if the
	// RMs change.
	//
	// If we are not subscribed already, we want to resubscribe if
	// either the RMs change or the DBVersion of the active topology
	// changes (which most likely indicates a bad read by the currently
	// attempting subscriber).

	topology := s.activeTopology
	if topology == nil {
		return false, nil
	}
	active, passive := s.topologyRMIds(topology)
	twoFInc := uint16(len(active))
	all := append(common.RMIds{}, active...)
	all = append(all, passive...)
	all.Sort()

	if rmsEq := all.Equal(s.rms); s.subscribed && rmsEq {
		return false, nil
	} else if !s.subscribed && s.runTxnMsg != nil { // not subscribed, but we're trying...
		if rmsEq && s.readVsn.Compare(topology.VerClock.Version) == common.EQ {
			return false, nil
		}
	}

	fmt.Println("Subscriber", s.readVsn, topology.VerClock, s.subscribed, s.rms, all)

	active, passive = s.formActivePassive(active, passive)
	if active == nil {
		return false, nil
	}

	s.inner.Logger.Log("msg", "Subscribing to topology", "topology", topology, "active", fmt.Sprint(active), "passive", fmt.Sprint(passive))

	txn := s.createSubscribeTxn(topology, active, passive, twoFInc)

	s.subscribed = false
	s.readVsn = topology.VerClock.Version
	s.rms = all
	s.runTxnMsg = &topologyTransmogrifierMsgAddSubscription{
		subscriber: s,
		backoff:    binarybackoff.NewBinaryBackoffEngine(s.rng, 2*time.Second, time.Duration(twoFInc+1)*2*time.Second),
		txn:        txn,
		target:     topology,
		active:     active,
		passive:    passive,
	}
	return s.runTxnMsg.Exec()
}

func (s *subscriber) topologyRMIds(topology *configuration.Topology) (active, passive common.RMIds) {
	if next := topology.NextConfiguration; next == nil {
		active = append(active, topology.RMs.NonEmpty()...)
	} else {
		active = append(active, next.RMs.NonEmpty()...)
		passive = append(passive, next.LostRMIds.NonEmpty()...)
	}
	return
}

func (s *subscriber) createSubscribeTxn(topology *configuration.Topology, active, passive common.RMIds, twoFInc uint16) *msgs.Txn {
	seg := capn.NewBuffer(nil)
	txn := msgs.NewRootTxn(seg)

	actionsSeg := capn.NewBuffer(nil)
	actionsWrapper := msgs.NewRootActionListWrapper(actionsSeg)
	actions := msgs.NewActionList(actionsSeg, 1)
	actionsWrapper.SetActions(actions)
	action := actions.At(0)
	action.SetVarId(configuration.TopologyVarUUId[:])
	value := action.Value()
	value.SetExisting()
	value.Existing().SetRead(topology.VerClock.Version[:])
	value.Existing().Modify().SetNot()
	meta := action.Meta()
	meta.SetAddSub(true)
	txn.SetActions(common.SegToBytes(actionsSeg))

	allocs := msgs.NewAllocationList(seg, len(active)+len(passive))
	txn.SetAllocations(allocs)

	offset := 0
	for idx, rmIds := range []common.RMIds{active, passive} {
		for idy, rmId := range rmIds {
			alloc := allocs.At(idy + offset)
			alloc.SetRmId(uint32(rmId))
			if idx == 0 {
				alloc.SetActive(s.activeConnections[rmId].BootCount)
			} else {
				alloc.SetActive(0)
			}
			indices := seg.NewUInt16List(1)
			alloc.SetActionIndices(indices)
			indices.Set(0, 0)
		}
		offset += len(rmIds)
	}

	txn.SetTwoFInc(twoFInc)
	txn.SetTopologyVersion(topology.Version)
	txn.SetIsTopology(true)

	return &txn
}
