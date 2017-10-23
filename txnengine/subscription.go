package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	"goshawkdb.io/server/utils/status"
)

type Subscriptions struct {
	vm      *VarManager
	current map[common.ClientId]*common.TxnId
}

func NewSubscriptions(vm *VarManager) *Subscriptions {
	return &Subscriptions{
		vm:      vm,
		current: make(map[common.ClientId]*common.TxnId),
	}
}

func (s *Subscriptions) LoadFromCap(subsCaps capn.DataList) {
	for idx, l := 0, subsCaps.Len(); idx < l; idx++ {
		txnId := common.MakeTxnId(subsCaps.At(idx))
		s.current[txnId.ClientId(s.vm.RMId)] = txnId
	}
}

func (s *Subscriptions) AddToSeg(seg *capn.Segment) capn.DataList {
	subsCap := seg.NewDataList(len(s.current))
	idx := 0
	for _, txnId := range s.current {
		subsCap.Set(idx, txnId[:])
		idx++
	}
	return subsCap
}

func (s *Subscriptions) hasSubscription(txnId *common.TxnId) bool {
	_, found := s.current[txnId.ClientId(s.vm.RMId)]
	return found
}

func (s *Subscriptions) canCancelWith(txnId *common.TxnId) bool {
	t, found := s.current[txnId.ClientId(s.vm.RMId)]
	return found && txnId.Compare(t) == common.EQ
}

func (s *Subscriptions) addSubscription(txnId *common.TxnId) {
	s.current[txnId.ClientId(s.vm.RMId)] = txnId
}

func (s *Subscriptions) cancelSubscription(txnId *common.TxnId) {
	delete(s.current, txnId.ClientId(s.vm.RMId))
}

func (s *Subscriptions) isEmpty() bool {
	return len(s.current) == 0
}

func (s *Subscriptions) Subscribers() common.TxnIds {
	return nil
}

func (s *Subscriptions) Status(sc *status.StatusConsumer) {
	sc.Emit("- Subscriptions:")
	for cId, txnId := range s.current {
		sc.Emit(fmt.Sprintf("  %v (%v)", cId, txnId))
	}
	sc.Join()
}
