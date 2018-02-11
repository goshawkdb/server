package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/utils/status"
)

type Subscriptions struct {
	vm     *VarManager
	subs   map[common.TxnId]bool
	active common.TxnIds
	data   []byte
}

func NewSubscriptions(vm *VarManager) *Subscriptions {
	return &Subscriptions{
		vm:   vm,
		subs: make(map[common.TxnId]bool),
		data: []byte{},
	}
}

func NewSubscriptionsFromData(vm *VarManager, data []byte) (*Subscriptions, error) {
	s := NewSubscriptions(vm)
	s.data = data
	if s.data == nil { // don't trust capnp to return a non nil empty slice!
		s.data = []byte{}
	}
	if len(data) > 0 {
		seg, _, err := capn.ReadFromMemoryZeroCopy(data)
		if err != nil {
			return nil, err
		}
		subsList := msgs.ReadRootSubscriptionListWrapper(seg).Subscriptions()
		for idx, l := 0, subsList.Len(); idx < l; idx++ {
			sub := subsList.At(idx)
			txnId := common.MakeTxnId(sub.TxnId())
			s.subs[*txnId] = sub.Added()
		}
	}
	return s, nil
}

func (s *Subscriptions) IsDirty() bool {
	return s.data == nil
}

func (s *Subscriptions) AsData() []byte {
	if s.data == nil {
		seg := capn.NewBuffer(nil)
		subsWrapperCap := msgs.NewRootSubscriptionListWrapper(seg)
		subsListCap := msgs.NewSubscriptionList(seg, len(s.subs))
		idx := 0
		for txnId, added := range s.subs {
			subsCap := subsListCap.At(idx)
			subsCap.SetTxnId(txnId[:])
			subsCap.SetAdded(added)
			idx++
		}
		subsWrapperCap.SetSubscriptions(subsListCap)
		s.data = common.SegToBytes(seg)
	}
	return s.data
}

func (s *Subscriptions) Subscribers() common.TxnIds {
	if s == nil {
		return nil
	}
	if s.active == nil {
		s.active = make(common.TxnIds, 0, len(s.subs))
		for txnId, added := range s.subs {
			if added {
				txnIdCopy := txnId
				s.active = append(s.active, &txnIdCopy)
			}
		}
	}
	return s.active
}

func (s *Subscriptions) Committed(action *localAction) {
	dirty := false
	if action.IsImmigrant() {
		data := action.immigrantVar.Subscriptions()
		if len(data) > 0 {
			seg, _, err := capn.ReadFromMemoryZeroCopy(data)
			if err != nil {
				return
			}
			subsList := msgs.ReadRootSubscriptionListWrapper(seg).Subscriptions()
			for idx, l := 0, subsList.Len(); idx < l; idx++ {
				sub := subsList.At(idx)
				subId := common.MakeTxnId(sub.TxnId())
				if sub.Added() {
					dirty = true
					s.addSub(subId)
				} else {
					dirty = true
					s.delSub(subId)
				}
			}
		}
	}

	if action.addSub {
		dirty = true
		s.addSub(action.Id)
	}

	if action.delSub != nil {
		dirty = true
		s.delSub(action.delSub)
	}

	if dirty {
		s.active = nil
		s.data = nil
	}
}

func (s *Subscriptions) addSub(subId *common.TxnId) {
	added, found := s.subs[*subId]
	if found && added {
		// duplicate, do nothing
		return
	} else if found {
		// we learnt the remove first! Now delete the whole thing
		delete(s.subs, *subId)
	} else if sconn, found := s.vm.Servers[subId.RMId(s.vm.RMId)]; found && sconn.BootCount <= subId.BootCount() {
		// new and it's valid
		s.subs[*subId] = true
	}
}

func (s *Subscriptions) delSub(subId *common.TxnId) {
	added, found := s.subs[*subId]
	if found && added {
		// cancelling
		delete(s.subs, *subId)
	} else if found {
		// duplicate
		return
	} else if sconn, found := s.vm.Servers[subId.RMId(s.vm.RMId)]; found && sconn.BootCount <= subId.BootCount() {
		s.subs[*subId] = false
	}
}

func (s *Subscriptions) Verify() {
	for txnId, _ := range s.subs {
		sconn, found := s.vm.Servers[txnId.RMId(s.vm.RMId)]
		if !found || sconn.BootCount <= txnId.BootCount() {
			delete(s.subs, txnId)
		}
	}
}

func (s *Subscriptions) Status(sc *status.StatusConsumer) {
	sc.Emit("- Subscriptions:")
	for txnId, added := range s.subs {
		sc.Emit(fmt.Sprintf("  %v (%v)", txnId, added))
	}
	sc.Join()
}
