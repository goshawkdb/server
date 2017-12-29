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
	switch {
	case action.IsImmigrant():
		// TOOO merge in from action.immigrantVar.Subscriptions()

	case action.addSubscription:
		added, found := s.subs[*action.Id]
		if found && added {
			// duplicate, do nothing
			return
		} else if found {
			// we learnt the remove first! Now delete the whole thing
			delete(s.subs, *action.Id)
		} else if sconn, found := s.vm.Servers[action.Id.RMId(s.vm.RMId)]; found && sconn.BootCount <= action.Id.BootCount() {
			// new and it's valid
			s.subs[*action.Id] = true
		}

	case action.delSubscription != nil:
		added, found := s.subs[*action.delSubscription]
		if found && added {
			// cancelling
			delete(s.subs, *action.Id)
		} else if found {
			// duplicate
			return
		} else {
			// we've got a cancel first, before the add! We choose not to validate this.
			s.subs[*action.Id] = false
		}
	}
	s.active = nil
	s.data = nil
}

func (s *Subscriptions) Verify() {
	for txnId, added := range s.subs {
		if !added {
			continue
		}
		sconn, found := s.vm.Servers[txnId.RMId(s.vm.RMId)]
		if !found || sconn.BootCount > txnId.BootCount() {
			s.cancelSub(txnId)
		}
	}
}

func (s *Subscriptions) cancelSub(txnId common.TxnId) {
	// TODO
}

func (s *Subscriptions) Status(sc *status.StatusConsumer) {
	sc.Emit("- Subscriptions:")
	for txnId, added := range s.subs {
		sc.Emit(fmt.Sprintf("  %v (%v)", txnId, added))
	}
	sc.Join()
}
