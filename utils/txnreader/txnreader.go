package txnreader

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
)

type TxnReader struct {
	Id       *common.TxnId
	actions  *TxnActions
	Data     []byte
	Txn      msgs.Txn
	deflated *TxnReader
}

func TxnReaderFromData(data []byte) *TxnReader {
	// always force decode
	seg, _, err := capn.ReadFromMemoryZeroCopy(data)
	if err != nil {
		panic(fmt.Sprintf("Error when decoding transaction: %v", err))
	}
	txnCap := msgs.ReadRootTxn(seg)
	txnId := common.MakeTxnId(txnCap.Id())
	return &TxnReader{
		Data: data,
		Txn:  txnCap,
		Id:   txnId,
	}
}

func (tr *TxnReader) Actions(forceDecode bool) *TxnActions {
	if tr == nil {
		return nil
	} else if tr.actions == nil {
		tr.actions = TxnActionsFromData(tr.Txn.Actions(), forceDecode)
	} else if forceDecode {
		tr.actions.decode()
	}
	if tr.deflated == nil && tr.actions.decoded && tr.actions.deflated {
		tr.deflated = tr
	}
	return tr.actions
}

func (a *TxnReader) Combine(b *TxnReader) *TxnReader {
	a.Actions(true)
	b.Actions(true)
	switch {
	case a.deflated != nil && a.deflated != a: // a has both
		return a
	case b.deflated != nil && b.deflated != b: // b has both
		return b
	case a.deflated == a && b.deflated == nil: // a is deflated, b is not
		b.deflated = a
		return b
	case a.deflated == nil && b.deflated == b: // b is deflated, a is not
		a.deflated = b
		return a
	default: // a and b must both be the same
		return a
	}
}

func (tr *TxnReader) IsDeflated() bool {
	return tr.Actions(true).deflated
}

func (tr *TxnReader) AsDeflated() *TxnReader {
	if tr.deflated == nil {
		if tr.IsDeflated() {
			tr.deflated = tr

		} else {
			actions := tr.actions.AsDeflated()
			cap := tr.Txn
			seg := capn.NewBuffer(nil)
			root := msgs.NewRootTxn(seg)
			root.SetId(cap.Id())
			root.SetActions(actions.Data)
			root.SetAllocations(cap.Allocations())
			root.SetTwoFInc(cap.TwoFInc())
			root.SetTopologyVersion(cap.TopologyVersion())

			tr.deflated = &TxnReader{
				Id:      tr.Id,
				actions: actions,
				Data:    common.SegToBytes(seg),
				Txn:     root,
			}
		}
	}
	return tr.deflated
}

type TxnActions struct {
	Data       []byte
	deflated   bool
	decoded    bool
	actionsCap msgs.Action_List
}

func TxnActionsFromData(data []byte, forceDecode bool) *TxnActions {
	actions := &TxnActions{Data: data}
	if forceDecode {
		actions.decode()
	}
	return actions
}

func (actions *TxnActions) decode() {
	if actions.decoded {
		return
	}
	actions.decoded = true
	seg, _, err := capn.ReadFromMemoryZeroCopy(actions.Data)
	if err != nil {
		panic(fmt.Sprintf("Error when decoding actions: %v", err))
	}
	actions.actionsCap = msgs.ReadRootActionListWrapper(seg).Actions()
	actions.deflated = actions.actionsCap.Len() == 0 || actions.actionsCap.At(0).Value().Which() == msgs.ACTIONVALUE_MISSING
}

func (actions *TxnActions) Actions() *msgs.Action_List {
	actions.decode()
	return &actions.actionsCap
}

func (actions *TxnActions) AsDeflated() *TxnActions {
	actions.decode()
	if actions.deflated {
		return actions
	}

	cap := &actions.actionsCap
	seg := capn.NewBuffer(nil)
	root := msgs.NewRootActionListWrapper(seg)
	l := cap.Len()
	list := msgs.NewActionList(seg, l)
	root.SetActions(list)
	for idx := 0; idx < l; idx++ {
		newAction := list.At(idx)
		newAction.SetVarId(cap.At(idx).VarId())
		newAction.Value().SetMissing()
	}

	return &TxnActions{
		Data:       common.SegToBytes(seg),
		deflated:   true,
		decoded:    true,
		actionsCap: list,
	}
}
