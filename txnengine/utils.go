package txnengine

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
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
	if tr.actions == nil {
		tr.actions = TxnActionsFromData(tr.Txn.Actions(), forceDecode)
	} else if forceDecode {
		tr.actions.decode()
	}
	return tr.actions
}

func (tr *TxnReader) HasDeflated() bool {
	return tr.deflated != nil || tr.Actions(true).deflated
}

func (tr *TxnReader) IsDeflated() bool {
	return tr.Actions(true).deflated
}

func (tr *TxnReader) AsDeflated() *TxnReader {
	if tr.deflated == nil {
		if tr.IsDeflated() {
			tr.deflated = tr
		}

		actions := tr.actions.AsDeflated()
		cap := tr.Txn
		seg := capn.NewBuffer(nil)
		root := msgs.NewRootTxn(seg)
		root.SetId(cap.Id())
		root.SetSubmitter(cap.Submitter())
		root.SetSubmitterBootCount(cap.SubmitterBootCount())
		root.SetRetry(cap.Retry())
		root.SetActions(actions.Data)
		root.SetAllocations(cap.Allocations())
		root.SetFInc(cap.FInc())
		root.SetTopologyVersion(cap.TopologyVersion())

		tr.deflated = &TxnReader{
			Id:      tr.Id,
			actions: actions,
			Data:    server.SegToBytes(seg),
			Txn:     root,
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
	actions.deflated = actions.actionsCap.Len() == 0 || actions.actionsCap.At(0).Which() == msgs.ACTION_MISSING
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
		newAction.SetMissing()
	}

	return &TxnActions{
		Data:       server.SegToBytes(seg),
		deflated:   true,
		decoded:    true,
		actionsCap: list,
	}
}
