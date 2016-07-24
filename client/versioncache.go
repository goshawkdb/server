package client

import (
	"fmt"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
	eng "goshawkdb.io/server/txnengine"
)

type versionCache map[common.VarUUId]*cached

type cached struct {
	txnId     *common.TxnId
	clockElem uint64
}

func NewVersionCache() versionCache {
	return make(map[common.VarUUId]*cached)
}

func (vc versionCache) UpdateFromCommit(txnId *common.TxnId, outcome *msgs.Outcome) {
	clock := eng.VectorClockFromData(outcome.Commit())
	actions := outcome.Txn().Actions()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if action.Which() != msgs.ACTION_READ {
			vUUId := common.MakeVarUUId(action.VarId())
			if c, found := vc[*vUUId]; found {
				c.txnId = txnId
				c.clockElem = clock.At(vUUId)
			} else {
				vc[*vUUId] = &cached{
					txnId:     txnId,
					clockElem: clock.At(vUUId),
				}
			}
		}
	}
}

func (vc versionCache) UpdateFromAbort(updates *msgs.Update_List) map[*msgs.Update][]*msgs.Action {
	validUpdates := make(map[*msgs.Update][]*msgs.Action)

	for idx, l := 0, updates.Len(); idx < l; idx++ {
		update := updates.At(idx)
		txnId := common.MakeTxnId(update.TxnId())
		clock := eng.VectorClockFromData(update.Clock())
		actions := update.Actions()
		validActions := make([]*msgs.Action, 0, actions.Len())

		for idy, m := 0, actions.Len(); idy < m; idy++ {
			action := actions.At(idy)
			vUUId := common.MakeVarUUId(action.VarId())
			clockElem := clock.At(vUUId)

			switch action.Which() {
			case msgs.ACTION_MISSING:
				if c, found := vc[*vUUId]; found {
					cmp := c.txnId.Compare(txnId)
					if cmp == common.EQ && clockElem != c.clockElem {
						panic(fmt.Sprintf("Clock version changed on missing for %v@%v (new:%v != old:%v)", vUUId, txnId, clockElem, c.clockElem))
					}
					if clockElem > c.clockElem || (clockElem == c.clockElem && cmp == common.LT) {
						delete(vc, *vUUId)
						validActions = append(validActions, &action)
					}
				}

			case msgs.ACTION_WRITE:
				if c, found := vc[*vUUId]; found {
					cmp := c.txnId.Compare(txnId)
					if cmp == common.EQ && clockElem != c.clockElem {
						panic(fmt.Sprintf("Clock version changed on write for %v@%v (new:%v != old:%v)", vUUId, txnId, clockElem, c.clockElem))
					}
					if clockElem > c.clockElem || (clockElem == c.clockElem && cmp == common.LT) {
						c.txnId = txnId
						c.clockElem = clockElem
						validActions = append(validActions, &action)
					}
				} else {
					vc[*vUUId] = &cached{
						txnId:     txnId,
						clockElem: clockElem,
					}
					validActions = append(validActions, &action)
				}

			default:
				panic(fmt.Sprintf("%v", action.Which()))
			}
		}

		if len(validActions) != 0 {
			validUpdates[&update] = validActions
		}
	}
	return validUpdates
}
