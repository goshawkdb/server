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
	clock := eng.VectorClockFromCap(outcome.Commit())
	actions := outcome.Txn().Actions()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		if action.Which() != msgs.ACTION_READ {
			vUUId := common.MakeVarUUId(action.VarId())
			if c, found := vc[*vUUId]; found {
				c.txnId = txnId
				c.clockElem = clock.Clock[*vUUId]
			} else {
				vc[*vUUId] = &cached{
					txnId:     txnId,
					clockElem: clock.Clock[*vUUId],
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
		clock := eng.VectorClockFromCap(update.Clock())
		actions := update.Actions()
		validActions := make([]*msgs.Action, 0, actions.Len())

		for idy, m := 0, actions.Len(); idy < m; idy++ {
			action := actions.At(idy)
			vUUId := common.MakeVarUUId(action.VarId())
			clockElem := clock.Clock[*vUUId]

			switch action.Which() {
			case msgs.ACTION_MISSING:
				if c, found := vc[*vUUId]; found {
					if clockElem > c.clockElem && c.txnId.Equal(txnId) {
						panic(fmt.Sprintf("Clock version increased on missing for %v@%v (%v > %v)", vUUId, txnId, clockElem, c.clockElem))
					}
					if clockElem > c.clockElem || (clockElem == c.clockElem && c.txnId.LessThan(txnId)) {
						delete(vc, *vUUId)
						validActions = append(validActions, &action)
					}
				}

			case msgs.ACTION_WRITE:
				if c, found := vc[*vUUId]; found {
					if clockElem > c.clockElem && c.txnId.Equal(txnId) {
						panic(fmt.Sprintf("Clock version increased on write for %v@%v (%v > %v)", vUUId, txnId, clockElem, c.clockElem))
					}
					if clockElem > c.clockElem || (clockElem == c.clockElem && c.txnId.LessThan(txnId)) {
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
