package localconnection

import (
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/utils/status"
	"goshawkdb.io/server/utils/txnreader"
)

type TranslationCallback func(*cmsgs.ClientAction, *msgs.Action, []common.RMId, map[common.RMId]bool) error

type LocalConnection interface {
	RunClientTransaction(*cmsgs.ClientTxn, bool, map[common.VarUUId]*common.Positions, TranslationCallback) (*txnreader.TxnReader, *msgs.Outcome, error)
	Status(*status.StatusConsumer)
}
