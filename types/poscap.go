package types

import (
	"goshawkdb.io/common"
)

type PosCapVer struct {
	Positions  *common.Positions
	Capability common.Capability
	Version    *common.TxnId
}
