package types

import (
	"fmt"
	"goshawkdb.io/common"
)

type PosCapVer struct {
	Positions  *common.Positions
	Capability common.Capability
	Version    *common.TxnId
}

func (pcv PosCapVer) String() string {
	return fmt.Sprintf("PosCapVer{Positions: %v, Capability: %v, Version: %v}",
		pcv.Positions, pcv.Capability, pcv.Version)
}
