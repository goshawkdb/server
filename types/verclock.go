package types

import (
	"fmt"
	"goshawkdb.io/common"
)

type VerClock struct {
	ClockElem uint64
	Version   *common.TxnId
}

func (a VerClock) Before(b VerClock) bool {
	return a.ClockElem < b.ClockElem ||
		(a.ClockElem == b.ClockElem && a.Version.Compare(b.Version) == common.LT)
}

func (vc VerClock) String() string {
	return fmt.Sprintf("[%d, %v]", vc.ClockElem, vc.Version)
}
