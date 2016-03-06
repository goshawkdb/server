package configuration

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/server/capnp"
)

var (
	TopologyVarUUId = common.MakeVarUUId([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	VersionOne      = common.MakeTxnId([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
)

type Topology struct {
	*Configuration
	FInc      uint8
	TwoFInc   uint16
	DBVersion *common.TxnId
	Root
}

type Root struct {
	VarUUId   *common.VarUUId
	Positions *common.Positions
}

func BlankTopology(clusterId string) *Topology {
	return &Topology{
		Configuration: &Configuration{
			ClusterId:                     clusterId,
			Version:                       0,
			Hosts:                         []string{},
			F:                             0,
			MaxRMCount:                    0,
			AsyncFlush:                    false,
			ClientCertificateFingerprints: []string{},
			rms:               []common.RMId{},
			fingerprints:      nil,
			nextConfiguration: nil,
		},
		FInc:      0,
		TwoFInc:   0,
		DBVersion: VersionOne,
	}
}

func NewTopology(txnId *common.TxnId, root *msgs.VarIdPos, config *Configuration) *Topology {
	t := &Topology{
		Configuration: config,
		FInc:          config.F + 1,
		TwoFInc:       (2 * uint16(config.F)) + 1,
		DBVersion:     txnId,
	}
	if root != nil {
		positions := root.Positions()
		t.Root = Root{
			VarUUId:   common.MakeVarUUId(root.Id()),
			Positions: (*common.Positions)(&positions),
		}
	}
	return t
}

func (t *Topology) Clone() *Topology {
	return &Topology{
		Configuration: t.Configuration.Clone(),
		FInc:          t.FInc,
		TwoFInc:       t.TwoFInc,
		DBVersion:     t.DBVersion,
		Root:          t.Root,
	}
}

func (t *Topology) SetConfiguration(config *Configuration) {
	t.Configuration = config
	t.FInc = config.F + 1
	t.TwoFInc = (2 * uint16(config.F)) + 1
}

func TopologyFromCap(txnId *common.TxnId, root *msgs.VarIdPos, data []byte) (*Topology, error) {
	seg, _, err := capn.ReadFromMemoryZeroCopy(data)
	if err != nil {
		return nil, err
	}
	configCap := msgs.ReadRootConfiguration(seg)
	config := ConfigurationFromCap(&configCap)
	return NewTopology(txnId, root, config), nil
}

func (t *Topology) String() string {
	if t == nil {
		return "nil"
	}
	root := "unset"
	if t.Root.VarUUId != nil {
		root = fmt.Sprintf("%v@%v", t.Root.VarUUId, (*capn.UInt8List)(t.Root.Positions).ToArray())
	}
	return fmt.Sprintf("Topology{%v, F+1: %v, 2F+1: %v, DBVersion: %v, Root: %v}",
		t.Configuration, t.FInc, t.TwoFInc, t.DBVersion, root)
}

func (t *Topology) IsBlank() bool {
	return t == nil || t.Version == 0
}
