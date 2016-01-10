package configuration

import (
	"crypto/sha256"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
)

var (
	TopologyVarUUId = common.MakeVarUUId([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	VersionOne      = common.MakeTxnId([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
)

var BlankTopology = &Topology{
	Configuration: &Configuration{
		F:          0,
		MaxRMCount: 1,
	},
	AllRMs:    []common.RMId{},
	FInc:      1,
	TwoFInc:   1,
	DBVersion: VersionOne,
}

type Topology struct {
	*Configuration
	AllRMs        common.RMIds
	FInc          uint8
	TwoFInc       uint16
	DBVersion     *common.TxnId
	RootVarUUId   *common.VarUUId
	RootPositions *common.Positions
}

func NewTopology(config *Configuration) *Topology {
	return &Topology{
		Configuration: config,
		AllRMs:        []common.RMId{},
		FInc:          config.F + 1,
		TwoFInc:       (2 * uint16(config.F)) + 1,
		DBVersion:     VersionOne,
	}
}

func (t *Topology) Clone() *Topology {
	return &Topology{
		Configuration: t.Configuration,
		AllRMs:        append(make([]common.RMId, 0, len(t.AllRMs)), t.AllRMs...),
		FInc:          t.FInc,
		TwoFInc:       t.TwoFInc,
		DBVersion:     t.DBVersion,
		RootVarUUId:   t.RootVarUUId,
		RootPositions: t.RootPositions,
	}
}

func TopologyDeserialize(txnId *common.TxnId, root *msgs.VarIdPos, data []byte) (*Topology, error) {
	seg, _, err := capn.ReadFromMemoryZeroCopy(data)
	if err != nil {
		return nil, err
	}
	topology := msgs.ReadRootTopology(seg)
	return TopologyFromCap(txnId, root, &topology), nil
}

func TopologyFromCap(txnId *common.TxnId, root *msgs.VarIdPos, topology *msgs.Topology) *Topology {
	t := &Topology{Configuration: &Configuration{}}
	t.ClusterId = topology.ClusterId()
	t.Version = topology.Version()
	t.Hosts = topology.Hosts().ToArray()
	t.F = topology.F()
	t.FInc = t.F + 1
	t.TwoFInc = (2 * uint16(t.F)) + 1
	t.MaxRMCount = topology.MaxRMCount()
	t.AsyncFlush = topology.AsyncFlush()
	rms := topology.Rms()
	t.AllRMs = make([]common.RMId, rms.Len())
	for idx := range t.AllRMs {
		t.AllRMs[idx] = common.RMId(rms.At(idx))
	}
	t.DBVersion = txnId
	if root != nil && len(root.Id()) == common.KeyLen {
		t.RootVarUUId = common.MakeVarUUId(root.Id())
		pos := common.Positions(root.Positions())
		t.RootPositions = &pos
	}
	fingerprints := topology.Fingerprints()
	fingerprintsMap := make(map[[sha256.Size]byte]server.EmptyStruct, fingerprints.Len())
	for idx, l := 0, fingerprints.Len(); idx < l; idx++ {
		ary := [sha256.Size]byte{}
		copy(ary[:], fingerprints.At(idx))
		fingerprintsMap[ary] = server.EmptyStructVal
	}
	t.fingerprints = fingerprintsMap
	return t
}

func (t *Topology) AddToSegAutoRoot(seg *capn.Segment) msgs.Topology {
	topology := msgs.AutoNewTopology(seg)
	topology.SetClusterId(t.ClusterId)
	topology.SetVersion(t.Version)
	hosts := seg.NewTextList(len(t.Hosts))
	topology.SetHosts(hosts)
	for idx, host := range t.Hosts {
		hosts.Set(idx, host)
	}
	topology.SetF(t.F)
	topology.SetMaxRMCount(t.MaxRMCount)
	topology.SetAsyncFlush(t.AsyncFlush)
	rms := seg.NewUInt32List(len(t.AllRMs))
	topology.SetRms(rms)
	for idx, rmId := range t.AllRMs {
		rms.Set(idx, uint32(rmId))
	}
	fingerprintsMap := t.fingerprints
	fingerprints := seg.NewDataList(len(fingerprintsMap))
	topology.SetFingerprints(fingerprints)
	idx := 0
	for fingerprint := range fingerprintsMap {
		fingerprints.Set(idx, fingerprint[:])
		idx++
	}
	return topology
}

func (t *Topology) Serialize() []byte {
	seg := capn.NewBuffer(nil)
	t.AddToSegAutoRoot(seg)
	return server.SegToBytes(seg)
}

func (a *Topology) Equal(b *Topology) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return a == b
	}
	if !(a.Configuration.Equal(b.Configuration) &&
		a.DBVersion.Equal(b.DBVersion) &&
		len(a.AllRMs) == len(b.AllRMs)) {
		return false
	}
	if (a.RootVarUUId == nil || b.RootVarUUId == nil) && a.RootVarUUId != b.RootVarUUId {
		return false
	}
	if a.RootVarUUId != nil && !a.RootVarUUId.Equal(b.RootVarUUId) {
		return false
	}
	for idx, aRM := range a.AllRMs {
		if aRM != b.AllRMs[idx] {
			return false
		}
	}
	return true
}

func (t *Topology) String() string {
	if t == nil {
		return "nil"
	}
	root := "unset"
	if t.RootVarUUId != nil {
		root = fmt.Sprintf("%v@%v", t.RootVarUUId, (*capn.UInt8List)(t.RootPositions).ToArray())
	}
	return fmt.Sprintf("Topology{%v, AllRMs: %v, F+1: %v, 2F+1: %v, DBVersion: %v, Root: %v}",
		t.Configuration, t.AllRMs, t.FInc, t.TwoFInc, t.DBVersion, root)
}
