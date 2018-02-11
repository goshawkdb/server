package client

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"goshawkdb.io/server/types"
	"goshawkdb.io/server/utils/consistenthash"
	"math/rand"
)

type Cache struct {
	m        map[common.VarUUId]*Cached
	resolver *consistenthash.Resolver
	rng      *rand.Rand
}

func NewCache(rng *rand.Rand, roots map[common.VarUUId]*types.PosCapVer) *Cache {
	c := &Cache{
		m:   make(map[common.VarUUId]*Cached),
		rng: rng,
	}
	for vUUId, posCap := range roots {
		c.m[vUUId] = &Cached{
			Cache:     c,
			VerClock:  VerClock{version: posCap.Version},
			caps:      posCap.Capability,
			positions: posCap.Positions,
		}
	}
	return c
}

func (c *Cache) SetResolver(resolver *consistenthash.Resolver) {
	if c.resolver != resolver {
		c.resolver = resolver
		for _, v := range c.m {
			v.hashCodes = nil
		}
	}
}

func (c *Cache) AddCached(vUUId *common.VarUUId, v *Cached) {
	v.Cache = c
	c.m[*vUUId] = v
}

func (c *Cache) Find(vUUId *common.VarUUId, exists bool) (*Cached, error) {
	if v, found := c.m[*vUUId]; found != exists {
		return nil, fmt.Errorf("Illegal use of %v", vUUId)
	} else {
		return v, nil
	}
}

func (c *Cache) copyReferences(dstSeg *capn.Segment, refsAcc *[]*msgs.VarIdPos, srcRefs cmsgs.ClientVarIdPos_List) (*msgs.VarIdPos_List, error) {
	dstRefs := msgs.NewVarIdPosList(dstSeg, srcRefs.Len())
	for idx, l := 0, srcRefs.Len(); idx < l; idx++ {
		srcRef := srcRefs.At(idx)
		dstRef := dstRefs.At(idx)
		target := common.MakeVarUUId(srcRef.VarId())
		dstRef.SetId(target[:])
		dstRef.SetCapability(srcRef.Capability())

		if v, found := c.m[*target]; found {
			if !v.caps.IsSubset(common.NewCapability(srcRef.Capability())) {
				return nil, fmt.Errorf("Attempt made to widen received capability to %v.", target)
			}
			dstRef.SetPositions((capn.UInt8List)(*v.positions))
		} else {
			// If it's not found, it's either to a new object, in which
			// case we have full caps on it, so anything is ok; or the
			// client is making things up, in which case when we finally
			// work back through refsAcc, we will validate caps there
			// too.
			*refsAcc = append(*refsAcc, &dstRef)
		}
	}
	return &dstRefs, nil
}

type Cached struct {
	*Cache
	VerClock
	onClient  bool
	counter   uint64
	caps      common.Capability
	refs      []msgs.VarIdPos
	positions *common.Positions
	hashCodes common.RMIds
}

func (c *Cached) EnsureHashCodes() error {
	if c.hashCodes == nil {
		positionsSlice := (*capn.UInt8List)(c.positions).ToArray()
		if hashCodes, err := c.resolver.ResolveHashCodes(positionsSlice); err == nil {
			c.hashCodes = hashCodes
			return nil
		} else {
			return err
		}
	} else {
		return nil
	}
}

func (c *Cached) UpdatePositions(pos *common.Positions) {
	if pos == nil {
		panic("Nil position")
	}
	if !c.positions.Equal(pos) {
		c.positions = pos
		c.hashCodes = nil
	}
}

// In here, we don't actually add to the cache because we don't know
// if the corresponding txn is going to commit or not.
func (c *Cached) CreatePositions(positionsLength int) {
	positionsCap := capn.NewBuffer(make([]byte, 0, positionsLength*2)).NewUInt8List(positionsLength)
	positionsCap.Set(0, 0)
	n, entropy := uint64(0), uint64(0)
	for idx := 1; idx < positionsLength; idx++ {
		idy := uint64(idx + 1)
		if entropy < idy {
			n, entropy = uint64(c.rng.Int63()), server.TwoToTheSixtyThree
		}
		pos := uint8(n % idy)
		n = n / idy
		entropy = entropy / idy
		positionsCap.Set(idx, pos)
	}
	c.positions = (*common.Positions)(&positionsCap)
}

func (c *Cached) UnionCaps(cap common.Capability) {
	c.caps = c.caps.Union(cap)
}
