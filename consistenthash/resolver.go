package consistenthash

import (
	sl "github.com/msackman/skiplist"
	"goshawkdb.io/common"
	"math/rand"
)

var (
	InsufficientPositionsError = &insufficientPositionsError{}
)

type insufficientPositionsError struct{}

func (ipe *insufficientPositionsError) Error() string {
	return "Insufficient Positions Error"
}

type Resolver struct {
	hashCodes       common.RMIds
	removed         *sl.SkipList
	straightIndices []uint8
}

func NewResolver(rng *rand.Rand, hashCodes []common.RMId) *Resolver {
	indices := make([]uint8, len(hashCodes))
	for idx := range indices {
		indices[idx] = uint8(idx)
	}
	res := &Resolver{
		hashCodes:       make([]common.RMId, len(hashCodes)),
		removed:         sl.New(rng),
		straightIndices: indices,
	}
	copy(res.hashCodes, hashCodes)
	for idx, hc := range hashCodes {
		if hc == common.RMIdEmpty {
			res.removed.Insert(intKey(idx), true)
		}
	}
	return res
}

type intKey int

func (a intKey) Compare(bC sl.Comparable) sl.Cmp {
	b := bC.(intKey)
	switch {
	case a < b:
		return sl.LT
	case a > b:
		return sl.GT
	default:
		return sl.EQ
	}
}

func (res *Resolver) AddHashCode(hashCode common.RMId) {
	// 1. make this idempotent.
	for _, hc := range res.hashCodes {
		if hc == hashCode {
			return
		}
	}
	// 2. ok, really add it.
	if res.removed.Len() == 0 {
		res.hashCodes = append(res.hashCodes, hashCode)
	} else {
		idxElem := res.removed.First()
		idx := int(idxElem.Key.(intKey))
		idxElem.Remove()
		res.hashCodes[idx] = hashCode
	}
	if len(res.hashCodes) == 1+len(res.straightIndices) {
		res.straightIndices = append(res.straightIndices, uint8(len(res.straightIndices)))
	}
}

func (res *Resolver) RemoveHashCode(hashCode common.RMId) {
	if idx := len(res.hashCodes) - 1; idx >= 0 && hashCode == res.hashCodes[idx] {
		for idx--; idx >= 0 && res.hashCodes[idx] == common.RMIdEmpty; idx-- {
			res.removed.Last().Remove()
		}
		res.hashCodes = res.hashCodes[:idx+1]
		res.straightIndices = res.straightIndices[:idx+1]
		return
	}
	for idx, hc := range res.hashCodes {
		if hc == hashCode {
			res.removed.Insert(intKey(idx), true)
			res.hashCodes[idx] = common.RMIdEmpty
			return
		}
	}
}

// Positions must be at least as long as we have nodes. I.e. at least as long as res.hashCodes.
// PermutationLength must be equal to the current number of nodes in topology.
// You can then take the 2F+1 prefix of the result.
func (res *Resolver) ResolveHashCodes(positions []uint8, permutationLength int) ([]common.RMId, error) {
	extras := 0
	for removedElem := res.removed.First(); removedElem != nil && int(removedElem.Key.(intKey)) < permutationLength; removedElem = removedElem.Next() {
		extras++
	}
	permutationLength += extras
	if len(positions) < permutationLength {
		return nil, InsufficientPositionsError
	}

	result := make([]common.RMId, permutationLength)
	indices := make([]uint8, permutationLength)
	copy(indices, res.straightIndices)

	for depth := permutationLength - 1; depth >= 0; depth-- {
		position := positions[depth]
		idx := indices[position]
		result[idx] = res.hashCodes[depth]
		indices = append(indices[:position], indices[position+1:]...)
	}

	for idx := len(result) - 1; extras > 0 && idx >= 0; idx-- {
		if result[idx] == common.RMIdEmpty {
			extras--
			result = append(result[:idx], result[idx+1:]...)
			idx--
		}
	}

	return result, nil
}
