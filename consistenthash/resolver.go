package consistenthash

import (
	"fmt"
	"goshawkdb.io/common"
	"sort"
)

var (
	InsufficientPositionsError = &insufficientPositionsError{}
)

type insufficientPositionsError struct{}

func (ipe *insufficientPositionsError) Error() string {
	return "Insufficient Positions Error"
}

type Resolver struct {
	hashCodes     common.RMIds
	desiredLength int
	permLen       uint16
	indices       []uint16
}

// hashCodes is the rmIds from topology - i.e. it can contain
// RMIdEmpty, and those RMIdEmpties do not contibute to the
// desiredLength. Here, you want desiredLength to be desiredLength
func NewResolver(hashCodes common.RMIds, desiredLength uint16) *Resolver {
	if hashCodes.NonEmptyLen() < int(desiredLength) {
		panic(fmt.Sprintf("Too few non-empty hashcodes: %v but need at least %v", hashCodes, desiredLength))
	}
	return &Resolver{
		hashCodes:     hashCodes,
		desiredLength: int(desiredLength),
		permLen:       desiredLength + uint16(hashCodes.EmptyLen()),
		indices:       straightIndices(len(hashCodes)),
	}
}

func straightIndices(count int) []uint16 {
	indices := make([]uint16, count)
	for idx := range indices {
		indices[idx] = uint16(idx)
	}
	return indices
}

func (r *Resolver) ResolveHashCodes(positions []uint8) (common.RMIds, error) {
	hcLen := len(r.hashCodes)
	if hcLen > len(positions) {
		return nil, InsufficientPositionsError
	}

	permLen := r.permLen
	empties := make([]int, 0, permLen)
	result := make([]common.RMId, permLen)
	indices := make([]uint16, hcLen)
	copy(indices, r.indices)
	for depth := hcLen - 1; depth >= 0; depth-- {
		position := positions[depth]
		index := indices[position]
		copy(indices[position:], indices[position+1:])
		if index < permLen {
			rmId := r.hashCodes[depth]
			result[index] = rmId
			if rmId == common.RMIdEmpty {
				empties = append(empties, int(index))
			}
		}
	}

	if len(empties) != 0 {
		sort.Ints(empties)
		for idx, idy := range empties {
			if idy-idx >= r.desiredLength {
				break
			}
			copy(result[idy-idx:], result[idy-idx+1:])
		}
	}

	return result[:r.desiredLength], nil
}
