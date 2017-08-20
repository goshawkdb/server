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
// desiredLength. Here, you want desiredLength to be TwoFInc
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

// rmIdIdx is the index of the rmId in question within the
// topology.RMs() slice.
func (r *Resolver) RMIdHasVar(rmIdIdx int, positions []uint8) (bool, error) {
	// We do a bunch of cheap checks first of all to avoid calculating
	// the permutation if we can avoid it.
	position := int(positions[rmIdIdx])
	// remainingSpace is how far we are from the right hand edge
	// (assuming no empties) at the point at which we'd be added to the
	// permutation.
	remainingSpace := r.desiredLength - (int(position) + 1)

	if remainingSpace >= (len(r.hashCodes) - (rmIdIdx + 1)) {
		// There's so much space left, and so few rmIds still to go,
		// that no matter what happens, we could not be pushed out.
		return true, nil
	}

	neLen := r.hashCodes.NonEmptyLen()
	if r.desiredLength == neLen {
		// every hashcode will always be included in every permutation
		return true, nil
	}

	emptiesCount := len(r.hashCodes) - neLen
	if position > r.desiredLength+emptiesCount {
		// there is no way we will ever be in the result
		return false, nil
	}

	perm, err := r.ResolveHashCodes(positions)
	if err != nil {
		return false, err
	}
	rmId := r.hashCodes[rmIdIdx]
	for _, r := range perm {
		if r == rmId {
			return true, nil
		}
	}
	return false, nil
}
