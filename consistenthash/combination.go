package consistenthash

import (
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	"sort"
)

/*
Here be dragons. Because this is a fairly expensive process, it's
optimised heavily. Be careful in here.

This solution guarantees a minimum of desiredLength RMIds from each
added permutation are included in the result. It may not be optimal,
but it's more important to be fast that to find the absolute minimum
solution.

The gist of the approach is to assume that every RMId of every perm is
in the result to start with. We then sort the result by frequency of
each RMId and then walk through starting at the rarest. For each RMId,
if we can remove it from the result without taking any of the
permutations that include this RMId below the desiredLength (i.e. the
number of elements of the permutation that appear in the result), then
we do so.

OverProvision represents the number of RMIds of a permutation included
in the result above the desiredLength.

We do a few tricks to make this as fast as possible:
- in maps, where the value would normally be a list, we use a pointer
  to a list so that we can append to the list (and perhaps change the
  list itself) without having to rewrite into the map.
- when we create lists, we give them a generous capacity to start with
  to reduce the number of times they have to grow.

These tricks and many other aspects of the design are based on the
benchmark results with cpu profiling.
*/

var (
	TooManyDisabledHashCodes = &tooManyDisabledHashCodes{}
)

type tooManyDisabledHashCodes struct{}

func (tmdhc *tooManyDisabledHashCodes) Error() string {
	return "Too Many Disabled Hash Codes - unable to meet minimum required length"
}

type CombinationPicker struct {
	desiredLen          int
	rmIdToOverProvision map[common.RMId]*[]*int
	disabledHashCodes   map[common.RMId]bool
	excluded            []common.RMId
	errored             bool
}

func NewCombinationPicker(desiredLen int, disabledHashCodes map[common.RMId]server.EmptyStruct) *CombinationPicker {
	dhc := make(map[common.RMId]bool, len(disabledHashCodes))
	for hc := range disabledHashCodes {
		dhc[hc] = false
	}
	return &CombinationPicker{
		desiredLen:          desiredLen,
		rmIdToOverProvision: make(map[common.RMId]*[]*int),
		disabledHashCodes:   dhc,
		excluded:            make([]common.RMId, 0, desiredLen),
	}
}

func (cp *CombinationPicker) AddPermutation(perm []common.RMId) {
	op := len(perm) - cp.desiredLen
	for _, rmId := range perm {
		if excluded, found := cp.disabledHashCodes[rmId]; found {
			op--
			if !excluded {
				cp.excluded = append(cp.excluded, rmId)
				cp.disabledHashCodes[rmId] = true
			}
			continue
		}
		if listPtr, found := cp.rmIdToOverProvision[rmId]; found {
			*listPtr = append(*listPtr, &op)
		} else {
			opL := make([]*int, 1, cp.desiredLen)
			opL[0] = &op
			cp.rmIdToOverProvision[rmId] = &opL
		}
	}
	if op < 0 {
		cp.errored = true
	}
}

// Consider that the lists here will always be the same length, and
// will be zipped together to pairs when used. I.e. this is a cheap
// way of doing a map in which we never need to do random lookups.
type rmToOPLs struct {
	rmIds           []common.RMId
	overProvisionsL []*[]*int
}

func (cp *CombinationPicker) freqAnalysis() ([]int, map[int]*rmToOPLs) {
	freqs := make([]int, 0, cp.desiredLen)
	freqToRMOPLs := make(map[int]*rmToOPLs)

	for rmId, overProvisions := range cp.rmIdToOverProvision {
		freq := len(*overProvisions)
		if r2opls, found := freqToRMOPLs[freq]; found {
			r2opls.rmIds = append(r2opls.rmIds, rmId)
			r2opls.overProvisionsL = append(r2opls.overProvisionsL, overProvisions)
		} else {
			rmIds := make([]common.RMId, 1, cp.desiredLen)
			rmIds[0] = rmId
			overProvisionsL := make([]*[]*int, 1, cp.desiredLen)
			overProvisionsL[0] = overProvisions
			r2opls := &rmToOPLs{
				rmIds:           rmIds,
				overProvisionsL: overProvisionsL,
			}
			freqToRMOPLs[freq] = r2opls
			freqs = append(freqs, freq)
		}
	}

	sort.Ints(freqs)

	return freqs, freqToRMOPLs
}

func (cp *CombinationPicker) Choose() ([]common.RMId, []common.RMId, error) {
	if cp.errored {
		return nil, nil, TooManyDisabledHashCodes
	}

	freqs, freqToRMOPLs := cp.freqAnalysis()
	included := make([]common.RMId, 0, cp.desiredLen)
	excluded := cp.excluded

	for _, freq := range freqs {
		r2opls := freqToRMOPLs[freq]
		rmIds := r2opls.rmIds
		overProvisionsL := r2opls.overProvisionsL
		for idx := 0; idx < len(rmIds); idx++ {
			rmId := rmIds[idx]
			overProvisions := overProvisionsL[idx]
			zeroEncountered := false
			for _, op := range *overProvisions {
				if *op < 0 {
					return nil, nil, TooManyDisabledHashCodes
				}
				if *op == 0 {
					zeroEncountered = true
					break
				}
			}
			if zeroEncountered {
				included = append(included, rmId)
			} else {
				excluded = append(excluded, rmId)
				for _, op := range *overProvisions {
					(*op)--
				}
			}
		}
	}
	return included, excluded, nil
}
