package consistenthash

import (
	"goshawkdb.io/common"
	"math/rand"
	"os"
	"testing"
)

var (
	randomPositions [][]uint8
	hashcodes       []common.RMId
	rng             = rand.New(rand.NewSource(0))
)

const (
	positionsCount = 10000
)

func checkHashCodeLen(t *testing.T, res *Resolver, expectedLen int) {
	if found := len(res.hashCodes) - int(res.removed.Len()); found != expectedLen {
		t.Errorf("Expected %v hashcodes, found %v\n", expectedLen, found)
	}
}

func TestPerms(t *testing.T) {
	res := NewResolver(rng, []common.RMId{})
	count := 0
	checkHashCodeLen(t, res, count)
	for _, hc := range hashcodes {
		// check removal is idempotent
		res.RemoveHashCode(hc)
		checkHashCodeLen(t, res, count)
		res.AddHashCode(hc)
		count++
		checkHashCodeLen(t, res, count)
		// check addition is idempotent
		res.AddHashCode(hc)
		checkHashCodeLen(t, res, count)
	}

	permLen := 6
	failuresLim := permLen + 1
	// we have at most one failure at a time, so we need at most
	// permLen+1 positions. However, give an extra one just to try and
	// confuse it further.
	positions := make([]uint8, permLen+2)
	workingHashCodes := make([]common.RMId, 0, len(hashcodes))

	count--
	for failIdx := 0; failIdx < failuresLim; failIdx++ {
		failed := hashcodes[failIdx]
		res.RemoveHashCode(failed)
		workingHashCodes = append(workingHashCodes[:0], hashcodes[:failIdx]...)
		workingHashCodes = append(workingHashCodes, hashcodes[failIdx+1:]...)

		consumer := func(positions []uint8) {
			for l := 0; l <= permLen; l++ {
				perm, err := res.ResolveHashCodes(positions, l)
				if err != nil {
					t.Fatal(err)
				}
				legalHashCodes := workingHashCodes[:l]
				if !isPermutationOf(perm, legalHashCodes) {
					t.Fatal("Not a valid permutation", perm, legalHashCodes, positions, l)
				}
			}
		}
		forEachPositions(consumer, positions, 0)

		res.AddHashCode(failed)
	}
}

// NB, I could not be bothered to make this non-recursive. Beware
// stack explosions with big permutations
func forEachPositions(f func([]uint8), positions []uint8, idx int) {
	if idx < len(positions) {
		for pos := uint8(0); pos <= uint8(idx); pos++ {
			positions[idx] = pos
			forEachPositions(f, positions, idx+1)
		}
	} else {
		f(positions)
	}
}

func isPermutationOf(perm, hashcodes []common.RMId) bool {
	if len(perm) != len(hashcodes) {
		return false
	}
	freq := make(map[common.RMId]int)
	for _, hc := range perm {
		if count := freq[hc]; count == 0 {
			freq[hc] = 1
		} else {
			return false
		}
	}
	for _, hc := range hashcodes {
		if _, found := freq[hc]; found {
			delete(freq, hc)
		} else {
			return false
		}
	}
	return len(freq) == 0
}

func BenchmarkHash4_4(b *testing.B) {
	benchmarkHash(NewResolver(rng, hashcodes[:4]), 4, b)
}

func BenchmarkHash8_4(b *testing.B) {
	benchmarkHash(NewResolver(rng, hashcodes[:8]), 4, b)
}
func BenchmarkHash8_8(b *testing.B) {
	benchmarkHash(NewResolver(rng, hashcodes[:8]), 8, b)
}

func BenchmarkHash16_4(b *testing.B) {
	benchmarkHash(NewResolver(rng, hashcodes[:16]), 4, b)
}
func BenchmarkHash16_8(b *testing.B) {
	benchmarkHash(NewResolver(rng, hashcodes[:16]), 8, b)
}
func BenchmarkHash16_16(b *testing.B) {
	benchmarkHash(NewResolver(rng, hashcodes[:16]), 16, b)
}

func BenchmarkHash32_4(b *testing.B) {
	benchmarkHash(NewResolver(rng, hashcodes[:32]), 4, b)
}
func BenchmarkHash32_8(b *testing.B) {
	benchmarkHash(NewResolver(rng, hashcodes[:32]), 8, b)
}
func BenchmarkHash32_16(b *testing.B) {
	benchmarkHash(NewResolver(rng, hashcodes[:32]), 16, b)
}
func BenchmarkHash32_32(b *testing.B) {
	benchmarkHash(NewResolver(rng, hashcodes[:32]), 32, b)
}

func benchmarkHash(res *Resolver, permLen int, b *testing.B) {
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		positions := randomPositions[idx%len(randomPositions)]
		res.ResolveHashCodes(positions, permLen)
	}
}

func TestMain(m *testing.M) {
	hashcodes = []common.RMId{
		common.RMId(1), common.RMId(2), common.RMId(3), common.RMId(4), common.RMId(5), common.RMId(6), common.RMId(7), common.RMId(8),
		common.RMId(9), common.RMId(10), common.RMId(11), common.RMId(12), common.RMId(13), common.RMId(14), common.RMId(15), common.RMId(16),
		common.RMId(17), common.RMId(18), common.RMId(19), common.RMId(20), common.RMId(21), common.RMId(22), common.RMId(23), common.RMId(24),
		common.RMId(25), common.RMId(26), common.RMId(27), common.RMId(28), common.RMId(29), common.RMId(30), common.RMId(31), common.RMId(32),
	}

	randomPositions = make([][]uint8, positionsCount)
	for idx := range randomPositions {
		positions := make([]uint8, len(hashcodes))
		randomPositions[idx] = positions
		for idy := range positions {
			if idy == 0 {
				positions[idy] = uint8(idy)
			} else {
				positions[idy] = uint8(rand.Intn(idy))
			}
		}
	}
	os.Exit(m.Run())
}
