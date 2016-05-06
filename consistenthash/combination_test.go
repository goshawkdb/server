package consistenthash

import (
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	"math/rand"
	"testing"
)

func TestCombination1Perm(t *testing.T) {
	perm := []common.RMId{}

	perm = append(perm, hashcodes[0])
	inc, exc, err := chooseComb(1, nil, perm) // [0
	if err != nil {
		t.Fatal(err)
	}

	if !isPermutationOf(inc, perm) {
		t.Errorf("Expecting combination to be %v, but it was actually %v", perm, inc)
	}
	if len(exc) != 0 {
		t.Errorf("Expecting nothing to be excluded, but %v was", exc)
	}

	perm = append(perm, hashcodes[1])
	inc, exc, err = chooseComb(1, nil, perm) // [0,1
	if err != nil {
		t.Fatal(err)
	}
	if !isPermutationOf(inc, perm[0:1]) && !isPermutationOf(inc, perm[1:2]) {
		t.Errorf("Expecting combination to be drawn from %v, but it was actually %v", perm, inc)
	}
	if !isPermutationOf(exc, perm[0:1]) && !isPermutationOf(exc, perm[1:2]) {
		t.Errorf("Expecting excluded to be drawn from %v, but it was actually %v", perm, exc)
	}

	inc, exc, err = chooseComb(2, nil, perm) // [0,1
	if err != nil {
		t.Fatal(err)
	}
	if !isPermutationOf(inc, perm) {
		t.Errorf("Expecting combination %v, but it was actually %v", perm, inc)
	}
	if len(exc) != 0 {
		t.Errorf("Expecting nothing to be excluded, but %v was", exc)
	}

	perm = append(perm, hashcodes[2])
	inc, exc, err = chooseComb(1, nil, perm) // [0,1,2
	if err != nil {
		t.Fatal(err)
	}
	if !isPermutationOf(inc, perm[0:1]) && !isPermutationOf(inc, perm[1:2]) && !isPermutationOf(inc, perm[2:3]) {
		t.Errorf("Expecting combination to be drawn from %v, but it was actually %v", perm, inc)
	}
	if len(exc) != 2 {
		t.Errorf("Expecting excluded to have length 2, but found %v", exc)
	}

	inc, exc, err = chooseComb(3, nil, perm) // [0,1,2
	if err != nil {
		t.Fatal(err)
	}
	if !isPermutationOf(inc, perm) {
		t.Errorf("Expecting combination %v, but it was actually %v", perm, inc)
	}
	if len(exc) != 0 {
		t.Errorf("Expecting nothing to be excluded, but %v was", exc)
	}
}

func TestCombination2Perm(t *testing.T) {
	permA, permB := []common.RMId{}, []common.RMId{}

	permA = append(permA, hashcodes[0])
	permB = append(permB, hashcodes[0])
	inc, exc, err := chooseComb(1, nil, permA, permB) // [0; [0
	if err != nil {
		t.Fatal(err)
	}

	if !isPermutationOf(inc, permA) {
		t.Errorf("Expecting combination to be %v, but it was actually %v", permA, inc)
	}
	if len(exc) != 0 {
		t.Errorf("Expecting nothing to be excluded, but %v was", exc)
	}

	permB[0] = hashcodes[1]
	inc, exc, err = chooseComb(1, nil, permA, permB) // [0; [1
	if err != nil {
		t.Fatal(err)
	}
	perm := []common.RMId{permA[0], permB[0]}
	if !isPermutationOf(inc, perm) {
		t.Errorf("Expecting combination to be %v, but it was actually %v", perm, inc)
	}
	if len(exc) != 0 {
		t.Errorf("Expecting nothing to be excluded, but %v was", exc)
	}

	permA = append(permA, hashcodes[2])
	permB = append(permB, hashcodes[2])
	inc, exc, err = chooseComb(1, nil, permA, permB) // [0,2; [1,2
	if err != nil {
		t.Fatal(err)
	}
	if !isPermutationOf(inc, permA[1:2]) {
		t.Errorf("Expecting combination to be %v, but it was actually %v", permA[1:2], inc)
	}
	permExc := []common.RMId{permA[0], permB[0]}
	if !isPermutationOf(exc, permExc) {
		t.Errorf("Expecting excluded to be %v, but it was actually %v", permExc, exc)
	}

	perm = append(perm, hashcodes[2])
	inc, exc, err = chooseComb(2, nil, permA, permB) // [0,2; [1,2
	if err != nil {
		t.Fatal(err)
	}
	if !isPermutationOf(inc, perm) {
		t.Errorf("Expecting combination to be %v, but it was actually %v", perm, inc)
	}
	if len(exc) != 0 {
		t.Errorf("Expecting nothing to be excluded, but %v was", exc)
	}

	permA = append(permA, hashcodes[3])
	permB = append(permB, hashcodes[3])
	perm = []common.RMId{hashcodes[2], hashcodes[3]}
	inc, exc, err = chooseComb(2, nil, permA, permB) // [0,2,3; [1,2,3
	if err != nil {
		t.Fatal(err)
	}
	if !isPermutationOf(inc, perm) {
		t.Errorf("Expecting combination to be %v, but it was actually %v", perm, inc)
	}
	if !isPermutationOf(exc, permExc) {
		t.Errorf("Expecting excluded to be %v, but it was actually %v", permExc, exc)
	}

	permA = append(permA, hashcodes[4])
	permB = append(permB, hashcodes[5])
	permExc = append(permExc, hashcodes[4])
	permExc = append(permExc, hashcodes[5])
	inc, exc, err = chooseComb(2, nil, permA, permB) // [0,2,3,4; [1,2,3,5
	if err != nil {
		t.Fatal(err)
	}
	if !isPermutationOf(inc, perm) {
		t.Errorf("Expecting combination to be %v, but it was actually %v", perm, inc)
	}
	if !isPermutationOf(exc, permExc) {
		t.Errorf("Expecting excluded to be %v, but it was actually %v", permExc, exc)
	}
}

func TestCombinationDisabled(t *testing.T) {
	permA, permB := []common.RMId{hashcodes[0], hashcodes[1]}, []common.RMId{hashcodes[1], hashcodes[0]}

	disabled := make(map[common.RMId]server.EmptyStruct)
	disabled[hashcodes[0]] = server.EmptyStructVal
	_, _, err := chooseComb(2, disabled, permA, permB)
	if err != TooManyDisabledHashCodes {
		t.Errorf("Expecting err to be TooManyDisabledHashCodes, but was actually %v", err)
	}

	inc, exe, err := chooseComb(1, disabled, permA, permB)
	if err != nil {
		t.Fatal(err)
	}
	if !isPermutationOf(inc, []common.RMId{hashcodes[1]}) {
		t.Errorf("Expecting combination to be %v, but was actually %v", []common.RMId{hashcodes[1]}, inc)
	}
	if !isPermutationOf(exe, []common.RMId{hashcodes[0]}) {
		t.Errorf("Expecting exclusion to be %v, but was actually %v", []common.RMId{hashcodes[0]}, exe)
	}

	delete(disabled, hashcodes[0])
	disabled[hashcodes[1]] = server.EmptyStructVal
	inc, exe, err = chooseComb(1, disabled, permA, permB)
	if err != nil {
		t.Fatal(err)
	}
	if !isPermutationOf(inc, []common.RMId{hashcodes[0]}) {
		t.Errorf("Expecting combination to be %v, but was actually %v", []common.RMId{hashcodes[0]}, inc)
	}
	if !isPermutationOf(exe, []common.RMId{hashcodes[1]}) {
		t.Errorf("Expecting exclusion to be %v, but was actually %v", []common.RMId{hashcodes[1]}, exe)
	}

	delete(disabled, hashcodes[1])
	disabled[hashcodes[0]] = server.EmptyStructVal
	permA = append(permA, hashcodes[2])
	permB = append(permB, hashcodes[2])
	inc, exe, err = chooseComb(1, disabled, permA, permB)
	if err != nil {
		t.Fatal(err)
	}
	if len(inc) != 1 || (inc[0] != hashcodes[1] && inc[0] != hashcodes[2]) {
		t.Errorf("Expecting combination to be %v or %v, but was actually %v", []common.RMId{hashcodes[1]}, []common.RMId{hashcodes[2]}, inc)
	}
	if len(exe) != 2 || !(exe[0] == hashcodes[0] || exe[1] == hashcodes[0]) {
		t.Errorf("Expecting exclusion to have length 2 and include %v, but was actually %v", hashcodes[0], exe)
	}

	delete(disabled, hashcodes[0])
	disabled[hashcodes[2]] = server.EmptyStructVal
	inc, exe, err = chooseComb(1, disabled, permA, permB)
	if err != nil {
		t.Fatal(err)
	}
	if len(inc) != 1 || (inc[0] != hashcodes[0] && inc[0] != hashcodes[1]) {
		t.Errorf("Expecting combination to be %v or %v, but was actually %v", []common.RMId{hashcodes[0]}, []common.RMId{hashcodes[1]}, inc)
	}
	if len(exe) != 2 || !(exe[0] == hashcodes[2] || exe[1] == hashcodes[2]) {
		t.Errorf("Expecting exclusion to have length 2 and include %v, but was actually %v", hashcodes[2], exe)
	}

	delete(disabled, hashcodes[2])
	disabled[hashcodes[1]] = server.EmptyStructVal
	inc, exe, err = chooseComb(2, disabled, permA, permB)
	if err != nil {
		t.Fatal(err)
	}
	perm := []common.RMId{hashcodes[0], hashcodes[2]}
	if !isPermutationOf(inc, perm) {
		t.Errorf("Expecting combination to be permutation of %v, but was actually %v", perm, inc)
	}
	if len(exe) != 1 || exe[0] != hashcodes[1] {
		t.Errorf("Expecting exclusion to be %v, but was actually %v", []common.RMId{hashcodes[1]}, exe)
	}

	disabled[hashcodes[2]] = server.EmptyStructVal
	inc, exe, err = chooseComb(1, disabled, permA, permB)
	if err != nil {
		t.Fatal(err)
	}
	if len(inc) != 1 || inc[0] != hashcodes[0] {
		t.Errorf("Expecting combination to be %v, but was actually %v", []common.RMId{hashcodes[0]}, inc)
	}
	if len(exe) != 2 || !isPermutationOf([]common.RMId{hashcodes[2], hashcodes[1]}, exe) {
		t.Errorf("Expecting exclusion to be permutation of %v, but was actually %v", []common.RMId{hashcodes[2], hashcodes[1]}, exe)
	}

	_, _, err = chooseComb(2, disabled, permA, permB)
	if err != TooManyDisabledHashCodes {
		t.Fatal(err)
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

func BenchmarkPicker_4PermsOf4_8HC_DL4(b *testing.B) {
	benchmarkPicker(8, 4, 4, 4, b)
}

func BenchmarkPicker_8PermsOf4_8HC_DL4(b *testing.B) {
	benchmarkPicker(8, 4, 8, 4, b)
}

func BenchmarkPicker_4PermsOf8_16HC_DL8(b *testing.B) {
	benchmarkPicker(16, 8, 4, 8, b)
}

func BenchmarkPicker_8PermsOf8_16HC_DL8(b *testing.B) {
	benchmarkPicker(16, 8, 8, 8, b)
}

func BenchmarkPicker_16PermsOf8_16HC_DL8(b *testing.B) {
	benchmarkPicker(16, 8, 16, 8, b)
}

func BenchmarkPicker_32PermsOf8_32HC_DL8(b *testing.B) {
	benchmarkPicker(32, 8, 32, 8, b)
}

func BenchmarkPicker_32PermsOf16_32HC_DL8(b *testing.B) {
	benchmarkPicker(32, 16, 32, 8, b)
}

func benchmarkPicker(drawLimit, permLen, permCount, desiredLen int, b *testing.B) {
	perms := make([][]common.RMId, permCount)
	for idx := range perms {
		perms[idx] = randPerm(drawLimit, permLen)
	}
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		cp := NewCombinationPicker(desiredLen, nil)
		for _, perm := range perms {
			cp.AddPermutation(perm)
		}
		cp.Choose()
	}
}

func randPerm(lim, length int) []common.RMId {
	result := make([]common.RMId, length)
	for idx, hcIdx := range rand.Perm(lim)[:length] {
		result[idx] = hashcodes[hcIdx]
	}
	return result
}

func chooseComb(desiredLen int, disabled map[common.RMId]server.EmptyStruct, perms ...[]common.RMId) ([]common.RMId, []common.RMId, error) {
	cp := NewCombinationPicker(desiredLen, disabled)
	for _, perm := range perms {
		cp.AddPermutation(perm)
	}
	return cp.Choose()
}
