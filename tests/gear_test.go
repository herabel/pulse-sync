package cdc

import (
	"testing"
	"github.com/herabel/pulse-sync/pkg/cdc"
)

func TestGearTableInitialization(t *testing.T) {
	// Checking if all 256 elements filled and not zero
	seen := make(map[uint64]bool)

	for i, val := range cdc.GearTable {
		if val == 0 {
			t.Fatalf("element GearTable[%d] is 0", i)
		}
		if seen[val] {
			t.Fatalf("table collision: number %x indexed again at %d", val, i)
		}
		seen[val] = true
	}

	if len(seen) != 256 {
		t.Fatalf("await 256 unique elements, got %d", len(seen))
	}
}

func TestUpdateDeterministic(t *testing.T) {
	// Update tests
	fp1 := cdc.Update(0, 'A')
	fp2 := cdc.Update(0, 'A')

	if fp1 != fp2 {
		t.Fatalf("Update isnt determined: %d != %d", fp1, fp2)
	}

	fpDifferent := cdc.Update(0, 'B')
	if fp1 == fpDifferent {
		t.Fatalf("Update gave the same fingerprint for different bytes")
	}
}
