package cdc

import (
	"math/rand/v2"
)

var GearTable [256]uint64

func init() {
	r := rand.New(rand.NewPCG(67, 1024))
	for i := 0; i < 256; i++ {
		GearTable[i] = r.Uint64()
	}
}

func Update(fingerprint uint64, b byte) uint64 {
	return (fingerprint << 1) + GearTable[b]
}
