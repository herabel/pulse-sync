package manifest

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/herabel/pulse-sync/pkg/manifest"
)

func TestHashes_Roundtrip(t *testing.T) {
	cases := []struct {
		name  string
		count int
	}{
		{"empty", 0},
		{"single", 1},
		{"ten", 10},
		{"hundred", 100},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			orig := make([][32]byte, tc.count)
			for i := range orig {
				_, _ = rand.Read(orig[i][:])
			}

			encoded := manifest.EncodeHashes(orig)
			if len(encoded) != tc.count*32 {
				t.Fatalf("length mismatch: got %d bytes, want %d", len(encoded), tc.count*32)
			}

			decoded, err := manifest.DecodeHashes(encoded)
			if err != nil {
				t.Fatalf("DecodeHashes failed: %v", err)
			}

			if len(decoded) != tc.count {
				t.Fatalf("count mismatch: got %d hashes, want %d", len(decoded), tc.count)
			}

			for i := range orig {
				if !bytes.Equal(decoded[i][:], orig[i][:]) {
					t.Fatalf("hash[%d] mismatch: got %x, want %x", i, decoded[i], orig[i])
				}
			}
		})
	}
}

func TestDecodeHashes_InvalidLength(t *testing.T) {
	badLengths := []int{1, 15, 31, 33, 63, 65, 100}

	for _, length := range badLengths {
		buf := make([]byte, length)
		_, err := manifest.DecodeHashes(buf)
		if err == nil {
			t.Fatalf("expected error for buffer length %d, got nil", length)
		}
	}
}

func BenchmarkHashes_Encode1024(b *testing.B) {
	hashes := make([][32]byte, 1024)
	for i := range hashes {
		hashes[i][0] = byte(i)
	}

	b.SetBytes(1024 * 32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = manifest.EncodeHashes(hashes)
	}
}

func BenchmarkHashes_Decode1024(b *testing.B) {
	hashes := make([][32]byte, 1024)
	for i := range hashes {
		hashes[i][0] = byte(i)
	}
	encoded := manifest.EncodeHashes(hashes)

	b.SetBytes(1024 * 32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manifest.DecodeHashes(encoded)
	}
}
