package manifest

import (
	"crypto/rand"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/herabel/pulse-sync/pkg/manifest"
)

func TestBuild(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.bin")

	data := make([]byte, 50*1024)
	rand.Read(data)
	err := os.WriteFile(filePath, data, 0644)

	if err != nil {
		t.Fatalf("Error while writing file: %v", err)
	}

	manifest, err := manifest.Build(filePath)

	if err != nil {
		t.Fatalf("Error while building manifest: %v", err)
	}

	if manifest.Size == int64(len(data)) {
		t.Logf("File size is: %d", len(data))
	} else {
		t.Fatalf("Builded manifest size is not correct. Expect: %d, Got: %d", len(data), manifest.Size)
	}

	var totalChunksBytes uint64
	for _, chunk := range manifest.Chunks {
		totalChunksBytes += uint64(chunk.Length)
	}
	if totalChunksBytes != uint64(len(data)) {
		t.Fatalf("Total chunk bytes mismatch: got %d, expected %d", totalChunksBytes, len(data))
	}
}

func TestCompare_Identical(t *testing.T) {
	hashNew := [32]byte{1}

	manifest1 := manifest.Manifest{
		Path:    "string",
		Size:    34,
		ModTime: 2432,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: 100, Hash: hashNew},
		},
	}

	manifest2 := manifest.Manifest{
		Path:    "string",
		Size:    34,
		ModTime: 2432,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: 100, Hash: hashNew},
		},
	}

	result := manifest.Compare(&manifest1, &manifest2)

	if len(result) != 0 {
		t.Fatalf("Chunks: got %d", len(result))
	}
}

func TestCompare_NonIdentical(t *testing.T) {
	hashNew := [32]byte{1}
	hashOld := [32]byte{2}

	manifest1 := manifest.Manifest{
		Path:    "string",
		Size:    34,
		ModTime: 2432,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: 100, Hash: hashNew},
		},
	}

	manifest2 := manifest.Manifest{
		Path:    "string",
		Size:    34344,
		ModTime: 25,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: 100, Hash: hashOld},
		},
	}

	result := manifest.Compare(&manifest1, &manifest2)

	if len(result) == 0 {
		t.Fatalf("Chunks: got %d", len(result))
	}
}

func TestCompare_Delta(t *testing.T) {
	hashA := [32]byte{1}
	hashB := [32]byte{2}
	hashC := [32]byte{3}
	hashD := [32]byte{4}

	manifest1 := manifest.Manifest{
		Path:    "string",
		Size:    34,
		ModTime: 2432,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: 100, Hash: hashA},
			{Offset: 0, Length: 100, Hash: hashB},
			{Offset: 0, Length: 100, Hash: hashC},
		},
	}

	manifest2 := manifest.Manifest{
		Path:    "string",
		Size:    34344,
		ModTime: 25,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: 100, Hash: hashA},
			{Offset: 0, Length: 100, Hash: hashD},
			{Offset: 0, Length: 100, Hash: hashC},
		},
	}

	result := manifest.Compare(&manifest1, &manifest2)

	if len(result) != 1 {
		t.Fatalf("Expected exactly 1 missing chunk, got %d", len(result))
	}
	if result[0].Hash != hashD {
		t.Fatalf("Expected missing chunk to be hashD, got %v", result[0].Hash)
	}
}

func BenchmarkCompare(b *testing.B) {
	numChunks := 1000
	chunksA := make([]manifest.ChunkDesc, numChunks)
	for i := 0; i < numChunks; i++ {
		var h [32]byte
		binary.BigEndian.PutUint64(h[0:8], uint64(i))

		chunksA[i] = manifest.ChunkDesc{
			Offset: uint64(i * 8192),
			Length: 8192,
			Hash:   h,
		}
	}

	chunksB := make([]manifest.ChunkDesc, numChunks)
	copy(chunksB, chunksA)

	chunksB[numChunks-1].Hash = [32]byte{0xFF}

	m1 := &manifest.Manifest{Size: 1000 * 8192, ModTime: 1, Chunks: chunksA}
	m2 := &manifest.Manifest{Size: 1000 * 8192, ModTime: 2, Chunks: chunksB}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = manifest.Compare(m1, m2)
	}
}

func BenchmarkCompare_Identical(b *testing.B) {
	numChunks := 1000
	chunksA := make([]manifest.ChunkDesc, numChunks)
	for i := 0; i < numChunks; i++ {
		var h [32]byte
		binary.BigEndian.PutUint64(h[0:8], uint64(i))

		chunksA[i] = manifest.ChunkDesc{
			Offset: uint64(i * 8192),
			Length: 8192,
			Hash:   h,
		}
	}

	m1 := &manifest.Manifest{Size: 1000 * 8192, ModTime: 1, Chunks: chunksA}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = manifest.Compare(m1, m1)
	}
}

func TestManifestCodec_Roundtrip(t *testing.T) {
	orig := &manifest.Manifest{
		Path:    "project/data/blob.bin",
		Size:    1048576,
		ModTime: 1731448800,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: 8192, Hash: [32]byte{0xAA, 0xBB, 0xCC}},
			{Offset: 8192, Length: 4096, Hash: [32]byte{0x11, 0x22, 0x33}},
		},
	}

	encoded := manifest.EncodeManifest(orig)
	decoded, err := manifest.DecodeManifest(encoded)
	if err != nil {
		t.Fatalf("DecodeManifest failed: %v", err)
	}

	if decoded.Path != orig.Path {
		t.Fatalf("Path mismatch: got %q, want %q", decoded.Path, orig.Path)
	}
	if decoded.Size != orig.Size || decoded.ModTime != orig.ModTime {
		t.Fatalf("Metadata mismatch: size %d vs %d, modTime %d vs %d",
			decoded.Size, orig.Size, decoded.ModTime, orig.ModTime)
	}
	if len(decoded.Chunks) != len(orig.Chunks) {
		t.Fatalf("Chunks count mismatch: got %d, want %d", len(decoded.Chunks), len(orig.Chunks))
	}
	for i := range orig.Chunks {
		if decoded.Chunks[i] != orig.Chunks[i] {
			t.Fatalf("Chunk[%d] mismatch: got %+v, want %+v", i, decoded.Chunks[i], orig.Chunks[i])
		}
	}
}

func TestDecodeManifest_Corrupted(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "too short (< 22 bytes)",
			data: []byte{0x00, 0x05, 0x01},
		},
		{
			name: "zero path len",
			data: func() []byte {
				buf := make([]byte, 30)
				binary.BigEndian.PutUint16(buf[0:2], 0) // pathLen = 0
				return buf
			}(),
		},
		{
			name: "path length exceeds buffer",
			data: func() []byte {
				buf := make([]byte, 25)
				binary.BigEndian.PutUint16(buf[0:2], 100) // pathLen = 100, but buffer has only 25 bytes
				return buf
			}(),
		},
		{
			name: "chunkCount OOM bomb",
			data: func() []byte {
				m := &manifest.Manifest{Path: "test.bin", Size: 100, ModTime: 100}
				buf := manifest.EncodeManifest(m)
				// spoofing chunkCount to 10 millions in valid manifest
				offset := 2 + len("test.bin") + 8 + 8
				binary.BigEndian.PutUint32(buf[offset:offset+4], 10_000_000)
				return buf
			}(),
		},
		{
			name: "truncated chunk list",
			data: func() []byte {
				m := &manifest.Manifest{
					Path: "test.bin",
					Chunks: []manifest.ChunkDesc{
						{Offset: 0, Length: 100},
						{Offset: 100, Length: 200},
					},
				}
				buf := manifest.EncodeManifest(m)
				return buf[:len(buf)-10] // truncate last 10 bytes of chunks
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := manifest.DecodeManifest(tc.data)
			if err == nil {
				t.Fatalf("expected error on %q, but got nil (panic or undetected corruption)", tc.name)
			}
		})
	}
}
