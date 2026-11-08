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
