package engine_test

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/herabel/pulse-sync/pkg/engine"
	"github.com/herabel/pulse-sync/pkg/manifest"
	"github.com/zeebo/blake3"
)

func hashBytes(data []byte) [32]byte {
	return blake3.Sum256(data)
}

func TestReconstruct_BrandNewFile(t *testing.T) {
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.bin")

	chunk1 := []byte("first chunk data 12345")
	chunk2 := []byte("second chunk with some extra bytes")

	h1 := hashBytes(chunk1)
	h2 := hashBytes(chunk2)

	newM := &manifest.Manifest{
		Path:    targetPath,
		Size:    int64(len(chunk1) + len(chunk2)),
		ModTime: 1731450000,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: uint32(len(chunk1)), Hash: h1},
			{Offset: uint64(len(chunk1)), Length: uint32(len(chunk2)), Hash: h2},
		},
	}

	deltas := map[[32]byte][]byte{
		h1: chunk1,
		h2: chunk2,
	}

	err := engine.Reconstruct(targetPath, newM, "", nil, deltas)
	if err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}

	gotData, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read reconstructed target: %v", err)
	}

	expected := append(chunk1, chunk2...)
	if !bytes.Equal(gotData, expected) {
		t.Fatalf("Data mismatch: got %q, want %q", gotData, expected)
	}
}

func TestReconstruct_DifferentialMerge(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.bin")
	targetPath := filepath.Join(dir, "target.bin")

	// Старый файл: 4 чанка по 8 КБ (32 КБ)
	chunkA := make([]byte, 8192)
	chunkB := make([]byte, 8192)
	chunkC := make([]byte, 8192)
	chunkD := make([]byte, 8192)

	_, _ = rand.Read(chunkA)
	_, _ = rand.Read(chunkB)
	_, _ = rand.Read(chunkC)
	_, _ = rand.Read(chunkD)

	oldFileContent := bytes.Join([][]byte{chunkA, chunkB, chunkC, chunkD}, nil)
	if err := os.WriteFile(basePath, oldFileContent, 0644); err != nil {
		t.Fatal(err)
	}

	hA := hashBytes(chunkA)
	hB := hashBytes(chunkB)
	hC := hashBytes(chunkC)
	hD := hashBytes(chunkD)

	oldM := &manifest.Manifest{
		Path:    basePath,
		Size:    int64(len(oldFileContent)),
		ModTime: 1000,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: 8192, Hash: hA},
			{Offset: 8192, Length: 8192, Hash: hB},
			{Offset: 16384, Length: 8192, Hash: hC},
			{Offset: 24576, Length: 8192, Hash: hD},
		},
	}

	// Новый файл: ChunkA (старый) + ChunkNEW (новый!) + ChunkC (старый, сдвинулся) + ChunkD (старый)
	chunkNEW := make([]byte, 4096)
	_, _ = rand.Read(chunkNEW)
	hNEW := hashBytes(chunkNEW)

	newM := &manifest.Manifest{
		Path:    targetPath,
		Size:    int64(8192 + 4096 + 8192 + 8192),
		ModTime: 2000,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: 8192, Hash: hA},
			{Offset: 8192, Length: 4096, Hash: hNEW},
			{Offset: 8192 + 4096, Length: 8192, Hash: hC},
			{Offset: 8192 + 4096 + 8192, Length: 8192, Hash: hD},
		},
	}

	// В дельту передаём ТОЛЬКО новый чанк (A, C, D должны подняться с локального диска!)
	deltas := map[[32]byte][]byte{
		hNEW: chunkNEW,
	}

	err := engine.Reconstruct(targetPath, newM, basePath, oldM, deltas)
	if err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}

	gotData, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read reconstructed target: %v", err)
	}

	expected := bytes.Join([][]byte{chunkA, chunkNEW, chunkC, chunkD}, nil)
	if !bytes.Equal(gotData, expected) {
		t.Fatalf("Reconstructed file content mismatch! Expected len %d, got len %d", len(expected), len(gotData))
	}
}

func TestReconstruct_MissingChunkCleansTemp(t *testing.T) {
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.bin")
	tmpPath := targetPath + ".pulsetmp"

	newM := &manifest.Manifest{
		Path: targetPath,
		Size: 8192,
		Chunks: []manifest.ChunkDesc{
			{Offset: 0, Length: 8192, Hash: [32]byte{0xDE, 0xAD}},
		},
	}

	// Передаём пустую дельту и пустую базу — кусок нигде не найден
	err := engine.Reconstruct(targetPath, newM, "", nil, map[[32]byte][]byte{})
	if err == nil {
		t.Fatalf("Expected error on missing chunk, got nil")
	}

	// Проверяем, что временный файл удалён и не остался мусором
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("Temporary file %s was not cleaned up after error", tmpPath)
	}
}
