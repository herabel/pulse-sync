package manifest

import (
	"crypto/rand"
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
		t.Fatalf("Error while building manifest: %d", err)
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
