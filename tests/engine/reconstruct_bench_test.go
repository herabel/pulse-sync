package engine_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/herabel/pulse-sync/pkg/cdc"
	"github.com/herabel/pulse-sync/pkg/manifest"
)

// reconstructReusedBuffer simulates the implementation with a single pre-allocated 64KB buffer reused via slicing.
func reconstructReusedBuffer(
	targetPath string,
	newManifest *manifest.Manifest,
	basePath string,
	oldManifest *manifest.Manifest,
	deltaChunks map[[32]byte][]byte,
) error {
	tmpPath := targetPath + ".pulsetmp"
	err := func() error {
		tmpFile, err := os.Create(tmpPath)
		if err != nil {
			return err
		}
		defer tmpFile.Close()

		var baseFile *os.File
		oldLookup := make(map[[32]byte]manifest.ChunkDesc)

		if basePath != "" && oldManifest != nil {
			baseFile, err = os.Open(basePath)
			if err != nil {
				return err
			}
			defer baseFile.Close()

			for _, chunk := range oldManifest.Chunks {
				oldLookup[chunk.Hash] = chunk
			}
		}

		buf := make([]byte, cdc.MaxChunkSize) // ALLOCATED ONCE OUTSIDE LOOP

		for _, chunk := range newManifest.Chunks {
			if data, ok := deltaChunks[chunk.Hash]; ok {
				if _, err := tmpFile.WriteAt(data, int64(chunk.Offset)); err != nil {
					return err
				}
			} else if oldChunk, ok := oldLookup[chunk.Hash]; ok && baseFile != nil {
				chunkBuf := buf[:oldChunk.Length] // ZERO ALLOC SLICING
				if _, err := baseFile.ReadAt(chunkBuf, int64(oldChunk.Offset)); err != nil {
					return err
				}
				if _, err := tmpFile.WriteAt(chunkBuf, int64(chunk.Offset)); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("missing chunk: %x", chunk.Hash)
			}
		}

		return tmpFile.Sync()
	}()

	if err != nil {
		os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, targetPath)
}

func prepareBenchmarkData(b *testing.B, size int) (string, *manifest.Manifest, *manifest.Manifest, map[[32]byte][]byte) {
	dir := b.TempDir()
	basePath := filepath.Join(dir, "base.bin")

	data := make([]byte, size)
	_, _ = rand.Read(data)
	if err := os.WriteFile(basePath, data, 0644); err != nil {
		b.Fatal(err)
	}

	var chunks []manifest.ChunkDesc
	for chunk := range cdc.HashChunk(cdc.Chunking(bytes.NewReader(data))) {
		chunks = append(chunks, manifest.ChunkDesc{
			Offset: chunk.Offset,
			Length: chunk.Length,
			Hash:   chunk.Hash,
		})
	}

	oldM := &manifest.Manifest{
		Path:    basePath,
		Size:    int64(size),
		ModTime: 1000,
		Chunks:  chunks,
	}

	// 95% of chunks from base, 5% delta
	deltaChunks := make(map[[32]byte][]byte)
	newChunks := make([]manifest.ChunkDesc, len(chunks))
	copy(newChunks, chunks)

	for i := 0; i < len(newChunks); i += 20 { // Every 20th chunk is modified
		newChunkData := make([]byte, newChunks[i].Length)
		_, _ = rand.Read(newChunkData)
		h := hashBytes(newChunkData)
		newChunks[i].Hash = h
		deltaChunks[h] = newChunkData
	}

	newM := &manifest.Manifest{
		Path:    filepath.Join(dir, "target.bin"),
		Size:    int64(size),
		ModTime: 2000,
		Chunks:  newChunks,
	}

	return basePath, oldM, newM, deltaChunks
}

func BenchmarkReconstruct_ReusedBuffer_16MB(b *testing.B) {
	fileSize := 16 * 1024 * 1024 // 16 MB (~2000 chunks)
	basePath, oldM, newM, deltaChunks := prepareBenchmarkData(b, fileSize)
	targetPath := filepath.Join(b.TempDir(), "target.bin")

	b.SetBytes(int64(fileSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := reconstructReusedBuffer(targetPath, newM, basePath, oldM, deltaChunks)
		if err != nil {
			b.Fatal(err)
		}
	}
}
