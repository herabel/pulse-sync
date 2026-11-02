package cdc

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/herabel/pulse-sync/pkg/cdc"
	"github.com/zeebo/blake3"
)

func TestCutPosition(t *testing.T) {
	// Generate 100 KB of random payload
	data := make([]byte, 100*1024)
	rand.Read(data)

	cut := cdc.СutPosition(data)

	t.Logf("Found chunk of size: %d bytes", cut)

	// Validate chunk invariants
	if cut < cdc.MinChunkSize {
		t.Fatalf("Chunk size %d is less than MinChunkSize %d", cut, cdc.MinChunkSize)
	}
	if cut > cdc.MaxChunkSize {
		t.Fatalf("Chunk size %d is greater than MaxChunkSize %d", cut, cdc.MaxChunkSize)
	}
}

func TestChunkingStream(t *testing.T) {
	// Test streaming against a 20 MB random payload
	totalSize := 20 * 1024 * 1024
	data := make([]byte, totalSize)
	rand.Read(data)

	reader := bytes.NewReader(data)
	chunksChan := cdc.Chunking(reader)

	var reconstructedBytes int
	chunkCount := 0

	for chunk := range chunksChan {
		chunkCount++
		reconstructedBytes += int(chunk.Length)

		if len(chunk.Data) != int(chunk.Length) {
			t.Fatalf("Chunk data length mismatch: got %d, expected %d", len(chunk.Data), chunk.Length)
		}
	}

	t.Logf("Stream finished! Chunks produced: %d, Total bytes processed: %d", chunkCount, reconstructedBytes)

	// Assert complete byte stream reconstruction
	if reconstructedBytes != totalSize {
		t.Fatalf("Total bytes mismatch: got %d, expected %d", reconstructedBytes, totalSize)
	}
}

func TestHashChunkPipeline(t *testing.T) {
	// 10 MB payload to test pipelined chunking and hashing
	totalSize := 10 * 1024 * 1024
	data := make([]byte, totalSize)
	rand.Read(data)

	reader := bytes.NewReader(data)

	// Chain stages: reader -> Chunking -> HashChunk
	chunks := cdc.Chunking(reader)
	hashedChunks := cdc.HashChunk(chunks)

	chunkCount := 0
	var zeroHash [32]byte

	for chunk := range hashedChunks {
		chunkCount++

		// Ensure hash is not empty
		if chunk.Hash == zeroHash {
			t.Fatalf("Chunk at offset %d has an empty zero hash", chunk.Offset)
		}

		// Verify cryptographic integrity: hash matches expected BLAKE3 digest
		expectedHash := blake3.Sum256(chunk.Data)
		if chunk.Hash != expectedHash {
			t.Fatalf("Hash mismatch at offset %d: expected %x, got %x", chunk.Offset, expectedHash, chunk.Hash)
		}
	}

	t.Logf("Pipeline passed! Verified cryptographic hashes for %d chunks", chunkCount)
}

func BenchmarkFastCDC_Pipeline(b *testing.B) {
	// benchmark chunking + hashing pipeline throughput
	payloadSize := 16 * 1024 * 1024 // 16 MB
	data := make([]byte, payloadSize)
	rand.Read(data)

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		hashedChan := cdc.HashChunk(cdc.Chunking(reader))

		for chunk := range hashedChan {
			_ = chunk.Hash
		}
	}
}

func TestChunkDistribution(t *testing.T) {
	// 50 mb synthetic random payload
	totalSize := 500 * 1024 * 1024
	data := make([]byte, totalSize)
	rand.Read(data)

	reader := bytes.NewReader(data)
	chunksChan := cdc.Chunking(reader)

	var count int
	minSize := int(^uint(0) >> 1)
	maxSize := 0

	// distribution buckets
	b2k_4k := 0
	b4k_8k := 0
	b8k_16k := 0
	b16k_32k := 0
	b32k_64k := 0
	forcedCuts := 0

	for chunk := range chunksChan {
		count++
		sz := int(chunk.Length)

		if sz < minSize {
			minSize = sz
		}
		if sz > maxSize {
			maxSize = sz
		}

		switch {
		case sz == cdc.MaxChunkSize:
			forcedCuts++
		case sz < 4*1024:
			b2k_4k++
		case sz < 8*1024:
			b4k_8k++
		case sz < 16*1024:
			b8k_16k++
		case sz < 32*1024:
			b16k_32k++
		default:
			b32k_64k++
		}
	}

	avgSize := totalSize / count

	t.Logf("=== FASTCDC DISTRIBUTION ===")
	t.Logf("Total chunks : %d", count)
	t.Logf("Average size : %d bytes (%.2f KB)", avgSize, float64(avgSize)/1024.0)
	t.Logf("Min / Max    : %d / %d bytes", minSize, maxSize)
	t.Logf("--- BUCKETS ---")
	t.Logf("  2 KB -  4 KB : %5d (%.3f%%)", b2k_4k, float64(b2k_4k)*100/float64(count))
	t.Logf("  4 KB -  8 KB : %5d (%.3f%%)", b4k_8k, float64(b4k_8k)*100/float64(count))
	t.Logf("  8 KB - 16 KB : %5d (%.3f%%)", b8k_16k, float64(b8k_16k)*100/float64(count))
	t.Logf(" 16 KB - 32 KB : %5d (%.3f%%)", b16k_32k, float64(b16k_32k)*100/float64(count))
	t.Logf(" 32 KB - 64 KB : %5d (%.3f%%)", b32k_64k, float64(b32k_64k)*100/float64(count))
	t.Logf(" Forced (64 KB): %5d (%.3f%%)", forcedCuts, float64(forcedCuts)*100/float64(count))
}
