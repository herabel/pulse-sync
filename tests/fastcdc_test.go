package cdc

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/herabel/pulse-sync/pkg/cdc"
	"lukechampine.com/blake3"
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
