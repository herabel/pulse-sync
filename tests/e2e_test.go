package tests

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/herabel/pulse-sync/pkg/cdc"
	"github.com/herabel/pulse-sync/pkg/manifest"
	"github.com/herabel/pulse-sync/pkg/protocol"
)

func BenchmarkEndToEnd_Pipeline(b *testing.B) {
	payloadSize := 16 * 1024 * 1024 // 16 MB
	dataOld := make([]byte, payloadSize)
	rand.Read(dataOld)

	var oldChunks []manifest.ChunkDesc
	for chunk := range cdc.HashChunk(cdc.Chunking(bytes.NewReader(dataOld))) {
		oldChunks = append(oldChunks, manifest.ChunkDesc{
			Offset: chunk.Offset,
			Length: chunk.Length,
			Hash:   chunk.Hash,
		})
	}
	mOld := &manifest.Manifest{
		Path:    "project/main.go",
		Size:    int64(payloadSize),
		ModTime: 1000,
		Chunks:  oldChunks,
	}

	dataNew := make([]byte, payloadSize)
	copy(dataNew, dataOld)
	dataNew[payloadSize/2] ^= 0xFF
	dataNew[payloadSize/2+1] ^= 0xAA

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(dataNew)
		estimated := payloadSize / cdc.AvgChunkSize
		newChunks := make([]manifest.ChunkDesc, 0, estimated)

		for chunk := range cdc.HashChunk(cdc.Chunking(reader)) {
			newChunks = append(newChunks, manifest.ChunkDesc{
				Offset: chunk.Offset,
				Length: chunk.Length,
				Hash:   chunk.Hash,
			})
		}

		mNew := &manifest.Manifest{
			Path:    "project/main.go",
			Size:    int64(payloadSize),
			ModTime: int64(2000 + i),
			Chunks:  newChunks,
		}

		missing := manifest.Compare(mOld, mNew)

		h := protocol.Header{
			Magic:      protocol.Magic,
			Version:    1,
			MsgType:    protocol.MsgNeededChunks,
			SeqID:      uint32(i),
			PayloadLen: uint32(len(missing) * 32),
		}
		_ = protocol.EncodeHeader(h)
	}
}

func BenchmarkEndToEndParallel_Pipeline(b *testing.B) {
	payloadSize := 16 * 1024 * 1024
	dataOld := make([]byte, payloadSize)
	rand.Read(dataOld)

	var oldChunks []manifest.ChunkDesc
	for chunk := range cdc.HashChunk(cdc.Chunking(bytes.NewReader(dataOld))) {
		oldChunks = append(oldChunks, manifest.ChunkDesc{
			Offset: chunk.Offset,
			Length: chunk.Length,
			Hash:   chunk.Hash,
		})
	}
	mOld := &manifest.Manifest{
		Path:    "project/main.go",
		Size:    int64(payloadSize),
		ModTime: 1000,
		Chunks:  oldChunks,
	}

	dataNew := make([]byte, payloadSize)
	copy(dataNew, dataOld)
	dataNew[payloadSize/2] ^= 0xFF
	dataNew[payloadSize/2+1] ^= 0xAA

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		iter := 0
		for pb.Next() {
			iter++
			reader := bytes.NewReader(dataNew)
			estimated := payloadSize / cdc.AvgChunkSize
			newChunks := make([]manifest.ChunkDesc, 0, estimated)
			for chunk := range cdc.HashChunk(cdc.Chunking(reader)) {
				newChunks = append(newChunks, manifest.ChunkDesc{
					Offset: chunk.Offset,
					Length: chunk.Length,
					Hash:   chunk.Hash,
				})
			}
			mNew := &manifest.Manifest{
				Path:    "project/main.go",
				Size:    int64(payloadSize),
				ModTime: int64(2000 + iter),
				Chunks:  newChunks,
			}
			missing := manifest.Compare(mOld, mNew)
			h := protocol.Header{
				Magic:      protocol.Magic,
				Version:    1,
				MsgType:    protocol.MsgNeededChunks,
				SeqID:      uint32(iter),
				PayloadLen: uint32(len(missing) * 32),
			}
			_ = protocol.EncodeHeader(h)
		}
	})
}
