package cdc

import (
	"io"

	"github.com/zeebo/blake3"
)

const (
	MinChunkSize = 2048
	AvgChunkSize = 8192
	MaxChunkSize = 65536
	MaskA        = 0x1FFF
	MaskB        = 0x03FF
)

func СutPosition(data []byte) int {
	if len(data) < MinChunkSize {
		return len(data)
	}

	var fp uint64 = 0

	maxLimit := min(len(data), MaxChunkSize)
	for i := MinChunkSize; i < maxLimit; i++ {
		fp = Update(fp, data[i])
		if i < AvgChunkSize {
			if (fp & MaskA) == 0 {
				return i + 1
			}
		} else {
			if (fp & MaskB) == 0 {
				return i + 1
			}
		}
	}

	return maxLimit
}

func Chunking(reader io.Reader) <-chan Chunk {
	ch := make(chan Chunk, 32)
	go func() {
		var currentOffset uint64 = 0
		defer close(ch)

		buf := make([]byte, 1024*1024)
		n, _ := reader.Read(buf)
		lastCut := 0

		for {
			cut := СutPosition(buf[lastCut:n])
			chunkBytes := make([]byte, cut)
			copy(chunkBytes, buf[lastCut:lastCut+cut])
			ch <- Chunk{
				Data:   chunkBytes,
				Length: uint32(cut),
				Offset: currentOffset,
			}
			lastCut += cut
			currentOffset += uint64(cut)

			if lastCut >= n {
				newN, err := reader.Read(buf)
				if err != nil || newN == 0 {
					break
				} else {
					n = newN
					lastCut = 0
				}
			}
		}

	}()
	return ch
}

func HashChunk(in <-chan Chunk) <-chan Chunk {
	out := make(chan Chunk, 32)

	go func() {
		defer close(out)

		Hasher := blake3.New()
		for chunk := range in {
			Hasher.Reset()
			Hasher.Write(chunk.Data)
			Hasher.Sum(chunk.Hash[:0])
			out <- chunk
		}
	}()

	return out
}
