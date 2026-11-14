package manifest

import (
	"os"

	"github.com/herabel/pulse-sync/pkg/cdc"
)

type Manifest struct {
	Path    string
	Size    int64
	ModTime int64
	Chunks  []ChunkDesc
}

type ChunkDesc struct {
	Offset uint64
	Length uint32
	Hash   [32]byte
}

func Build(Path string) (*Manifest, error) {
	file, err := os.Open(Path)

	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	defer file.Close()

	estimated := info.Size() / cdc.AvgChunkSize

	chunks := make([]ChunkDesc, 0, estimated)

	pipeline := cdc.HashChunk(cdc.Chunking(file))

	for chunk := range pipeline {
		desc := ChunkDesc{
			Offset: chunk.Offset,
			Length: chunk.Length,
			Hash:   chunk.Hash,
		}
		chunks = append(chunks, desc)
	}

	return &Manifest{
		Path:    Path,
		Size:    info.Size(),
		ModTime: info.ModTime().Unix(),
		Chunks:  chunks,
	}, nil
}

func Compare(oldManifest *Manifest, newManifest *Manifest) []ChunkDesc {
	if len(oldManifest.Chunks) == len(newManifest.Chunks) {
		identical := true
		for i := range oldManifest.Chunks {
			if oldManifest.Chunks[i].Hash != newManifest.Chunks[i].Hash {
				identical = false
				break
			}
		}
		if identical {
			return nil
		}
	}

	var missing []ChunkDesc

	knownChunks := make(map[[32]byte]struct{}, len(oldManifest.Chunks))

	for _, chunk := range oldManifest.Chunks {
		knownChunks[chunk.Hash] = struct{}{}
	}

	for _, chunk := range newManifest.Chunks {
		if _, exists := knownChunks[chunk.Hash]; !exists {
			missing = append(missing, chunk)
		}
	}

	return missing
}
