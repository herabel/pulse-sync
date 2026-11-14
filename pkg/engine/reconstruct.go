package engine

import (
	"fmt"
	"os"

	"github.com/herabel/pulse-sync/pkg/cdc"
	"github.com/herabel/pulse-sync/pkg/manifest"
)

func Reconstruct(
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

		buf := make([]byte, cdc.MaxChunkSize)

		for _, chunk := range newManifest.Chunks {
			if data, ok := deltaChunks[chunk.Hash]; ok {
				_, err := tmpFile.WriteAt(data, int64(chunk.Offset))
				if err != nil {
					return err
				}
			} else if oldChunk, ok := oldLookup[chunk.Hash]; ok && baseFile != nil {
				chunkBuf := buf[:oldChunk.Length]
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

		if err := tmpFile.Sync(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, targetPath)
}
