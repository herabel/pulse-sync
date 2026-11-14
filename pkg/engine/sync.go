package engine

import (
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/herabel/pulse-sync/pkg/cdc"
	"github.com/herabel/pulse-sync/pkg/manifest"
	"github.com/herabel/pulse-sync/pkg/protocol"
)

func SendFile(conn net.Conn, filePath string) error {
	if filePath == "" {
		return errors.New("file path is blank")
	}

	newManifest, err := manifest.Build(filePath)
	if err != nil {
		return err
	}

	payload := manifest.EncodeManifest(newManifest)

	err = protocol.WriteFrame(conn, protocol.Header{MsgType: protocol.MsgManifest}, payload)
	if err != nil {
		return err
	}

	header, payload, err := protocol.ReadFrame(conn)
	if err != nil {
		return err
	}

	if header.MsgType != protocol.MsgNeededChunks {
		return errors.New("msgType is not MsgNeededChunks")
	}

	neededHashes, err := manifest.DecodeHashes(payload)
	if err != nil {
		return err
	}

	if len(neededHashes) == 0 {
		return nil
	}

	oldLookup := make(map[[32]byte]manifest.ChunkDesc, len(newManifest.Chunks))
	for _, chunk := range newManifest.Chunks {
		oldLookup[chunk.Hash] = chunk
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 32+cdc.MaxChunkSize)

	for _, hash := range neededHashes {
		chunk, ok := oldLookup[hash]
		if !ok {
			return fmt.Errorf("unknown chunk requested: %x", hash)
		}

		packet := buf[:32+chunk.Length]
		copy(packet[:32], chunk.Hash[:])

		if _, err := file.ReadAt(packet[32:], int64(chunk.Offset)); err != nil {
			return err
		}

		if err := protocol.WriteFrame(conn, protocol.Header{MsgType: protocol.MsgChunkData}, packet); err != nil {
			return err
		}
	}

	return nil
}

func ReceiveFile(conn net.Conn, targetPath string) error {
	header, payload, err := protocol.ReadFrame(conn)
	if err != nil {
		return err
	}
	if header.MsgType != protocol.MsgManifest {
		return fmt.Errorf("unexpected frame type: expected MsgManifest (0x01), got 0x%02x", header.MsgType)
	}

	newManifest, err := manifest.DecodeManifest(payload)
	if err != nil {
		return err
	}

	if targetPath == "" {
		targetPath = newManifest.Path
	}

	var oldManifest *manifest.Manifest
	var missingChunks []manifest.ChunkDesc
	basePath := ""

	if _, err := os.Stat(targetPath); err == nil {
		basePath = targetPath
		oldM, err := manifest.Build(targetPath)
		if err == nil {
			oldManifest = oldM
			missingChunks = manifest.Compare(oldManifest, newManifest)
		} else {
			oldManifest = nil
			basePath = ""
			missingChunks = newManifest.Chunks
		}
	} else {
		missingChunks = newManifest.Chunks
	}

	neededHashes := make([][32]byte, len(missingChunks))
	for i, chunk := range missingChunks {
		neededHashes[i] = chunk.Hash
	}

	hashPayload := manifest.EncodeHashes(neededHashes)
	if err := protocol.WriteFrame(conn, protocol.Header{MsgType: protocol.MsgNeededChunks}, hashPayload); err != nil {
		return err
	}

	if len(neededHashes) == 0 {
		return nil
	}

	deltaChunks := make(map[[32]byte][]byte, len(neededHashes))
	for i := 0; i < len(neededHashes); i++ {
		h, chunkPayload, err := protocol.ReadFrame(conn)
		if err != nil {
			return err
		}
		if h.MsgType != protocol.MsgChunkData {
			return fmt.Errorf("unexpected frame type: expected MsgChunkData (0x03), got 0x%02x", h.MsgType)
		}
		if len(chunkPayload) < 32 {
			return errors.New("MsgChunkData payload too short for hash header")
		}

		var chunkHash [32]byte
		copy(chunkHash[:], chunkPayload[:32])
		deltaChunks[chunkHash] = chunkPayload[32:]
	}

	return Reconstruct(targetPath, newManifest, basePath, oldManifest, deltaChunks)
}
