package manifest

import (
	"encoding/binary"
	"errors"
)

func EncodeManifest(m *Manifest) []byte {
	offset := 0
	ManifestHeaderSize := 2 + len(m.Path) + 8 + 8 + 4  // pathlen (prefix in memory) + path size in bytes + size + modtime + chunkcount
	totalSize := ManifestHeaderSize + 44*len(m.Chunks) // every chunk is 44 bytes long, so + 44 * amount of chunks
	manifestBuffer := make([]byte, totalSize)

	binary.BigEndian.PutUint16(manifestBuffer[offset:], uint16(len(m.Path)))
	offset += 2

	copy(manifestBuffer[offset:], m.Path)
	offset += len(m.Path)

	binary.BigEndian.PutUint64(manifestBuffer[offset:], uint64(m.Size))
	offset += 8

	binary.BigEndian.PutUint64(manifestBuffer[offset:], uint64(m.ModTime))
	offset += 8

	binary.BigEndian.PutUint32(manifestBuffer[offset:], uint32(len(m.Chunks)))
	offset += 4

	for _, chunk := range m.Chunks {
		binary.BigEndian.PutUint64(manifestBuffer[offset:], chunk.Offset)
		offset += 8

		binary.BigEndian.PutUint32(manifestBuffer[offset:], chunk.Length)
		offset += 4

		copy(manifestBuffer[offset:], chunk.Hash[:])
		offset += 32
	}

	return manifestBuffer
}

func DecodeManifest(b []byte) (*Manifest, error) {
	if len(b) < 22 {
		return nil, errors.New("buffer is too short")
	}
	offset := 0

	pathLen := binary.BigEndian.Uint16(b[0:2])
	offset += 2

	if len(b)-offset < int(pathLen)+20 { // 20 byte = size (8) + modTime (8) + chunksAmount (4)
		return nil, errors.New("buffer is too short for path and header")
	}

	if pathLen < 1 {
		return nil, errors.New("got 0 path len")
	}

	path := b[offset : offset+int(pathLen)]
	offset += int(pathLen)

	size := binary.BigEndian.Uint64(b[offset:])
	offset += 8

	modTime := binary.BigEndian.Uint64(b[offset:])
	offset += 8

	chunksAmount := binary.BigEndian.Uint32(b[offset:])
	offset += 4

	if uint64(chunksAmount)*44 > uint64(len(b)-offset) {
		return nil, errors.New("malformed manifest: chunkCount exceeds buffer")
	}

	chunks := make([]ChunkDesc, chunksAmount)

	for i := 0; i < int(chunksAmount); i++ {
		chunks[i].Offset = binary.BigEndian.Uint64(b[offset:])
		offset += 8
		chunks[i].Length = binary.BigEndian.Uint32(b[offset:])
		offset += 4

		copy(chunks[i].Hash[:], b[offset:])
		offset += 32
	}

	return &Manifest{
		Path:    string(path),
		Size:    int64(size),
		ModTime: int64(modTime),
		Chunks:  chunks,
	}, nil
}

func EncodeHashes(hashes [][32]byte) []byte {
	hashesBuf := make([]byte, len(hashes)*32)

	for i, hash := range hashes {
		copy(hashesBuf[i*32:], hash[:])
	}

	return hashesBuf
}

func DecodeHashes(b []byte) ([][32]byte, error) {
	if len(b)%32 != 0 {
		return nil, errors.New("buffer length % 32 != 0")
	}

	buf := make([][32]byte, len(b)/32)

	for i := range buf {
		copy(buf[i][:], b[i*32:(i+1)*32])
	}

	return buf, nil
}
