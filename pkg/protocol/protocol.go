package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	Magic           uint32 = 0x504C5343
	HeaderSize             = 16
	MsgManifest     uint8  = 0x01
	MsgNeededChunks uint8  = 0x02
	MsgChunkData    uint8  = 0x03
)

type Header struct {
	Magic      uint32
	Version    uint16
	MsgType    uint8
	Reserved   uint8 // just to make header 16 bytes long. at least for now
	SeqID      uint32
	PayloadLen uint32
}

type protocolError struct {
	arg     int
	message string
}

func (e *protocolError) Error() string {
	return fmt.Sprintf("%d - %s", e.arg, e.message)
}

func EncodeHeader(h Header) []byte {
	buf := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint16(buf[4:6], h.Version)
	buf[6] = h.MsgType
	buf[7] = h.Reserved
	binary.BigEndian.PutUint32(buf[8:12], h.SeqID)
	binary.BigEndian.PutUint32(buf[12:16], h.PayloadLen)

	return buf
}

func DecodeHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, &protocolError{len(b), "header is too short"}
	}

	magic := binary.BigEndian.Uint32(b[0:4])

	if magic != Magic {
		return Header{}, &protocolError{int(magic), "magic bytes isn't correct"}
	}

	version := binary.BigEndian.Uint16(b[4:6])
	msgType := b[6]
	reserved := b[7]
	seqID := binary.BigEndian.Uint32(b[8:12])
	payloadLen := binary.BigEndian.Uint32(b[12:16])

	return Header{
		Magic:      magic,
		Version:    version,
		MsgType:    msgType,
		Reserved:   reserved,
		SeqID:      seqID,
		PayloadLen: payloadLen,
	}, nil
}
