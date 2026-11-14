package protocol

import (
	"io"
)

const MaxFrameSize = 16 * 1024 * 1024

// can be optimized somewhere somehow, 3 allocs/op, but benchmark shows 6700mb/s per core during roundtrip, so it's not really crucial

/*
go test -bench=BenchmarkFrame -benchmem ./tests/protocol/...

goos: windows
goarch: amd64
pkg: github.com/herabel/pulse-sync/tests/protocol
cpu: AMD Ryzen 5 7500F 6-Core Processor
BenchmarkFrame_Roundtrip-12      1000000              1227 ns/op        6678.85 MB/s        8224 B/op          3 allocs/op
PASS
ok      github.com/herabel/pulse-sync/tests/protocol    1.674s
*/

func WriteFrame(w io.Writer, h Header, payload []byte) error {
	if h.Magic == 0 {
		h.Magic = Magic
	}
	if h.Version == 0 {
		h.Version = 1
	}
	h.PayloadLen = uint32(len(payload))

	headerBytes := EncodeHeader(h)

	_, err := w.Write(headerBytes)
	if err != nil {
		return err
	}

	if len(payload) > 0 {
		_, err := w.Write(payload)
		if err != nil {
			return err
		}
	}

	return nil
}

// you may want to get buffer as function parameter, but it's mostly useless for current API and goals
func ReadFrame(r io.Reader) (Header, []byte, error) {
	headerBuf := make([]byte, HeaderSize)
	_, err := io.ReadFull(r, headerBuf)

	if err != nil {
		return Header{}, nil, err
	}

	header, err := DecodeHeader(headerBuf)

	if err != nil {
		return Header{}, nil, err
	}

	if header.PayloadLen == 0 {
		return header, nil, nil
	}

	if header.PayloadLen > MaxFrameSize {
		return Header{}, nil, &protocolError{int(header.PayloadLen), "frame is too big"}
	}

	payloadBuf := make([]byte, header.PayloadLen)

	_, err = io.ReadFull(r, payloadBuf)

	if err != nil {
		return Header{}, nil, err
	}

	return header, payloadBuf, nil
}
