package protocol

import (
	"encoding/binary"
	"testing"

	"github.com/herabel/pulse-sync/pkg/protocol"
)

func Test_Roundtrip(t *testing.T) {
	h := protocol.Header{
		Magic:      protocol.Magic,
		Version:    1,
		MsgType:    protocol.MsgManifest,
		SeqID:      42,
		PayloadLen: 1024,
	}

	buf := protocol.EncodeHeader(h)

	decoded, err := protocol.DecodeHeader(buf)

	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if decoded != h {
		t.Fatalf("headers are not the same: %v is not same with %v", h, decoded)
	}
}

func Test_ShortHeader(t *testing.T) {
	buf := make([]byte, 10)

	result, err := protocol.DecodeHeader(buf)

	t.Logf("result buffer is: %d", result)

	if err == nil {
		t.Fatalf("didn't got a error")
	}
}

func Test_InvalidMagic(t *testing.T) {
	buf := make([]byte, 16)

	binary.BigEndian.PutUint16(buf[0:2], 0x5053)

	result, err := protocol.DecodeHeader(buf)

	t.Logf("result buffer is: %d", result)

	if err == nil {
		t.Fatalf("didn't got a error")
	}
}

func BenchmarkHeaderRoundtrip(b *testing.B) {
	h := protocol.Header{
		Magic:      protocol.Magic,
		Version:    1,
		MsgType:    protocol.MsgManifest,
		SeqID:      42,
		PayloadLen: 1024,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ { // b.Loop may be used, but this adds 4-5 ns/op
		buf := protocol.EncodeHeader(h)
		_, _ = protocol.DecodeHeader(buf)
	}
}
