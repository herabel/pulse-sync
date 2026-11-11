package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
	"testing/iotest"

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

func TestFrame_Roundtrip(t *testing.T) {
	var buf bytes.Buffer
	h := protocol.Header{
		Magic:   protocol.Magic,
		Version: 1,
		MsgType: protocol.MsgChunkData,
		SeqID:   7,
	}
	payload := []byte("hello pulsesync")

	if err := protocol.WriteFrame(&buf, h, payload); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	gotHeader, gotPayload, err := protocol.ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	h.PayloadLen = uint32(len(payload))
	if gotHeader != h {
		t.Fatalf("header mismatch: got %+v, want %+v", gotHeader, h)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload mismatch: got %q, want %q", gotPayload, payload)
	}
}

func TestFrame_EmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	h := protocol.Header{
		Magic:   protocol.Magic,
		Version: 1,
		MsgType: protocol.MsgManifest,
		SeqID:   1,
	}

	if err := protocol.WriteFrame(&buf, h, nil); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	gotHeader, gotPayload, err := protocol.ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if gotHeader.PayloadLen != 0 || len(gotPayload) != 0 {
		t.Fatalf("expected empty payload, got len=%d, slice=%v", gotHeader.PayloadLen, gotPayload)
	}
}

func TestFrame_FragmentedAndTruncated(t *testing.T) {
	var buf bytes.Buffer
	h := protocol.Header{
		Magic:   protocol.Magic,
		Version: 1,
		MsgType: protocol.MsgChunkData,
		SeqID:   99,
	}
	payload := []byte("fragmented tcp stream test payload")

	if err := protocol.WriteFrame(&buf, h, payload); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	oneByteReader := iotest.OneByteReader(bytes.NewReader(buf.Bytes()))
	gotHeader, gotPayload, err := protocol.ReadFrame(oneByteReader)
	if err != nil {
		t.Fatalf("ReadFrame on fragmented stream failed: %v", err)
	}
	if gotHeader.SeqID != h.SeqID || !bytes.Equal(gotPayload, payload) {
		t.Fatalf("fragmented read corrupted data: header=%+v payload=%q", gotHeader, gotPayload)
	}

	truncated := buf.Bytes()[:len(buf.Bytes())-5]
	if _, _, err := protocol.ReadFrame(bytes.NewReader(truncated)); err == nil {
		t.Fatalf("expected error on truncated frame, got nil")
	}
}

func BenchmarkFrame_Roundtrip(b *testing.B) {
	payload := make([]byte, 8192)
	h := protocol.Header{
		Magic:   protocol.Magic,
		Version: 1,
		MsgType: protocol.MsgChunkData,
		SeqID:   1,
	}

	var buf bytes.Buffer
	buf.Grow(protocol.HeaderSize + len(payload))

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := protocol.WriteFrame(&buf, h, payload); err != nil {
			b.Fatal(err)
		}
		if _, _, err := protocol.ReadFrame(&buf); err != nil {
			b.Fatal(err)
		}
	}
}
