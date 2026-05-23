package encoder

import (
	"crypto/rand"
	"hash/crc32"
	"math"
	"testing"
)

func TestE1_RoundTripRandomEntries(t *testing.T) {
	for i := 0; i < 100000; i++ {
		size := int(randInt(1, 64<<10))
		payload := make([]byte, size)
		_, _ = rand.Read(payload)
		entry := Entry{
			Seq:     uint64(i),
			Payload: payload,
		}

		encoded, err := Encode(entry)
		if err != nil {
			t.Fatalf("encode failed at entry %d: %v", i, err)
		}

		decoded, err := Decode(encoded)
		if err != nil {
			t.Fatalf("decode failed at entry %d: %v", i, err)
		}

		if decoded.Seq != entry.Seq {
			t.Fatalf("seq mismatch at %d: got %d, want %d", i, decoded.Seq, entry.Seq)
		}

		if len(decoded.Payload) != len(entry.Payload) {
			t.Fatalf("payload length mismatch at %d: got %d, want %d", i, len(decoded.Payload), len(entry.Payload))
		}

		for j := range entry.Payload {
			if decoded.Payload[j] != entry.Payload[j] {
				t.Fatalf("payload content mismatch at entry %d byte %d", i, j)
			}
		}

		if decoded.EncodedSize != len(encoded) {
			t.Fatalf("encoded size mismatch at %d: got %d, want %d", i, decoded.EncodedSize, len(encoded))
		}
	}
}

func TestE2_CorruptionDetection(t *testing.T) {
	entry := Entry{
		Seq:     42,
		Payload: []byte("this is a test payload for corruption detection"),
	}

	encoded, err := Encode(entry)
	if err != nil {
		t.Fatal(err)
	}

	orig := make([]byte, len(encoded))
	copy(orig, encoded)

	payloadStart := HeaderSize

	for bit := 0; bit < len(entry.Payload)*8; bit++ {
		copy(encoded, orig)
		byteIdx := payloadStart + bit/8
		bitIdx := bit % 8
		encoded[byteIdx] ^= 1 << bitIdx

		_, err := Decode(encoded)
		if err != ErrCorrupted {
			t.Fatalf("expected ErrCorrupted for bit flip %d (byte %d, bit %d), got %v", bit, byteIdx, bitIdx, err)
		}
	}
}

func TestE3_TruncatedEntry(t *testing.T) {
	entry := Entry{
		Seq:     99,
		Payload: []byte("truncation test payload that should be long enough"),
	}

	encoded, err := Encode(entry)
	if err != nil {
		t.Fatal(err)
	}

	truncLen := len(encoded) / 2
	truncated := encoded[:truncLen]

	_, err = Decode(truncated)
	if err != ErrTruncated {
		t.Fatalf("expected ErrTruncated for buffer length %d, got %v", truncLen, err)
	}

	_, err = Decode(nil)
	if err != ErrTruncated {
		t.Fatalf("expected ErrTruncated for nil buffer, got %v", err)
	}

	_, err = Decode([]byte{})
	if err != ErrTruncated {
		t.Fatalf("expected ErrTruncated for empty buffer, got %v", err)
	}

	_, err = Decode(encoded[:HeaderSize-1])
	if err != ErrTruncated {
		t.Fatalf("expected ErrTruncated for short header, got %v", err)
	}
}

func TestE4_MaxSizeEnforcement(t *testing.T) {
	entry := Entry{
		Seq:     1,
		Payload: make([]byte, MaxBodySize+1),
	}

	_, err := Encode(entry)
	if err != ErrBodyTooLarge {
		t.Fatalf("expected ErrBodyTooLarge for oversized body, got %v", err)
	}

	smallBuf := make([]byte, HeaderSize+10)
	_, err = EncodeInto(smallBuf, Entry{Seq: 1, Payload: make([]byte, 100)})
	if err == nil {
		t.Fatal("expected error for buffer too small")
	}
}

func TestEncodeIntoRoundTrip(t *testing.T) {
	payload := []byte("encode-into test")
	entry := Entry{Seq: 7, Payload: payload}

	buf := make([]byte, HeaderSize+len(payload)+64)
	n, err := EncodeInto(buf, entry)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := Decode(buf[:n])
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Seq != 7 {
		t.Fatalf("seq: got %d, want 7", decoded.Seq)
	}
	if string(decoded.Payload) != "encode-into test" {
		t.Fatalf("payload: got %q, want %q", decoded.Payload, "encode-into test")
	}
}

func TestDecodeEntryHeader(t *testing.T) {
	payload := []byte("header-only decode")
	entry := Entry{Seq: 100, Payload: payload}
	encoded, _ := Encode(entry)

	bodyLen, seq, crc, err := DecodeEntryHeader(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if bodyLen != uint32(len(payload)) {
		t.Fatalf("bodyLen: got %d, want %d", bodyLen, len(payload))
	}
	if seq != 100 {
		t.Fatalf("seq: got %d, want 100", seq)
	}

	expectedCRC := crc32.ChecksumIEEE(payload)
	if crc != expectedCRC {
		t.Fatalf("crc: got %d, want %d", crc, expectedCRC)
	}

	_, _, _, err = DecodeEntryHeader(nil)
	if err != ErrTruncated {
		t.Fatal("expected ErrTruncated for nil")
	}

	_, _, _, err = DecodeEntryHeader(encoded[:15])
	if err != ErrTruncated {
		t.Fatal("expected ErrTruncated for short header")
	}
}

func TestEncodeMaxSeq(t *testing.T) {
	entry := Entry{
		Seq:     math.MaxUint64,
		Payload: []byte("max seq"),
	}
	encoded, err := Encode(entry)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Seq != math.MaxUint64 {
		t.Fatal("seq mismatch")
	}
}

func TestDecodeNoCopy(t *testing.T) {
	payload := []byte("no-copy decode test")
	entry := Entry{Seq: 5, Payload: payload}
	encoded, _ := Encode(entry)

	decoded, err := DecodeNoCopy(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Seq != 5 {
		t.Fatalf("seq: got %d, want 5", decoded.Seq)
	}

	payloadAddr := &decoded.Payload[0]
	origAddr := &encoded[HeaderSize]
	if payloadAddr != origAddr {
		t.Fatal("DecodeNoCopy did not return a reference to the original buffer")
	}

	_, err = DecodeNoCopy([]byte{1, 2, 3})
	if err != ErrTruncated {
		t.Fatal("expected ErrTruncated")
	}
}

func TestZeroSizedPayload(t *testing.T) {
	entry := Entry{Seq: 0, Payload: []byte{}}
	encoded, err := Encode(entry)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != HeaderSize {
		t.Fatalf("zero payload should produce exactly %d bytes, got %d", HeaderSize, len(encoded))
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Seq != 0 {
		t.Fatalf("seq: got %d, want 0", decoded.Seq)
	}
	if len(decoded.Payload) != 0 {
		t.Fatalf("payload should be empty, got %d bytes", len(decoded.Payload))
	}
	if decoded.EncodedSize != HeaderSize {
		t.Fatalf("EncodedSize: got %d, want %d", decoded.EncodedSize, HeaderSize)
	}
}

func randInt(min, max int) int {
	if min == max {
		return min
	}
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	n := int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	if n < 0 {
		n = -n
	}
	return min + (n % (max - min + 1))
}
