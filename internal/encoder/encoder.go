package encoder

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

const (
	HeaderSize  = 16
	MaxBodySize = 64 << 20 // 64 MB
	MaxSeq      = math.MaxUint64
)

var (
	ErrBodyTooLarge = errors.New("entry body exceeds max size")
	ErrCorrupted    = errors.New("entry payload corrupted")
	ErrTruncated    = errors.New("entry data truncated")
	ErrSeqMismatch  = errors.New("sequence number mismatch")
	ErrNegativeLen  = errors.New("negative entry length in header")
)

type Entry struct {
	Seq     uint64
	Payload []byte
}

func (e Entry) BodyLen() int {
	return len(e.Payload)
}

func Encode(entry Entry) ([]byte, error) {
	bodyLen := len(entry.Payload)
	if bodyLen > MaxBodySize {
		return nil, ErrBodyTooLarge
	}
	if bodyLen < 0 {
		return nil, ErrNegativeLen
	}

	buf := make([]byte, HeaderSize+bodyLen)

	crc := crc32.ChecksumIEEE(entry.Payload)

	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))
	binary.BigEndian.PutUint64(buf[4:12], entry.Seq)
	binary.BigEndian.PutUint32(buf[12:16], crc)

	copy(buf[16:], entry.Payload)

	return buf, nil
}

func EncodeInto(buf []byte, entry Entry) (int, error) {
	bodyLen := len(entry.Payload)
	if bodyLen > MaxBodySize {
		return 0, ErrBodyTooLarge
	}
	if bodyLen < 0 {
		return 0, ErrNegativeLen
	}
	needed := HeaderSize + bodyLen
	if len(buf) < needed {
		return 0, errors.New("buffer too small")
	}

	crc := crc32.ChecksumIEEE(entry.Payload)

	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))
	binary.BigEndian.PutUint64(buf[4:12], entry.Seq)
	binary.BigEndian.PutUint32(buf[12:16], crc)

	copy(buf[16:], entry.Payload)

	return needed, nil
}

type DecodedEntry struct {
	Entry
	EncodedSize int
}

func Decode(data []byte) (DecodedEntry, error) {
	if len(data) < HeaderSize {
		return DecodedEntry{}, ErrTruncated
	}

	bodyLen := int(binary.BigEndian.Uint32(data[0:4]))
	if bodyLen < 0 {
		return DecodedEntry{}, ErrNegativeLen
	}
	seq := binary.BigEndian.Uint64(data[4:12])
	storedCRC := binary.BigEndian.Uint32(data[12:16])

	if bodyLen > MaxBodySize {
		return DecodedEntry{}, ErrBodyTooLarge
	}

	if len(data) < HeaderSize+bodyLen {
		return DecodedEntry{}, ErrTruncated
	}

	payload := data[HeaderSize : HeaderSize+bodyLen]

	actualCRC := crc32.ChecksumIEEE(payload)
	if actualCRC != storedCRC {
		return DecodedEntry{}, ErrCorrupted
	}

	payloadCopy := make([]byte, bodyLen)
	copy(payloadCopy, payload)

	return DecodedEntry{
		Entry:       Entry{Seq: seq, Payload: payloadCopy},
		EncodedSize: HeaderSize + bodyLen,
	}, nil
}

func DecodeNoCopy(data []byte) (DecodedEntry, error) {
	if len(data) < HeaderSize {
		return DecodedEntry{}, ErrTruncated
	}

	bodyLen := int(binary.BigEndian.Uint32(data[0:4]))
	if bodyLen < 0 {
		return DecodedEntry{}, ErrNegativeLen
	}
	seq := binary.BigEndian.Uint64(data[4:12])
	storedCRC := binary.BigEndian.Uint32(data[12:16])

	if bodyLen > MaxBodySize {
		return DecodedEntry{}, ErrBodyTooLarge
	}

	if len(data) < HeaderSize+bodyLen {
		return DecodedEntry{}, ErrTruncated
	}

	payload := data[HeaderSize : HeaderSize+bodyLen]

	actualCRC := crc32.ChecksumIEEE(payload)
	if actualCRC != storedCRC {
		return DecodedEntry{}, ErrCorrupted
	}

	return DecodedEntry{
		Entry:       Entry{Seq: seq, Payload: payload},
		EncodedSize: HeaderSize + bodyLen,
	}, nil
}

func DecodeEntryHeader(data []byte) (bodyLen uint32, seq uint64, crc uint32, err error) {
	if len(data) < HeaderSize {
		return 0, 0, 0, ErrTruncated
	}
	bodyLen = binary.BigEndian.Uint32(data[0:4])
	seq = binary.BigEndian.Uint64(data[4:12])
	crc = binary.BigEndian.Uint32(data[12:16])
	return
}
