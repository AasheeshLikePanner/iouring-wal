package recovery

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"urlog/internal/encoder"
	"urlog/internal/segment"
)

var (
	ErrCorruptSegment   = errors.New("segment corrupted, cannot recover")
	ErrInconsistentLog  = errors.New("log state inconsistent")
	ErrBadTrailer       = errors.New("bad segment trailer")
)

type Result struct {
	LastValidSeq  uint64
	EntryCount    uint64
	SegmentsFound int
	SegmentsValid int
	WasPartial    bool
	NextSeq       uint64
}

func Recover(dir string, segmentSize int64) (*Result, error) {
	mgr, err := segment.NewManager(dir, segmentSize)
	if err != nil {
		return nil, fmt.Errorf("open log for recovery: %w", err)
	}
	defer mgr.Close()

	result := &Result{
		SegmentsFound: len(mgr.AllSegments()),
	}

	allSegments := mgr.AllSegments()
	if len(allSegments) == 0 {
		return result, nil
	}

	for i, seg := range allSegments {
		if i < len(allSegments)-1 || seg.Sealed {
			result.SegmentsValid++
		}
	}

	lastSeg := allSegments[len(allSegments)-1]

	trailerBuf := make([]byte, segment.TrailerSize)
	_, err = lastSeg.File.ReadAt(trailerBuf, lastSeg.Size-int64(segment.TrailerSize))
	hasTrailer := err == nil

	if hasTrailer {
		t, decodeErr := decodeTrailerBytes(trailerBuf)
		if decodeErr == nil && t.EntryCount > 0 {
			result.LastValidSeq = t.LastSeq
			result.EntryCount = t.EntryCount
			result.WasPartial = false
			result.NextSeq = t.LastSeq + 1
			return result, nil
		}
	}

	result.WasPartial = true

	lastValidSeq, entryCount, err := scanEntries(lastSeg)
	if err != nil {
		return nil, fmt.Errorf("scan last segment: %w", err)
	}

	result.LastValidSeq = lastValidSeq
	result.EntryCount = entryCount
	result.NextSeq = lastValidSeq + 1

	return result, nil
}

type trailerData struct {
	LastSeq    uint64
	EntryCount uint64
	CRC32      uint32
}

func decodeTrailerBytes(buf []byte) (trailerData, error) {
	if len(buf) < 20 {
		return trailerData{}, ErrBadTrailer
	}
	return trailerData{
		LastSeq:    binary.BigEndian.Uint64(buf[0:8]),
		EntryCount: binary.BigEndian.Uint64(buf[8:16]),
		CRC32:      binary.BigEndian.Uint32(buf[16:20]),
	}, nil
}

func scanEntries(seg *segment.Segment) (lastSeq uint64, count uint64, err error) {
	buf := make([]byte, 32<<10)
	offset := int64(segment.HeaderSize)

	for {
		remaining := seg.Size - int64(segment.TrailerSize) - offset
		if remaining < encoder.HeaderSize {
			return lastSeq, count, nil
		}

		readSize := len(buf)
		if int64(readSize) > remaining {
			readSize = int(remaining)
		}

		n, readErr := seg.File.ReadAt(buf[:readSize], offset)
		if readErr != nil && readErr != io.EOF {
			return lastSeq, count, nil
		}
		if n < encoder.HeaderSize {
			return lastSeq, count, nil
		}

		data := buf[:n]
		pos := 0

		for pos+encoder.HeaderSize <= len(data) {
			bodyLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			if bodyLen <= 0 || bodyLen > encoder.MaxBodySize {
				break
			}

			totalEntrySize := encoder.HeaderSize + bodyLen
			if pos+totalEntrySize > len(data) {
				break
			}

			entrySeq := binary.BigEndian.Uint64(data[pos+4 : pos+12])
			storedCRC := binary.BigEndian.Uint32(data[pos+12 : pos+16])
			payload := data[pos+16 : pos+totalEntrySize]

			if crc32.ChecksumIEEE(payload) != storedCRC {
				return lastSeq, count, nil
			}

			if entrySeq > lastSeq {
				lastSeq = entrySeq
			}
			count++
			pos += totalEntrySize
		}

		if pos == 0 {
			return lastSeq, count, nil
		}
		offset += int64(pos)
	}
}

func TruncateAfter(seg *segment.Segment, lastOffset int64) error {
	_, err := seg.File.Seek(lastOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek: %w", err)
	}
	return nil
}

type RecoveryScanner struct {
	dir         string
	segmentSize int64
}

func NewRecoveryScanner(dir string, segmentSize int64) *RecoveryScanner {
	if segmentSize <= 0 {
		segmentSize = segment.DefaultSegmentSize
	}
	return &RecoveryScanner{
		dir:         dir,
		segmentSize: segmentSize,
	}
}

func (rs *RecoveryScanner) Scan() (*Result, error) {
	return Recover(rs.dir, rs.segmentSize)
}

func (rs *RecoveryScanner) ValidateAll() error {
	result, err := Recover(rs.dir, rs.segmentSize)
	if err != nil {
		return err
	}
	if result.WasPartial {
		return errors.New("last segment was not cleanly sealed")
	}
	return nil
}

var _ = os.ModePerm
