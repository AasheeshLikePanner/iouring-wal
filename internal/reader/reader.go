package reader

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync"

	"urlog/internal/encoder"
	"urlog/internal/segment"
)

var (
	ErrNotFound      = errors.New("entry not found")
	ErrSegmentClosed = errors.New("segment closed during read")
	ErrReadOnly      = errors.New("read-only log")
)

type MmapSegment struct {
	Data []byte
	Seg  *segment.Segment
}

type Reader struct {
	manager   *segment.Manager
	mmapCache map[uint64]*MmapSegment
	cacheLock sync.RWMutex
}

func NewReader(mgr *segment.Manager) *Reader {
	return &Reader{
		manager:   mgr,
		mmapCache: make(map[uint64]*MmapSegment),
	}
}

func (r *Reader) ReadAt(seq uint64) (encoder.Entry, error) {
	segments := r.manager.AllSegments()

	var targetSeg *segment.Segment
	for _, seg := range segments {
		if seg.Sealed {
			if seg.ID >= seq {
				targetSeg = seg
				break
			}
		}
	}

	if targetSeg == nil {
		active := r.manager.ActiveSegment()
		if active != nil {
			targetSeg = active
		}
	}

	if targetSeg == nil {
		return encoder.Entry{}, ErrNotFound
	}

	ms, err := r.getMmap(targetSeg)
	if err != nil {
		return encoder.Entry{}, fmt.Errorf("mmap segment %d: %w", targetSeg.ID, err)
	}

	offset := ms.findEntry(seq)
	if offset < 0 {
		return encoder.Entry{}, ErrNotFound
	}

	entry, err := r.decodeAt(ms, offset, seq)
	if err != nil {
		return encoder.Entry{}, err
	}

	return entry, nil
}

func (r *Reader) ReadAll() ([]encoder.Entry, error) {
	segments := r.manager.AllSegments()
	var allEntries []encoder.Entry

	for _, seg := range segments {
		ms, err := r.getMmap(seg)
		if err != nil {
			return nil, fmt.Errorf("mmap segment %d: %w", seg.ID, err)
		}

		entries, err := r.readSegment(ms)
		if err != nil {
			return nil, fmt.Errorf("read segment %d: %w", seg.ID, err)
		}
		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}

func (r *Reader) readSegment(ms *MmapSegment) ([]encoder.Entry, error) {
	offset := segment.HeaderSize
	data := ms.Data
	var entries []encoder.Entry

	for offset+encoder.HeaderSize <= len(data) {
		bodyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		if bodyLen <= 0 || bodyLen > encoder.MaxBodySize {
			break
		}

		totalSize := encoder.HeaderSize + bodyLen
		if offset+totalSize > len(data) {
			break
		}

		seq := binary.BigEndian.Uint64(data[offset+4 : offset+12])
		storedCRC := binary.BigEndian.Uint32(data[offset+12 : offset+16])
		payload := data[offset+16 : offset+totalSize]

		if crc32.ChecksumIEEE(payload) != storedCRC {
			break
		}

		payloadCopy := make([]byte, bodyLen)
		copy(payloadCopy, payload)

		entries = append(entries, encoder.Entry{
			Seq:     seq,
			Payload: payloadCopy,
		})

		offset += totalSize
	}

	return entries, nil
}

func (r *Reader) getMmap(seg *segment.Segment) (*MmapSegment, error) {
	r.cacheLock.RLock()
	ms, ok := r.mmapCache[seg.ID]
	r.cacheLock.RUnlock()
	if ok {
		return ms, nil
	}

	r.cacheLock.Lock()
	defer r.cacheLock.Unlock()

	if ms, ok := r.mmapCache[seg.ID]; ok {
		return ms, nil
	}

	f, err := os.Open(seg.Path)
	if err != nil {
		return nil, fmt.Errorf("open for mmap: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat for mmap: %w", err)
	}

	size := info.Size()
	if size < segment.HeaderSize {
		f.Close()
		return nil, errors.New("segment file too small for mmap")
	}

	data, err := mmapFile(f, int(size))
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap: %w", err)
	}

	ms = &MmapSegment{
		Data: data,
		Seg:  seg,
	}
	r.mmapCache[seg.ID] = ms

	return ms, nil
}

func (r *Reader) findEntryBySeq(ms *MmapSegment, seq uint64) int {
	return ms.findEntry(seq)
}

func (ms *MmapSegment) findEntry(seq uint64) int {
	offset := segment.HeaderSize
	data := ms.Data

	for offset+encoder.HeaderSize <= len(data) {
		bodyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		if bodyLen <= 0 || bodyLen > encoder.MaxBodySize {
			return -1
		}

		totalSize := encoder.HeaderSize + bodyLen
		if offset+totalSize > len(data) {
			return -1
		}

		entrySeq := binary.BigEndian.Uint64(data[offset+4 : offset+12])
		if entrySeq == seq {
			return offset
		}

		offset += totalSize
	}

	return -1
}

func (r *Reader) decodeAt(ms *MmapSegment, offset int, seq uint64) (encoder.Entry, error) {
	data := ms.Data
	if offset+encoder.HeaderSize > len(data) {
		return encoder.Entry{}, encoder.ErrTruncated
	}

	bodyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	entrySeq := binary.BigEndian.Uint64(data[offset+4 : offset+12])
	storedCRC := binary.BigEndian.Uint32(data[offset+12 : offset+16])

	if entrySeq != seq {
		return encoder.Entry{}, encoder.ErrSeqMismatch
	}

	if bodyLen < 0 || bodyLen > encoder.MaxBodySize {
		return encoder.Entry{}, encoder.ErrBodyTooLarge
	}

	totalSize := encoder.HeaderSize + bodyLen
	if offset+totalSize > len(data) {
		return encoder.Entry{}, encoder.ErrTruncated
	}

	payload := data[offset+16 : offset+totalSize]

	if crc32.ChecksumIEEE(payload) != storedCRC {
		return encoder.Entry{}, encoder.ErrCorrupted
	}

	payloadCopy := make([]byte, bodyLen)
	copy(payloadCopy, payload)

	return encoder.Entry{
		Seq:     entrySeq,
		Payload: payloadCopy,
	}, nil
}

func (r *Reader) Close() error {
	r.cacheLock.Lock()
	defer r.cacheLock.Unlock()

	var lastErr error
	for id, ms := range r.mmapCache {
		if err := munmapFile(ms.Data); err != nil {
			lastErr = err
		}
		delete(r.mmapCache, id)
	}
	return lastErr
}
