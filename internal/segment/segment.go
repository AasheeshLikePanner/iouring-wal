package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"time"
)

const (
	Magic        = "URLOGSEG"
	MagicSize    = 8
	IDSize       = 8
	TimeSize     = 8
	FlagsSize    = 4
	ReservedSize = 12
	TrailerReservedSize = 20

	HeaderSize  = MagicSize + IDSize + TimeSize + FlagsSize + ReservedSize // 40
	TrailerSize = 8 + 8 + 4 + TrailerReservedSize                         // 40

	DefaultSegmentSize = 128 << 20
	MaxEntrySize       = 64 << 20

	FlagSealed uint32 = 1 << 0
)

var (
	ErrSegmentFull     = errors.New("segment is full")
	ErrSegmentCorrupt  = errors.New("segment header corrupted")
	ErrBadMagic        = errors.New("bad segment magic")
	ErrSegmentClosed   = errors.New("segment is closed")
	ErrSegmentNotFound = errors.New("segment not found for sequence")
	ErrNoActiveSegment = errors.New("no active segment")
	ErrAlreadySealed   = errors.New("segment already sealed")
)

type Header struct {
	Magic      [8]byte
	SegmentID  uint64
	CreatedAt  int64
	Flags      uint32
	Reserved   [12]byte
}

type Trailer struct {
	LastSeq    uint64
	EntryCount uint64
	CRC32      uint32
	Reserved   [20]byte
}

type Segment struct {
	ID         uint64
	File       *os.File
	Path       string
	Header     Header
	Size       int64
	WriteOffset int64
	Sealed     bool
	Mu         sync.Mutex
}

func CreateSegment(dir string, id uint64, segSize int64) (*Segment, error) {
	if segSize <= 0 {
		segSize = DefaultSegmentSize
	}

	filename := segmentFilename(id)
	path := filepath.Join(dir, filename)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("create segment file: %w", err)
	}

	if err := f.Truncate(segSize); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("truncate segment: %w", err)
	}

	seg := &Segment{
		ID:          id,
		File:        f,
		Path:        path,
		Size:        segSize,
		WriteOffset: HeaderSize,
	}

	h := Header{
		SegmentID: id,
		CreatedAt: time.Now().Unix(),
	}
	copy(h.Magic[:], Magic)

	headerBuf := make([]byte, HeaderSize)
	encodeHeader(headerBuf, h)

	n, err := f.WriteAt(headerBuf, 0)
	if err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("write header: %w", err)
	}
	if n != HeaderSize {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("short header write: %d", n)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("sync header: %w", err)
	}

	seg.Header = h
	return seg, nil
}

func OpenSegment(path string) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat segment: %w", err)
	}

	seg := &Segment{
		File: f,
		Path: path,
		Size: info.Size(),
	}

	id, err := segmentIDFromFilename(path)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("parse segment id: %w", err)
	}
	seg.ID = id

	headerBuf := make([]byte, HeaderSize)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}

	h, err := decodeHeader(headerBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	if string(h.Magic[:]) != Magic {
		f.Close()
		return nil, ErrBadMagic
	}

	seg.Header = h

	trailerBuf := make([]byte, TrailerSize)
	n, err := f.ReadAt(trailerBuf, info.Size()-int64(TrailerSize))
	if err != nil || n < TrailerSize {
		seg.WriteOffset = HeaderSize
		seg.Sealed = false
		return seg, nil
	}

	t, err := decodeTrailer(trailerBuf)
	if err == nil && t.LastSeq > 0 {
		seg.Sealed = true
		seg.WriteOffset = info.Size()
	}

	if !seg.Sealed {
		seg.WriteOffset = HeaderSize
	}

	return seg, nil
}

func (s *Segment) WriteEntryAt(buf []byte, offset int64) (int, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if s.Sealed {
		return 0, ErrSegmentClosed
	}

	end := offset + int64(len(buf))
	if end > s.Size {
		return 0, ErrSegmentFull
	}

	n, err := s.File.WriteAt(buf, offset)
	if err != nil {
		return n, err
	}

	if offset+int64(n) > s.WriteOffset {
		s.WriteOffset = offset + int64(n)
	}

	return n, nil
}

func (s *Segment) Seal(lastSeq uint64, entryCount uint64) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if s.Sealed {
		return ErrAlreadySealed
	}

	t := Trailer{
		LastSeq:    lastSeq,
		EntryCount: entryCount,
	}
	t.CRC32 = crc32.ChecksumIEEE(s.Header.Magic[:])

	trailerBuf := make([]byte, TrailerSize)
	encodeTrailer(trailerBuf, t)

	trailerOffset := s.Size - int64(TrailerSize)
	if _, err := s.File.WriteAt(trailerBuf, trailerOffset); err != nil {
		return fmt.Errorf("write trailer: %w", err)
	}

	if err := s.File.Sync(); err != nil {
		return fmt.Errorf("sync trailer: %w", err)
	}

	s.Sealed = true
	return nil
}

func (s *Segment) Close() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.File != nil {
		return s.File.Close()
	}
	return nil
}

func (s *Segment) Fd() int {
	return int(s.File.Fd())
}

func segmentFilename(id uint64) string {
	return fmt.Sprintf("segment_%010d.urlog", id)
}

func segmentIDFromFilename(path string) (uint64, error) {
	base := filepath.Base(path)
	base = strings.TrimSuffix(base, ".urlog")
	parts := strings.SplitN(base, "_", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid segment filename: %s", path)
	}
	id, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid segment id in %s: %w", path, err)
	}
	return id, nil
}

func encodeHeader(buf []byte, h Header) {
	copy(buf[0:8], h.Magic[:])
	binary.BigEndian.PutUint64(buf[8:16], h.SegmentID)
	binary.BigEndian.PutUint64(buf[16:24], uint64(h.CreatedAt))
	binary.BigEndian.PutUint32(buf[24:28], h.Flags)
}

func decodeHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderSize {
		return Header{}, ErrSegmentCorrupt
	}
	var h Header
	copy(h.Magic[:], buf[0:8])
	h.SegmentID = binary.BigEndian.Uint64(buf[8:16])
	h.CreatedAt = int64(binary.BigEndian.Uint64(buf[16:24]))
	h.Flags = binary.BigEndian.Uint32(buf[24:28])
	return h, nil
}

func encodeTrailer(buf []byte, t Trailer) {
	binary.BigEndian.PutUint64(buf[0:8], t.LastSeq)
	binary.BigEndian.PutUint64(buf[8:16], t.EntryCount)
	binary.BigEndian.PutUint32(buf[16:20], t.CRC32)
}

func decodeTrailer(buf []byte) (Trailer, error) {
	if len(buf) < TrailerSize {
		return Trailer{}, errors.New("trailer too short")
	}
	return Trailer{
		LastSeq:    binary.BigEndian.Uint64(buf[0:8]),
		EntryCount: binary.BigEndian.Uint64(buf[8:16]),
		CRC32:      binary.BigEndian.Uint32(buf[16:20]),
	}, nil
}

type Manager struct {
	dir          string
	segmentSize  int64

	activeSegment   *Segment
	activeMu        sync.RWMutex

	allSegments     []*Segment
	segmentLookup   map[uint64]*Segment

	nextSeq         atomic.Uint64
	entryCount      atomic.Uint64

	closed          atomic.Bool
}

func NewManager(dir string, segmentSize int64) (*Manager, error) {
	if segmentSize <= 0 {
		segmentSize = DefaultSegmentSize
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}

	m := &Manager{
		dir:           dir,
		segmentSize:   segmentSize,
		segmentLookup: make(map[uint64]*Segment),
	}

	if err := m.scanExisting(); err != nil {
		return nil, fmt.Errorf("scan existing: %w", err)
	}

	if m.activeSegment == nil {
		nextID := uint64(1)
		if len(m.allSegments) > 0 {
			nextID = m.allSegments[len(m.allSegments)-1].ID + 1
		}
		if err := m.createNewSegment(nextID); err != nil {
			return nil, fmt.Errorf("create initial segment: %w", err)
		}
	}

	return m, nil
}

func (m *Manager) scanExisting() error {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}

	var segPaths []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "segment_") && strings.HasSuffix(e.Name(), ".urlog") {
			segPaths = append(segPaths, filepath.Join(m.dir, e.Name()))
		}
	}

	sort.Strings(segPaths)

	var last *Segment
	for _, path := range segPaths {
		seg, err := OpenSegment(path)
		if err != nil {
			return fmt.Errorf("open existing segment %s: %w", path, err)
		}
		m.allSegments = append(m.allSegments, seg)
		m.segmentLookup[seg.ID] = seg

		if !seg.Sealed {
			last = seg
		}
	}

	if last != nil {
		m.activeSegment = last
	}

	if m.activeSegment != nil {
		m.setNextSeqFromScan()
	}

	return nil
}

func (m *Manager) setNextSeqFromScan() {
	seg := m.activeSegment
	offset := int64(HeaderSize)
	var maxSeq uint64
	var count uint64

	buf := make([]byte, 32<<10)
	for {
		remaining := seg.Size - int64(TrailerSize) - offset
		if remaining < 16 {
			break
		}
		readSize := len(buf)
		if int64(readSize) > remaining {
			readSize = int(remaining)
		}
		n, err := seg.File.ReadAt(buf[:readSize], offset)
		if err != nil || n < 16 {
			break
		}
		data := buf[:n]
		pos := 0
		for pos+16 <= len(data) {
			bodyLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			if bodyLen == 0 {
				break
			}
			if bodyLen > MaxEntrySize || bodyLen < 0 {
				m.nextSeq.Store(maxSeq + 1)
				m.entryCount.Store(count)
				return
			}
			totalEntrySize := 16 + bodyLen
			if pos+totalEntrySize > len(data) {
				m.nextSeq.Store(maxSeq + 1)
				m.entryCount.Store(count)
				return
			}
			seq := binary.BigEndian.Uint64(data[pos+4 : pos+12])
			crc := binary.BigEndian.Uint32(data[pos+12 : pos+16])
			payload := data[pos+16 : pos+16+bodyLen]
			if crc32.ChecksumIEEE(payload) != crc {
				m.nextSeq.Store(maxSeq + 1)
				m.entryCount.Store(count)
				return
			}
			if seq > maxSeq {
				maxSeq = seq
			}
			count++
			pos += totalEntrySize
		}
		offset += int64(pos)
		if pos == 0 {
			break
		}
	}
	m.nextSeq.Store(maxSeq + 1)
	m.entryCount.Store(count)
}

func (m *Manager) createNewSegment(id uint64) error {
	seg, err := CreateSegment(m.dir, id, m.segmentSize)
	if err != nil {
		return err
	}

	m.activeSegment = seg
	m.allSegments = append(m.allSegments, seg)
	m.segmentLookup[seg.ID] = seg
	return nil
}

func (m *Manager) ActiveSegment() *Segment {
	m.activeMu.RLock()
	defer m.activeMu.RUnlock()
	return m.activeSegment
}

func (m *Manager) Rotate() error {
	m.activeMu.Lock()
	defer m.activeMu.Unlock()

	if m.closed.Load() {
		return ErrSegmentClosed
	}

	cur := m.activeSegment
	if cur == nil {
		return ErrNoActiveSegment
	}

	lastSeq := m.nextSeq.Load() - 1
	count := m.entryCount.Load()

	if err := cur.Seal(lastSeq, count); err != nil {
		return fmt.Errorf("seal segment %d: %w", cur.ID, err)
	}

	nextID := cur.ID + 1
	seg, err := CreateSegment(m.dir, nextID, m.segmentSize)
	if err != nil {
		return fmt.Errorf("create segment %d: %w", nextID, err)
	}

	m.activeSegment = seg
	m.allSegments = append(m.allSegments, seg)
	m.segmentLookup[seg.ID] = seg

	return nil
}

func (m *Manager) SegmentForSeq(seq uint64) (*Segment, error) {
	m.activeMu.RLock()
	defer m.activeMu.RUnlock()

	for _, seg := range m.allSegments {
		if seg.Sealed {
			if seq <= seg.Header.SegmentID { continue }
		}
	}

	lastID := m.activeSegment.ID
	var candidates []*Segment
	for _, seg := range m.allSegments {
		if seg.Sealed {
			off := seg.WriteOffset
			if off > int64(HeaderSize) {
				candidates = append(candidates, seg)
			}
		}
	}

	if m.activeSegment != nil {
		candidates = append(candidates, m.activeSegment)
	}

	for _, seg := range candidates {
		if seg.ID == lastID {
			return seg, nil
		}
	}

	if len(m.allSegments) > 0 {
		return m.allSegments[len(m.allSegments)-1], nil
	}

	return nil, ErrSegmentNotFound
}

func (m *Manager) SegmentForID(id uint64) (*Segment, error) {
	seg, ok := m.segmentLookup[id]
	if !ok {
		return nil, ErrSegmentNotFound
	}
	return seg, nil
}

func (m *Manager) AllSegments() []*Segment {
	m.activeMu.RLock()
	defer m.activeMu.RUnlock()
	result := make([]*Segment, len(m.allSegments))
	copy(result, m.allSegments)
	return result
}

func (m *Manager) NextSeq() uint64 {
	return m.nextSeq.Load()
}

func (m *Manager) AdvanceSeq() {
	m.nextSeq.Add(1)
}

func (m *Manager) AllocateSeq() uint64 {
	return m.nextSeq.Add(1) - 1
}

func (m *Manager) SetNextSeq(seq uint64) {
	m.nextSeq.Store(seq)
}

func (m *Manager) EntryCount() uint64 {
	return m.entryCount.Load()
}

func (m *Manager) IncrementEntryCount() {
	m.entryCount.Add(1)
}

func (m *Manager) Dir() string {
	return m.dir
}

func (m *Manager) SegmentSize() int64 {
	return m.segmentSize
}

func (m *Manager) Close() error {
	m.closed.Store(true)
	m.activeMu.Lock()
	defer m.activeMu.Unlock()

	var lastErr error
	for _, seg := range m.allSegments {
		if err := seg.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (m *Manager) createNewSegmentForTest() error {
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	id := uint64(1)
	if m.activeSegment != nil {
		id = m.activeSegment.ID + 1
	}
	return m.createNewSegment(id)
}


