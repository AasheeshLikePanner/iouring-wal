package segment

import (
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "urlog-segment-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestS1_SegmentRotation(t *testing.T) {
	dir := tempDir(t)
	segSize := int64(1 << 20) // 1MB for fast test

	mgr, err := NewManager(dir, segSize)
	if err != nil {
		t.Fatal(err)
	}

	entrySize := 4096
	payload := make([]byte, entrySize)
	_, _ = rand.Read(payload)

	encoder := func(seq uint64) []byte {
		buf := make([]byte, 16+entrySize)
		crc := crc32.ChecksumIEEE(payload)
		binary.BigEndian.PutUint32(buf[0:4], uint32(entrySize))
		binary.BigEndian.PutUint64(buf[4:12], seq)
		binary.BigEndian.PutUint32(buf[12:16], crc)
		copy(buf[16:], payload)
		return buf
	}

	var maxWrites = int(segSize / int64(16+entrySize) * 3)

	for i := 0; i < maxWrites; i++ {
		seg := mgr.ActiveSegment()
		if seg == nil {
			t.Fatal("no active segment")
		}

		entry := encoder(uint64(i))
		_, err := seg.WriteEntryAt(entry, seg.WriteOffset)
		if err == ErrSegmentFull {
			if err := mgr.Rotate(); err != nil {
				t.Fatal(err)
			}
			seg = mgr.ActiveSegment()
			_, err = seg.WriteEntryAt(entry, seg.WriteOffset)
			if err != nil {
				t.Fatalf("write after rotation: %v", err)
			}
		} else if err != nil {
			t.Fatalf("write at entry %d: %v", i, err)
		}
	}

	allSegs := mgr.AllSegments()
	if len(allSegs) < 2 {
		t.Fatalf("expected at least 2 segments after rotation, got %d", len(allSegs))
	}

	for _, seg := range allSegs {
		if seg.ID == mgr.ActiveSegment().ID {
			continue
		}
		if !seg.Sealed {
			t.Errorf("segment %d should be sealed", seg.ID)
		}
	}

	for _, seg := range allSegs {
		header := make([]byte, HeaderSize)
		_, err := seg.File.ReadAt(header, 0)
		if err != nil {
			t.Fatal(err)
		}
		h, err := decodeHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if string(h.Magic[:]) != Magic {
			t.Errorf("segment %d: bad magic", seg.ID)
		}
		if h.SegmentID != seg.ID {
			t.Errorf("segment %d: header id mismatch", seg.ID)
		}
	}

	mgr.Close()
}

func TestS2_ResumeAfterRestart(t *testing.T) {
	dir := tempDir(t)

	mgr, err := NewManager(dir, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("hello resume")
	var lastSeq uint64
	for i := 0; i < 1000; i++ {
		seg := mgr.ActiveSegment()
		buf := make([]byte, 16+len(payload))
		crc := crc32.ChecksumIEEE(payload)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint64(buf[4:12], uint64(i))
		binary.BigEndian.PutUint32(buf[12:16], crc)
		copy(buf[16:], payload)
		_, err := seg.WriteEntryAt(buf, seg.WriteOffset)
		if err != nil {
			t.Fatal(err)
		}
		lastSeq = uint64(i)
		mgr.IncrementEntryCount()
	}
	mgr.SetNextSeq(lastSeq + 1)

	if err := mgr.Close(); err != nil {
		t.Fatal(err)
	}

	mgr2, err := NewManager(dir, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.Close()

	if mgr2.NextSeq() != lastSeq+1 {
		t.Errorf("nextSeq: got %d, want %d", mgr2.NextSeq(), lastSeq+1)
	}

	files, _ := os.ReadDir(dir)
	if len(files) == 0 {
		t.Fatal("no segment files after restart")
	}
}

func TestS3_RejectCorruptedHeader(t *testing.T) {
	dir := tempDir(t)

	mgr, err := NewManager(dir, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	seg := mgr.ActiveSegment()
	segPath := seg.Path
	mgr.Close()

	f, err := os.OpenFile(segPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	headerBuf := make([]byte, HeaderSize)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatal(err)
	}
	headerBuf[0] = 0xFF
	if _, err := f.WriteAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()

	_, err = OpenSegment(segPath)
	if err == nil {
		t.Fatal("expected error for corrupted header, got nil")
	}
}

func TestS3_SkipCorruptedSegment(t *testing.T) {
	dir := tempDir(t)

	mgr, err := NewManager(dir, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("data before corruption")
	for i := 0; i < 100; i++ {
		seg := mgr.ActiveSegment()
		buf := make([]byte, 16+len(payload))
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint64(buf[4:12], uint64(i))
		copy(buf[16:], payload)
		seg.WriteEntryAt(buf, seg.WriteOffset)
		mgr.IncrementEntryCount()
	}
	mgr.SetNextSeq(100)
	mgr.Close()

	segments := mgr.AllSegments()
	path := segments[0].Path

	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte("BADMAGIC"), 0)
	f.Close()

	_, err = NewManager(dir, DefaultSegmentSize)
	if err == nil {
		t.Fatal("expected error for corrupted segment, got nil")
	}
}

func TestS4_ConcurrentReaderDuringRotation(t *testing.T) {
	dir := tempDir(t)
	segSize := int64(64 << 10) // 64KB for fast rotation

	mgr, err := NewManager(dir, segSize)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	payload := []byte("concurrent-rotation")
	readerDone := make(chan struct{})

	go func() {
		defer close(readerDone)
		allSegs := mgr.AllSegments()
		for _, seg := range allSegs {
			_ = seg
			buf := make([]byte, 16)
			seg.File.ReadAt(buf, 0)
		}
	}()

	for i := 0; i < 5000; i++ {
		seg := mgr.ActiveSegment()
		if seg == nil {
			continue
		}
		buf := make([]byte, 16+len(payload))
		crc := crc32.ChecksumIEEE(payload)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint64(buf[4:12], uint64(i))
		binary.BigEndian.PutUint32(buf[12:16], crc)
		copy(buf[16:], payload)
		_, err := seg.WriteEntryAt(buf, seg.WriteOffset)
		if err == ErrSegmentFull {
			if rotateErr := mgr.Rotate(); rotateErr != nil {
				t.Errorf("rotation failed: %v", rotateErr)
				return
			}
			seg = mgr.ActiveSegment()
			seg.WriteEntryAt(buf, seg.WriteOffset)
		} else if err != nil {
			t.Errorf("write failed: %v", err)
			return
		}
		mgr.IncrementEntryCount()
	}
	mgr.SetNextSeq(5000)

	<-readerDone
}

func TestSegmentFileNaming(t *testing.T) {
	filename := segmentFilename(42)
	expected := "segment_0000000042.urlog"
	if filename != expected {
		t.Errorf("filename: got %s, want %s", filename, expected)
	}

	id, err := segmentIDFromFilename("/some/path/segment_0000000042.urlog")
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Errorf("id: got %d, want 42", id)
	}

	_, err = segmentIDFromFilename("bad_filename.urlog")
	if err == nil {
		t.Fatal("expected error for bad filename")
	}
}

func TestSegmentFullError(t *testing.T) {
	dir := tempDir(t)
	segSize := int64(HeaderSize + TrailerSize + 50)

	seg, err := CreateSegment(dir, 1, segSize)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	buf := make([]byte, 100)
	_, err = seg.WriteEntryAt(buf, seg.WriteOffset)
	if err != ErrSegmentFull {
		t.Fatalf("expected ErrSegmentFull, got %v", err)
	}
}

func TestSegmentWriteAndSync(t *testing.T) {
	dir := tempDir(t)
	seg, err := CreateSegment(dir, 1, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	payload := []byte("sync test payload")
	buf := make([]byte, 16+len(payload))
	crc := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint64(buf[4:12], 1)
	binary.BigEndian.PutUint32(buf[12:16], crc)
	copy(buf[16:], payload)

	n, err := seg.WriteEntryAt(buf, HeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatalf("wrote %d bytes, expected %d", n, len(buf))
	}

	readBuf := make([]byte, len(buf))
	_, err = seg.File.ReadAt(readBuf, HeaderSize)
	if err != nil {
		t.Fatal(err)
	}

	readLen := binary.BigEndian.Uint32(readBuf[0:4])
	if readLen != uint32(len(payload)) {
		t.Fatalf("read length %d, expected %d", readLen, len(payload))
	}

	readPayload := readBuf[16:]
	if string(readPayload) != string(payload) {
		t.Fatalf("payload mismatch: got %q, want %q", readPayload, payload)
	}
}

func TestSegmentSealTwice(t *testing.T) {
	dir := tempDir(t)
	seg, err := CreateSegment(dir, 1, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	if err := seg.Seal(100, 101); err != nil {
		t.Fatal(err)
	}

	if err := seg.Seal(100, 101); err != ErrAlreadySealed {
		t.Fatalf("expected ErrAlreadySealed, got %v", err)
	}
}

func TestSealedSegmentRejectsWrites(t *testing.T) {
	dir := tempDir(t)
	seg, err := CreateSegment(dir, 1, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	if err := seg.Seal(10, 11); err != nil {
		t.Fatal(err)
	}

	_, err = seg.WriteEntryAt([]byte("test"), HeaderSize)
	if err != ErrSegmentClosed {
		t.Fatalf("expected ErrSegmentClosed, got %v", err)
	}
}

func TestManagerSegmentForSeq(t *testing.T) {
	dir := tempDir(t)
	mgr, err := NewManager(dir, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	seg, err := mgr.SegmentForSeq(0)
	if err != nil {
		t.Fatal(err)
	}
	if seg == nil {
		t.Fatal("SegmentForSeq returned nil")
	}

	seg2, err := mgr.SegmentForID(seg.ID)
	if err != nil {
		t.Fatal(err)
	}
	if seg2 != seg {
		t.Fatal("SegmentForID returned different segment")
	}

	_, err = mgr.SegmentForID(99999)
	if err != ErrSegmentNotFound {
		t.Fatalf("expected ErrSegmentNotFound, got %v", err)
	}
}

func TestManagerActiveSegment(t *testing.T) {
	dir := tempDir(t)
	mgr, err := NewManager(dir, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	seg := mgr.ActiveSegment()
	if seg == nil {
		t.Fatal("active segment is nil")
	}
	if seg.ID != 1 {
		t.Fatalf("expected segment id 1, got %d", seg.ID)
	}
	if seg.WriteOffset != HeaderSize {
		t.Fatalf("expected write offset %d, got %d", HeaderSize, seg.WriteOffset)
	}

	activeID := mgr.ActiveSegment().ID
	if err := mgr.Rotate(); err != nil {
		t.Fatal(err)
	}

	newSeg := mgr.ActiveSegment()
	if newSeg.ID != activeID+1 {
		t.Fatalf("after rotate: expected id %d, got %d", activeID+1, newSeg.ID)
	}
}

func TestManagerDir(t *testing.T) {
	dir := tempDir(t)
	mgr, err := NewManager(dir, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	if mgr.Dir() != dir {
		t.Fatalf("Dir: got %s, want %s", mgr.Dir(), dir)
	}
	if mgr.SegmentSize() != DefaultSegmentSize {
		t.Fatalf("SegmentSize: got %d, want %d", mgr.SegmentSize(), DefaultSegmentSize)
	}
}

func TestManagerCreatesDir(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "urlog-test-autocreate-"+t.Name())
	os.RemoveAll(dir)

	mgr, err := NewManager(dir, DefaultSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	mgr.Close()

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Fatal("manager did not create directory")
	}
	os.RemoveAll(dir)
}

func TestManagerZeroSegmentSize(t *testing.T) {
	dir := tempDir(t)
	mgr, err := NewManager(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if mgr.SegmentSize() != DefaultSegmentSize {
		t.Fatalf("expected default segment size %d, got %d", DefaultSegmentSize, mgr.SegmentSize())
	}
	mgr.Close()
}
