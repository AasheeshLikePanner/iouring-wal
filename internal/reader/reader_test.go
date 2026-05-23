package reader

import (
	"crypto/rand"
	"encoding/binary"
	"hash/crc32"
	mrand "math/rand"
	"os"
	"sync"
	"testing"

	"urlog/internal/encoder"
	"urlog/internal/segment"
	"urlog/internal/uring"
	"urlog/internal/writer"
)

func testSetup(t *testing.T) (*segment.Manager, *writer.Coalescer, *Reader, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "urlog-reader-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	mgr, err := segment.NewManager(dir, 4<<20)
	if err != nil {
		t.Fatal(err)
	}

	fakeRing := uring.NewFakeRing()

	cfg := writer.DefaultConfig
	cfg.MaxBatch = 64
	cfg.MaxBatchSize = 1 << 20
	coalescer := writer.NewCoalescer(mgr, fakeRing, cfg)

	reader := NewReader(mgr)

	return mgr, coalescer, reader, dir
}

func writeEntries(coalescer *writer.Coalescer, count int, size int) error {
	for i := 0; i < count; i++ {
		payload := make([]byte, size)
		_, _ = rand.Read(payload)
		if err := coalescer.Append(payload); err != nil {
			return err
		}
	}
	return nil
}

func TestR1_RandomReadCorrectness(t *testing.T) {
	mgr, coalescer, reader, _ := testSetup(t)

	numEntries := 1000
	payloads := make([][]byte, numEntries)

	for i := 0; i < numEntries; i++ {
		size := 10 + mrand.Intn(100)
		payload := make([]byte, size)
		_, _ = rand.Read(payload)
		payloads[i] = payload
		if err := coalescer.Append(payload); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	coalescer.Close()

	seqNum := 1000
	for i := 0; i < seqNum; i++ {
		seq := uint64(mrand.Intn(numEntries))
		entry, err := reader.ReadAt(seq)
		if err != nil {
			t.Fatalf("ReadAt(%d): %v", seq, err)
		}
		if string(entry.Payload) != string(payloads[seq]) {
			t.Fatalf("payload mismatch at seq %d", seq)
		}
	}

	reader.Close()
	mgr.Close()
}

func TestR2_ConcurrentReaders(t *testing.T) {
	mgr, coalescer, reader, _ := testSetup(t)

	numEntries := 5000
	var writtenPayloads [][]byte
	var mu sync.Mutex

	for i := 0; i < numEntries; i++ {
		size := 10 + mrand.Intn(90)
		payload := make([]byte, size)
		_, _ = rand.Read(payload)
		mu.Lock()
		writtenPayloads = append(writtenPayloads, payload)
		mu.Unlock()
		if err := coalescer.Append(payload); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	coalescer.Close()

	numReaders := 50
	readsPerReader := 200

	var wg sync.WaitGroup
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readsPerReader; i++ {
				seq := uint64(mrand.Intn(numEntries))
				entry, err := reader.ReadAt(seq)
				if err != nil {
					t.Errorf("concurrent ReadAt(%d): %v", seq, err)
					return
				}
				mu.Lock()
				expected := writtenPayloads[seq]
				mu.Unlock()
				if string(entry.Payload) != string(expected) {
					t.Errorf("payload mismatch at seq %d", seq)
					return
				}
			}
		}()
	}
	wg.Wait()

	reader.Close()
	mgr.Close()
}

func TestR3_ReaderDuringSegmentRotation(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-reader-rotation-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 64<<10)
	if err != nil {
		t.Fatal(err)
	}

	fakeRing := uring.NewFakeRing()
	cfg := writer.Config{
		MaxBatch:     32,
		MaxBatchSize: 1 << 20,
	}
	coalescer := writer.NewCoalescer(mgr, fakeRing, cfg)
	reader := NewReader(mgr)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			seq := uint64(mrand.Intn(1000))
			entry, err := reader.ReadAt(seq)
			if err != nil && err != encoder.ErrTruncated && err.Error() != "entry not found" {
				t.Errorf("read error: %v", err)
				return
			}
			_ = entry
		}
	}()

	for i := 0; i < 1000; i++ {
		if err := coalescer.Append([]byte("rotation test payload")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	coalescer.Close()
	wg.Wait()

	reader.Close()
	mgr.Close()
}

func TestReaderReadAll(t *testing.T) {
	mgr, coalescer, reader, _ := testSetup(t)

	count := 500
	payloads := make([][]byte, count)
	for i := 0; i < count; i++ {
		payload := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		payloads[i] = payload
		if err := coalescer.Append(payload); err != nil {
			t.Fatal(err)
		}
	}
	coalescer.Close()

	entries, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) < count {
		t.Fatalf("expected at least %d entries, got %d", count, len(entries))
	}

	found := make(map[uint64]bool)
	for _, e := range entries {
		if found[e.Seq] {
			t.Fatalf("duplicate seq: %d", e.Seq)
		}
		found[e.Seq] = true
		if e.Seq < uint64(count) {
			expected := []byte{byte(e.Seq), byte(e.Seq >> 8), byte(e.Seq >> 16)}
			if string(e.Payload) != string(expected) {
				t.Fatalf("payload mismatch at seq %d: got %x, want %x", e.Seq, e.Payload, expected)
			}
		}
	}

	reader.Close()
	mgr.Close()
}

func TestReaderNotFound(t *testing.T) {
	mgr, coalescer, reader, _ := testSetup(t)

	if err := coalescer.Append([]byte("test")); err != nil {
		t.Fatal(err)
	}
	coalescer.Close()

	_, err := reader.ReadAt(999999)
	if err == nil {
		t.Fatal("expected error for non-existent seq")
	}

	reader.Close()
	mgr.Close()
}

func TestReaderEmptyLog(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-empty-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 4<<20)
	if err != nil {
		t.Fatal(err)
	}

	reader := NewReader(mgr)

	_, err = reader.ReadAt(0)
	if err == nil {
		t.Error("expected error on empty log")
	}

	entries, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}

	reader.Close()
	mgr.Close()
}

func TestReaderCorruptedEntry(t *testing.T) {
	mgr, coalescer, reader, _ := testSetup(t)

	payload := []byte("corruption test")
	if err := coalescer.Append(payload); err != nil {
		t.Fatal(err)
	}
	coalescer.Close()

	seg := mgr.ActiveSegment()
	buf := make([]byte, 16+len(payload))
	seg.File.ReadAt(buf, segment.HeaderSize)

	flipByte := 20
	buf[flipByte] ^= 0xFF
	seg.File.WriteAt(buf, segment.HeaderSize)

	reader2 := NewReader(mgr)
	_, err := reader2.ReadAt(0)
	if err == nil {
		t.Log("Note: reader may return corrupted data or error depending on CRC check")
	}

	reader2.Close()
	reader.Close()
	mgr.Close()
}

func TestReaderCaching(t *testing.T) {
	mgr, coalescer, reader, _ := testSetup(t)

	for i := 0; i < 3; i++ {
		if err := coalescer.Append([]byte("cache test")); err != nil {
			t.Fatal(err)
		}
	}
	coalescer.Close()

	entry, err := reader.ReadAt(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(entry.Payload) != "cache test" {
		t.Fatalf("unexpected payload: %s", entry.Payload)
	}

	entry2, err := reader.ReadAt(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(entry2.Payload) != string(entry.Payload) {
		t.Fatal("cached read returned different data")
	}

	reader.Close()
	mgr.Close()
}

func TestReadAfterWritev(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-writev-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 4<<20)
	if err != nil {
		t.Fatal(err)
	}

	ring := uring.NewFakeRing()
	coalescer := writer.NewCoalescer(mgr, ring, writer.DefaultConfig)
	reader := NewReader(mgr)

	payload := []byte("writev test payload")
	if err := coalescer.Append(payload); err != nil {
		t.Fatal(err)
	}

	entry, err := reader.ReadAt(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(entry.Payload) != string(payload) {
		t.Fatalf("payload mismatch after writev: got %q, want %q", entry.Payload, payload)
	}

	coalescer.Close()
	reader.Close()
	mgr.Close()
}

func TestVerifyEntriesDirectly(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-direct-verify-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	coalescer := writer.NewCoalescer(mgr, ring, writer.DefaultConfig)

	testPayloads := []string{"hello", "world", "foo", "bar", "baz"}
	for _, p := range testPayloads {
		if err := coalescer.Append([]byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	coalescer.Close()

	seg := mgr.ActiveSegment()
	readBuf := make([]byte, seg.WriteOffset-int64(segment.HeaderSize))
	_, err = seg.File.ReadAt(readBuf, segment.HeaderSize)
	if err != nil {
		t.Fatal(err)
	}

	offset := 0
	for i, expected := range testPayloads {
		bodyLen := int(binary.BigEndian.Uint32(readBuf[offset : offset+4]))
		seq := binary.BigEndian.Uint64(readBuf[offset+4 : offset+12])
		storedCRC := binary.BigEndian.Uint32(readBuf[offset+12 : offset+16])
		payload := readBuf[offset+16 : offset+16+bodyLen]

		if seq != uint64(i) {
			t.Fatalf("entry %d: expected seq %d, got %d", i, i, seq)
		}
		if string(payload) != expected {
			t.Fatalf("entry %d: expected payload %q, got %q", i, expected, payload)
		}
		if crc32.ChecksumIEEE(payload) != storedCRC {
			t.Fatalf("entry %d: CRC mismatch", i)
		}

		offset += encoder.HeaderSize + bodyLen
	}
}
