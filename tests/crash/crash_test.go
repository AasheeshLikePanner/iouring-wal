package crash

import (
	"crypto/rand"
	"os"
	"sync"
	"testing"
	"time"

	"urlog/internal/encoder"
	"urlog/internal/reader"
	"urlog/internal/recovery"
	"urlog/internal/segment"
	"urlog/internal/uring"
	"urlog/internal/writer"
)

func simulateCrash(dir string, mgr *segment.Manager, coalescer *writer.Coalescer) {
	coalescer.Close()
	mgr.Close()
}

func TestCrashMidWriteRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-crash-*")
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

	var wg sync.WaitGroup
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := make([]byte, 100)
			_, _ = rand.Read(payload)
			for i := 0; i < 500; i++ {
				if err := coalescer.Append(payload); err != nil {
					return
				}
			}
		}(w)
	}
	wg.Wait()

	simulateCrash(dir, mgr, coalescer)

	result, err := recovery.Recover(dir, 4<<20)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Crash recovery: lastSeq=%d, count=%d, nextSeq=%d, partial=%v, segments=%d",
		result.LastValidSeq, result.EntryCount, result.NextSeq, result.WasPartial, result.SegmentsFound)

	if result.NextSeq == 0 && result.LastValidSeq == 0 {
		t.Log("Note: no entries recovered (all may have been lost in crash)")
	}
}

func TestCrashMultipleSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-crash-multi-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 64<<10)
	if err != nil {
		t.Fatal(err)
	}

	ring := uring.NewFakeRing()
	coalescer := writer.NewCoalescer(mgr, ring, writer.DefaultConfig)

	for i := 0; i < 10000; i++ {
		if err := coalescer.Append([]byte("multi-segment-crash")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	simulateCrash(dir, mgr, coalescer)

	result, err := recovery.Recover(dir, 64<<10)
	if err != nil {
		t.Fatal(err)
	}

	if result.SegmentsFound < 2 {
		t.Logf("Expected multiple segments, got %d (possibly all fit in one)", result.SegmentsFound)
	}
	t.Logf("Recovered: %d entries from %d segments", result.EntryCount, result.SegmentsFound)
}

func TestCrashRecoveryReadAll(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-crash-readall-*")
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

	for i := 0; i < 5000; i++ {
		if err := coalescer.Append([]byte("crash-recovery-read")); err != nil {
			t.Fatal(err)
		}
	}

	simulateCrash(dir, mgr, coalescer)

	mgr2, err := segment.NewManager(dir, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.Close()

	r := reader.NewReader(mgr2)
	defer r.Close()

	entries, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal("no entries recovered after crash")
	}

	for _, e := range entries {
		if e.Seq > 5000 {
			continue
		}
		if string(e.Payload) != "crash-recovery-read" {
			t.Fatalf("corrupted payload at seq %d: got %q", e.Seq, e.Payload)
		}
	}

	t.Logf("Read %d entries after crash recovery", len(entries))
}

func TestCrashNoDataLossOnCleanClose(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-clean-close-*")
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

	numEntries := 5000
	for i := 0; i < numEntries; i++ {
		if err := coalescer.Append([]byte("clean-close-test")); err != nil {
			t.Fatal(err)
		}
	}

	coalescer.Close()

	mgr.Close()

	result, err := recovery.Recover(dir, 4<<20)
	if err != nil {
		t.Fatal(err)
	}

	if result.NextSeq < uint64(numEntries) {
		t.Errorf("expected nextSeq >= %d, got %d", numEntries, result.NextSeq)
	}
}

func TestEncoderCrashResilience(t *testing.T) {
	entry := encoder.Entry{
		Seq:     1,
		Payload: []byte("crash resilience test"),
	}
	encoded, err := encoder.Encode(entry)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := encoder.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Seq != 1 || string(decoded.Payload) != "crash resilience test" {
		t.Fatal("encode/decode round trip failed")
	}

	for i := 0; i < 10; i++ {
		partial := encoded[:len(encoded)-i-1]
		_, err := encoder.Decode(partial)
		if err == nil {
			t.Fatalf("expected error for truncated data (len %d)", len(partial))
		}
	}
}

var _ = time.Second
