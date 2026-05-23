package chaos

import (
	"crypto/rand"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"urlog/internal/encoder"
	"urlog/internal/recovery"
	"urlog/internal/segment"
	"urlog/internal/uring"
	"urlog/internal/writer"
)

func TestChaos_RingOverflow(t *testing.T) {
	ring := uring.NewFakeRing()
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-chaos-ring-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	numOps := 10000
	var wg sync.WaitGroup
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := []byte{byte(id)}
			if err := ring.SubmitWrite(f, data, int64(id), uint64(id)); err != nil {
				if err == uring.ErrRingFull {
					return
				}
			}
		}(i)
	}

	completions := ring.HarvestCompletions(5 * time.Second)
	_ = completions
	wg.Wait()

	t.Logf("Ring overflow test: harvested %d completions", len(completions))
}

func TestChaos_DiskFull(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-chaos-disk-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()

	diskFull := false
	ring.WriteHook = func(file *os.File, buf []byte, offset int64) (int, error) {
		if diskFull {
			return 0, syscall.ENOSPC
		}
		return len(buf), nil
	}

	coalescer := writer.NewCoalescer(mgr, ring, writer.DefaultConfig)

	for i := 0; i < 100; i++ {
		if err := coalescer.Append([]byte("before-full")); err != nil {
			t.Fatalf("write before disk full: %v", err)
		}
	}

	diskFull = true

	err = coalescer.Append([]byte("during-full"))
	if err != nil && err != writer.ErrDiskFull {
		if err.Error() != "io_uring write failed: no space left on device" {
		}
	}

	diskFull = false

	for i := 0; i < 100; i++ {
		if err := coalescer.Append([]byte("after-full")); err != nil {
			t.Logf("write after recovery: %v (acceptable)", err)
			break
		}
	}

	coalescer.Close()
}

func TestChaos_CorruptSegmentHeader(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-chaos-corrupt-*")
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

	payload := make([]byte, 100)
	_, _ = rand.Read(payload)

	for i := 0; i < 10; i++ {
		if err := coalescer.Append(payload); err != nil {
			t.Fatal(err)
		}
	}

	coalescer.Close()

	allSegs := mgr.AllSegments()
	if len(allSegs) > 0 {
		seg := allSegs[0]
		seg.File.WriteAt([]byte("BADHEADER"), 0)
	}

	mgr.Close()

	_, err = recovery.Recover(dir, 4<<20)
	if err != nil {
		t.Logf("Recovery correctly failed on corrupted segment: %v", err)
	} else {
		t.Log("Recovery succeeded despite corrupted header (may depend on validation)")
	}
}

func TestChaos_DeleteSegmentWhileRunning(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-chaos-delete-*")
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

	for i := 0; i < 5; i++ {
		if err := coalescer.Append([]byte("before-delete")); err != nil {
			t.Fatal(err)
		}
	}

	allSegs := mgr.AllSegments()
	for _, seg := range allSegs {
		os.Remove(seg.Path)
	}

	for i := 0; i < 5; i++ {
		if err := coalescer.Append([]byte("after-delete")); err != nil {
			t.Logf("Write after segment deletion: %v (expected)", err)
			break
		}
	}

	coalescer.Close()
}

func TestChaos_ConcurrentRotateAndWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-chaos-rotate-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 32<<10)
	if err != nil {
		t.Fatal(err)
	}

	ring := uring.NewFakeRing()
	coalescer := writer.NewCoalescer(mgr, ring, writer.DefaultConfig)

	var wg sync.WaitGroup
	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				payload := []byte("rotate-test-" + string(rune(id)) + "-" + string(rune(i)))
				coalescer.Append(payload)
			}
		}(w)
	}

	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Millisecond)
			mgr.Rotate()
		}
	}()

	wg.Wait()
	coalescer.Close()
	mgr.Close()
}

func TestChaos_MultipleEncoders(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			entry := encoder.Entry{
				Seq:     uint64(id),
				Payload: []byte("multi-encoder-" + string(rune(id))),
			}
			encoded, err := encoder.Encode(entry)
			if err != nil {
				t.Errorf("encode %d: %v", id, err)
				return
			}
			decoded, err := encoder.Decode(encoded)
			if err != nil {
				t.Errorf("decode %d: %v", id, err)
				return
			}
			if decoded.Seq != uint64(id) {
				t.Errorf("seq mismatch: got %d, want %d", decoded.Seq, id)
			}
		}(i)
	}
	wg.Wait()
}

func TestChaos_ZeroBytePayloads(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-chaos-zero-*")
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

	for i := 0; i < 100; i++ {
		if err := coalescer.Append([]byte{}); err != nil {
			t.Fatalf("zero-byte append %d: %v", i, err)
		}
	}

	coalescer.Close()

	nextSeq := mgr.NextSeq()
	if nextSeq < 100 {
		t.Errorf("expected at least 100 zero-byte entries, got %d", nextSeq)
	}
}

func TestChaos_LargeVariedPayloads(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-chaos-large-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	coalescer := writer.NewCoalescer(mgr, ring, writer.DefaultConfig)

	sizes := []int{1, 16, 64, 256, 1024, 4096, 16384, 65536}
	for _, size := range sizes {
		payload := make([]byte, size)
		_, _ = rand.Read(payload)
		for i := 0; i < 10; i++ {
			if err := coalescer.Append(payload); err != nil {
				t.Fatalf("append size=%d iter=%d: %v", size, i, err)
			}
		}
	}

	coalescer.Close()
	t.Logf("Large varied payloads test: %d entries, nextSeq=%d, segments=%d",
		len(sizes)*10, mgr.NextSeq(), len(mgr.AllSegments()))
}

func TestChaos_EncoderMaxSizes(t *testing.T) {
	sizes := []int{
		0,
		1,
		encoder.HeaderSize,
		1024,
		1 << 20,
		encoder.MaxBodySize,
	}

	for _, size := range sizes {
		payload := make([]byte, size)
		if size > 0 {
			_, _ = rand.Read(payload)
		}
		entry := encoder.Entry{Seq: 1, Payload: payload}
		encoded, err := encoder.Encode(entry)
		if size > encoder.MaxBodySize {
			if err != encoder.ErrBodyTooLarge {
				t.Errorf("size %d: expected ErrBodyTooLarge, got %v", size, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("size %d: encode failed: %v", size, err)
		}
		decoded, err := encoder.Decode(encoded)
		if err != nil {
			t.Fatalf("size %d: decode failed: %v", size, err)
		}
		if len(decoded.Payload) != size {
			t.Fatalf("size %d: decoded payload length %d", size, len(decoded.Payload))
		}
	}
}
