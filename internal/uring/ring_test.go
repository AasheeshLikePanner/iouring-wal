package uring

import (
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestUR1_SingleWriteCompletes(t *testing.T) {
	ring := NewFakeRing()
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-uring-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	data := []byte("hello")
	if err := ring.SubmitWrite(f, data, 0, 1); err != nil {
		t.Fatal(err)
	}

	completions := ring.HarvestCompletions(time.Second)
	if len(completions) != 1 {
		t.Fatalf("expected 1 completion, got %d", len(completions))
	}

	if completions[0].Err != nil {
		t.Fatalf("completion error: %v", completions[0].Err)
	}
	if completions[0].Userdata != 1 {
		t.Fatalf("expected userdata 1, got %d", completions[0].Userdata)
	}

	readBuf := make([]byte, len(data))
	if _, err := f.ReadAt(readBuf, 0); err != nil {
		t.Fatal(err)
	}
	if string(readBuf) != "hello" {
		t.Fatalf("expected 'hello', got '%s'", readBuf)
	}
}

func TestUR2_BatchedSubmission(t *testing.T) {
	ring := NewFakeRing()
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-batch-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	numWrites := 1000
	for i := 0; i < numWrites; i++ {
		data := []byte{byte(i)}
		if err := ring.SubmitWrite(f, data, int64(i), uint64(i)); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	completions := ring.HarvestCompletions(time.Second)
	if len(completions) != numWrites {
		t.Fatalf("expected %d completions, got %d", numWrites, len(completions))
	}

	successCount := 0
	for _, c := range completions {
		if c.Err != nil {
			t.Errorf("completion %d failed: %v", c.Userdata, c.Err)
		} else {
			successCount++
		}
	}
	if successCount != numWrites {
		t.Fatalf("only %d/%d succeeded", successCount, numWrites)
	}

	for i := 0; i < numWrites; i++ {
		readBuf := make([]byte, 1)
		f.ReadAt(readBuf, int64(i))
		if readBuf[0] != byte(i) {
			t.Fatalf("at offset %d: expected %d, got %d", i, byte(i), readBuf[0])
		}
	}
}

func TestUR3_SQRingOverflow(t *testing.T) {
	ring := NewFakeRing()
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-overflow-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	for i := 0; i < 100; i++ {
		if err := ring.SubmitWrite(f, []byte("test"), 0, uint64(i)); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	completions := ring.HarvestCompletions(time.Second)
	if len(completions) != 100 {
		t.Fatalf("expected 100 completions, got %d", len(completions))
	}
}

func TestUR5_CleanupOnShutdown(t *testing.T) {
	ring := NewFakeRing()

	f, err := os.CreateTemp("", "urlog-cleanup-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	for i := 0; i < 100; i++ {
		ring.SubmitWrite(f, []byte("x"), int64(i), uint64(i))
	}

	if err := ring.Close(); err != nil {
		t.Fatal(err)
	}

	if err := ring.SubmitWrite(f, []byte("x"), 0, 200); err != ErrShutdown {
		t.Fatalf("expected ErrShutdown, got %v", err)
	}

	f.Close()
}

func TestFakeRingWritev(t *testing.T) {
	ring := NewFakeRing()
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-writev-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	buf1 := []byte("hello ")
	buf2 := []byte("world")

	iovecs := []syscall.Iovec{
		{Base: &buf1[0], Len: uint64(len(buf1))},
		{Base: &buf2[0], Len: uint64(len(buf2))},
	}

	if err := ring.SubmitWritev(f, iovecs, 0, 42); err != nil {
		t.Fatal(err)
	}

	completions := ring.HarvestCompletions(time.Second)
	if len(completions) != 1 {
		t.Fatalf("expected 1 completion, got %d", len(completions))
	}
	if completions[0].Err != nil {
		t.Fatalf("completion error: %v", completions[0].Err)
	}
	if completions[0].Userdata != 42 {
		t.Fatalf("expected userdata 42, got %d", completions[0].Userdata)
	}
	if completions[0].Result != 11 {
		t.Fatalf("expected result 11, got %d", completions[0].Result)
	}

	readBuf := make([]byte, 11)
	f.ReadAt(readBuf, 0)
	if string(readBuf) != "hello world" {
		t.Fatalf("expected 'hello world', got '%s'", readBuf)
	}
}

func TestFakeRingFsync(t *testing.T) {
	ring := NewFakeRing()
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-fsync-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	f.Write([]byte("data"))
	if err := ring.SubmitFsync(f, 1); err != nil {
		t.Fatal(err)
	}

	completions := ring.HarvestCompletions(time.Second)
	if len(completions) != 1 {
		t.Fatalf("expected 1 completion, got %d", len(completions))
	}
	if completions[0].Err != nil {
		t.Fatalf("fsync error: %v", completions[0].Err)
	}
}

func TestFakeRingDelay(t *testing.T) {
	ring := NewFakeRing()
	ring.Delay = 10 * time.Millisecond
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-delay-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	ring.SubmitWrite(f, []byte("delayed"), 0, 1)

	start := time.Now()
	completions := ring.HarvestCompletions(time.Second)
	elapsed := time.Since(start)

	if elapsed < 10*time.Millisecond {
		t.Fatalf("expected at least 10ms delay, got %v", elapsed)
	}
	if len(completions) != 1 {
		t.Fatalf("expected 1 completion, got %d", len(completions))
	}
}

func TestFakeRingConcurrentSubmissions(t *testing.T) {
	ring := NewFakeRing()
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-concur-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := []byte{byte(id)}
			ring.SubmitWrite(f, data, int64(id), uint64(id))
		}(i)
	}
	wg.Wait()

	completions := ring.HarvestCompletions(time.Second)
	if len(completions) != 50 {
		t.Fatalf("expected 50 completions, got %d", len(completions))
	}
}

func TestFakeRingWriteHook(t *testing.T) {
	ring := NewFakeRing()
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-hook-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	hookCalled := false
	ring.WriteHook = func(file *os.File, buf []byte, offset int64) (int, error) {
		hookCalled = true
		return len(buf), nil
	}

	ring.SubmitWrite(f, []byte("hook"), 0, 1)
	ring.HarvestCompletions(time.Second)

	if !hookCalled {
		t.Fatal("WriteHook was not called")
	}
}

func TestFakeRingCloseTwice(t *testing.T) {
	ring := NewFakeRing()
	if err := ring.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ring.Close(); err != nil {
		t.Fatal("closing twice should not error")
	}
}
