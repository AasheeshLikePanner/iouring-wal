package uring

import (
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type Completion struct {
	Userdata uint64
	Result   int32
	Err      error
}

type OpType int

const (
	OpWrite OpType = iota
	OpWritev
	OpFsync
)

type PendingOp struct {
	Type     OpType
	Userdata uint64
	FD       int
	Buf      []byte
	Iovecs   []syscall.Iovec
	Offset   int64
}

type Ring interface {
	SubmitWrite(file *os.File, buf []byte, offset int64, userdata uint64) error
	SubmitWritev(file *os.File, iovecs []syscall.Iovec, offset int64, userdata uint64) error
	SubmitFsync(file *os.File, userdata uint64) error
	HarvestCompletions(timeout time.Duration) []Completion
	Close() error
}

type FakeRing struct {
	mu         sync.Mutex
	pending    []PendingOp
	Delay      time.Duration
	WriteHook  func(file *os.File, buf []byte, offset int64) (int, error)
	closed     bool
}

func NewFakeRing() *FakeRing {
	return &FakeRing{}
}

func (f *FakeRing) SubmitWrite(file *os.File, buf []byte, offset int64, userdata uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrShutdown
	}
	f.pending = append(f.pending, PendingOp{
		Type:     OpWrite,
		Userdata: userdata,
		FD:       int(file.Fd()),
		Buf:      append([]byte(nil), buf...),
		Offset:   offset,
	})
	return nil
}

func (f *FakeRing) SubmitWritev(file *os.File, iovecs []syscall.Iovec, offset int64, userdata uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrShutdown
	}
	iovecsCopy := make([]syscall.Iovec, len(iovecs))
	copy(iovecsCopy, iovecs)
	f.pending = append(f.pending, PendingOp{
		Type:     OpWritev,
		Userdata: userdata,
		FD:       int(file.Fd()),
		Iovecs:   iovecsCopy,
		Offset:   offset,
	})
	return nil
}

func (f *FakeRing) SubmitFsync(file *os.File, userdata uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrShutdown
	}
	f.pending = append(f.pending, PendingOp{
		Type:     OpFsync,
		Userdata: userdata,
		FD:       int(file.Fd()),
	})
	return nil
}

func (f *FakeRing) HarvestCompletions(timeout time.Duration) []Completion {
	f.mu.Lock()
	pending := f.pending
	f.pending = nil
	f.mu.Unlock()

	if f.Delay > 0 {
		time.Sleep(f.Delay)
	}

	completions := make([]Completion, 0, len(pending))
	for _, op := range pending {
		c := Completion{Userdata: op.Userdata}
		switch op.Type {
		case OpWritev:
			var total int
			for _, iv := range op.Iovecs {
				buf := unsafe.Slice(iv.Base, iv.Len)
				n, err := syscall.Pwrite(op.FD, buf, op.Offset+int64(total))
				if err != nil {
					c.Err = err
					c.Result = -1
					break
				}
				total += n
				if n != len(buf) {
					c.Err = syscall.EAGAIN
					c.Result = -1
					break
				}
			}
			if c.Err == nil {
				c.Result = int32(total)
			}
		case OpWrite:
			if f.WriteHook != nil {
				file := os.NewFile(uintptr(op.FD), "")
				n, err := f.WriteHook(file, op.Buf, op.Offset)
				file.Close()
				c.Result = int32(n)
				c.Err = err
			} else {
				n, err := syscall.Pwrite(op.FD, op.Buf, op.Offset)
				c.Result = int32(n)
				c.Err = err
			}
		case OpFsync:
			err := syscall.Fsync(op.FD)
			if err != nil {
				c.Err = err
				c.Result = -1
			} else {
				c.Result = 0
			}
		}
		completions = append(completions, c)
	}
	return completions
}

func (f *FakeRing) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	f.pending = nil
	return nil
}
