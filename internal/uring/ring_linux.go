//go:build linux

package uring

import (
	"math/bits"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	ioringSetupSQPoll      = 1 << 1
	ioringEnterGetEvents   = 1 << 0
	ioringEnterSQWakeup    = 1 << 1 // also SQWait in interrupt mode
	ioringFeatSingleMMap   = 1 << 0

	defaultSqpollIdle = 2000 // ms

	ioringOpWrite  = 2
	ioringOpWritev = 1
	ioringOpFsync  = 7

	sysIouringSetup uintptr = 425
	sysIouringEnter uintptr = 426
)

type ioUringSQOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv        [3]uint32
}

type ioUringCQOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv        [3]uint32
}

type ioUringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFd         uint32
	resv         [3]uint32
	sqOff        ioUringSQOffsets
	cqOff        ioUringCQOffsets
}

// Matches Linux kernel struct io_uring_sqe (64 bytes on arm64).
// Layout verified against include/uapi/linux/io_uring.h.
type ioUringSQE struct {
	OpCode      uint8    // 0
	Flags       uint8    // 1
	Ioprio      uint16   // 2
	Fd          int32    // 4
	Off         uint64   // 8
	Addr        uint64   // 16
	Len         uint32   // 24
	OpcodeFlags uint32   // 28
	UserData    uint64   // 32
	BufIG       uint16   // 40
	_pad1       uint16   // 42
	_pad2       uint32   // 44
	Personality uint64   // 48
	SpliceFdIn  int32    // 56
	_pad3       int32    // 60
	// Total: 64 bytes
}

type ioUringCQE struct {
	UserData uint64
	Res      int32
	Flags    uint32
}

type ringMmaps struct {
	sqRing []byte
	cqRing []byte
	sqes   []byte
	single bool
}

type LinuxRing struct {
	ringFd    int
	params    ioUringParams
	mmaps     ringMmaps

	sqHead  *uint32
	sqTail  *uint32
	sqMask  *uint32
	sqFlags *uint32
	sqArray *uint32
	sqesPtr unsafe.Pointer

	cqHead   *uint32
	cqTail   *uint32
	cqMask   *uint32
	cqes     unsafe.Pointer
	cqeSz    uintptr

	closeOnce sync.Once
	closed    bool
	mu        sync.Mutex

	sqeTail          uint32
	submissionsPending uint32
}

func NewRing(entries uint32) (*LinuxRing, error) {
	if entries == 0 {
		entries = 4096
	}
	entries = roundupPow2(entries)
	r, err := setupRing(entries, 0)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func NewRingWithSQPoll(entries uint32) (*LinuxRing, error) {
	if entries == 0 {
		entries = 4096
	}
	entries = roundupPow2(entries)
	return setupRingWithIdle(entries, ioringSetupSQPoll, defaultSqpollIdle)
}

func setupRingWithIdle(entries uint32, flags uint32, idleMs uint32) (*LinuxRing, error) {
	p := &ioUringParams{}
	p.flags = flags
	p.sqThreadIdle = idleMs

	fd, _, errno := syscall.Syscall(sysIouringSetup, uintptr(entries), uintptr(unsafe.Pointer(p)), 0)
	if errno != 0 {
		return nil, errno
	}
	return initRing(int(fd), p)
}

func roundupPow2(v uint32) uint32 {
	if v&(v-1) == 0 {
		return v
	}
	return 1 << (32 - bits.LeadingZeros32(v))
}

func setupRing(entries uint32, flags uint32) (*LinuxRing, error) {
	p := &ioUringParams{}
	p.flags = flags

	fd, _, errno := syscall.Syscall(sysIouringSetup, uintptr(entries), uintptr(unsafe.Pointer(p)), 0)
	if errno != 0 {
		return nil, errno
	}
	return initRing(int(fd), p)
}

func initRing(fd int, p *ioUringParams) (*LinuxRing, error) {
	r := &LinuxRing{
		ringFd: fd,
		params: *p,
	}

	sqOff := &p.sqOff
	cqOff := &p.cqOff

	sqeSize := unsafe.Sizeof(ioUringSQE{})

	sqRingSz := int(uintptr(sqOff.array) + uintptr(p.sqEntries)*4)

	var sqMap, cqMap []byte
	var sqeMap []byte
	var err error

	sqMap, err = syscall.Mmap(r.ringFd, 0, sqRingSz, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		syscall.Close(r.ringFd)
		return nil, err
	}

	if p.features&ioringFeatSingleMMap != 0 {
		cqMap = sqMap
		r.mmaps.single = true
	} else {
		cqRingSz := int(uintptr(cqOff.cqes) + uintptr(p.cqEntries)*uintptr(unsafe.Sizeof(ioUringCQE{})))
		cqMap, err = syscall.Mmap(r.ringFd, 0x8000000, cqRingSz, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
		if err != nil {
			syscall.Munmap(sqMap)
			syscall.Close(r.ringFd)
			return nil, err
		}
	}

	sqeMapSz := int(sqeSize * uintptr(p.sqEntries))
	sqeMap, err = syscall.Mmap(r.ringFd, 0x10000000, sqeMapSz, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		syscall.Munmap(sqMap)
		if !r.mmaps.single {
			syscall.Munmap(cqMap)
		}
		syscall.Close(r.ringFd)
		return nil, err
	}

	r.mmaps.sqRing = sqMap
	r.mmaps.cqRing = cqMap
	r.mmaps.sqes = sqeMap

	r.sqHead = (*uint32)(unsafe.Pointer(&sqMap[sqOff.head]))
	r.sqTail = (*uint32)(unsafe.Pointer(&sqMap[sqOff.tail]))
	r.sqMask = (*uint32)(unsafe.Pointer(&sqMap[sqOff.ringMask]))
	r.sqFlags = (*uint32)(unsafe.Pointer(&sqMap[sqOff.flags]))
	r.sqArray = (*uint32)(unsafe.Pointer(&sqMap[sqOff.array]))
	r.sqesPtr = unsafe.Pointer(unsafe.SliceData(sqeMap))

	r.cqHead = (*uint32)(unsafe.Pointer(&cqMap[cqOff.head]))
	r.cqTail = (*uint32)(unsafe.Pointer(&cqMap[cqOff.tail]))
	r.cqMask = (*uint32)(unsafe.Pointer(&cqMap[cqOff.ringMask]))
	r.cqes = unsafe.Pointer(&cqMap[cqOff.cqes])
	r.cqeSz = unsafe.Sizeof(ioUringCQE{})

	return r, nil
}

func (r *LinuxRing) SubmitWrite(file *os.File, buf []byte, offset int64, userdata uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrShutdown
	}

	sqe := r.getSQE()
	if sqe == nil {
		return ErrRingFull
	}

	sqe.OpCode = ioringOpWrite
	sqe.Fd = int32(file.Fd())
	if len(buf) > 0 {
		sqe.Addr = uint64(uintptr(unsafe.Pointer(&buf[0])))
	} else {
		sqe.Addr = 0
	}
	sqe.Len = uint32(len(buf))
	sqe.Off = uint64(offset)
	sqe.UserData = userdata

	r.submitPending()
	return nil
}

func (r *LinuxRing) SubmitWritev(file *os.File, iovecs []syscall.Iovec, offset int64, userdata uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrShutdown
	}

	sqe := r.getSQE()
	if sqe == nil {
		return ErrRingFull
	}

	sqe.OpCode = ioringOpWritev
	sqe.Fd = int32(file.Fd())
	sqe.Addr = uint64(uintptr(unsafe.Pointer(&iovecs[0])))
	sqe.Len = uint32(len(iovecs))
	sqe.Off = uint64(offset)
	sqe.UserData = userdata

	r.submitPending()
	return nil
}

func (r *LinuxRing) SubmitFsync(file *os.File, userdata uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrShutdown
	}

	sqe := r.getSQE()
	if sqe == nil {
		return ErrRingFull
	}

	sqe.OpCode = ioringOpFsync
	sqe.Fd = int32(file.Fd())
	sqe.UserData = userdata

	r.submitPending()
	return nil
}

func (r *LinuxRing) HarvestCompletions(timeout time.Duration) []Completion {
	var completions []Completion
	deadline := time.Now().Add(timeout)
	sqpoll := r.params.flags&ioringSetupSQPoll != 0

	for {
		head := atomic.LoadUint32(r.cqHead)
		tail := atomic.LoadUint32(r.cqTail)

		if head == tail {
			if len(completions) > 0 || time.Now().After(deadline) {
				return completions
			}
			flags := uint32(ioringEnterGetEvents)
			if sqpoll {
				flags |= uint32(ioringEnterSQWakeup)
			}
			r.enter(0, 1, flags)
			continue
		}

		mask := *r.cqMask
		idx := head & mask
		cqe := (*ioUringCQE)(unsafe.Add(r.cqes, uintptr(idx)*r.cqeSz))

		c := Completion{
			Userdata: cqe.UserData,
			Result:   cqe.Res,
		}
		if cqe.Res < 0 {
			c.Err = syscall.Errno(-cqe.Res)
		}
		completions = append(completions, c)

		atomic.StoreUint32(r.cqHead, head+1)
	}
}

func (r *LinuxRing) getSQE() *ioUringSQE {
	head := atomic.LoadUint32(r.sqHead)
	if r.sqeTail-head >= *r.sqMask+1 {
		return nil
	}
	mask := *r.sqMask
	idx := r.sqeTail & mask

	*(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(r.sqArray)) + uintptr(idx)*4)) = idx

	r.sqeTail++
	r.submissionsPending++

	return (*ioUringSQE)(unsafe.Add(r.sqesPtr, uintptr(idx)*unsafe.Sizeof(ioUringSQE{})))
}

func (r *LinuxRing) submitPending() {
	if r.submissionsPending == 0 {
		return
	}
	atomic.StoreUint32(r.sqTail, r.sqeTail)
	n := r.submissionsPending
	r.submissionsPending = 0

	if r.params.flags&ioringSetupSQPoll != 0 {
		// Always wake SQPOLL kernel thread so it processes SQEs
		// immediately. If we skip the wakeup (rely on thread polling),
		// the kernel thread's cond_resched() interval adds 1-4ms latency
		// per batch on contended systems.
		r.enter(0, 0, ioringEnterSQWakeup)
	} else {
		r.enter(n, 0, 0)
	}
}

func (r *LinuxRing) enter(submitted, waitNr, flags uint32) {
	syscall.Syscall6(sysIouringEnter, uintptr(r.ringFd), uintptr(submitted), uintptr(waitNr), uintptr(flags), 0, 0)
}

func (r *LinuxRing) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()

		syscall.Munmap(r.mmaps.sqes)
		if !r.mmaps.single {
			syscall.Munmap(r.mmaps.cqRing)
		}
		syscall.Munmap(r.mmaps.sqRing)
		syscall.Close(r.ringFd)
	})
	return nil
}
