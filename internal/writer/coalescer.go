package writer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"syscall"
	"time"

	"urlog/internal/encoder"
	"urlog/internal/segment"
	"urlog/internal/uring"
)

var (
	ErrLogClosed = errors.New("log is closed")
	ErrDiskFull  = errors.New("disk full")
)

type pendingEntry struct {
	seq     uint64
	payload []byte
}

type batch struct {
	entries []pendingEntry
	size    int
	mu      sync.Mutex
	cond    *sync.Cond
	err     error
	ready   bool
}

func newBatch(capacity int) *batch {
	b := &batch{
		entries: make([]pendingEntry, 0, capacity),
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

type Coalescer struct {
	manager *segment.Manager
	ring    uring.Ring

	mu          sync.Mutex
	active      *batch
	closed      bool

	flushChan   chan struct{}
	done        chan struct{}
	flusherDone chan struct{}

	maxBatch     int
	maxBatchSize int
	maxWait      time.Duration

	lastUserdata uint64
	userdataMu   sync.Mutex
}

type Config struct {
	MaxBatch     int
	MaxBatchSize int
	MaxWait      time.Duration
}

var DefaultConfig = Config{
	MaxBatch:     64,
	MaxBatchSize: 1 << 20,
	MaxWait:      10 * time.Microsecond,
}

func NewCoalescer(mgr *segment.Manager, ring uring.Ring, cfg Config) *Coalescer {
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = DefaultConfig.MaxBatch
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = DefaultConfig.MaxBatchSize
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = DefaultConfig.MaxWait
	}

	c := &Coalescer{
		manager:      mgr,
		ring:         ring,
		active:       newBatch(cfg.MaxBatch),
		maxBatch:     cfg.MaxBatch,
		maxBatchSize: cfg.MaxBatchSize,
		maxWait:      cfg.MaxWait,
		flushChan:    make(chan struct{}, 1),
		done:         make(chan struct{}),
		flusherDone:  make(chan struct{}),
	}

	go c.flusherLoop()
	return c
}

func mapError(err error) error {
	if err != nil && errors.Is(err, syscall.ENOSPC) {
		return ErrDiskFull
	}
	return err
}

func (c *Coalescer) Append(payload []byte) error {
	if len(payload) > encoder.MaxBodySize {
		return encoder.ErrBodyTooLarge
	}

	seq := c.manager.NextSeq()
	c.manager.AdvanceSeq()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrLogClosed
	}

	c.active.entries = append(c.active.entries, pendingEntry{
		seq:     seq,
		payload: payload,
	})
	c.active.size += len(payload) + encoder.HeaderSize

	// Signal flusher to collect this batch.
	select {
	case c.flushChan <- struct{}{}:
	default:
	}

	b := c.active
	c.mu.Unlock()

	b.mu.Lock()
	for !b.ready {
		b.cond.Wait()
	}
	err := b.err
	b.mu.Unlock()
	return mapError(err)
}

func (c *Coalescer) flusherLoop() {
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		select {
		case <-c.flushChan:
			c.flushCurrent()
			timer.Reset(c.maxWait)

			select {
			case <-timer.C:
			case <-c.done:
				if !timer.Stop() {
					<-timer.C
				}
				c.flushRemaining()
				close(c.flusherDone)
				return
			}

		case <-c.done:
			c.flushRemaining()
			close(c.flusherDone)
			return
		}
	}
}

func (c *Coalescer) flushCurrent() {
	c.mu.Lock()
	if len(c.active.entries) == 0 {
		c.mu.Unlock()
		return
	}
	b := c.active
	c.active = newBatch(c.maxBatch)
	c.mu.Unlock()

	err := c.submitBatch(b)

	b.mu.Lock()
	b.err = err
	b.ready = true
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (c *Coalescer) flushRemaining() {
	c.mu.Lock()
	if len(c.active.entries) > 0 {
		b := c.active
		c.active = newBatch(c.maxBatch)
		c.mu.Unlock()

		err := c.submitBatch(b)
		b.mu.Lock()
		b.err = err
		b.ready = true
		b.cond.Broadcast()
		b.mu.Unlock()
	} else {
		c.mu.Unlock()
	}
}

func (c *Coalescer) submitBatch(b *batch) error {
	seg := c.manager.ActiveSegment()
	if seg == nil {
		return segment.ErrNoActiveSegment
	}

	totalSize := b.size
	if seg.WriteOffset+int64(totalSize)+int64(segment.TrailerSize) >= seg.Size {
		if err := c.manager.Rotate(); err != nil {
			return fmt.Errorf("rotate: %w", err)
		}
		seg = c.manager.ActiveSegment()
		if seg == nil {
			return segment.ErrNoActiveSegment
		}
	}

	writeOffset := seg.WriteOffset

	iovecs := make([]syscall.Iovec, 0, len(b.entries))
	totalBytes := 0

	for _, p := range b.entries {
		buf := make([]byte, encoder.HeaderSize+len(p.payload))
		crc := crc32.ChecksumIEEE(p.payload)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(p.payload)))
		binary.BigEndian.PutUint64(buf[4:12], p.seq)
		binary.BigEndian.PutUint32(buf[12:16], crc)
		copy(buf[16:], p.payload)

		iovecs = append(iovecs, syscall.Iovec{
			Base: &buf[0],
			Len:  uint64(len(buf)),
		})
		totalBytes += len(buf)
	}

	userdata := c.nextUserdata()

	if err := c.ring.SubmitWritev(seg.File, iovecs, writeOffset, userdata); err != nil {
		return fmt.Errorf("submit writev: %w", err)
	}

	completions := c.ring.HarvestCompletions(5 * time.Second)

	var compErr error
	found := false
	for _, comp := range completions {
		if comp.Userdata == userdata {
			found = true
			if comp.Err != nil {
				compErr = fmt.Errorf("io_uring write failed: %w", comp.Err)
			} else if comp.Result <= 0 {
				compErr = errors.New("io_uring write returned zero bytes")
			}
			break
		}
	}
	if !found {
		compErr = errors.New("io_uring completion not received for batch")
	}
	if compErr != nil {
		return compErr
	}

	seg.Mu.Lock()
	if writeOffset+int64(totalBytes) > seg.WriteOffset {
		seg.WriteOffset = writeOffset + int64(totalBytes)
	}
	seg.Mu.Unlock()

	for range b.entries {
		c.manager.IncrementEntryCount()
	}

	return nil
}

func (c *Coalescer) nextUserdata() uint64 {
	c.userdataMu.Lock()
	defer c.userdataMu.Unlock()
	c.lastUserdata++
	return c.lastUserdata
}

func (c *Coalescer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	close(c.done)
	<-c.flusherDone
	return nil
}
