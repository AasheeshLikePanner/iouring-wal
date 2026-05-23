package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"urlog/internal/recovery"
	"urlog/internal/segment"
	"urlog/internal/uring"
	"urlog/internal/writer"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: urlog <dir> [ops]\n")
		os.Exit(1)
	}

	dir := os.Args[1]
	ops := 100000
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &ops)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("create dir: %v", err)
	}

	mgr, err := segment.NewManager(dir, 128<<20)
	if err != nil {
		log.Fatalf("create manager: %v", err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	defer ring.Close()

	cfg := writer.DefaultConfig
	cfg.MaxBatch = 64
	cfg.MaxBatchSize = 1 << 20
	cfg.MaxWait = 100 * time.Microsecond

	coalescer := writer.NewCoalescer(mgr, ring, cfg)
	defer coalescer.Close()

	fmt.Printf("Writing %d entries to %s...\n", ops, dir)

	var written atomic.Uint64
	payload := make([]byte, 256)
	_, _ = rand.Read(payload)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	start := time.Now()
	done := make(chan struct{})

	go func() {
		for i := 0; i < ops; i++ {
			if err := coalescer.Append(payload); err != nil {
				log.Printf("append error at %d: %v", i, err)
				break
			}
			written.Add(1)
			if (i+1)%10000 == 0 {
				fmt.Printf("  %d entries written...\n", i+1)
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-sigCh:
		fmt.Println("\nInterrupted, shutting down...")
	}

	elapsed := time.Since(start)
	n := written.Load()
	rate := float64(n) / elapsed.Seconds()

	fmt.Printf("\nResults:\n")
	fmt.Printf("  Entries written: %d\n", n)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Throughput: %.0f ops/sec\n", rate)
	fmt.Printf("  Next sequence: %d\n", mgr.NextSeq())
	fmt.Printf("  Segments: %d\n", len(mgr.AllSegments()))

	fmt.Println("\nRunning recovery check...")
	result, err := recovery.Recover(dir, 128<<20)
	if err != nil {
		log.Printf("Recovery error: %v", err)
	} else {
		fmt.Printf("  Segments found: %d\n", result.SegmentsFound)
		fmt.Printf("  Last valid seq: %d\n", result.LastValidSeq)
		fmt.Printf("  Entry count: %d\n", result.EntryCount)
		fmt.Printf("  Was partial: %v\n", result.WasPartial)
		fmt.Printf("  Next seq: %d\n", result.NextSeq)
	}
}
