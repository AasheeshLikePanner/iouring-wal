package uring

import "errors"

var (
	ErrRingFull       = errors.New("submission queue full")
	ErrNotSupported   = errors.New("io_uring not supported on this platform")
	ErrShutdown       = errors.New("ring is shut down")
)
