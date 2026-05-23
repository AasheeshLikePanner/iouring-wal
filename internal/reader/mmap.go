package reader

import (
	"os"
	"syscall"
)

func mmapFile(f *os.File, size int) ([]byte, error) {
	return syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmapFile(data []byte) error {
	return syscall.Munmap(data)
}
