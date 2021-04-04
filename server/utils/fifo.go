package utils

import (
	"log"
	"os"
	"syscall"
)

func MakeFifo(filePath string, bufSize int) {
	syscall.Mkfifo(filePath, 0666)
	// For some reason named pipes don't come out the permissions we want
	os.Chmod(filePath, 0666)

	// Now bump the pipe size to the buffer we want
	fd, ferr := os.OpenFile(filePath, syscall.O_RDONLY|syscall.O_NONBLOCK, 0666)
	if ferr == nil {
		defer fd.Close()
		syscall.RawSyscall(
			syscall.SYS_FCNTL,
			fd.Fd(),
			syscall.F_SETPIPE_SZ,
			uintptr(bufSize),
		)
	} else {
		log.Printf("Failed to open fifo [%s]: %s", filePath, ferr)
	}
}
