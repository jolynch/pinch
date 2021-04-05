package utils

import (
	"log"
	"os"
	"path"
	"syscall"
)

func MakeFifo(filePath string, bufSize int) *os.File {
	syscall.Mkfifo(filePath, 0666)
	// For some reason named pipes don't come out the permissions we want
	os.Chmod(filePath, 0666)

	// Now bump the pipe size to the buffer we want
	fd, ferr := os.OpenFile(filePath, syscall.O_RDONLY|syscall.O_NONBLOCK, 0666)
	if ferr == nil {
		_, _, errno := syscall.RawSyscall(
			syscall.SYS_FCNTL,
			fd.Fd(),
			syscall.F_SETPIPE_SZ,
			uintptr(bufSize),
		)
		if errno != 0 {
			log.Printf("[fifo] Failed to raise [%s] buffer size - %s", filePath, errno)
		} else {
			log.Printf("[fifo] Succeeded at raising [%s] buffer size to %d", filePath, bufSize)
		}
	} else {
		log.Printf("[fifo] Failed to open fifo [%s]: %s", filePath, ferr)
		return nil
	}
	return fd
}

func MakeFifoPair(inDir, outDir, handle string, bufSize int) FifoPair {
	return FifoPair{
		Handle:  handle,
		In:      MakeFifo(path.Join(inDir, handle), bufSize),
		InPath:  path.Join(inDir, handle),
		Out:     MakeFifo(path.Join(outDir, handle), bufSize),
		OutPath: path.Join(outDir, handle),
	}
}

type FifoPair struct {
	Handle  string
	In      *os.File
	InPath  string
	Out     *os.File
	OutPath string
}

func (fp FifoPair) Close() {
	log.Printf("[%s][fifo] Closing", fp.Handle)
	os.Remove(fp.InPath)
	os.Remove(fp.OutPath)
	if fp.In != nil {
		fp.In.Close()
	}
	if fp.Out != nil {
		fp.Out.Close()
	}
}
