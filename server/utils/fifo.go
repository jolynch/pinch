package utils

import (
	"log"
	"os"
	"path"
	"syscall"

	"github.com/jolynch/pinch/state"
)

func makeFifo(filePath string) string {
	syscall.Mkfifo(filePath, 0666)
	// For some reason named pipes don't come out the permissions we want
	os.Chmod(filePath, 0666)
	return filePath
}

func openToSetFifoSize(filePath string, flag int, bufSize int) *os.File {
	// Pipe sizes are only held larger than the default 64KiB if the
	// file is held open on the FD that called FCNTL.

	fd, ferr := os.OpenFile(filePath, flag, 0666)
	if ferr == nil {
		trySetFifoSize(filePath, fd, bufSize)
	} else {
		log.Printf("[fifo] Failed to open fifo [%s]: %s", filePath, ferr)
		return nil
	}
	return fd
}

func trySetFifoSize(filePath string, fd *os.File, bufSize int) {
	// Bump the pipe size to the buffer we want
	_, _, errno := syscall.RawSyscall(
		syscall.SYS_FCNTL,
		fd.Fd(),
		syscall.F_SETPIPE_SZ,
		uintptr(bufSize),
	)
	if errno != 0 {
		log.Printf("[fifo][%s] Failed to set pipe size - %s", filePath, errno)
	} else {
		log.Printf("[fifo][%s] Succeeded at setting pipe size to %d", filePath, bufSize)
	}
}

func makeFifoAndSetSize(filePath string, flag int, bufSize int) *os.File {
	return openToSetFifoSize(makeFifo(filePath), flag, bufSize)
}

func MakeFifoPair(inDir, outDir, handle string, bufSize int) FifoPair {
	var (
		in  *os.File
		out *os.File
	)
	in = makeFifoAndSetSize(path.Join(inDir, handle), os.O_RDONLY|syscall.O_NONBLOCK, bufSize)

	// Acquire a writer to the input side so we can control when it closes
	w := state.AcquireWriter(inDir, handle)
	if w.Fd == nil {
		if in != nil {
			in.Close()
		}
		log.Printf("[%s][fifo]: Could not open writers %s", handle, w)
		return FifoPair{Handle: handle, In: nil, Out: nil}
	}

	out = makeFifoAndSetSize(path.Join(outDir, handle), os.O_RDONLY|syscall.O_NONBLOCK, bufSize)

	return FifoPair{
		Handle:        handle,
		In:            in,
		InPath:        path.Join(inDir, handle),
		Out:           out,
		OutPath:       path.Join(outDir, handle),
		BuffSizeBytes: bufSize,
	}
}

type FifoPair struct {
	Handle        string
	In            *os.File
	InPath        string
	Out           *os.File
	OutPath       string
	BuffSizeBytes int
}

func (fp FifoPair) Close() {
	log.Printf("[%s][fifo] Closing", fp.Handle)

	state.MaybeReleaseWriter(fp.Handle)
	if fp.In != nil {
		os.Remove(fp.InPath)
		fp.In.Close()
	}
	if fp.Out != nil {
		os.Remove(fp.OutPath)
		fp.Out.Close()
	}
}
