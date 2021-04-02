package state

import (
	"log"
	"os"
	"path"
	"sync"
	"time"
)

type Checksum struct {
	Start  time.Time
	Xxh128 string
	Blake3 string
}

type write struct {
	fd *os.File
}

var (
	checksums sync.Map
	writers   sync.Map
)

func StoreChecksum(name string, checksum Checksum) {
	checksums.Store(name, checksum)
}

func LoadChecksum(name string) (value Checksum, ok bool) {
	val, ok := checksums.Load(name)
	if ok {
		return val.(Checksum), true
	} else {
		return Checksum{}, false
	}
}

func CleanupChecksum(name string, start time.Time, expire time.Duration, output string) {
	log.Printf("[%s]: Keeping digests available for %s", name, expire)
	time.Sleep(expire)

	value, ok := LoadChecksum(name)
	if ok {
		if value.Start == start {
			log.Printf("[%s]: Cleaned up for [%s]", name, start.Format(time.RFC3339))
			checksums.Delete(name)
			os.Remove(output + ".xxh128")
			os.Remove(output + ".blake3")
		} else {
			log.Printf("[%s]: Detected re-use, skipping clean-up", name)
		}
	} else {
		log.Printf("[%s]: No digests found, command failed", name)
		os.Remove(output + ".xxh128")
		os.Remove(output + ".blake3")
		ReleaseWriter(name)
	}
}

func AcquireWriter(writeDir string, name string) *os.File {
	value, ok := writers.Load(name)
	if !ok {
		fd, err := os.OpenFile(path.Join(writeDir, name), os.O_WRONLY, 0666)
		if err != nil {
			return nil
		}
		value, ok = writers.LoadOrStore(name, write{
			fd: fd,
		})
		// Race, someone else made a FD
		// TODO: I'm pretty sure closing the write side is bad ... should
		// probably just use a locked map instead of a sync map
		if ok {
			fd.Close()
		}
	}
	return value.(write).fd
}

func ReleaseWriter(name string) {
	value, hasWriter := writers.Load(name)
	if hasWriter {
		log.Printf("[%s]: Cleaning up open writer", name)
		writers.Delete(name)
		value.(write).fd.Close()
	}
}
