package filexfer

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// StartProgressFileWriter starts a background goroutine that periodically
// writes an integer percentage (0–100) to a file or named pipe at path.
// The file is opened non-blocking so FIFOs without a reader don't hang.
// If the file/pipe doesn't exist or can't be opened, it silently retries
// on the next tick.
//
// The returned stop function must be called when the operation completes.
// If success is true the final write is "100\n"; otherwise the last-known
// percentage is written so external tools can distinguish success from failure.
func StartProgressFileWriter(ctx context.Context, path string, interval time.Duration, pctFn func() int) (stop func(success bool)) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)

	var lastPct int

	writeOnce := func(pct int) {
		fd, err := unix.Open(path, unix.O_WRONLY|unix.O_CREAT|unix.O_TRUNC|unix.O_NONBLOCK, 0644)
		if err != nil {
			return
		}
		f := os.NewFile(uintptr(fd), path)
		fmt.Fprintf(f, "%d\n", pct)
		f.Close()
	}

	lastWritten := -1

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lastPct = pctFn()
				if lastPct != lastWritten {
					writeOnce(lastPct)
					lastWritten = lastPct
				}
			}
		}
	}()

	return func(success bool) {
		cancel()
		wg.Wait()
		if success {
			writeOnce(100)
		} else {
			writeOnce(lastPct)
		}
	}
}
