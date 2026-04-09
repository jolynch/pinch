package filexfer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jolynch/pinch/internal/filexfer/encoding"
	"golang.org/x/sys/unix"
)

const (
	progressStatusBytesWidth = 10
)

// StartProgressFileWriter starts a background goroutine that periodically
// writes a two-line status record to a file or named pipe at path.
// The file is opened non-blocking so FIFOs without a reader don't hang.
// If the file/pipe doesn't exist or can't be opened, it silently retries
// on the next tick.
//
// The returned stop function must be called when the operation completes.
// The first successful write truncates any existing file content; later writes
// append two lines: a single-line status string and an integer percentage.
// If success is true the final write forces the percentage to 100; otherwise the
// current percentage from statusFn is written.
func StartProgressFileWriter(ctx context.Context, path string, interval time.Duration, statusFn func() (string, int)) (stop func(success bool)) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)

	firstWrite := true

	writeOnce := func(status string, pct int, truncate bool) {
		flags := unix.O_WRONLY | unix.O_CREAT | unix.O_NONBLOCK
		if truncate {
			flags |= unix.O_TRUNC
		} else {
			flags |= unix.O_APPEND
		}
		fd, err := unix.Open(path, flags, 0644)
		if err != nil {
			return
		}
		f := os.NewFile(uintptr(fd), path)
		fmt.Fprintf(f, "%s\n%d\n", status, normalizeProgressPct(pct))
		f.Close()
		firstWrite = false
	}

	var lastStatus string
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
				status, pct := statusFn()
				pct = normalizeProgressPct(pct)
				if status != lastStatus || pct != lastWritten {
					writeOnce(status, pct, firstWrite)
					lastStatus = status
					lastWritten = pct
				}
			}
		}
	}()

	return func(success bool) {
		cancel()
		wg.Wait()
		status, pct := statusFn()
		if success {
			pct = 100
		}
		writeOnce(status, pct, firstWrite)
	}
}

type progressStatusCounts struct {
	Done    uint64 `json:"done"`
	Total   uint64 `json:"total"`
	Percent string `json:"percent"`
}

type progressStatusBytes struct {
	Done    int64  `json:"done"`
	Total   int64  `json:"total"`
	Percent string `json:"percent"`
}

type progressStatusHumanBytes struct {
	Done  string `json:"done"`
	Total string `json:"total"`
}

type progressStatusJSON struct {
	Source     string                   `json:"source,omitempty"`
	TxID       string                   `json:"txid,omitempty"`
	Files      progressStatusCounts     `json:"files"`
	Bytes      progressStatusBytes      `json:"bytes"`
	BytesHuman progressStatusHumanBytes `json:"bytes_human"`
}

func FormatProgressStatusLine(source string, txid string, doneFiles uint64, totalFiles uint64, doneBytes int64, totalBytes int64) string {
	if totalFiles > 0 && doneFiles > totalFiles {
		doneFiles = totalFiles
	}
	if doneBytes < 0 {
		doneBytes = 0
	}
	if totalBytes < 0 {
		totalBytes = 0
	}
	if totalBytes > 0 && doneBytes > totalBytes {
		doneBytes = totalBytes
	}

	var filesPct float64
	if totalFiles > 0 {
		filesPct = float64(doneFiles) * 100 / float64(totalFiles)
	}
	var bytesPct float64
	if totalBytes > 0 {
		bytesPct = float64(doneBytes) * 100 / float64(totalBytes)
	}

	payload := progressStatusJSON{
		Source: source,
		TxID:   txid,
		Files: progressStatusCounts{
			Done:    doneFiles,
			Total:   totalFiles,
			Percent: formatProgressPercent(filesPct),
		},
		Bytes: progressStatusBytes{
			Done:    doneBytes,
			Total:   totalBytes,
			Percent: formatProgressPercent(bytesPct),
		},
		BytesHuman: progressStatusHumanBytes{
			Done:  encoding.HumanBytesFixedWidth(doneBytes, progressStatusBytesWidth),
			Total: encoding.HumanBytesFixedWidth(totalBytes, progressStatusBytesWidth),
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return `{"source":"unknown","error":"marshal"}`
	}
	return string(raw)
}

func formatProgressPercent(pct float64) string {
	return fmt.Sprintf("%4.1f", pct)
}

func normalizeProgressPct(pct int) int {
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}
