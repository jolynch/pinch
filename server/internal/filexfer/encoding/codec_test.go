package encoding

import (
	"fmt"
	"sync"
	"testing"
)

func TestMaxEncodedFrameSizeBytesZstdStableAcrossCalls(t *testing.T) {
	const logicalSize = int64(8 * 1024 * 1024)
	first, err := maxEncodedFrameSizeBytes(EncodingZstd, logicalSize)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	for i := 0; i < 20; i++ {
		next, err := maxEncodedFrameSizeBytes(EncodingZstd, logicalSize)
		if err != nil {
			t.Fatalf("repeat call %d failed: %v", i, err)
		}
		if next != first {
			t.Fatalf("repeat call %d mismatch: got=%d want=%d", i, next, first)
		}
	}
}

func TestMaxEncodedFrameSizeBytesMultipleModesUnaffected(t *testing.T) {
	const logicalSize = int64(1024)

	noneSize, err := maxEncodedFrameSizeBytes("none", logicalSize)
	if err != nil {
		t.Fatalf("none sizing failed: %v", err)
	}
	if noneSize != logicalSize {
		t.Fatalf("none sizing mismatch: got=%d want=%d", noneSize, logicalSize)
	}

	lz4Size, err := maxEncodedFrameSizeBytes(EncodingLz4, logicalSize)
	if err != nil {
		t.Fatalf("lz4 sizing failed: %v", err)
	}
	if lz4Size < logicalSize {
		t.Fatalf("lz4 sizing unexpectedly small: %d", lz4Size)
	}

	zstdSize, err := maxEncodedFrameSizeBytes(EncodingZstd, logicalSize)
	if err != nil {
		t.Fatalf("zstd sizing failed: %v", err)
	}
	if zstdSize < logicalSize {
		t.Fatalf("zstd sizing unexpectedly small: %d", zstdSize)
	}
}

func TestMaxEncodedFrameSizeBytesZstdConcurrent(t *testing.T) {
	const goroutines = 32
	const logicalSize = int64(4 * 1024 * 1024)

	want, err := maxEncodedFrameSizeBytes(EncodingZstd, logicalSize)
	if err != nil {
		t.Fatalf("baseline call failed: %v", err)
	}

	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, callErr := maxEncodedFrameSizeBytes(EncodingZstd, logicalSize)
			if callErr != nil {
				errCh <- callErr
				return
			}
			if got != want {
				errCh <- fmt.Errorf("concurrent size mismatch: got=%d want=%d", got, want)
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for callErr := range errCh {
		if callErr != nil {
			t.Fatalf("concurrent call failed: %v", callErr)
		}
	}
}
