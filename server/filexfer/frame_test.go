package filexfer

import (
	"bytes"
	"fmt"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestWriteFrameReturnsStats(t *testing.T) {
	w := httptest.NewRecorder()
	payload := bytes.Repeat([]byte("x"), 1<<20)
	stats, err := writeFrame(w, frameWriteArgs{
		FileID:     1,
		Offset:     0,
		Size:       int64(len(payload)),
		WSize:      int64(len(payload)),
		Comp:       "none",
		Enc:        "none",
		HeaderHash: "xxh128:00",
		HeaderTS:   time.Now().UnixMilli(),
		Payload:    payload,
		TrailerTS:  time.Now().UnixMilli(),
		HashTokens: []string{"xxh128:00"},
		Next:       0,
	})
	if err != nil {
		t.Fatalf("writeFrame failed: %v", err)
	}
	if stats.WriteLatency <= 0 {
		t.Fatalf("expected positive write latency, got %v", stats.WriteLatency)
	}
	if stats.WireThroughputBps <= 0 {
		t.Fatalf("expected positive wire throughput, got %f", stats.WireThroughputBps)
	}
}

func TestFrameCompressorPoolReuseAcrossModes(t *testing.T) {
	compressor, release, err := acquireFrameCompressor(2 * 1024 * 1024)
	if err != nil {
		t.Fatalf("acquire compressor: %v", err)
	}
	payload := []byte("hello pooled compressor")
	for _, mode := range []CompressionMode{
		CompressionModeNone,
		CompressionModeLz4,
		CompressionModeZstdLevel1,
	} {
		wire, err := compressor.Compress(payload, mode)
		if err != nil {
			release()
			t.Fatalf("compress mode %v failed: %v", mode, err)
		}
		logical := decodeFramePayload(t, frameCompTokenForMode(mode), wire)
		if !bytes.Equal(logical, payload) {
			release()
			t.Fatalf("mode %v roundtrip mismatch", mode)
		}
	}
	release()

	compressor2, release2, err := acquireFrameCompressor(2 * 1024 * 1024)
	if err != nil {
		t.Fatalf("reacquire compressor: %v", err)
	}
	defer release2()
	secondPayload := []byte("second request payload")
	wire, err := compressor2.Compress(secondPayload, CompressionModeZstdLevel1)
	if err != nil {
		t.Fatalf("compress second payload failed: %v", err)
	}
	logical := decodeFramePayload(t, frameCompTokenForMode(CompressionModeZstdLevel1), wire)
	if !bytes.Equal(logical, secondPayload) {
		t.Fatalf("reused compressor returned stale/corrupt payload")
	}
}

func TestFrameCompressorPoolConcurrent(t *testing.T) {
	const workers = 24
	const iterations = 40

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				compressor, release, err := acquireFrameCompressor(8 * 1024 * 1024)
				if err != nil {
					errCh <- fmt.Errorf("worker=%d iter=%d acquire: %w", id, i, err)
					return
				}
				payload := []byte(fmt.Sprintf("worker=%d iter=%d payload", id, i))
				wire, err := compressor.Compress(payload, CompressionModeZstdLevel1)
				wireCopy := append([]byte(nil), wire...)
				release()
				if err != nil {
					errCh <- fmt.Errorf("worker=%d iter=%d compress: %w", id, i, err)
					return
				}
				logical, err := decodePayloadForMode(frameCompTokenForMode(CompressionModeZstdLevel1), wireCopy)
				if err != nil {
					errCh <- fmt.Errorf("worker=%d iter=%d decode: %w", id, i, err)
					return
				}
				if !bytes.Equal(logical, payload) {
					errCh <- fmt.Errorf("worker=%d iter=%d roundtrip mismatch", id, i)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func decodePayloadForMode(comp string, payload []byte) ([]byte, error) {
	reader, err := decodePayloadReaderByComp(bytes.NewReader(payload), comp)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
