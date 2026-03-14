package encoding

import (
	"bytes"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWriteFrameReturnsStats(t *testing.T) {
	w := httptest.NewRecorder()
	payload := bytes.Repeat([]byte("x"), 1<<20)
	stats, err := WriteFrame(w, WriteArgs{
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
		t.Fatalf("WriteFrame failed: %v", err)
	}
	if stats.WriteLatency <= 0 {
		t.Fatalf("expected positive write latency, got %v", stats.WriteLatency)
	}
	if stats.WireThroughputBps <= 0 {
		t.Fatalf("expected positive wire throughput, got %f", stats.WireThroughputBps)
	}
}

func TestParseFXTrailerAllowsMissingTrailerHash(t *testing.T) {
	trailer, err := ParseFXTrailer("FXT/1 2 status=ok ts=1001 next=0 file-hash=xxh128:0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("ParseFXTrailer failed: %v", err)
	}
	if trailer.FileID != 2 {
		t.Fatalf("unexpected file id: %d", trailer.FileID)
	}
	if trailer.HashToken != "" {
		t.Fatalf("expected empty trailer hash token, got %q", trailer.HashToken)
	}
}
