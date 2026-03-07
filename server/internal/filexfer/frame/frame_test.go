package frame

import (
	"bytes"
	"net/http/httptest"
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
