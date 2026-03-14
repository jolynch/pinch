package limit

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestParseRateBytesPerSecond(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    int64
		wantErr bool
	}{
		{name: "disabled empty", in: "", want: 0},
		{name: "disabled zero", in: "0", want: 0},
		{name: "mib", in: "100MiB", want: 100 * 1024 * 1024},
		{name: "mb", in: "100MB", want: 100 * 1000 * 1000},
		{name: "mbps bits", in: "1000mbps", want: 125 * 1000 * 1000},
		{name: "bad", in: "wat", wantErr: true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseRateBytesPerSecond(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("unexpected value: got=%d want=%d", got, tc.want)
			}
		})
	}
}

func TestRateLimitedWriterNoopWhenDisabled(t *testing.T) {
	var out bytes.Buffer
	lw := wrapRateLimitedWriter(&out, context.Background(), fileStreamLimitState{
		cfg: fileStreamLimitConfig{},
	})

	if _, err := lw.Write([]byte("hello")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if out.String() != "hello" {
		t.Fatalf("unexpected body: %q", out.String())
	}
}

func TestRateLimitedWriterTimeout(t *testing.T) {
	var out bytes.Buffer
	lw := wrapRateLimitedWriter(&out, context.Background(), fileStreamLimitState{
		cfg: fileStreamLimitConfig{
			TimeLimit: 1 * time.Millisecond,
		},
	})
	time.Sleep(3 * time.Millisecond)
	_, err := lw.Write([]byte("hello"))
	if !errors.Is(err, errFileStreamTimeLimitExceeded) {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestWaitRateLimitedOverBurst(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(1024*1024*1024), 1024) // high rate, tiny burst
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if err := waitRateLimited(ctx, limiter, 8*1024); err != nil {
		t.Fatalf("waitRateLimited should handle n > burst without hanging: %v", err)
	}
}

func TestNewLimiter(t *testing.T) {
	limiter, err := NewLimiter(Config{
		Rate:      "100MiB",
		Burst:     "1MiB",
		TimeLimit: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("new limiter failed: %v", err)
	}
	cfg := limiter.Config()
	if cfg.RateBps != 100*1024*1024 {
		t.Fatalf("unexpected rate: %d", cfg.RateBps)
	}
	if cfg.BurstBytes != 1*1024*1024 {
		t.Fatalf("unexpected burst: %d", cfg.BurstBytes)
	}
	if cfg.TimeLimit != 10*time.Second {
		t.Fatalf("unexpected time limit: %s", cfg.TimeLimit)
	}
	if limiter.state.bucket == nil {
		t.Fatalf("expected rate limiter bucket")
	}
}

func TestNewLimiterRejectsBurstWhenRateEnabled(t *testing.T) {
	if _, err := NewLimiter(Config{Rate: "100MiB", Burst: "0"}); err == nil {
		t.Fatalf("expected error for zero burst when rate enabled")
	}
}
