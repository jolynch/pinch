package limit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "1MiB", want: 1 * 1024 * 1024},
		{in: "4MB", want: 4 * 1000 * 1000},
		{in: "512", want: 512},
		{in: "", wantErr: true},
		{in: "-1MiB", wantErr: true},
	}
	for _, tc := range tests {
		got, err := parseByteSize(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("expected error for %q", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("unexpected value for %q: got=%d want=%d", tc.in, got, tc.want)
		}
	}
}

func TestLimitedResponseWriterNoopWhenDisabled(t *testing.T) {
	rec := httptest.NewRecorder()
	lw := wrapLimitedResponseWriter(rec, context.Background(), fileStreamLimitState{
		cfg: fileStreamLimitConfig{},
	})

	if _, err := lw.Write([]byte("hello")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	if rec.Body.String() != "hello" {
		t.Fatalf("unexpected body: %q", rec.Body.String())
	}
}

func TestLimitedResponseWriterTimeout(t *testing.T) {
	rec := httptest.NewRecorder()
	lw := wrapLimitedResponseWriter(rec, context.Background(), fileStreamLimitState{
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

func TestConfigureFileStreamLimiter(t *testing.T) {
	t.Cleanup(func() {
		if err := configureFileStreamLimits(fileStreamLimitConfig{}); err != nil {
			t.Fatalf("reset stream limits: %v", err)
		}
	})

	if err := ConfigureFileStreamLimiter("100MiB", "1MiB", 10*time.Second); err != nil {
		t.Fatalf("configure failed: %v", err)
	}
	state := currentFileStreamLimitState()
	if state.cfg.RateBps != 100*1024*1024 {
		t.Fatalf("unexpected rate: %d", state.cfg.RateBps)
	}
	if state.cfg.BurstBytes != 1*1024*1024 {
		t.Fatalf("unexpected burst: %d", state.cfg.BurstBytes)
	}
	if state.cfg.TimeLimit != 10*time.Second {
		t.Fatalf("unexpected time limit: %s", state.cfg.TimeLimit)
	}
	if state.bucket == nil {
		t.Fatalf("expected rate limiter bucket")
	}
}

func TestConfigureFileStreamLimiterRejectsBurstWhenRateEnabled(t *testing.T) {
	if err := ConfigureFileStreamLimiter("100MiB", "0", 0); err == nil {
		t.Fatalf("expected error for zero burst when rate enabled")
	}
}
