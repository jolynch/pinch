package filexfer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

var (
	errFileStreamTimeLimitExceeded = errors.New("file stream time limit exceeded")
)

type fileStreamLimitState struct {
	cfg    fileStreamLimitConfig
	bucket *rate.Limiter
}

type fileStreamLimitConfig struct {
	RateBps    int64
	BurstBytes int64
	TimeLimit  time.Duration
}

var fileStreamLimiterState atomic.Pointer[fileStreamLimitState]

func ConfigureFileStreamLimiter(rateRaw string, burstRaw string, timeLimit time.Duration) error {
	parsedRateBps, err := parseRateBytesPerSecond(rateRaw)
	if err != nil {
		return fmt.Errorf("invalid rate: %w", err)
	}
	parsedBurstBytes, err := parseByteSize(burstRaw)
	if err != nil {
		return fmt.Errorf("invalid burst: %w", err)
	}
	if parsedRateBps > 0 && parsedBurstBytes <= 0 {
		return errors.New("burst must be > 0 when rate limiting is enabled")
	}
	return configureFileStreamLimits(fileStreamLimitConfig{
		RateBps:    parsedRateBps,
		BurstBytes: parsedBurstBytes,
		TimeLimit:  timeLimit,
	})
}

func configureFileStreamLimits(cfg fileStreamLimitConfig) error {
	if cfg.RateBps < 0 {
		return errors.New("file stream rate must be >= 0")
	}
	if cfg.BurstBytes < 0 {
		return errors.New("file stream burst must be >= 0")
	}
	if cfg.TimeLimit < 0 {
		return errors.New("file stream time limit must be >= 0")
	}
	if cfg.RateBps > 0 && cfg.BurstBytes <= 0 {
		return errors.New("file stream burst must be > 0 when rate limiting is enabled")
	}

	state := &fileStreamLimitState{cfg: cfg}
	if cfg.RateBps > 0 {
		state.bucket = rate.NewLimiter(rate.Limit(float64(cfg.RateBps)), int(cfg.BurstBytes))
	}
	fileStreamLimiterState.Store(state)
	return nil
}

func currentFileStreamLimitState() fileStreamLimitState {
	if state := fileStreamLimiterState.Load(); state != nil {
		return *state
	}
	return fileStreamLimitState{
		cfg: fileStreamLimitConfig{},
	}
}

type limitedResponseWriter struct {
	w         http.ResponseWriter
	ctx       context.Context
	bucket    *rate.Limiter
	deadline  time.Time
	hasDL     bool
	wroteBody bool
}

func wrapLimitedResponseWriter(w http.ResponseWriter, ctx context.Context, state fileStreamLimitState) *limitedResponseWriter {
	lw := &limitedResponseWriter{
		w:      w,
		ctx:    ctx,
		bucket: state.bucket,
	}
	if state.cfg.TimeLimit > 0 {
		lw.deadline = time.Now().Add(state.cfg.TimeLimit)
		lw.hasDL = true
	}
	return lw
}

func (w *limitedResponseWriter) Header() http.Header {
	return w.w.Header()
}

func (w *limitedResponseWriter) WriteHeader(statusCode int) {
	w.w.WriteHeader(statusCode)
}

func (w *limitedResponseWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.hasDL && time.Now().After(w.deadline) {
		return 0, errFileStreamTimeLimitExceeded
	}
	if w.bucket != nil {
		if err := waitRateLimited(w.ctx, w.bucket, len(p)); err != nil {
			return 0, err
		}
	}
	n, err := w.w.Write(p)
	if n > 0 {
		w.wroteBody = true
	}
	return n, err
}

func (w *limitedResponseWriter) Flush() {
	if fl, ok := w.w.(http.Flusher); ok {
		fl.Flush()
	}
}

func (w *limitedResponseWriter) wroteAnyBody() bool {
	return w.wroteBody
}

func waitRateLimited(ctx context.Context, limiter *rate.Limiter, n int) error {
	if n <= 0 {
		return nil
	}
	if limiter == nil {
		return nil
	}
	remaining := n
	chunkMax := limiter.Burst()
	if chunkMax <= 0 {
		chunkMax = 1
	}
	for remaining > 0 {
		wantInt := remaining
		if wantInt > chunkMax {
			wantInt = chunkMax
		}
		if err := limiter.WaitN(ctx, wantInt); err != nil {
			return err
		}
		remaining -= wantInt
	}
	return nil
}

func parseRateBytesPerSecond(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "0" {
		return 0, nil
	}
	value, unit, err := parseHumanValueAndUnit(raw)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, errors.New("rate must be >= 0")
	}

	origUnit := unit
	lowerUnit := strings.ToLower(strings.TrimSpace(unit))
	if lowerUnit == "" {
		return 0, errors.New("rate unit is required")
	}

	switch {
	case hasBitRateSuffix(origUnit):
		base := normalizePerSecondUnit(lowerUnit)
		mult, ok := bitUnitMultiplier(base)
		if !ok {
			return 0, fmt.Errorf("unsupported bit-rate unit: %s", unit)
		}
		return floatToInt64(value * mult / 8.0)
	case hasByteRateSuffix(origUnit):
		base := normalizePerSecondUnit(lowerUnit)
		mult, ok := byteUnitMultiplier(base)
		if !ok {
			return 0, fmt.Errorf("unsupported byte-rate unit: %s", unit)
		}
		return floatToInt64(value * mult)
	default:
		base := strings.ToLower(strings.TrimSpace(unit))
		mult, ok := byteUnitMultiplier(base)
		if !ok {
			return 0, fmt.Errorf("unsupported rate unit: %s", unit)
		}
		return floatToInt64(value * mult)
	}
}

func parseByteSize(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("size is required")
	}
	value, unit, err := parseHumanValueAndUnit(raw)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, errors.New("size must be >= 0")
	}
	if unit == "" {
		return floatToInt64(value)
	}
	mult, ok := byteUnitMultiplier(strings.ToLower(strings.TrimSpace(unit)))
	if !ok {
		return 0, fmt.Errorf("unsupported size unit: %s", unit)
	}
	return floatToInt64(value * mult)
}

func parseHumanValueAndUnit(raw string) (float64, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, "", errors.New("value is required")
	}
	cut := 0
	for cut < len(raw) && (raw[cut] == '.' || (raw[cut] >= '0' && raw[cut] <= '9')) {
		cut++
	}
	if cut == 0 {
		return 0, "", errors.New("numeric value is required")
	}
	numPart := strings.TrimSpace(raw[:cut])
	unitPart := strings.TrimSpace(raw[cut:])
	v, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, "", err
	}
	return v, unitPart, nil
}

func hasBitRateSuffix(unit string) bool {
	l := strings.ToLower(strings.TrimSpace(unit))
	return strings.HasSuffix(l, "bps") || strings.HasSuffix(l, "b/s")
}

func hasByteRateSuffix(unit string) bool {
	u := strings.TrimSpace(unit)
	return strings.HasSuffix(u, "Bps") || strings.HasSuffix(u, "B/s")
}

func normalizePerSecondUnit(lowerUnit string) string {
	s := strings.TrimSpace(lowerUnit)
	if before, ok := strings.CutSuffix(s, "ps"); ok {
		s = before
	}
	if before, ok := strings.CutSuffix(s, "/s"); ok {
		s = before
	}
	return s
}

func byteUnitMultiplier(unit string) (float64, bool) {
	switch strings.TrimSpace(strings.ToLower(unit)) {
	case "", "b", "byte", "bytes":
		return 1, true
	case "kb":
		return 1000, true
	case "mb":
		return 1000 * 1000, true
	case "gb":
		return 1000 * 1000 * 1000, true
	case "tb":
		return 1000 * 1000 * 1000 * 1000, true
	case "kib":
		return 1024, true
	case "mib":
		return 1024 * 1024, true
	case "gib":
		return 1024 * 1024 * 1024, true
	case "tib":
		return 1024 * 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
}

func bitUnitMultiplier(unit string) (float64, bool) {
	switch strings.TrimSpace(strings.ToLower(unit)) {
	case "", "b", "bit", "bits":
		return 1, true
	case "kb":
		return 1000, true
	case "mb":
		return 1000 * 1000, true
	case "gb":
		return 1000 * 1000 * 1000, true
	case "tb":
		return 1000 * 1000 * 1000 * 1000, true
	case "kib":
		return 1024, true
	case "mib":
		return 1024 * 1024, true
	case "gib":
		return 1024 * 1024 * 1024, true
	case "tib":
		return 1024 * 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
}

func floatToInt64(v float64) (int64, error) {
	if v < 0 {
		return 0, errors.New("value must be >= 0")
	}
	if v > float64(^uint64(0)>>1) {
		return 0, errors.New("value overflows int64")
	}
	return int64(v + 0.5), nil
}
