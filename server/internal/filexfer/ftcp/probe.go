package ftcp

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jolynch/pinch/internal/filexfer/encoding"
	"github.com/jolynch/pinch/internal/filexfer/limit"
	"github.com/jolynch/pinch/utils"
)

const maxProbeBytes int64 = 32 * 1024 * 1024
const probeCopyBufferBytes = 64 * 1024

type probeRequest struct {
	ClientCPU        int
	ProbeBytes       int64
	ClientTS0        int64
	TransferID       string
	ObservedLinkMbps int64
}

func handlePROBECommand(context.Context, Request, io.Writer, Deps) error {
	return protocolErr{code: "BAD_COMMAND", message: "invalid PROBE invocation"}
}

func parsePROBERequest(req Request) (probeRequest, error) {
	if req.Verb != VerbPROBE {
		return probeRequest{}, protocolErr{code: "BAD_COMMAND", message: "not PROBE"}
	}
	if len(req.Params) != 1 {
		return probeRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid PROBE arguments"}
	}
	p := req.Params[0]
	clientCPU, err := strconv.Atoi(strings.TrimSpace(p["cpu"]))
	if err != nil || clientCPU <= 0 {
		return probeRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid PROBE cpu"}
	}
	probeBytes, err := strconv.ParseInt(strings.TrimSpace(p["probe-bytes"]), 10, 64)
	if err != nil || probeBytes < 0 {
		return probeRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid PROBE probe-bytes"}
	}
	if probeBytes > maxProbeBytes {
		return probeRequest{}, protocolErr{code: "BAD_REQUEST", message: "PROBE probe-bytes too large"}
	}
	clientTS0, err := strconv.ParseInt(strings.TrimSpace(p["cts0"]), 10, 64)
	if err != nil || clientTS0 < 0 {
		return probeRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid PROBE cts0"}
	}
	transferID := strings.TrimSpace(p["txferid"])
	observedLinkMbps := int64(0)
	if raw := strings.TrimSpace(p["obs-link-mbps"]); raw != "" {
		observedLinkMbps, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || observedLinkMbps < 0 {
			return probeRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid PROBE obs-link-mbps"}
		}
		if transferID == "" {
			return probeRequest{}, protocolErr{code: "BAD_REQUEST", message: "PROBE obs-link-mbps requires txferid"}
		}
	}
	return probeRequest{
		ClientCPU:        clientCPU,
		ProbeBytes:       probeBytes,
		ClientTS0:        clientTS0,
		TransferID:       transferID,
		ObservedLinkMbps: observedLinkMbps,
	}, nil
}

func handlePROBEWithInput(_ context.Context, req Request, in io.Reader, out io.Writer, deps Deps, ioDepth int, gentleCPUPct int, gentleBWPct int, gentleBurstBytes int64) error {
	parsed, err := parsePROBERequest(req)
	if err != nil {
		return err
	}
	if in == nil {
		return protocolErr{code: "BAD_REQUEST", message: "missing PROBE payload stream"}
	}
	if ioDepth <= 0 {
		ioDepth = 8
	}
	gentleCPUPct = limit.NormalizeGentleCPUPct(gentleCPUPct)
	gentleBWPct = limit.NormalizeGentleBWPct(gentleBWPct)
	sts0 := time.Now().UnixMilli()
	if parsed.ProbeBytes > 0 {
		if err := drainProbePayload(in, parsed.ProbeBytes, &sts0); err != nil {
			return protocolErr{code: "BAD_REQUEST", message: "invalid PROBE payload"}
		}
	}
	if deps != nil && parsed.TransferID != "" && parsed.ObservedLinkMbps > 0 {
		if update, ok := deps.ReportTransferObservedLink(parsed.TransferID, parsed.ObservedLinkMbps, gentleBWPct, gentleBurstBytes, 0.2); ok && update.NewRateBps != update.OldRateBps {
			log.Printf(
				"filexfer probe tid=%s observed_link=%dMbps ema_link=%.1fMbps limiter=%s->%s",
				parsed.TransferID,
				update.ObservedLinkMbps,
				update.EMALinkMbps,
				encoding.HumanRate(float64(update.OldRateBps)),
				encoding.HumanRate(float64(update.NewRateBps)),
			)
		}
	}
	sts1 := time.Now().UnixMilli()
	limiterBps := int64(0)
	if deps != nil && parsed.TransferID != "" {
		limiterBps = deps.GetTransferLimiterBps(parsed.TransferID)
	}
	respLine := fmt.Sprintf(
		"PROBE cpu=%d io-depth=%d cts0=%d sts0=%d sts1=%d probe-bytes=%d wmem=%d gentle-cpu-pct=%d gentle-bw-pct=%d limiter-bps=%d\n",
		runtime.NumCPU(),
		ioDepth,
		parsed.ClientTS0,
		sts0,
		sts1,
		parsed.ProbeBytes,
		utils.MaxSocketWriteBufferBytes(),
		gentleCPUPct,
		gentleBWPct,
		limiterBps,
	)
	if _, err := io.WriteString(out, respLine); err != nil {
		return err
	}
	if parsed.ProbeBytes > 0 {
		if _, err := io.CopyN(out, rand.Reader, parsed.ProbeBytes); err != nil {
			return err
		}
	}
	return nil
}

func drainProbePayload(in io.Reader, total int64, sts0 *int64) error {
	remaining := total
	var first [1]byte
	if _, err := io.ReadFull(in, first[:]); err != nil {
		return err
	}
	if sts0 != nil {
		*sts0 = time.Now().UnixMilli()
	}
	remaining--
	if remaining <= 0 {
		return nil
	}
	buf := make([]byte, probeCopyBufferBytes)
	for remaining > 0 {
		step := int64(len(buf))
		if step > remaining {
			step = remaining
		}
		n, err := io.ReadFull(in, buf[:step])
		if n > 0 {
			remaining -= int64(n)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
