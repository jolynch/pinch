package ftcp

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	intframe "github.com/jolynch/pinch/internal/filexfer/frame"
)

type ackRequest struct {
	TransferID string
	FileID     uint64
	AckToken   string
	DeltaBytes int64
	RecvMS     int64
	SyncMS     int64
	Path       string
}

func parseACKRequest(req Request) (ackRequest, error) {
	if req.Verb != VerbACK {
		return ackRequest{}, protocolErr{code: "BAD_COMMAND", message: "not ACK"}
	}
	if len(req.Params) != 1 {
		return ackRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK arguments"}
	}
	p := req.Params[0]
	txferID := p["txferid"]
	if txferID == "" {
		return ackRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing transfer id"}
	}
	fid, err := strconv.ParseUint(p["fid"], 10, 64)
	if err != nil {
		return ackRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid file id"}
	}
	ackToken := p["ack-token"]
	if strings.TrimSpace(ackToken) == "" {
		return ackRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing ack token"}
	}
	deltaBytes, recvMS, syncMS, err := parseAckTelemetryQuery(url.Values{
		"delta-bytes": []string{p["delta-bytes"]},
		"recv-ms":     []string{p["recv-ms"]},
		"sync-ms":     []string{p["sync-ms"]},
	}, 0)
	if ackToken == "-1" {
		deltaBytes, recvMS, syncMS = 0, 0, 0
		err = nil
	}
	if err != nil {
		return ackRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK telemetry"}
	}
	path := p["path"]
	if path == "" {
		return ackRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing path"}
	}
	return ackRequest{
		TransferID: txferID,
		FileID:     fid,
		AckToken:   ackToken,
		DeltaBytes: deltaBytes,
		RecvMS:     recvMS,
		SyncMS:     syncMS,
		Path:       path,
	}, nil
}

func handleACK(_ context.Context, req Request, out io.Writer, deps Deps) error {
	parsed, err := parseACKRequest(req)
	if err != nil {
		return err
	}
	ackBytes, ackTS, ackHashToken, ackProvided, err := parseAckToken(parsed.AckToken)
	if err != nil || !ackProvided {
		return protocolErr{code: "BAD_REQUEST", message: "invalid ack token"}
	}

	fileRef, err := deps.GetFileRef(parsed.TransferID, parsed.FileID, parsed.Path)
	if err != nil {
		return mapLookupError(err)
	}

	maxAck := fileRef.FileSize
	ackTarget := ackBytes
	if ackTarget > maxAck {
		ackTarget = maxAck
	}
	if ackTarget < 0 {
		ackTarget = 0
	}
	if ackBytes >= 0 {
		if ackHashToken == "" {
			return protocolErr{code: "BAD_REQUEST", message: "missing window ack hash token"}
		}
		if !deps.VerifyTransferFileWindowHash(parsed.TransferID, parsed.FileID, ackTarget, ackHashToken) {
			return protocolErr{code: "CONFLICT", message: "window ack hash token mismatch"}
		}
	}
	if ok := deps.AcknowledgeTransferFile(parsed.TransferID, parsed.FileID, ackBytes); !ok {
		return protocolErr{code: "INTERNAL", message: "failed to acknowledge file progress"}
	}

	if ackBytes >= 0 {
		nowTS := time.Now().UnixMilli()
		lagMS := nowTS - ackTS
		receiverMS := parsed.RecvMS + parsed.SyncMS
		receiverBps := 0.0
		recvBps := 0.0
		syncBps := 0.0
		if parsed.DeltaBytes > 0 {
			if receiverMS > 0 {
				receiverBps = float64(parsed.DeltaBytes) / (float64(receiverMS) / 1000.0)
			}
			if parsed.RecvMS > 0 {
				recvBps = float64(parsed.DeltaBytes) / (float64(parsed.RecvMS) / 1000.0)
			}
			if parsed.SyncMS > 0 {
				syncBps = float64(parsed.DeltaBytes) / (float64(parsed.SyncMS) / 1000.0)
			}
		}
		log.Printf(
			"filexfer ack tid=%s fid=%d ack_bytes=%d ack_hash=%s acked_server_ts=%d ack_recv_ts=%d lag_ms=%d delta_bytes=%d recv_ms=%d sync_ms=%d receiver_ms=%d receiver_throughput=%s recv_throughput=%s sync_throughput=%s",
			parsed.TransferID,
			parsed.FileID,
			ackBytes,
			ackHashToken,
			ackTS,
			nowTS,
			lagMS,
			parsed.DeltaBytes,
			parsed.RecvMS,
			parsed.SyncMS,
			receiverMS,
			humanRate(receiverBps),
			humanRate(recvBps),
			humanRate(syncBps),
		)
	}
	return writeOKLine(out, "")
}

func parseAckToken(raw string) (ackBytes int64, ackTS int64, ackHashToken string, provided bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, "", false, nil
	}
	if raw == "-1" {
		return -1, 0, "", true, nil
	}
	parts := strings.SplitN(raw, "@", 3)
	if len(parts) < 2 {
		return 0, 0, "", true, fmt.Errorf("invalid ack format")
	}
	ackBytes, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ackBytes < 0 {
		return 0, 0, "", true, fmt.Errorf("invalid ack bytes")
	}
	ackTS, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || ackTS < 0 {
		return 0, 0, "", true, fmt.Errorf("invalid ack timestamp")
	}
	if len(parts) == 3 {
		ackHashToken = strings.TrimSpace(parts[2])
		if !intframe.ValidHashToken(ackHashToken) {
			return 0, 0, "", true, fmt.Errorf("invalid ack hash token")
		}
	}
	return ackBytes, ackTS, ackHashToken, true, nil
}

func parseAckTelemetryQuery(values url.Values, ackBytes int64) (deltaBytes int64, recvMS int64, syncMS int64, err error) {
	if ackBytes < 0 {
		return 0, 0, 0, nil
	}
	deltaRaw := strings.TrimSpace(values.Get("delta-bytes"))
	recvRaw := strings.TrimSpace(values.Get("recv-ms"))
	syncRaw := strings.TrimSpace(values.Get("sync-ms"))
	if deltaRaw == "" || recvRaw == "" || syncRaw == "" {
		return 0, 0, 0, fmt.Errorf("missing required ack telemetry")
	}
	deltaBytes, err = strconv.ParseInt(deltaRaw, 10, 64)
	if err != nil || deltaBytes < 0 {
		return 0, 0, 0, fmt.Errorf("invalid delta-bytes")
	}
	recvMS, err = strconv.ParseInt(recvRaw, 10, 64)
	if err != nil || recvMS < 0 {
		return 0, 0, 0, fmt.Errorf("invalid recv-ms")
	}
	syncMS, err = strconv.ParseInt(syncRaw, 10, 64)
	if err != nil || syncMS < 0 {
		return 0, 0, 0, fmt.Errorf("invalid sync-ms")
	}
	return deltaBytes, recvMS, syncMS, nil
}
