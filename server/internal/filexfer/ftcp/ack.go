package ftcp

import (
	"context"
	"fmt"
	"io"
	"log"
	"runtime/trace"
	"strconv"
	"strings"
	"time"

	"github.com/jolynch/pinch/internal/filexfer/encoding"
)

type ackItem struct {
	TransferID string
	FileID     uint64
	AckToken   string
	DeltaBytes int64
	RecvMS     int64
	SyncMS     int64
	Path       string
}

type ackRequest struct {
	Items []ackItem
}

type validatedAckItem struct {
	item         ackItem
	ackBytes     int64
	ackTS        int64
	ackHashToken string
}

func parseACKRequest(req Request) (ackRequest, error) {
	if req.Verb != VerbACK {
		return ackRequest{}, protocolErr{code: "BAD_COMMAND", message: "not ACK"}
	}
	if len(req.Params) == 0 {
		return ackRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK arguments"}
	}
	items := make([]ackItem, 0, len(req.Params))
	for _, p := range req.Params {
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
		deltaBytes, recvMS, syncMS, err := parseAckTelemetryFields(p["delta-bytes"], p["recv-ms"], p["sync-ms"])
		if err != nil {
			return ackRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK telemetry"}
		}
		path := p["path"]
		if path == "" {
			return ackRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing path"}
		}
		items = append(items, ackItem{
			TransferID: txferID,
			FileID:     fid,
			AckToken:   ackToken,
			DeltaBytes: deltaBytes,
			RecvMS:     recvMS,
			SyncMS:     syncMS,
			Path:       path,
		})
	}
	return ackRequest{Items: items}, nil
}

func handleACK(ctx context.Context, req Request, out io.Writer, deps Deps) error {
	parsed, err := parseACKRequest(req)
	if err != nil {
		return err
	}
	validated := make([]validatedAckItem, 0, len(parsed.Items))
	for _, item := range parsed.Items {
		ackBytes, ackTS, ackHashToken, ackProvided, err := parseAckToken(item.AckToken)
		if err != nil || !ackProvided {
			return protocolErr{code: "BAD_REQUEST", message: "invalid ack token"}
		}
		if ackBytes == -1 {
			item.DeltaBytes, item.RecvMS, item.SyncMS = 0, 0, 0
		}

		fileRef, err := deps.GetFileRef(item.TransferID, item.FileID, item.Path)
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
			if !deps.VerifyTransferFileWindowHash(item.TransferID, item.FileID, ackTarget, ackHashToken) {
				return protocolErr{code: "CONFLICT", message: "window ack hash token mismatch"}
			}
		}
		validated = append(validated, validatedAckItem{
			item:         item,
			ackBytes:     ackBytes,
			ackTS:        ackTS,
			ackHashToken: ackHashToken,
		})
	}

	for _, v := range validated {
		_, ackTask := trace.NewTask(ctx, "ack")
		if ok := deps.AcknowledgeTransferFile(v.item.TransferID, v.item.FileID, v.ackBytes); !ok {
			ackTask.End()
			return protocolErr{code: "INTERNAL", message: "failed to acknowledge file progress"}
		}
		if v.ackBytes >= 0 {
			nowTS := time.Now().UnixMilli()
			lagMS := nowTS - v.ackTS
			receiverMS := v.item.RecvMS + v.item.SyncMS
			receiverBps := 0.0
			recvBps := 0.0
			syncBps := 0.0
			if v.item.DeltaBytes > 0 {
				if receiverMS > 0 {
					receiverBps = float64(v.item.DeltaBytes) / (float64(receiverMS) / 1000.0)
				}
				if v.item.RecvMS > 0 {
					recvBps = float64(v.item.DeltaBytes) / (float64(v.item.RecvMS) / 1000.0)
				}
				if v.item.SyncMS > 0 {
					syncBps = float64(v.item.DeltaBytes) / (float64(v.item.SyncMS) / 1000.0)
				}
			}
			log.Printf(
				"filexfer ack tid=%s fid=%d ack_bytes=%d ack_hash=%s acked_server_ts=%d ack_recv_ts=%d lag_ms=%d delta_bytes=%d recv_ms=%d sync_ms=%d receiver_ms=%d receiver_throughput=%s recv_throughput=%s sync_throughput=%s",
				v.item.TransferID,
				v.item.FileID,
				v.ackBytes,
				encoding.AbbrevHashToken(v.ackHashToken),
				v.ackTS,
				nowTS,
				lagMS,
				v.item.DeltaBytes,
				v.item.RecvMS,
				v.item.SyncMS,
				receiverMS,
				encoding.HumanRate(receiverBps),
				encoding.HumanRate(recvBps),
				encoding.HumanRate(syncBps),
			)
		}
		ackTask.End()
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
		if !encoding.ValidHashToken(ackHashToken) {
			return 0, 0, "", true, fmt.Errorf("invalid ack hash token")
		}
	}
	return ackBytes, ackTS, ackHashToken, true, nil
}

func parseAckTelemetryFields(deltaRaw string, recvRaw string, syncRaw string) (deltaBytes int64, recvMS int64, syncMS int64, err error) {
	deltaRaw = strings.TrimSpace(deltaRaw)
	recvRaw = strings.TrimSpace(recvRaw)
	syncRaw = strings.TrimSpace(syncRaw)
	if deltaRaw == "" {
		deltaBytes = 0
	} else {
		deltaBytes, err = strconv.ParseInt(deltaRaw, 10, 64)
		if err != nil || deltaBytes < 0 {
			return 0, 0, 0, fmt.Errorf("invalid delta-bytes")
		}
	}
	if recvRaw == "" {
		recvMS = 0
	} else {
		recvMS, err = strconv.ParseInt(recvRaw, 10, 64)
		if err != nil || recvMS < 0 {
			return 0, 0, 0, fmt.Errorf("invalid recv-ms")
		}
	}
	if syncRaw == "" {
		syncMS = 0
	} else {
		syncMS, err = strconv.ParseInt(syncRaw, 10, 64)
		if err != nil || syncMS < 0 {
			return 0, 0, 0, fmt.Errorf("invalid sync-ms")
		}
	}
	return deltaBytes, recvMS, syncMS, nil
}
