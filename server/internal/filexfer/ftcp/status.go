package ftcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

type DownloadStatus struct {
	Started int `json:"started"`
	Running int `json:"running"`
	Done    int `json:"done"`
	Missing int `json:"missing"`
}

type TransferStatus struct {
	TransferID     string         `json:"transfer_id"`
	Directory      string         `json:"directory"`
	NumFiles       int            `json:"num_files"`
	TotalSize      int64          `json:"total_size"`
	Done           uint64         `json:"done"`
	DoneSize       int64          `json:"done_size"`
	PercentFiles   float64        `json:"percent_files"`
	PercentBytes   float64        `json:"percent_bytes"`
	DownloadStatus DownloadStatus `json:"download_status"`
}

type statusRequest struct {
	TransferID string
}

func parseSTATUSRequest(req Request) (statusRequest, error) {
	if req.Verb != VerbSTATUS {
		return statusRequest{}, protocolErr{code: "BAD_COMMAND", message: "not STATUS"}
	}
	if len(req.Params) != 1 {
		return statusRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid STATUS arguments"}
	}
	txferID := req.Params[0]["txferid"]
	return statusRequest{TransferID: txferID}, nil
}

func transferToStatus(id string, transfer Transfer) TransferStatus {
	status := TransferStatus{
		TransferID: id,
		Directory:  transfer.Directory,
		NumFiles:   transfer.NumFiles,
		TotalSize:  transfer.TotalSize,
		Done:       transfer.Done,
		DoneSize:   transfer.DoneSize,
	}
	if transfer.NumFiles > 0 {
		status.PercentFiles = float64(transfer.Done) * 100.0 / float64(transfer.NumFiles)
	}
	if transfer.TotalSize > 0 {
		status.PercentBytes = float64(transfer.DoneSize) * 100.0 / float64(transfer.TotalSize)
	}
	for _, s := range transfer.State {
		switch s {
		case TransferStateStarted:
			status.DownloadStatus.Started++
		case TransferStateRunning:
			status.DownloadStatus.Running++
		case TransferStateDone:
			status.DownloadStatus.Done++
		case TransferStateMissing:
			status.DownloadStatus.Missing++
		}
	}
	return status
}

func handleSTATUS(_ context.Context, req Request, out io.Writer, deps Deps) error {
	parsed, err := parseSTATUSRequest(req)
	if err != nil {
		return err
	}

	if parsed.TransferID == "" {
		// List all active transfers: write count header then one JSON line per transfer.
		transfers := deps.ListTransfers()
		if _, err := fmt.Fprintf(out, "OK %d\r\n", len(transfers)); err != nil {
			return err
		}
		for _, t := range transfers {
			payload, err := json.Marshal(transferToStatus(t.ID, t))
			if err != nil {
				return protocolErr{code: "INTERNAL", message: "failed to encode status"}
			}
			if _, err := fmt.Fprintf(out, "%s\r\n", payload); err != nil {
				return err
			}
		}
		return nil
	}

	transfer, ok := deps.GetTransfer(parsed.TransferID)
	if !ok {
		return protocolErr{code: "NOT_FOUND", message: "transfer not found"}
	}

	status := transferToStatus(parsed.TransferID, transfer)
	payload, err := json.Marshal(status)
	if err != nil {
		return protocolErr{code: "INTERNAL", message: "failed to encode status"}
	}
	return writeOKLine(out, string(payload))
}
