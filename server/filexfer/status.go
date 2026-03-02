package filexfer

import (
	"encoding/json"
	"net/http"
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

func TransferStatusHandler(w http.ResponseWriter, req *http.Request) {
	txferID := req.PathValue("txferid")
	if txferID == "" {
		http.Error(w, "missing required path parameter: txferid", http.StatusBadRequest)
		return
	}
	transfer, ok := GetTransfer(txferID)
	if !ok {
		http.Error(w, "transfer not found", http.StatusNotFound)
		return
	}

	status := TransferStatus{
		TransferID: txferID,
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

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}
