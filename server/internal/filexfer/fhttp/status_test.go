package fhttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zeebo/xxh3"
)

func TestTransferStatusHandler(t *testing.T) {
	resetTransferStore()
	transfer, err := NewTransfer("/tmp", 2, 10)
	if err != nil {
		t.Fatalf("NewTransfer: %v", err)
	}
	RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/0")), FileSize: 5},
		{FileID: 1, PathHash: xxh3.Hash128([]byte("/tmp/1")), FileSize: 5},
	}, TransferStateStarted)
	_ = SetTransferFileState(transfer.ID, 0, TransferStateRunning)
	_ = AcknowledgeTransferFile(transfer.ID, 1, 5)

	req := httptest.NewRequest(http.MethodGet, "/fs/transfer/"+transfer.ID+"/status", nil)
	req.SetPathValue("txferid", transfer.ID)
	w := httptest.NewRecorder()
	TransferStatusHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var status TransferStatus
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status json: %v", err)
	}
	if status.TransferID != transfer.ID {
		t.Fatalf("unexpected transfer id: %s", status.TransferID)
	}
	if status.DownloadStatus.Running != 1 || status.DownloadStatus.Done != 1 {
		t.Fatalf("unexpected download status: %+v", status.DownloadStatus)
	}
}
