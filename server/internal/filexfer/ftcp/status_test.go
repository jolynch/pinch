package ftcp

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/pinch/internal/filexfer/limit"
)

type fakeDeps struct {
	transfer   Transfer
	transferOK bool
}

func (f fakeDeps) NewTransfer(string, int, int64) (Transfer, error) { return Transfer{}, nil }
func (f fakeDeps) DeleteTransfer(string) bool                       { return true }
func (f fakeDeps) RegisterTransferFileState(string, <-chan TransferFileStateUpdate, uint8) <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (f fakeDeps) ClipTransfer(string) bool                         { return true }
func (f fakeDeps) SetTransferHints(string, string, int64, int) bool { return true }
func (f fakeDeps) GetTransferGentleLimiter(string, int64, int, int64) *limit.Limiter {
	return nil
}
func (f fakeDeps) ReportTransferObservedLink(string, int64, int, int64, float64) (TransferObservedLinkUpdate, bool) {
	return TransferObservedLinkUpdate{}, false
}
func (f fakeDeps) GetTransfer(string) (Transfer, bool) {
	return f.transfer, f.transferOK
}
func (f fakeDeps) ListTransfers() []Transfer { return nil }
func (f fakeDeps) GetFile(string, uint64, string) (*os.File, FileRef, error) {
	return nil, FileRef{}, nil
}
func (f fakeDeps) GetFileRef(string, uint64, string) (FileRef, error) {
	return FileRef{}, nil
}
func (f fakeDeps) SetTransferFileState(string, uint64, uint8) bool { return true }
func (f fakeDeps) SetTransferFileWindowHash(string, uint64, int64, string) bool {
	return true
}
func (f fakeDeps) VerifyTransferFileWindowHash(string, uint64, int64, string) bool {
	return true
}
func (f fakeDeps) AcknowledgeTransferFile(string, uint64, int64) bool { return true }

func (f fakeDeps) SetTransferDeadline(string, int64) bool           { return false }
func (f fakeDeps) RecordTransferFirstSend(string) (time.Time, bool) { return time.Time{}, false }
func (f fakeDeps) MarkTransferTooSlow(string) bool                  { return false }
func (f fakeDeps) GetTransferLimiterBps(string) int64                { return 0 }
func (f fakeDeps) Root() string                                     { return "/" }

func TestHandleSTATUSWritesStatusLine(t *testing.T) {
	req := Request{Verb: VerbSTATUS, Params: []map[string]string{{"txferid": "tx1"}}}
	deps := fakeDeps{
		transferOK: true,
		transfer: Transfer{
			ID:        "tx1",
			Directory: "/tmp",
			NumFiles:  1,
			TotalSize: 100,
			Done:      1,
			DoneSize:  100,
			State:     []uint8{TransferStateDone},
		},
	}

	var out bytes.Buffer
	if err := handleSTATUS(context.Background(), req, &out, deps); err != nil {
		t.Fatalf("handleSTATUS err: %v", err)
	}
	line := strings.TrimSpace(out.String())
	if !strings.HasPrefix(line, "OK ") {
		t.Fatalf("unexpected status line: %q", line)
	}
	if !strings.Contains(line, `"transfer_id":"tx1"`) {
		t.Fatalf("unexpected payload: %s", line)
	}
}
