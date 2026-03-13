package ftcp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type txferTestDeps struct {
	setHintsCalls int
	setHintsTxID  string
	setHintsMode  string
	setHintsMbps  int64
	setHintsConc  int
}

func (d *txferTestDeps) NewTransfer(string, int, int64) (Transfer, error) {
	return Transfer{ID: "tx123"}, nil
}

func (d *txferTestDeps) DeleteTransfer(string) bool { return true }

func (d *txferTestDeps) RegisterTransferFileState(string, <-chan TransferFileStateUpdate, uint8) <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (d *txferTestDeps) ClipTransfer(string) bool { return true }

func (d *txferTestDeps) GetTransfer(string) (Transfer, bool) { return Transfer{}, false }

func (d *txferTestDeps) SetTransferHints(txferID string, mode string, linkMbps int64, concurrency int) bool {
	d.setHintsCalls++
	d.setHintsTxID = txferID
	d.setHintsMode = mode
	d.setHintsMbps = linkMbps
	d.setHintsConc = concurrency
	return true
}

func (d *txferTestDeps) GetFile(string, uint64, string) (*os.File, FileRef, error) {
	return nil, FileRef{}, nil
}

func (d *txferTestDeps) GetFileRef(string, uint64, string) (FileRef, error) {
	return FileRef{}, nil
}

func (d *txferTestDeps) SetTransferFileState(string, uint64, uint8) bool { return true }

func (d *txferTestDeps) SetTransferFileWindowHash(string, uint64, int64, string) bool { return true }

func (d *txferTestDeps) VerifyTransferFileWindowHash(string, uint64, int64, string) bool { return true }

func (d *txferTestDeps) AcknowledgeTransferFile(string, uint64, int64) bool { return true }

func TestParseTXFERRequestRequiresHints(t *testing.T) {
	req, err := ParseRequest([]byte(`TXFER "/tmp" mode=fast link-mbps=900 concurrency=12`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err := parseTXFERRequest(req)
	if err != nil {
		t.Fatalf("parseTXFERRequest failed: %v", err)
	}
	if parsed.Mode != "fast" || parsed.LinkMbps != 900 || parsed.Concurrency != 12 {
		t.Fatalf("unexpected parsed request: %+v", parsed)
	}

	bad := []string{
		`TXFER "/tmp" link-mbps=900 concurrency=12`,
		`TXFER "/tmp" mode=fast concurrency=12`,
		`TXFER "/tmp" mode=fast link-mbps=900`,
		`TXFER "/tmp" mode=slow link-mbps=900 concurrency=12`,
		`TXFER "/tmp" mode=fast link-mbps=-1 concurrency=12`,
		`TXFER "/tmp" mode=fast link-mbps=900 concurrency=0`,
	}
	for _, raw := range bad {
		t.Run(raw, func(t *testing.T) {
			req, err := ParseRequest([]byte(raw))
			if err != nil {
				t.Fatalf("ParseRequest failed: %v", err)
			}
			if _, err := parseTXFERRequest(req); err == nil {
				t.Fatalf("expected parseTXFERRequest to fail for %q", raw)
			}
		})
	}
}

func TestHandleTXFERStoresHintsAndEmitsFM2(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	reqRaw := fmt.Sprintf(`TXFER %q mode=gentle link-mbps=700 concurrency=6`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	deps := &txferTestDeps{}
	var out bytes.Buffer
	if err := handleTXFER(context.Background(), req, &out, deps); err != nil {
		t.Fatalf("handleTXFER failed: %v", err)
	}
	if deps.setHintsCalls != 1 {
		t.Fatalf("expected one SetTransferHints call, got %d", deps.setHintsCalls)
	}
	if deps.setHintsTxID != "tx123" || deps.setHintsMode != "gentle" || deps.setHintsMbps != 700 || deps.setHintsConc != 6 {
		t.Fatalf("unexpected SetTransferHints values: tx=%s mode=%s mbps=%d conc=%d", deps.setHintsTxID, deps.setHintsMode, deps.setHintsMbps, deps.setHintsConc)
	}
	manifest := out.String()
	if !strings.HasPrefix(manifest, "FM/2 tx123 ") {
		t.Fatalf("expected FM/2 header, got: %q", manifest)
	}
	if !strings.Contains(manifest, "mode=gentle") || !strings.Contains(manifest, "link-mbps=700") || !strings.Contains(manifest, "concurrency=6") {
		t.Fatalf("manifest missing required metadata: %q", manifest)
	}
	if _, err := io.WriteString(io.Discard, manifest); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
}
