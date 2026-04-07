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
	"time"

	"github.com/jolynch/pinch/internal/filexfer/limit"
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
func (d *txferTestDeps) ListTransfers() []Transfer           { return nil }

func (d *txferTestDeps) SetTransferHints(txferID string, mode string, linkMbps int64, concurrency int) bool {
	d.setHintsCalls++
	d.setHintsTxID = txferID
	d.setHintsMode = mode
	d.setHintsMbps = linkMbps
	d.setHintsConc = concurrency
	return true
}

func (d *txferTestDeps) GetTransferGentleLimiter(string, int64, int, int64) *limit.Limiter {
	return nil
}

func (d *txferTestDeps) ReportTransferObservedLink(string, int64, int, int64, float64) (TransferObservedLinkUpdate, bool) {
	return TransferObservedLinkUpdate{}, false
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

func (d *txferTestDeps) SetTransferDeadline(string, int64) bool           { return false }
func (d *txferTestDeps) RecordTransferFirstSend(string) (time.Time, bool) { return time.Time{}, false }
func (d *txferTestDeps) MarkTransferTooSlow(string) bool                  { return false }
func (d *txferTestDeps) GetTransferLimiterBps(string) int64                { return 0 }
func (d *txferTestDeps) Root() string                                     { return "/" }

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
	if !strings.HasPrefix(manifest, "FM/1 tx123 ") {
		t.Fatalf("expected FM/1 header, got: %q", manifest)
	}
	if !strings.Contains(manifest, "mode=gentle") || !strings.Contains(manifest, "link-mbps=700") || !strings.Contains(manifest, "concurrency=6") {
		t.Fatalf("manifest missing required metadata: %q", manifest)
	}
	if _, err := io.WriteString(io.Discard, manifest); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
}

func TestEncodeManifestHardlinks(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello world")
	if err := os.Link(filepath.Join(root, "a.txt"), filepath.Join(root, "b.txt")); err != nil {
		t.Fatalf("hardlink: %v", err)
	}

	raw := runTXFERTest(t, root)
	entries, _ := parseSYNCResponseEntries(raw, nil)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// Without hardlink dedup, both entries have size=11 (total=22).
	// With hardlink dedup, one F entry has size=11 and one H entry has size=0 (total=11).
	var totalSize int64
	for _, e := range entries {
		totalSize += e.Size
	}
	if totalSize != 11 {
		t.Fatalf("expected total size 11 (hardlink dedup), got %d", totalSize)
	}

	// Exactly one entry should be type H.
	var hCount int
	for _, e := range entries {
		if e.Type == 'H' {
			hCount++
			if e.Size != 0 {
				t.Errorf("H entry %q has size=%d, want 0", e.Path, e.Size)
			}
			if e.LinkTarget < 0 {
				t.Errorf("H entry %q has no link target", e.Path)
			}
		}
	}
	if hCount != 1 {
		t.Fatalf("expected 1 H entry, got %d", hCount)
	}
}

func TestEncodeManifestSymlinks(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	if err := os.Symlink("a.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	raw := runTXFERTest(t, root)
	entries, _ := parseSYNCResponseEntries(raw, nil)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	var fCount, sCount int
	for _, e := range entries {
		switch e.Type {
		case 'F':
			fCount++
			if e.Size != 5 {
				t.Errorf("F entry %q has size=%d, want 5", e.Path, e.Size)
			}
		case 'S':
			sCount++
			if e.Size != 0 {
				t.Errorf("S entry %q has size=%d, want 0", e.Path, e.Size)
			}
			if e.LinkPath != "a.txt" {
				t.Errorf("S entry %q has LinkPath=%q, want %q", e.Path, e.LinkPath, "a.txt")
			}
		}
	}
	if fCount != 1 || sCount != 1 {
		t.Fatalf("expected 1 F + 1 S, got %d F + %d S", fCount, sCount)
	}
}

func TestEncodeManifestDirectories(t *testing.T) {
	root := t.TempDir()
	subDir := filepath.Join(root, "sub")
	if err := os.Mkdir(subDir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeTestFile(t, root, "sub/b.txt", "world")

	raw := runTXFERTest(t, root)
	entries, _ := parseSYNCResponseEntries(raw, nil)

	var dCount, fCount int
	for _, e := range entries {
		switch e.Type {
		case 'D':
			dCount++
			if e.Size != 0 {
				t.Errorf("D entry %q has size=%d, want 0", e.Path, e.Size)
			}
			if e.Mode&0o777 != 0o750 {
				t.Errorf("D entry %q has mode=%o, want 0750", e.Path, e.Mode)
			}
		case 'F':
			fCount++
		}
	}
	if dCount != 1 {
		t.Fatalf("expected 1 D entry, got %d", dCount)
	}
	if fCount != 1 {
		t.Fatalf("expected 1 F entry, got %d", fCount)
	}
}
