package filexfer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type progressStatusFixture struct {
	mu     sync.Mutex
	status string
	pct    int
}

func (f *progressStatusFixture) set(status string, pct int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = status
	f.pct = pct
}

func (f *progressStatusFixture) get() (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status, f.pct
}

func waitForFileContent(t *testing.T, path string, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil && string(raw) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read progress file %s: %v", path, err)
	}
	t.Fatalf("progress file mismatch:\n got=%q\nwant=%q", string(raw), want)
}

func TestStartProgressFileWriterTruncatesThenAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.txt")
	if err := os.WriteFile(path, []byte("stale\n99\n"), 0o644); err != nil {
		t.Fatalf("seed progress file: %v", err)
	}

	status := &progressStatusFixture{}
	status.set("first", 10)
	stop := StartProgressFileWriter(context.Background(), path, 10*time.Millisecond, status.get)
	t.Cleanup(func() { stop(false) })

	waitForFileContent(t, path, "first\n10\n")
	status.set("second", 20)
	waitForFileContent(t, path, "first\n10\nsecond\n20\n")
}

func TestStartProgressFileWriterDeduplicatesByStatusAndPct(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.txt")
	status := &progressStatusFixture{}
	status.set("same", 42)
	stop := StartProgressFileWriter(context.Background(), path, 10*time.Millisecond, status.get)
	t.Cleanup(func() { stop(false) })

	waitForFileContent(t, path, "same\n42\n")
	time.Sleep(40 * time.Millisecond)
	waitForFileContent(t, path, "same\n42\n")

	status.set("other", 42)
	waitForFileContent(t, path, "same\n42\nother\n42\n")

	status.set("other", 43)
	waitForFileContent(t, path, "same\n42\nother\n42\nother\n43\n")
}

func TestStartProgressFileWriterStopSuccessWritesFinal100(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.txt")
	status := &progressStatusFixture{}
	status.set("done", 64)

	stop := StartProgressFileWriter(context.Background(), path, time.Hour, status.get)
	stop(true)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read progress file: %v", err)
	}
	if got, want := string(raw), "done\n100\n"; got != want {
		t.Fatalf("final progress mismatch: got %q want %q", got, want)
	}
}

func TestStartProgressFileWriterStopFailureWritesCurrentPct(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.txt")
	status := &progressStatusFixture{}
	status.set("failed", 64)

	stop := StartProgressFileWriter(context.Background(), path, time.Hour, status.get)
	stop(false)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read progress file: %v", err)
	}
	if got, want := string(raw), "failed\n64\n"; got != want {
		t.Fatalf("final progress mismatch: got %q want %q", got, want)
	}
}

func TestFormatProgressStatusLine(t *testing.T) {
	got := FormatProgressStatusLine("server", "1ef1a42b", 9121, 10127, 18_350_000_000, 18_790_000_000)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("expected valid json, got %q err=%v", got, err)
	}
	if parsed["source"] != "server" {
		t.Fatalf("expected server source, got %#v", parsed["source"])
	}
	if parsed["txid"] != "1ef1a42b" {
		t.Fatalf("expected txid, got %#v", parsed["txid"])
	}
	files, ok := parsed["files"].(map[string]any)
	if !ok {
		t.Fatalf("expected files object, got %#v", parsed["files"])
	}
	if files["percent"] != "90.1" {
		t.Fatalf("expected files percent string, got %#v", files["percent"])
	}
	bytes, ok := parsed["bytes"].(map[string]any)
	if !ok {
		t.Fatalf("expected bytes object, got %#v", parsed["bytes"])
	}
	if bytes["percent"] != "97.7" {
		t.Fatalf("expected bytes percent string, got %#v", bytes["percent"])
	}
	if _, ok := parsed["bytes_human"].(map[string]any); !ok {
		t.Fatalf("expected bytes_human object, got %#v", parsed["bytes_human"])
	}
}
