package filexfer

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zeebo/xxh3"
)

func xxh128HexCLI(data []byte) string {
	h := xxh3.Hash128(data).Bytes()
	return hex.EncodeToString(h[:])
}

func buildCLIFrame(fileID uint64, body []byte, offset int64) string {
	xsum := xxh128HexCLI(body)
	return fmt.Sprintf(
		"FX/1 %d offset=%d size=%d wsize=%d comp=none enc=none hash=xxh128:%s ts=1000\n%sFXT/1 %d status=ok ts=1001 hash=xxh128:%s\n",
		fileID,
		offset,
		len(body),
		len(body),
		xsum,
		string(body),
		fileID,
		xsum,
	)
}

func TestRunCLITransferAndGet(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txcli 7:/remote",
		"0 5 0:100 0:5:a.txt",
		"",
	}, "\n")
	fileBody := []byte("hello")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/fs/transfer":
			_, _ = w.Write([]byte(manifestRaw))
		case r.Method == http.MethodGet && r.URL.Path == "/fs/file/txcli/0":
			if ack := r.URL.Query().Get("ack-bytes"); ack != "" {
				expectedAck := "5@1001@xxh128:" + xxh128HexCLI(fileBody)
				if ack != expectedAck {
					t.Fatalf("expected ack-bytes=%s, got %q", expectedAck, ack)
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			_, _ = w.Write([]byte(buildCLIFrame(0, fileBody, 0)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	manifestPath := filepath.Join(tmp, "txcli.fm1")
	code := RunCLI(
		[]string{srv.URL, "transfer", "-s", "/remote", "-o", manifestPath},
		&stdout,
		&stderr,
	)
	if code != 0 {
		t.Fatalf("transfer: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("manifest not written: %v", err)
	}

	manifestPath2 := filepath.Join(tmp, "txcli-long.fm1")
	stdout.Reset()
	stderr.Reset()
	code = RunCLI(
		[]string{srv.URL, "transfer", "--source-directory", "/remote", "-o", manifestPath2},
		&stdout,
		&stderr,
	)
	if code != 0 {
		t.Fatalf("transfer long flag: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(manifestPath2); err != nil {
		t.Fatalf("manifest not written for long flag: %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	outRoot := filepath.Join(tmp, "out")
	code = RunCLI(
		[]string{srv.URL, "get", "--tid", "txcli", "--fd", "0", "--manifest", manifestPath, "--out-root", outRoot},
		&stdout,
		&stderr,
	)
	if code != 0 {
		t.Fatalf("get: expected 0, got %d stderr=%s", code, stderr.String())
	}
	outFile := filepath.Join(outRoot, "a.txt")
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("unexpected output: %q", string(got))
	}
}

func TestRunCLIStartDownloadsAll(t *testing.T) {
	tmp := t.TempDir()
	manifestPath := filepath.Join(tmp, "txstart.fm1")
	manifestRaw := strings.Join([]string{
		"FM/1 txstart 7:/remote",
		"0 5 0:100 0:5:a.txt",
		"1 4 0:101 0:5:b.txt",
		"",
	}, "\n")
	if err := os.WriteFile(manifestPath, []byte(manifestRaw), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fs/file/txstart/0":
			if r.URL.Query().Get("ack-bytes") != "" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			body := []byte("hello")
			_, _ = w.Write([]byte(buildCLIFrame(0, body, 0)))
		case "/fs/file/txstart/1":
			if r.URL.Query().Get("ack-bytes") != "" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			body := []byte("test")
			_, _ = w.Write([]byte(buildCLIFrame(1, body, 0)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	outRoot := filepath.Join(tmp, "out")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI(
		[]string{srv.URL, "start", "--tid", "txstart", "--manifest", manifestPath, "--out-root", outRoot, "--concurrency", "2"},
		&stdout,
		&stderr,
	)
	if code != 0 {
		t.Fatalf("start: expected 0, got %d stderr=%s", code, stderr.String())
	}
	for _, p := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(outRoot, p)); err != nil {
			t.Fatalf("missing output %s: %v", p, err)
		}
	}
}

func TestRunCLIStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fs/transfer/abc/status" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"transfer_id":"abc","directory":"/r","num_files":10,"total_size":1000,"done":3,"done_size":200,"percent_files":30,"percent_bytes":20,"download_status":{"started":5,"running":2,"done":3,"missing":0}}`))
	}))
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "status", "--tid", "abc"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("status: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "complete: files=30.00% bytes=20.00%") {
		t.Fatalf("unexpected status output: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "downloads: started=5 running=2 done=3 missing=0") {
		t.Fatalf("unexpected status downloads output: %s", stdout.String())
	}
}

func TestRunCLIUsageErrors(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if code := RunCLI([]string{}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2, got %d", code)
	}
	if code := RunCLI([]string{"http://x", "bogus"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for unknown cmd, got %d", code)
	}
	if code := RunCLI([]string{"http://x", "get", "--tid", "t"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for missing --fd, got %d", code)
	}
	if code := RunCLI([]string{"http://x", "transfer", "--directory", "/tmp"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for legacy --directory flag, got %d", code)
	}
	stderr.Reset()
	if code := RunCLI([]string{"--tid", "tx", "get"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for missing server URL, got %d", code)
	}
	if !strings.Contains(stderr.String(), "first argument must be server URL") {
		t.Fatalf("expected explicit server-url error, got: %s", stderr.String())
	}
}
