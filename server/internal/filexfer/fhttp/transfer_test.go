package fhttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
)

type errorWriter struct {
	err       error
	failAfter int
	wrote     int
}

func (w *errorWriter) Write(p []byte) (int, error) {
	if w.failAfter >= 0 && w.wrote >= w.failAfter {
		return 0, w.err
	}
	n := len(p)
	if w.failAfter >= 0 && w.wrote+n > w.failAfter {
		allowed := w.failAfter - w.wrote
		w.wrote += allowed
		return allowed, w.err
	}
	w.wrote += n
	return n, nil
}

type failingResponseWriter struct {
	header http.Header
	status int
	body   *errorWriter
}

func newFailingResponseWriter(err error, failAfter int) *failingResponseWriter {
	return &failingResponseWriter{
		header: make(http.Header),
		body: &errorWriter{
			err:       err,
			failAfter: failAfter,
		},
	}
}

func (w *failingResponseWriter) Header() http.Header {
	return w.header
}

func (w *failingResponseWriter) Write(p []byte) (int, error) {
	return w.body.Write(p)
}

func (w *failingResponseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}

func createManifestFixtureDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	file := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(file, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write fixture file: %v", err)
	}
	return dir
}

func TestTransferHandlerBrokenPipeDeletesTransfer(t *testing.T) {
	resetTransferStore()
	dir := createManifestFixtureDir(t)
	req := httptest.NewRequest(http.MethodGet, "/transfer?directory="+url.QueryEscape(dir), nil)
	w := newFailingResponseWriter(syscall.EPIPE, 0)

	TransferHandler(w, req)

	if count := transferCountForTest(); count != 0 {
		t.Fatalf("expected broken pipe to delete transfer, found %d transfers", count)
	}
}

func TestTransferHandlerNonBrokenWriteErrorKeepsTransfer(t *testing.T) {
	resetTransferStore()
	dir := createManifestFixtureDir(t)
	req := httptest.NewRequest(http.MethodGet, "/transfer?directory="+url.QueryEscape(dir), nil)
	w := newFailingResponseWriter(io.ErrUnexpectedEOF, 0)

	TransferHandler(w, req)

	if count := transferCountForTest(); count != 1 {
		t.Fatalf("expected non-broken error to keep transfer, found %d transfers", count)
	}
	for _, transfer := range listTransfersForTest() {
		if len(transfer.State) != transfer.NumFiles {
			t.Fatalf("expected %d file states, got %d", transfer.NumFiles, len(transfer.State))
		}
		if len(transfer.PathHash) != transfer.NumFiles {
			t.Fatalf("expected %d file hashes, got %d", transfer.NumFiles, len(transfer.PathHash))
		}
		if len(transfer.FileSize) != transfer.NumFiles {
			t.Fatalf("expected %d file sizes, got %d", transfer.NumFiles, len(transfer.FileSize))
		}
		if len(transfer.AckedSize) != transfer.NumFiles {
			t.Fatalf("expected %d file acked sizes, got %d", transfer.NumFiles, len(transfer.AckedSize))
		}
	}
}

func TestTransferHandlerRegistersFullPathHash(t *testing.T) {
	resetTransferStore()
	dir := createManifestFixtureDir(t)
	req := httptest.NewRequest(http.MethodGet, "/transfer?directory="+url.QueryEscape(dir), nil)
	w := httptest.NewRecorder()

	TransferHandler(w, req)

	transfers := listTransfersForTest()
	if len(transfers) != 1 {
		t.Fatalf("expected one transfer, got %d", len(transfers))
	}
	txferID := transfers[0].ID
	expected := xxh3.Hash128([]byte(filepath.Clean(filepath.Join(dir, "a.txt"))))

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		states, ok := GetTransferFileStates(txferID)
		if ok && len(states) == 1 && states[0].PathHash == expected {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	states, ok := GetTransferFileStates(txferID)
	if !ok {
		t.Fatalf("transfer %q not found", txferID)
	}
	if len(states) != 1 {
		t.Fatalf("expected one file state, got %d", len(states))
	}
	t.Fatalf("expected hash %v, got %v", expected, states[0].PathHash)
}
