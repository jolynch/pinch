package filexfer

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
)

func setupSingleFileTransfer(t *testing.T) (string, string) {
	t.Helper()
	resetTransferStore()
	dir := t.TempDir()
	fullPath := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(fullPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write fixture file: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/transfer?directory="+url.QueryEscape(dir), nil)
	w := httptest.NewRecorder()
	TransferHandler(w, req)

	transfers := listTransfersForTest()
	if len(transfers) != 1 {
		t.Fatalf("expected one transfer, got %d", len(transfers))
	}
	txferID := transfers[0].ID
	expectedHash := xxh3.Hash128([]byte(filepath.Clean(fullPath)))

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		tx, ok := GetTransfer(txferID)
		if ok && len(tx.PathHash) == 1 && tx.PathHash[0] == expectedHash && tx.FileSize[0] == 5 {
			return txferID, fullPath
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for transfer registration")
	return "", ""
}

func splitFrame(body []byte) (string, []byte, string, bool) {
	headerEnd := bytes.IndexByte(body, '\n')
	if headerEnd < 0 {
		return "", nil, "", false
	}
	header := string(body[:headerEnd])
	wsize := -1
	for _, tok := range strings.Fields(header) {
		if !strings.HasPrefix(tok, "wsize=") {
			continue
		}
		v, err := strconv.Atoi(strings.TrimPrefix(tok, "wsize="))
		if err != nil || v < 0 {
			return "", nil, "", false
		}
		wsize = v
		break
	}
	if wsize < 0 {
		return "", nil, "", false
	}

	payloadStart := headerEnd + 1
	payloadEnd := payloadStart + wsize
	if payloadEnd > len(body) {
		return "", nil, "", false
	}

	trailerBytes := body[payloadEnd:]
	if len(trailerBytes) == 0 || trailerBytes[len(trailerBytes)-1] != '\n' {
		return "", nil, "", false
	}
	trailer := string(trailerBytes[:len(trailerBytes)-1])
	payload := append([]byte(nil), body[payloadStart:payloadEnd]...)
	return header, payload, trailer, true
}

func newFileRequest(txferID string, fileID string, query string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/fs/file/"+url.PathEscape(txferID)+"/"+fileID+query, nil)
	req.SetPathValue("txferid", txferID)
	req.SetPathValue("fid", fileID)
	return req
}

func TestFileHandlerIdentityFrame(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath))
	w := httptest.NewRecorder()

	FileHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Encoding"); got != "" {
		t.Fatalf("expected no content-encoding, got %q", got)
	}
	header, payload, trailer, ok := splitFrame(w.Body.Bytes())
	if !ok {
		t.Fatalf("expected frame header, payload, and trailer")
	}
	if !strings.HasPrefix(header, "FX/1 0 comp=none enc=none offset=0 size=5 wsize=5") {
		t.Fatalf("unexpected frame header: %q", header)
	}
	if string(payload) != "hello" {
		t.Fatalf("unexpected payload: %q", payload)
	}
	expectedTrailer := fmt.Sprintf("FXT/1 0 status=ok xsum:xxh3=%016x", xxh3.Hash([]byte("hello")))
	if trailer != expectedTrailer {
		t.Fatalf("unexpected frame trailer: %q", trailer)
	}
}

func TestFileHandlerCompressedFrameHeaders(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	for _, enc := range []string{EncodingZstd, EncodingLz4} {
		req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath))
		req.Header.Set("Accept-Encoding", enc)
		w := httptest.NewRecorder()

		FileHandler(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("encoding %s: expected 200, got %d", enc, resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Encoding"); got != enc {
			t.Fatalf("encoding %s: expected content-encoding %q, got %q", enc, enc, got)
		}
		header, _, trailer, ok := splitFrame(w.Body.Bytes())
		if !ok {
			t.Fatalf("encoding %s: expected frame header/payload/trailer", enc)
		}
		if !strings.Contains(header, " comp="+enc+" ") {
			t.Fatalf("encoding %s: unexpected frame header: %q", enc, header)
		}
		if !strings.HasPrefix(trailer, "FXT/1 0 status=ok xsum:xxh3=") {
			t.Fatalf("encoding %s: unexpected frame trailer: %q", enc, trailer)
		}
	}
}

func TestFileHandlerRejectsPathMismatch(t *testing.T) {
	txferID, _ := setupSingleFileTransfer(t)
	wrongPath := filepath.Clean(filepath.Join(t.TempDir(), "wrong.txt"))
	if err := os.WriteFile(wrongPath, []byte("x"), 0o644); err != nil {
		t.Fatalf("write wrong fixture: %v", err)
	}
	req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(wrongPath))
	w := httptest.NewRecorder()

	FileHandler(w, req)

	if got := w.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", got)
	}
}

func TestFileHandlerRejectsPathOutsideRoot(t *testing.T) {
	txferID, _ := setupSingleFileTransfer(t)
	outside := filepath.Clean(filepath.Join(t.TempDir(), "outside.txt"))
	if err := os.WriteFile(outside, []byte("x"), 0o644); err != nil {
		t.Fatalf("write outside fixture: %v", err)
	}
	req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(outside))
	w := httptest.NewRecorder()

	FileHandler(w, req)

	if got := w.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", got)
	}
}

func TestFileHandlerAckBytesUpdatesTransferProgress(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	makeReq := func(ack string) *http.Request {
		return newFileRequest(
			txferID,
			"0",
			"?path="+url.QueryEscape(fullPath)+"&ack-bytes="+url.QueryEscape(ack),
		)
	}

	w := httptest.NewRecorder()
	FileHandler(w, makeReq("2"))
	if got := w.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("expected 200, got %d", got)
	}
	tx, ok := GetTransfer(txferID)
	if !ok {
		t.Fatalf("transfer not found")
	}
	if tx.Done != 0 || tx.DoneSize != 2 {
		t.Fatalf("unexpected counters after partial ack: done=%d doneSize=%d", tx.Done, tx.DoneSize)
	}

	w = httptest.NewRecorder()
	FileHandler(w, makeReq("5"))
	if got := w.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("expected 200, got %d", got)
	}
	tx, _ = GetTransfer(txferID)
	if tx.Done != 1 || tx.DoneSize != 5 {
		t.Fatalf("unexpected counters after full ack: done=%d doneSize=%d", tx.Done, tx.DoneSize)
	}
	if tx.State[0] != TransferStateDone {
		t.Fatalf("expected file state done, got %d", tx.State[0])
	}
}
