package filexfer

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/zeebo/xxh3"
)

func TestFileChecksumHandlerDefault(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	req := httptest.NewRequest(http.MethodGet, "/fs/file/"+url.PathEscape(txferID)+"/0/checksum?path="+url.QueryEscape(fullPath), nil)
	req.SetPathValue("txferid", txferID)
	req.SetPathValue("fid", "0")
	w := httptest.NewRecorder()

	FileChecksumHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body := w.Body.String()
	if !strings.Contains(body, " wsize=0 ") {
		t.Fatalf("expected wsize=0 frame output, got %q", body)
	}
	if !strings.Contains(body, " hash=xxh128:") {
		t.Fatalf("expected xxh128 hash token")
	}
	if !strings.Contains(body, " file-hash=xxh128:") {
		t.Fatalf("expected final file-hash token")
	}
	if !strings.Contains(body, " next=0 ") {
		t.Fatalf("expected terminal next=0")
	}
	if !strings.Contains(body, " meta:size=5") {
		t.Fatalf("expected terminal metadata")
	}
}

func TestFileChecksumHandlerWindowAndAlgorithms(t *testing.T) {
	content := []byte("hello world")
	txferID, fullPath := setupTransferWithContent(t, content)
	req := httptest.NewRequest(
		http.MethodGet,
		"/fs/file/"+url.PathEscape(txferID)+"/0/checksum?path="+url.QueryEscape(fullPath)+"&window-size=4&checksum=xxh128&checksum=xxh64",
		nil,
	)
	req.SetPathValue("txferid", txferID)
	req.SetPathValue("fid", "0")
	w := httptest.NewRecorder()

	FileChecksumHandler(w, req)

	if got := w.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("expected 200, got %d", got)
	}
	frames := decodeFrameSequence(t, w.Body.Bytes())
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
	if frames[0].meta.Size != 4 || frames[1].meta.Size != 4 || frames[2].meta.Size != 3 {
		t.Fatalf("unexpected chunk sizes: %d,%d,%d", frames[0].meta.Size, frames[1].meta.Size, frames[2].meta.Size)
	}
	for i, frame := range frames {
		if frame.meta.WireSize != 0 {
			t.Fatalf("frame %d expected wsize=0, got %d", i, frame.meta.WireSize)
		}
	}
	if frames[2].trailer.Next == nil || *frames[2].trailer.Next != 0 {
		t.Fatalf("expected terminal next=0")
	}

	full128 := xxh3.Hash128(content).Bytes()
	expected128 := "xxh128:" + hex.EncodeToString(full128[:])
	expected64 := formatXXH64HashToken(xxh3.Hash(content))
	body := string(w.Body.Bytes())
	if !strings.Contains(body, " hash=xxh64:") {
		t.Fatalf("expected per-window xxh64 hash tokens")
	}
	if !strings.Contains(body, " file-hash="+expected128) {
		t.Fatalf("missing expected full xxh128 token")
	}
	if !strings.Contains(body, " file-hash="+expected64) {
		t.Fatalf("missing expected full xxh64 token")
	}
	if !strings.Contains(body, " meta:size=11") {
		t.Fatalf("expected terminal metadata")
	}
}

func TestFileChecksumHandlerRejectsBadChecksum(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	req := httptest.NewRequest(http.MethodGet, "/fs/file/"+url.PathEscape(txferID)+"/0/checksum?path="+url.QueryEscape(fullPath)+"&checksum=sha256", nil)
	req.SetPathValue("txferid", txferID)
	req.SetPathValue("fid", "0")
	w := httptest.NewRecorder()

	FileChecksumHandler(w, req)

	if got := w.Result().StatusCode; got != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", got)
	}
	if !bytes.Contains(w.Body.Bytes(), []byte("checksum")) {
		t.Fatalf("expected checksum error message")
	}
}
