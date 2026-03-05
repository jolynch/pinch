package filexfer

import (
	"bytes"
	"encoding/hex"
	"io"
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

func xxh128Hex(data []byte) string {
	h := xxh3.Hash128(data).Bytes()
	return hex.EncodeToString(h[:])
}

func setupSingleFileTransfer(t *testing.T) (string, string) {
	t.Helper()
	resetTransferStore()
	dir := t.TempDir()
	fullPath := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(fullPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write fixture file: %v", err)
	}
	return setupTransferForPath(t, dir, fullPath, 5)
}

func setupTransferWithContent(t *testing.T, content []byte) (string, string) {
	t.Helper()
	resetTransferStore()
	dir := t.TempDir()
	fullPath := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(fullPath, content, 0o644); err != nil {
		t.Fatalf("write fixture file: %v", err)
	}
	return setupTransferForPath(t, dir, fullPath, int64(len(content)))
}

func setupTransferForPath(t *testing.T, dir string, fullPath string, expectedSize int64) (string, string) {
	t.Helper()

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
		if ok && len(tx.PathHash) == 1 && tx.PathHash[0] == expectedHash && tx.FileSize[0] == expectedSize {
			return txferID, fullPath
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for transfer registration")
	return "", ""
}

type decodedFrame struct {
	header      string
	meta        FileFrameMeta
	payload     []byte
	trailerLine string
	trailer     frameTrailer
}

func decodeFrameSequence(t *testing.T, body []byte) []decodedFrame {
	t.Helper()
	var frames []decodedFrame
	cursor := 0
	for cursor < len(body) {
		headerEnd := bytes.IndexByte(body[cursor:], '\n')
		if headerEnd < 0 {
			t.Fatalf("missing frame header newline")
		}
		headerLine := string(body[cursor : cursor+headerEnd])
		meta, err := parseFXHeader(headerLine)
		if err != nil {
			t.Fatalf("parseFXHeader failed: %v", err)
		}
		cursor += headerEnd + 1
		payloadEnd := cursor + int(meta.WireSize)
		if payloadEnd > len(body) {
			t.Fatalf("frame payload exceeds body")
		}
		payload := append([]byte(nil), body[cursor:payloadEnd]...)
		cursor = payloadEnd

		trailerEnd := bytes.IndexByte(body[cursor:], '\n')
		if trailerEnd < 0 {
			t.Fatalf("missing frame trailer newline")
		}
		trailerLine := string(body[cursor : cursor+trailerEnd])
		trailer, err := parseFXTrailer(trailerLine)
		if err != nil {
			t.Fatalf("parseFXTrailer failed: %v", err)
		}
		cursor += trailerEnd + 1

		frames = append(frames, decodedFrame{
			header:      headerLine,
			meta:        meta,
			payload:     payload,
			trailerLine: trailerLine,
			trailer:     trailer,
		})
	}
	return frames
}

func expectedFrameHashToken(header string, payload []byte, trailerLine string) string {
	idx := strings.LastIndex(trailerLine, " hash=")
	if idx < 0 {
		return ""
	}
	trailerPrefix := trailerLine[:idx]
	h := xxh3.New()
	_, _ = h.Write([]byte(header + "\n"))
	_, _ = h.Write(payload)
	_, _ = h.Write([]byte(trailerPrefix))
	return formatXXH64HashToken(h.Sum64())
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

func headerProp(header string, key string) (string, bool) {
	for _, tok := range strings.Fields(header) {
		if !strings.HasPrefix(tok, key+"=") {
			continue
		}
		return strings.TrimPrefix(tok, key+"="), true
	}
	return "", false
}

func decodeFramePayload(t *testing.T, comp string, payload []byte) []byte {
	t.Helper()
	reader, err := decodePayloadReaderByComp(bytes.NewReader(payload), comp)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	defer reader.Close()
	logical, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read decoded payload: %v", err)
	}
	return logical
}

func newFileRequest(txferID string, fileID string, query string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/fs/file/"+url.PathEscape(txferID)+"/"+fileID+query, nil)
	req.SetPathValue("txferid", txferID)
	req.SetPathValue("fid", fileID)
	return req
}

func newFileAckRequest(txferID string, fileID string, query string) *http.Request {
	req := httptest.NewRequest(http.MethodPut, "/fs/file/"+url.PathEscape(txferID)+"/"+fileID+"/ack"+query, nil)
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
	if !strings.HasPrefix(header, "FX/1 0 offset=0 size=5 wsize=5 comp=none enc=none hash=xxh128:"+xxh128Hex([]byte("hello"))+" max-wsize=") {
		t.Fatalf("unexpected frame header: %q", header)
	}
	if got, ok := headerProp(header, "max-wsize"); !ok || got != "1048576" {
		t.Fatalf("unexpected max-wsize: %q (present=%v)", got, ok)
	}
	if string(payload) != "hello" {
		t.Fatalf("unexpected payload: %q", payload)
	}
	parsedTrailer, err := parseFXTrailer(trailer)
	if err != nil {
		t.Fatalf("parse trailer: %v", err)
	}
	if parsedTrailer.TS <= 0 {
		t.Fatalf("missing/invalid trailer ts: %d", parsedTrailer.TS)
	}
	expectedHash := expectedFrameHashToken(header, payload, trailer)
	if parsedTrailer.HashToken != expectedHash {
		t.Fatalf("unexpected hash token: %q", parsedTrailer.HashToken)
	}
	if !strings.Contains(trailer, " meta:size=5 ") && !strings.HasSuffix(trailer, " meta:size=5") {
		t.Fatalf("expected terminal metadata in trailer: %q", trailer)
	}
}

func TestFileHandlerCompressedFrameHeaders(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	for _, enc := range []string{EncodingZstd, EncodingLz4} {
		req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath)+"&comp="+url.QueryEscape(enc))
		w := httptest.NewRecorder()

		FileHandler(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("encoding %s: expected 200, got %d", enc, resp.StatusCode)
		}
		header, _, trailer, ok := splitFrame(w.Body.Bytes())
		if !ok {
			t.Fatalf("encoding %s: expected frame header/payload/trailer", enc)
		}
		if !strings.Contains(header, " comp="+enc+" ") {
			t.Fatalf("encoding %s: unexpected frame header: %q", enc, header)
		}
		if !strings.HasPrefix(trailer, "FXT/1 0 status=ok ts=") {
			t.Fatalf("encoding %s: unexpected frame trailer: %q", enc, trailer)
		}
		if !strings.Contains(trailer, " hash=xxh64:") {
			t.Fatalf("encoding %s: missing hash token in trailer: %q", enc, trailer)
		}
	}
}

func TestFileHandlerPartialWindow(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath)+"&offset=1&size=3")
	w := httptest.NewRecorder()

	FileHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	header, payload, trailer, ok := splitFrame(w.Body.Bytes())
	if !ok {
		t.Fatalf("expected frame header/payload/trailer")
	}
	if !strings.HasPrefix(header, "FX/1 0 offset=1 size=3 wsize=3 comp=none enc=none hash=xxh128:"+xxh128Hex([]byte("ell"))+" max-wsize=") {
		t.Fatalf("unexpected frame header: %q", header)
	}
	if string(payload) != "ell" {
		t.Fatalf("unexpected payload: %q", payload)
	}
	parsedTrailer, err := parseFXTrailer(trailer)
	if err != nil {
		t.Fatalf("parse trailer: %v", err)
	}
	expectedHash := expectedFrameHashToken(header, payload, trailer)
	if parsedTrailer.HashToken != expectedHash {
		t.Fatalf("unexpected hash token: %q", parsedTrailer.HashToken)
	}
}

func TestFileHandlerMaxWSizeHintOnlyOnFirstFrame(t *testing.T) {
	content := bytes.Repeat([]byte("a"), int(defaultFileFrameLogicalSize+1024))
	txferID, fullPath := setupTransferWithContent(t, content)
	req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath))
	w := httptest.NewRecorder()

	FileHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	frames := decodeFrameSequence(t, w.Body.Bytes())
	if len(frames) < 2 {
		t.Fatalf("expected at least 2 frames, got %d", len(frames))
	}
	if frames[0].meta.MaxWireSizeHint <= 0 {
		t.Fatalf("expected first frame max-wsize hint, got %d", frames[0].meta.MaxWireSizeHint)
	}
	if frames[1].meta.MaxWireSizeHint != 0 {
		t.Fatalf("expected second frame max-wsize to be omitted, got %d", frames[1].meta.MaxWireSizeHint)
	}
}

func TestCeilingMaxWSizeBucketBytes(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		want int64
	}{
		{name: "8MiB", in: 8 * 1024 * 1024, want: 8 * 1024 * 1024},
		{name: "9MiB", in: 9 * 1024 * 1024, want: 16 * 1024 * 1024},
		{name: "16MiB", in: 16 * 1024 * 1024, want: 16 * 1024 * 1024},
		{name: "17MiB", in: 17 * 1024 * 1024, want: 32 * 1024 * 1024},
		{name: "32MiB", in: 32 * 1024 * 1024, want: 32 * 1024 * 1024},
		{name: "33MiB", in: 33 * 1024 * 1024, want: 64 * 1024 * 1024},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ceilingMaxWSizeBucketBytes(tc.in); got != tc.want {
				t.Fatalf("ceilingMaxWSizeBucketBytes(%d)=%d want=%d", tc.in, got, tc.want)
			}
		})
	}
}

func TestFileHandlerMultiFrameDefaultChunkSize(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789ABCDEF"), int((17*1024*1024)/16))
	txferID, fullPath := setupTransferWithContent(t, content)
	req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath))
	w := httptest.NewRecorder()

	FileHandler(w, req)

	if got := w.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("expected 200, got %d", got)
	}
	frames := decodeFrameSequence(t, w.Body.Bytes())
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
	expectedSizes := []int64{8 * 1024 * 1024, 8 * 1024 * 1024, 1 * 1024 * 1024}
	var offset int64
	for i, frame := range frames {
		if frame.meta.Size != expectedSizes[i] {
			t.Fatalf("frame %d: expected size %d got %d", i, expectedSizes[i], frame.meta.Size)
		}
		if frame.meta.Offset != offset {
			t.Fatalf("frame %d: expected offset %d got %d", i, offset, frame.meta.Offset)
		}
		if int64(len(frame.payload)) != frame.meta.WireSize {
			t.Fatalf("frame %d: wire size mismatch", i)
		}
		logical := frame.payload
		if frame.meta.Comp != "none" {
			logical = decodeFramePayload(t, frame.meta.Comp, frame.payload)
		}
		if int64(len(logical)) != frame.meta.Size {
			t.Fatalf("frame %d: logical size mismatch", i)
		}
		if frame.trailer.TS <= 0 {
			t.Fatalf("frame %d: missing trailer ts", i)
		}
		expectedHash := expectedFrameHashToken(frame.header, frame.payload, frame.trailerLine)
		if frame.trailer.HashToken != expectedHash {
			t.Fatalf("frame %d: invalid hash token %q", i, frame.trailer.HashToken)
		}
		offset += frame.meta.Size
		if i < len(frames)-1 {
			if frame.trailer.Next == nil || *frame.trailer.Next != offset {
				t.Fatalf("frame %d: expected next offset %d", i, offset)
			}
		} else if frame.trailer.Next == nil || *frame.trailer.Next != 0 {
			t.Fatalf("last frame should carry next=0")
		}
	}
	if !strings.Contains(w.Body.String(), " meta:size=") {
		t.Fatalf("expected terminal metadata tokens")
	}
}

func TestFileHandlerCompressedMultiFrameChecksumsAndNext(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789ABCDEF"), int((17*1024*1024)/16))
	txferID, fullPath := setupTransferWithContent(t, content)
	req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath)+"&comp="+EncodingZstd)
	w := httptest.NewRecorder()

	FileHandler(w, req)

	if got := w.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("expected 200, got %d", got)
	}
	frames := decodeFrameSequence(t, w.Body.Bytes())
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
	expectedSizes := []int64{8 * 1024 * 1024, 8 * 1024 * 1024, 1 * 1024 * 1024}
	var offset int64
	for i, frame := range frames {
		switch frame.meta.Comp {
		case EncodingZstd, EncodingLz4, "none":
		default:
			t.Fatalf("frame %d: unexpected comp=%s", i, frame.meta.Comp)
		}
		if frame.meta.Size != expectedSizes[i] {
			t.Fatalf("frame %d: expected size %d got %d", i, expectedSizes[i], frame.meta.Size)
		}
		if frame.meta.Offset != offset {
			t.Fatalf("frame %d: expected offset %d got %d", i, offset, frame.meta.Offset)
		}
		if int64(len(frame.payload)) != frame.meta.WireSize {
			t.Fatalf("frame %d: wire size mismatch", i)
		}
		logical := decodeFramePayload(t, frame.meta.Comp, frame.payload)
		if int64(len(logical)) != frame.meta.Size {
			t.Fatalf("frame %d: logical size mismatch", i)
		}
		expectedHash := expectedFrameHashToken(frame.header, frame.payload, frame.trailerLine)
		if frame.trailer.HashToken != expectedHash {
			t.Fatalf("frame %d: invalid hash token %q", i, frame.trailer.HashToken)
		}
		offset += frame.meta.Size
		if i < len(frames)-1 {
			if frame.trailer.Next == nil || *frame.trailer.Next != offset {
				t.Fatalf("frame %d: expected next offset %d", i, offset)
			}
		} else if frame.trailer.Next == nil || *frame.trailer.Next != 0 {
			t.Fatalf("last frame should carry next=0")
		}
	}
}

func TestFileHandlerWindowChunkingRespectsOffsetAndSize(t *testing.T) {
	content := bytes.Repeat([]byte("abcdefgh"), int((20*1024*1024)/8))
	txferID, fullPath := setupTransferWithContent(t, content)
	req := newFileRequest(
		txferID,
		"0",
		"?path="+url.QueryEscape(fullPath)+"&offset="+strconv.FormatInt(5*1024*1024, 10)+"&size="+strconv.FormatInt(10*1024*1024, 10),
	)
	w := httptest.NewRecorder()

	FileHandler(w, req)

	if got := w.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("expected 200, got %d", got)
	}
	frames := decodeFrameSequence(t, w.Body.Bytes())
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(frames))
	}
	if frames[0].meta.Offset != 5*1024*1024 || frames[0].meta.Size != 8*1024*1024 {
		t.Fatalf("unexpected first frame offset/size: off=%d size=%d", frames[0].meta.Offset, frames[0].meta.Size)
	}
	if frames[1].meta.Offset != 13*1024*1024 || frames[1].meta.Size != 2*1024*1024 {
		t.Fatalf("unexpected second frame offset/size: off=%d size=%d", frames[1].meta.Offset, frames[1].meta.Size)
	}
	if frames[0].trailer.Next == nil || *frames[0].trailer.Next != 13*1024*1024 {
		t.Fatalf("expected next=13631488 on first trailer")
	}
	if frames[1].trailer.Next == nil || *frames[1].trailer.Next != 0 {
		t.Fatalf("last trailer should include next=0")
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

func TestFileAckHandlerAckBytesUpdatesTransferProgress(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	makeReq := func(ack string) *http.Request {
		return newFileAckRequest(
			txferID,
			"0",
			"?path="+url.QueryEscape(fullPath)+"&ack-bytes="+url.QueryEscape(ack)+"&delta-bytes=1&recv-ms=1&sync-ms=1",
		)
	}

	w := httptest.NewRecorder()
	FileAckHandler(w, makeReq("2@1"))
	if got := w.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", got)
	}
	tx, ok := GetTransfer(txferID)
	if !ok {
		t.Fatalf("transfer not found")
	}
	if tx.Done != 0 || tx.DoneSize != 2 {
		t.Fatalf("unexpected counters after partial ack: done=%d doneSize=%d", tx.Done, tx.DoneSize)
	}

	// Seed full-file hash state by streaming the file once.
	w = httptest.NewRecorder()
	FileHandler(w, newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath)))
	if got := w.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("expected 200 while seeding file hash state, got %d", got)
	}

	w = httptest.NewRecorder()
	FileAckHandler(w, makeReq("5@1@xxh128:"+xxh128Hex([]byte("hello"))))
	if got := w.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", got)
	}
	tx, _ = GetTransfer(txferID)
	if tx.Done != 1 || tx.DoneSize != 5 {
		t.Fatalf("unexpected counters after full ack: done=%d doneSize=%d", tx.Done, tx.DoneSize)
	}
	if tx.State[0] != TransferStateDone {
		t.Fatalf("expected file state done, got %d", tx.State[0])
	}
}

func TestFileAckHandlerRejectsLegacyPositiveAckBytes(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	req := newFileAckRequest(txferID, "0", "?path="+url.QueryEscape(fullPath)+"&ack-bytes=2")
	w := httptest.NewRecorder()

	FileAckHandler(w, req)

	if got := w.Result().StatusCode; got != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", got)
	}
}

func TestFileAckHandlerAckReturnsNoContent(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	seed := newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath))
	FileHandler(httptest.NewRecorder(), seed)

	req := newFileAckRequest(
		txferID,
		"0",
		"?path="+url.QueryEscape(fullPath)+"&ack-bytes="+url.QueryEscape("5@1@xxh128:"+xxh128Hex([]byte("hello")))+"&delta-bytes=5&recv-ms=1&sync-ms=1",
	)
	w := httptest.NewRecorder()

	FileAckHandler(w, req)

	if got := w.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", got)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("expected empty body for ack-only request")
	}
}

func TestFileHandlerMissingFileReturns404AndFileAckMissing(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	if err := os.Remove(fullPath); err != nil {
		t.Fatalf("remove file: %v", err)
	}

	req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath))
	w := httptest.NewRecorder()
	FileHandler(w, req)
	if got := w.Result().StatusCode; got != http.StatusNotFound {
		t.Fatalf("expected 404 for missing file, got %d", got)
	}

	req = newFileAckRequest(txferID, "0", "?path="+url.QueryEscape(fullPath)+"&ack-bytes=-1")
	w = httptest.NewRecorder()
	FileAckHandler(w, req)
	if got := w.Result().StatusCode; got != http.StatusNoContent {
		t.Fatalf("expected 204 for missing-file ack, got %d", got)
	}

	tx, ok := GetTransfer(txferID)
	if !ok {
		t.Fatalf("transfer not found")
	}
	if tx.State[0] != TransferStateMissing {
		t.Fatalf("expected missing state, got %d", tx.State[0])
	}
	if tx.Done != 1 {
		t.Fatalf("expected done=1 after missing ack, got %d", tx.Done)
	}
}

func TestFileHandlerRejectsAckBytesOnGet(t *testing.T) {
	txferID, fullPath := setupSingleFileTransfer(t)
	req := newFileRequest(txferID, "0", "?path="+url.QueryEscape(fullPath)+"&ack-bytes=1@1")
	w := httptest.NewRecorder()

	FileHandler(w, req)

	if got := w.Result().StatusCode; got != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", got)
	}
}

func TestCompressionRatio(t *testing.T) {
	if got := compressionRatio(100, 120); got <= 0 {
		t.Fatalf("expected positive ratio, got %f", got)
	}
	if got := compressionRatio(100, 0); got != 0 {
		t.Fatalf("expected zero ratio when wire size is zero, got %f", got)
	}
}
