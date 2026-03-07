package filexfer

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zeebo/xxh3"
)

func readAndClose(t *testing.T, r io.ReadCloser) ([]byte, error) {
	t.Helper()
	b, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil {
		return b, readErr
	}
	return b, closeErr
}

func xxh128HexTest(data []byte) string {
	h := xxh3.Hash128(data).Bytes()
	return hex.EncodeToString(h[:])
}

func frameHash64Token(header string, payload []byte, trailerPrefix string) string {
	h := xxh3.New()
	_, _ = h.Write([]byte(header))
	if len(payload) > 0 {
		_, _ = h.Write(payload)
	}
	_, _ = h.Write([]byte(trailerPrefix))
	return formatXXH64HashToken(h.Sum64())
}

func buildFXFrame(t *testing.T, fileID uint64, comp string, offset int64, logical []byte, next *int64) string {
	t.Helper()
	payload, err := encodeSingleFramePayload(logical, comp)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	headerTS := int64(1000 + offset)
	trailerTS := headerTS + 1
	xsum := xxh128HexTest(logical)
	header := fmt.Sprintf(
		"FX/1 %d offset=%d size=%d wsize=%d comp=%s enc=none hash=xxh128:%s ts=%d\n",
		fileID,
		offset,
		len(logical),
		len(payload),
		comp,
		xsum,
		headerTS,
	)
	trailerPrefix := fmt.Sprintf("FXT/1 %d status=ok ts=%d", fileID, trailerTS)
	if next != nil {
		trailerPrefix += fmt.Sprintf(" next=%d", *next)
	}
	trailer := trailerPrefix + " hash=" + frameHash64Token(header, payload, trailerPrefix)
	return header + string(payload) + trailer + "\n"
}

func TestParseFXHeaderMaxWSizeHint(t *testing.T) {
	meta, err := parseFXHeader("FX/1 7 offset=0 size=5 wsize=5 comp=none enc=none hash=xxh128:abc max-wsize=16777216 ts=1000")
	if err != nil {
		t.Fatalf("parseFXHeader failed: %v", err)
	}
	if meta.MaxWireSizeHint != 16*1024*1024 {
		t.Fatalf("unexpected max-wire hint: %d", meta.MaxWireSizeHint)
	}
}

func TestParseFXHeaderInvalidMaxWSizeHint(t *testing.T) {
	if _, err := parseFXHeader("FX/1 7 offset=0 size=5 wsize=5 comp=none enc=none hash=xxh128:abc max-wsize=-1 ts=1000"); err == nil {
		t.Fatalf("expected parse error for negative max-wsize")
	}
	if _, err := parseFXHeader("FX/1 7 offset=0 size=5 wsize=5 comp=none enc=none hash=xxh128:abc max-wsize=nope ts=1000"); err == nil {
		t.Fatalf("expected parse error for malformed max-wsize")
	}
}

func TestEffectiveFrameReadBufferSize(t *testing.T) {
	tests := []struct {
		name     string
		base     int
		hint     int64
		cap      int
		expected int
	}{
		{name: "hint below cap", base: 8 * 1024 * 1024, hint: 16 * 1024 * 1024, cap: 64 * 1024 * 1024, expected: 16 * 1024 * 1024},
		{name: "hint above cap", base: 8 * 1024 * 1024, hint: 128 * 1024 * 1024, cap: 64 * 1024 * 1024, expected: 64 * 1024 * 1024},
		{name: "fallback to base", base: 2 * 1024 * 1024, hint: 0, cap: 64 * 1024 * 1024, expected: 2 * 1024 * 1024},
		{name: "default base", base: 0, hint: 0, cap: 64 * 1024 * 1024, expected: defaultClientFrameBufferBytes},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveFrameReadBufferSize(tc.base, tc.hint, tc.cap); got != tc.expected {
				t.Fatalf("effectiveFrameReadBufferSize(%d,%d,%d)=%d want=%d", tc.base, tc.hint, tc.cap, got, tc.expected)
			}
		})
	}
}

func TestParseManifestSingleChunk(t *testing.T) {
	raw := strings.Join([]string{
		"FM/1 tx123 5:/root",
		"0 5 0:100 0:5:a.txt",
		"1 7 2:1 0:9:dir/b.txt",
		"",
	}, "\n")

	manifest, err := parseManifest([]byte(raw))
	if err != nil {
		t.Fatalf("parseManifest failed: %v", err)
	}
	if manifest.TransferID != "tx123" {
		t.Fatalf("unexpected transfer id: %q", manifest.TransferID)
	}
	if manifest.Root != "/root" {
		t.Fatalf("unexpected root: %q", manifest.Root)
	}
	if len(manifest.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(manifest.Entries))
	}
	if manifest.Entries[1].Mtime != 101 {
		t.Fatalf("expected decoded mtime 101, got %d", manifest.Entries[1].Mtime)
	}
}

func TestParseManifestMultiChunk(t *testing.T) {
	raw := strings.Join([]string{
		"FM/1 tx456 6:/root2",
		"0 5 0:100 0:5:a.txt",
		"",
		"FM/1 tx456 6:/root2",
		"1 3 0:200 0:5:b.txt",
		"",
	}, "\n")

	manifest, err := parseManifest([]byte(raw))
	if err != nil {
		t.Fatalf("parseManifest failed: %v", err)
	}
	if len(manifest.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(manifest.Entries))
	}
}

func TestParseManifestMalformed(t *testing.T) {
	raw := strings.Join([]string{
		"FM/1 tx789 5:/root",
		"0 5 0:abc 0:5:a.txt",
		"",
	}, "\n")
	if _, err := parseManifest([]byte(raw)); err == nil {
		t.Fatalf("expected parseManifest error")
	}
}

func TestFetchFileDecodesByHeaderComp(t *testing.T) {
	logical := []byte("hello world")
	for _, comp := range []string{"none", EncodingZstd, EncodingLz4} {
		comp := comp
		t.Run(comp, func(t *testing.T) {
			frame := buildFXFrame(t, 7, comp, 0, logical, nil)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Encoding", "zstd")
				_, _ = w.Write([]byte(frame))
			}))
			defer srv.Close()

			client := NewClient(srv.URL, nil)
			resp, err := client.FetchFile(context.Background(), FetchFileRequest{
				TransferID:     "tx",
				FileID:         7,
				FullPath:       "/root/a.txt",
				AcceptEncoding: defaultCLIEncodings,
				AckBytes:       -1,
			})
			if err != nil {
				t.Fatalf("FetchFile failed: %v", err)
			}
			got, err := readAndClose(t, resp.Reader)
			if err != nil {
				t.Fatalf("FetchFile read failed: %v", err)
			}
			if !bytes.Equal(got, logical) {
				t.Fatalf("unexpected logical bytes: %q", got)
			}
			if resp.Meta.Comp != comp {
				t.Fatalf("expected comp %q, got %q", comp, resp.Meta.Comp)
			}
		})
	}
}

func TestFetchFileRejectsChecksumMismatch(t *testing.T) {
	logical := []byte("hello")
	payload, err := encodeSingleFramePayload(logical, "none")
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	frame := fmt.Sprintf(
		"FX/1 0 offset=0 size=%d wsize=%d comp=none enc=none hash=xxh128:%s ts=1000\n",
		len(logical),
		len(payload),
		"00000000000000000000000000000000",
	)
	trailerPrefix := "FXT/1 0 status=ok ts=1001"
	badHash := "xxh64:0000000000000000"
	frame += string(payload) + trailerPrefix + " hash=" + badHash + "\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(frame))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", FileID: 0, FullPath: "/root/a.txt", AcceptEncoding: defaultCLIEncodings, AckBytes: -1})
	if err != nil {
		t.Fatalf("FetchFile setup failed: %v", err)
	}
	if _, err := readAndClose(t, resp.Reader); err == nil {
		t.Fatalf("expected checksum mismatch error")
	}
}

func TestFetchFileRejectsMalformedTrailer(t *testing.T) {
	logical := []byte("hello")
	frame := fmt.Sprintf(
		"FX/1 0 offset=0 size=%d wsize=%d comp=none enc=none hash=xxh128:%s ts=1000\n%sFXT/1 0 status=ok ts=1001\n",
		len(logical),
		len(logical),
		xxh128HexTest(logical),
		string(logical),
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(frame))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", FileID: 0, FullPath: "/root/a.txt", AcceptEncoding: defaultCLIEncodings, AckBytes: -1})
	if err != nil {
		t.Fatalf("FetchFile setup failed: %v", err)
	}
	if _, err := readAndClose(t, resp.Reader); err == nil {
		t.Fatalf("expected malformed trailer error")
	}
}

func TestFetchFileRejectsPayloadLengthMismatch(t *testing.T) {
	header := "FX/1 0 offset=0 size=5 wsize=8 comp=none enc=none hash=xxh128:00000000000000000000000000000000 ts=1000\n"
	payload := []byte("hello")
	trailerPrefix := "FXT/1 0 status=ok ts=1001"
	frame := header + string(payload) + trailerPrefix + " hash=" + frameHash64Token(header, payload, trailerPrefix) + "\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(frame))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", FileID: 0, FullPath: "/root/a.txt", AcceptEncoding: defaultCLIEncodings, AckBytes: -1})
	if err != nil {
		t.Fatalf("FetchFile setup failed: %v", err)
	}
	if _, err := readAndClose(t, resp.Reader); err == nil {
		t.Fatalf("expected payload length error")
	}
}

func TestFetchFileRejectsUnsupportedEnc(t *testing.T) {
	logical := []byte("hello")
	header := fmt.Sprintf(
		"FX/1 0 offset=0 size=%d wsize=%d comp=none enc=age hash=xxh128:%s ts=1000\n",
		len(logical),
		len(logical),
		xxh128HexTest(logical),
	)
	trailerPrefix := "FXT/1 0 status=ok ts=1001"
	frame := header + string(logical) + trailerPrefix + " hash=" + frameHash64Token(header, logical, trailerPrefix) + "\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(frame))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	if _, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", FileID: 0, FullPath: "/root/a.txt", AcceptEncoding: defaultCLIEncodings, AckBytes: -1}); err == nil {
		t.Fatalf("expected unsupported enc error")
	}
}

func TestFetchFileDecodesMultiFrameSequence(t *testing.T) {
	var body strings.Builder
	next := int64(5)
	body.WriteString(buildFXFrame(t, 0, "none", 0, []byte("hello"), &next))
	body.WriteString(buildFXFrame(t, 0, "none", 5, []byte(" world"), nil))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body.String())
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", FileID: 0, FullPath: "/root/a.txt", AcceptEncoding: defaultCLIEncodings, AckBytes: -1})
	if err != nil {
		t.Fatalf("FetchFile failed: %v", err)
	}
	got, err := readAndClose(t, resp.Reader)
	if err != nil {
		t.Fatalf("read stream failed: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("unexpected stream body: %q", got)
	}
	if resp.Meta.Size != int64(len(got)) {
		t.Fatalf("expected aggregated logical size %d, got %d", len(got), resp.Meta.Size)
	}
	if resp.Meta.HeaderTS <= 0 || resp.Meta.TrailerTS <= 0 {
		t.Fatalf("expected parsed frame timestamps, got header=%d trailer=%d", resp.Meta.HeaderTS, resp.Meta.TrailerTS)
	}
	if resp.Meta.HashToken == "" {
		t.Fatalf("expected parsed hash token")
	}
}

func TestFetchFileRejectsMissingNextFrame(t *testing.T) {
	next := int64(5)
	frame := buildFXFrame(t, 0, "none", 0, []byte("hello"), &next)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, frame)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", FileID: 0, FullPath: "/root/a.txt", AcceptEncoding: defaultCLIEncodings, AckBytes: -1})
	if err != nil {
		t.Fatalf("FetchFile setup failed: %v", err)
	}
	_, err = readAndClose(t, resp.Reader)
	if err == nil || !strings.Contains(err.Error(), "missing next frame") {
		t.Fatalf("expected missing next frame error, got %v", err)
	}
}

func TestFetchFileRejectsNonContiguousFrameOffsets(t *testing.T) {
	var body strings.Builder
	next := int64(5)
	body.WriteString(buildFXFrame(t, 0, "none", 0, []byte("hello"), &next))
	body.WriteString(buildFXFrame(t, 0, "none", 6, []byte("world"), nil))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body.String())
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", FileID: 0, FullPath: "/root/a.txt", AcceptEncoding: defaultCLIEncodings, AckBytes: -1})
	if err != nil {
		t.Fatalf("FetchFile setup failed: %v", err)
	}
	_, err = readAndClose(t, resp.Reader)
	if err == nil || !strings.Contains(err.Error(), "non-contiguous frame offset") {
		t.Fatalf("expected non-contiguous frame offset error, got %v", err)
	}
}

func TestDownloadFileFromManifestWritesToOutRoot(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "tx1",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "dir/a.txt"},
		},
	}
	logical := []byte("hello")
	var sawDataReq bool
	var sawAckReq bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/fs/file/tx1/0" {
			sawDataReq = true
			if got := r.URL.Query().Get("path"); got != "/remote/dir/a.txt" {
				t.Fatalf("unexpected path query: %q", got)
			}
			frame := buildFXFrame(t, 0, "none", 0, logical, nil)
			_, _ = w.Write([]byte(frame))
			return
		}
		if r.Method == http.MethodPut && r.URL.Path == "/fs/file/tx1/0/ack" {
			sawAckReq = true
			gotAck := r.URL.Query().Get("ack-bytes")
			expectedAck := "5@1001@xxh128:" + xxh128HexTest(logical)
			if gotAck != expectedAck {
				t.Fatalf("expected ack-bytes=%s, got %q", expectedAck, gotAck)
			}
			if got := r.URL.Query().Get("path"); got != "/remote/dir/a.txt" {
				t.Fatalf("unexpected path query: %q", got)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	downloadResp, err := client.DownloadFileFromManifest(context.Background(), DownloadFileRequest{
		Manifest:       manifest,
		FileID:         0,
		OutRoot:        outRoot,
		AcceptEncoding: defaultCLIEncodings,
	})
	if err != nil {
		t.Fatalf("DownloadFileFromManifest failed: %v", err)
	}
	outPath := downloadResp.DestinationPath
	expected := filepath.Join(outRoot, "dir", "a.txt")
	if outPath != expected {
		t.Fatalf("expected output path %q, got %q", expected, outPath)
	}
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !bytes.Equal(got, logical) {
		t.Fatalf("unexpected output bytes: %q", got)
	}
	if !sawDataReq || !sawAckReq {
		t.Fatalf("expected both data and ack-only requests")
	}
}

func TestDownloadFileFromManifestOnAckCallback(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txack",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	logical := []byte("hello")
	var events []AckProgressEvent

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/fs/file/txack/0" {
			frame := buildFXFrame(t, 0, "none", 0, logical, nil)
			_, _ = w.Write([]byte(frame))
			return
		}
		if r.Method == http.MethodPut && r.URL.Path == "/fs/file/txack/0/ack" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	_, err := client.DownloadFileFromManifest(context.Background(), DownloadFileRequest{
		Manifest:       manifest,
		FileID:         0,
		OutRoot:        outRoot,
		AcceptEncoding: defaultCLIEncodings,
		AckEveryBytes:  1,
		OnAck: func(evt AckProgressEvent) {
			events = append(events, evt)
		},
	})
	if err != nil {
		t.Fatalf("DownloadFileFromManifest failed: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("expected at least one ack progress event")
	}
	last := events[len(events)-1]
	if last.AckBytes != 5 || last.TargetBytes != 5 {
		t.Fatalf("unexpected final ack bytes/target: %+v", last)
	}
	if last.AckTime.IsZero() {
		t.Fatalf("expected non-zero ack time")
	}
	if len(events) > 1 && last.PrevAckTime.IsZero() {
		t.Fatalf("expected non-zero previous ack time when multiple events are present")
	}
}

func TestDownloadFileFromManifestWritesToStdout(t *testing.T) {
	manifest := &Manifest{
		TransferID: "tx2",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "x.txt"},
		},
	}
	logical := []byte("hello")
	var acked bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/ack") {
			acked = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		frame := buildFXFrame(t, 0, "none", 0, logical, nil)
		_, _ = w.Write([]byte(frame))
	}))
	defer srv.Close()

	var out bytes.Buffer
	client := NewClient(srv.URL, nil)
	downloadResp, err := client.DownloadFileFromManifest(context.Background(), DownloadFileRequest{
		Manifest:       manifest,
		FileID:         0,
		OutRoot:        ".",
		OutFile:        "-",
		Stdout:         &out,
		AcceptEncoding: defaultCLIEncodings,
	})
	if err != nil {
		t.Fatalf("DownloadFileFromManifest failed: %v", err)
	}
	outPath := downloadResp.DestinationPath
	if outPath != "-" {
		t.Fatalf("expected outPath '-', got %q", outPath)
	}
	if out.String() != "hello" {
		t.Fatalf("unexpected stdout output: %q", out.String())
	}
	if acked {
		t.Fatalf("did not expect ack-only request for stdout destination")
	}
}

func TestGetTransferStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fs/transfer/tx123/status" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"transfer_id":"tx123","directory":"/r","num_files":10,"total_size":1000,"done":4,"done_size":300,"percent_files":40,"percent_bytes":30,"download_status":{"started":4,"running":2,"done":4,"missing":0}}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	statusResp, err := client.GetTransferStatus(context.Background(), GetTransferStatusRequest{
		TransferID: "tx123",
	})
	if err != nil {
		t.Fatalf("GetTransferStatus failed: %v", err)
	}
	status := statusResp.Status
	if status.TransferID != "tx123" || status.DownloadStatus.Running != 2 {
		t.Fatalf("unexpected status payload: %+v", status)
	}
}

func TestFetchFileMissingDoesNotAckInline(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "file not found", http.StatusNotFound)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	_, err := client.FetchFile(context.Background(), FetchFileRequest{
		TransferID:     "tx",
		FileID:         0,
		FullPath:       "/root/missing.txt",
		AcceptEncoding: defaultCLIEncodings,
		AckBytes:       -1,
	})
	if err == nil || !errors.Is(err, ErrFileMissing) {
		t.Fatalf("expected ErrFileMissing, got %v", err)
	}
	if requests != 1 {
		t.Fatalf("expected one request, got %d", requests)
	}
}

func TestFetchFileTransferNotFoundDoesNotAckMissing(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "transfer not found", http.StatusNotFound)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, nil)
	_, err := client.FetchFile(context.Background(), FetchFileRequest{
		TransferID:     "tx",
		FileID:         0,
		FullPath:       "/root/missing.txt",
		AcceptEncoding: defaultCLIEncodings,
		AckBytes:       -1,
	})
	if err == nil || !errors.Is(err, ErrFileMissing) {
		t.Fatalf("expected ErrFileMissing, got %v", err)
	}
	if requests != 1 {
		t.Fatalf("expected only one request without missing-ack retry, got %d", requests)
	}
}
