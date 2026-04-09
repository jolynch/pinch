package filexfer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	intencoding "github.com/jolynch/pinch/internal/filexfer/encoding"
	intftcp "github.com/jolynch/pinch/internal/filexfer/ftcp"
	"github.com/jolynch/pinch/utils"
	"github.com/zeebo/xxh3"
)

type ftcpTestServer struct {
	URL      string
	closeFn  func()
	listener net.Listener
	wg       sync.WaitGroup
	handler  func(intftcp.Request, io.Writer) error
}

func (s *ftcpTestServer) Close() {
	if s == nil {
		return
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
	if s.closeFn != nil {
		s.closeFn()
	}
}

func newFTCPTestServer(t *testing.T, handler func(intftcp.Request, io.Writer) error) *ftcpTestServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	s := &ftcpTestServer{
		URL:      ln.Addr().String(),
		listener: ln,
		handler:  handler,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func(c net.Conn) {
				defer s.wg.Done()
				defer c.Close()
				serveFTCPTestConn(c, handler)
			}(conn)
		}
	}()
	return s
}

func serveFTCPTestConn(conn net.Conn, handler func(intftcp.Request, io.Writer) error) {
	br := bufio.NewReader(conn)
	first, err := readCompatLine(br)
	if err != nil {
		return
	}
	firstReq, err := intftcp.ParseRequest([]byte(first))
	if err != nil {
		_, _ = io.WriteString(conn, "ERR BAD_REQUEST "+err.Error()+"\r\n")
		return
	}
	responseOut := io.Writer(conn)
	closeResponse := func() error { return nil }
	cmdReq := firstReq
	if firstReq.Verb == intftcp.VerbAUTH {
		// The test server doesn't exercise encryption, so just skip the
		// AUTH blob and read the next command line.
		cmdLine, cmdErr := readCompatLine(br)
		if cmdErr != nil {
			_, _ = io.WriteString(responseOut, "ERR BAD_REQUEST missing command\r\n")
			_ = closeResponse()
			return
		}
		cmdReq, err = intftcp.ParseRequest([]byte(cmdLine))
		if err != nil {
			_, _ = io.WriteString(responseOut, "ERR BAD_REQUEST "+err.Error()+"\r\n")
			_ = closeResponse()
			return
		}
	}
	if cmdReq.Verb == intftcp.VerbPROBE && len(cmdReq.Params) > 0 {
		n, convErr := strconv.ParseInt(strings.TrimSpace(cmdReq.Params[0]["probe-bytes"]), 10, 64)
		if convErr != nil || n < 0 {
			_, _ = io.WriteString(responseOut, "ERR BAD_REQUEST invalid probe-bytes\r\n")
			_ = closeResponse()
			return
		}
		if n > 0 {
			if _, drainErr := io.CopyN(io.Discard, br, n); drainErr != nil {
				_, _ = io.WriteString(responseOut, "ERR BAD_REQUEST invalid probe payload\r\n")
				_ = closeResponse()
				return
			}
		}
	}
	if err := handler(cmdReq, responseOut); err != nil {
		_, _ = io.WriteString(responseOut, "ERR INTERNAL "+err.Error()+"\r\n")
	}
	_ = closeResponse()
}

func readCompatLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func TestNewClientOptions(t *testing.T) {
	dialer := func(context.Context, string) (net.Conn, error) { return nil, errors.New("unused") }

	client := NewClient(
		" 127.0.0.1:3453 ",
		WithFileRequestWindowBytes(123),
		WithFrameBufferBytes(456),
		WithMaxFrameReadBufferBytes(789),
		WithAckRequestTimeout(2*time.Second),
		WithSocketReadBufferBytes(321),
		WithContextDialer(dialer),
	)

	if client.FileAddr != "127.0.0.1:3453" {
		t.Fatalf("unexpected file addr: %q", client.FileAddr)
	}
	if client.FileRequestWindowBytes != 123 {
		t.Fatalf("unexpected file request window bytes: %d", client.FileRequestWindowBytes)
	}
	if client.FrameBufferBytes != 456 {
		t.Fatalf("unexpected frame buffer bytes: %d", client.FrameBufferBytes)
	}
	if client.MaxFrameReadBufferBytes != 789 {
		t.Fatalf("unexpected max frame read buffer bytes: %d", client.MaxFrameReadBufferBytes)
	}
	if client.AckRequestTimeout != 2*time.Second {
		t.Fatalf("unexpected ack request timeout: %s", client.AckRequestTimeout)
	}
	if client.SocketReadBufferBytes != 321 {
		t.Fatalf("unexpected socket read buffer bytes: %d", client.SocketReadBufferBytes)
	}
	if client.contextDialer == nil {
		t.Fatalf("expected context dialer to be configured")
	}
}

func TestNewClientLoadStrategyOption(t *testing.T) {
	client := NewClient("127.0.0.1:3453")
	if client.LoadStrategy != LoadStrategyFast {
		t.Fatalf("expected default load strategy %q, got %q", LoadStrategyFast, client.LoadStrategy)
	}
	client = NewClient("127.0.0.1:3453", WithLoadStrategy("gentle"))
	if client.LoadStrategy != LoadStrategyGentle {
		t.Fatalf("expected load strategy %q, got %q", LoadStrategyGentle, client.LoadStrategy)
	}
	client = NewClient("127.0.0.1:3453", WithLoadStrategy("unknown"))
	if client.LoadStrategy != LoadStrategyFast {
		t.Fatalf("expected fallback load strategy %q, got %q", LoadStrategyFast, client.LoadStrategy)
	}
}

func TestSummarizeProbeResultsAndConcurrency(t *testing.T) {
	results := []probeResponse{
		{ServerCPU: 24, GentleCPUPct: 25, CTS0: 1000, CTS1: 1040, STS0: 1005, STS1: 1010},
		{ServerCPU: 24, GentleCPUPct: 25, CTS0: 2000, CTS1: 2045, STS0: 2005, STS1: 2010},
		{ServerCPU: 24, GentleCPUPct: 25, CTS0: 3000, CTS1: 3030, STS0: 3005, STS1: 3010},
	}
	summary := summarizeProbeSamples(results, 1*1024*1024)
	if summary.ServerCPU != 24 {
		t.Fatalf("expected server cpu 24, got %d", summary.ServerCPU)
	}
	if summary.AvgLatencyMS <= 0 {
		t.Fatalf("expected avg ms > 0, got %d", summary.AvgLatencyMS)
	}
	if summary.LinkMbps <= 0 {
		t.Fatalf("expected rounded mbps > 0, got %d", summary.LinkMbps)
	}
	if got := suggestedConcurrencyFromProbe(24, 8, LoadStrategyGentle, 25); got != 6 {
		t.Fatalf("expected gentle concurrency 6, got %d", got)
	}
	if got := suggestedConcurrencyFromProbe(24, 8, LoadStrategyFast, 25); got != 192 {
		t.Fatalf("expected fast concurrency 192, got %d", got)
	}
}

func TestSuggestedConcurrencyFromProbeUsesServerGentlePercent(t *testing.T) {
	if got := suggestedConcurrencyFromProbe(24, 8, LoadStrategyGentle, 50); got != 12 {
		t.Fatalf("expected gentle concurrency 12, got %d", got)
	}
	if got := suggestedConcurrencyFromProbe(3, 8, LoadStrategyGentle, 34); got != 2 {
		t.Fatalf("expected gentle concurrency 2, got %d", got)
	}
}

func TestParseProbeResponseLineGentlePercents(t *testing.T) {
	resp, err := parseProbeResponseLine("PROBE cpu=24 io-depth=8 cts0=100 sts0=110 sts1=120 probe-bytes=1024 wmem=4096 gentle-cpu-pct=30 gentle-bw-pct=40")
	if err != nil {
		t.Fatalf("parseProbeResponseLine failed: %v", err)
	}
	if resp.GentleCPUPct != 30 {
		t.Fatalf("expected gentle cpu pct 30, got %d", resp.GentleCPUPct)
	}
	if resp.GentleBWPct != 40 {
		t.Fatalf("expected gentle bw pct 40, got %d", resp.GentleBWPct)
	}
}

func TestParseProbeResponseLineDefaultsGentlePercents(t *testing.T) {
	resp, err := parseProbeResponseLine("PROBE cpu=24 io-depth=8 cts0=100 sts0=110 sts1=120 probe-bytes=1024 wmem=4096")
	if err != nil {
		t.Fatalf("parseProbeResponseLine failed: %v", err)
	}
	if resp.GentleCPUPct != defaultGentleCPUPct {
		t.Fatalf("expected default gentle cpu pct %d, got %d", defaultGentleCPUPct, resp.GentleCPUPct)
	}
	if resp.GentleBWPct != defaultGentleBWPct {
		t.Fatalf("expected default gentle bw pct %d, got %d", defaultGentleBWPct, resp.GentleBWPct)
	}
}

func encodeSingleFramePayload(data []byte, comp string) ([]byte, error) {
	switch comp {
	case "none":
		return data, nil
	case EncodingZstd, EncodingLz4:
		var buf bytes.Buffer
		out, closeEncoded, _, err := intencoding.WrapCompressedWriter(&buf, comp, "")
		if err != nil {
			return nil, err
		}
		if _, err := out.Write(data); err != nil {
			return nil, err
		}
		if err := closeEncoded(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, errors.New("unsupported compression mode")
	}
}

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
	return intencoding.FormatXXH64HashToken(h.Sum64())
}

func buildFXFrame(t *testing.T, fileID uint64, comp string, offset int64, logical []byte, next *int64) string {
	return buildFXFrameWithTrailerTokens(t, fileID, comp, offset, logical, next)
}

func buildFXFrameWithTrailerTokens(t *testing.T, fileID uint64, comp string, offset int64, logical []byte, next *int64, trailerTokens ...string) string {
	t.Helper()
	payload, err := encodeSingleFramePayload(logical, comp)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	headerTS := int64(1000 + offset)
	trailerTS := headerTS + 1
	xsum := xxh128HexTest(logical)
	header := fmt.Sprintf(
		"FX/1 %d offset=%d size=%d wsize=%d comp=%s hash=xxh128:%s ts=%d\n",
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
	terminal := next == nil || (next != nil && *next == 0)
	hasFileHash := false
	for _, token := range trailerTokens {
		if strings.TrimSpace(token) == "" {
			continue
		}
		if strings.HasPrefix(token, "file-hash=") {
			hasFileHash = true
		}
		trailerPrefix += " " + token
	}
	if terminal && !hasFileHash {
		trailerPrefix += " file-hash=xxh128:" + xsum
	}
	trailer := trailerPrefix + " hash=" + frameHash64Token(header, payload, trailerPrefix)
	return header + string(payload) + trailer + "\n"
}

type noOpWriteCloser struct {
	io.Writer
}

func (n noOpWriteCloser) Close() error {
	return nil
}

type errCloseWriter struct {
	io.Writer
	err error
}

func (w errCloseWriter) Close() error {
	return w.err
}

type singleDownloadRequest struct {
	Manifest        *Manifest
	FileID          uint64
	OutRoot         string
	OutFile         string
	Stdout          io.Writer
	AckEveryBytes   int64
	ResumeFromBytes int64
	NoSync          bool
	ProgressUpdates chan<- DownloadProgressUpdate
}

func downloadSingle(ctx context.Context, client *Client, req singleDownloadRequest) (DownloadFileResponse, error) {
	_ = req.AckEveryBytes
	if req.Manifest != nil && req.ResumeFromBytes > 0 {
		for i := range req.Manifest.Entries {
			if req.Manifest.Entries[i].ID == req.FileID {
				req.Manifest.Entries[i].Progress.AckBytes = req.ResumeFromBytes
				break
			}
		}
	}
	if req.ResumeFromBytes != 0 {
		// Keep helper signature stable; resume is sourced from entry progress.
	}
	resp, err := client.GetFiles(ctx, GetFilesRequest{
		Manifest: req.Manifest,
		FileIDs:  []uint64{req.FileID},
		OutputWriter: func(entry ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
			destPath := strings.TrimSpace(req.OutFile)
			if destPath == "" {
				outRoot := req.OutRoot
				if outRoot == "" {
					outRoot = "."
				}
				destPath = filepath.Clean(filepath.Join(outRoot, filepath.FromSlash(entry.Path)))
			}
			if destPath == "-" {
				if offset > 0 {
					return nil, nil, errors.New("cannot resume when output is stdout")
				}
				out := req.Stdout
				if out == nil {
					out = os.Stdout
				}
				return noOpWriteCloser{Writer: out}, func() error { return nil }, nil
			}
			if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
				return nil, nil, fmt.Errorf("create output parent directory: %w", err)
			}
			var (
				fd  *os.File
				err error
			)
			if offset > 0 {
				fd, err = os.OpenFile(destPath, os.O_RDWR, 0)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						return nil, nil, fmt.Errorf("resume requested at offset %d but output file is missing", offset)
					}
					return nil, nil, fmt.Errorf("open output file for resume: %w", err)
				}
				info, statErr := fd.Stat()
				if statErr != nil {
					_ = fd.Close()
					return nil, nil, fmt.Errorf("stat output file for resume: %w", statErr)
				}
				if info.Size() < offset {
					_ = fd.Close()
					return nil, nil, fmt.Errorf("resume requested at offset %d but output file has only %d bytes", offset, info.Size())
				}
				if _, err := fd.Seek(offset, io.SeekStart); err != nil {
					_ = fd.Close()
					return nil, nil, fmt.Errorf("seek output file for resume: %w", err)
				}
			} else {
				fd, err = os.Create(destPath)
				if err != nil {
					return nil, nil, fmt.Errorf("create output file: %w", err)
				}
			}
			syncOutput := func() error {
				if req.NoSync {
					return nil
				}
				return syscall.Fdatasync(int(fd.Fd()))
			}
			return fd, syncOutput, nil
		},
		ProgressUpdates: req.ProgressUpdates,
	})
	if err != nil {
		return DownloadFileResponse{}, err
	}
	if len(resp.Files) != 1 {
		return DownloadFileResponse{}, fmt.Errorf("expected one downloaded file, got %d", len(resp.Files))
	}
	return resp.Files[0], nil
}

func TestParseFXHeaderMaxWSizeHint(t *testing.T) {
	meta, err := parseFXHeader("FX/1 7 offset=0 size=5 wsize=5 comp=none max-wsize=16777216 ts=1000")
	if err != nil {
		t.Fatalf("parseFXHeader failed: %v", err)
	}
	if meta.MaxWireSizeHint != 16*1024*1024 {
		t.Fatalf("unexpected max-wire hint: %d", meta.MaxWireSizeHint)
	}
}

func TestParseFXHeaderInvalidMaxWSizeHint(t *testing.T) {
	if _, err := parseFXHeader("FX/1 7 offset=0 size=5 wsize=5 comp=none max-wsize=-1 ts=1000"); err == nil {
		t.Fatalf("expected parse error for negative max-wsize")
	}
	if _, err := parseFXHeader("FX/1 7 offset=0 size=5 wsize=5 comp=none max-wsize=nope ts=1000"); err == nil {
		t.Fatalf("expected parse error for malformed max-wsize")
	}
}

func TestParseFXTrailerParsesMetadata(t *testing.T) {
	trailer, err := parseFXTrailer("FXT/1 7 status=ok ts=1001 next=0 meta:mode=0640 meta:uid=123 meta:gid=456 meta:user=alice meta:group=dev meta:unknown=x hash=xxh64:0123456789abcdef")
	if err != nil {
		t.Fatalf("parseFXTrailer failed: %v", err)
	}
	if trailer.Metadata == nil {
		t.Fatalf("expected metadata")
	}
	if trailer.Metadata.Mode != "0640" || trailer.Metadata.UID != "123" || trailer.Metadata.GID != "456" {
		t.Fatalf("unexpected metadata: %+v", trailer.Metadata)
	}
	if trailer.Metadata.User != "alice" || trailer.Metadata.Group != "dev" {
		t.Fatalf("unexpected user/group metadata: %+v", trailer.Metadata)
	}
}

func TestParseFXTrailerWithoutTrailerHash(t *testing.T) {
	trailer, err := parseFXTrailer("FXT/1 7 status=ok ts=1001 next=0 file-hash=xxh128:0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("parseFXTrailer failed: %v", err)
	}
	if trailer.FileID != 7 {
		t.Fatalf("unexpected file id: %d", trailer.FileID)
	}
	if trailer.HashToken != "" {
		t.Fatalf("expected empty trailer hash token, got %q", trailer.HashToken)
	}
	if trailer.FileHashToken == "" {
		t.Fatalf("expected file-hash token")
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

func TestFileStreamBufferHint(t *testing.T) {
	client := NewClient(
		"127.0.0.1:3453",
		WithFrameBufferBytes(4*1024*1024),
		WithMaxFrameReadBufferBytes(32*1024*1024),
	)

	if got := client.fileStreamBufferHint(64*1024*1024, 2*1024*1024); got != int64(effectiveFrameReadBufferSize(4*1024*1024, 64*1024*1024, 32*1024*1024)) {
		t.Fatalf("expected file-size hint to win, got=%d want=%d", got, effectiveFrameReadBufferSize(4*1024*1024, 64*1024*1024, 32*1024*1024))
	}

	if got := client.fileStreamBufferHint(0, 2*1024*1024); got != int64(effectiveFrameReadBufferSize(4*1024*1024, 2*1024*1024, 32*1024*1024)) {
		t.Fatalf("expected frame-size fallback, got=%d want=%d", got, effectiveFrameReadBufferSize(4*1024*1024, 2*1024*1024, 32*1024*1024))
	}

	if got := client.fileStreamBufferHint(0, 0); got != int64(effectiveFrameReadBufferSize(4*1024*1024, 0, 32*1024*1024)) {
		t.Fatalf("expected default behavior when no hint, got=%d want=%d", got, effectiveFrameReadBufferSize(4*1024*1024, 0, 32*1024*1024))
	}

	client = NewClient(
		"127.0.0.1:3453",
		WithFrameBufferBytes(0),
		WithMaxFrameReadBufferBytes(1),
	)
	if got := client.fileStreamBufferHint(0, 4*1024*1024); got != int64(effectiveFrameReadBufferSize(0, 4*1024*1024, 1)) {
		t.Fatalf("expected clamp via effective policy, got=%d want=%d", got, effectiveFrameReadBufferSize(0, 4*1024*1024, 1))
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
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbSEND {
			sawDataReq = true
			if got := req.Params[0]["txferid"]; got != "tx1" {
				return fmt.Errorf("unexpected transfer id: %q", got)
			}
			if got := req.Params[1]["path"]; got != "/remote/dir/a.txt" {
				return fmt.Errorf("unexpected path: %q", got)
			}
			frame := buildFXFrame(t, 0, "none", 0, logical, nil)
			_, err := io.WriteString(out, frame)
			return err
		}
		if req.Verb == intftcp.VerbACK {
			sawAckReq = true
			gotAck := req.Params[0]["ack-token"]
			expectedAck := "5@1001@xxh128:" + xxh128HexTest(logical)
			if gotAck != expectedAck {
				return fmt.Errorf("expected ack-token=%s, got %q", expectedAck, gotAck)
			}
			if got := req.Params[0]["path"]; got != "/remote/dir/a.txt" {
				return fmt.Errorf("unexpected ack path: %q", got)
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		}
		return fmt.Errorf("unexpected verb: %v", req.Verb)
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest: manifest,
		FileID:   0,
		OutRoot:  outRoot,
	})
	if err != nil {
		t.Fatalf("DownloadFileFromManifest failed: %v", err)
	}
	expected := filepath.Join(outRoot, "dir", "a.txt")
	got, err := os.ReadFile(expected)
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

func TestDownloadFileFromManifestReturnsTrailerMetadata(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txmode",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "dir/a.txt"},
		},
	}
	logical := []byte("hello")
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbSEND {
			frame := buildFXFrameWithTrailerTokens(t, 0, "none", 0, logical, nil, "meta:mode=0600")
			_, err := io.WriteString(out, frame)
			return err
		}
		if req.Verb == intftcp.VerbACK {
			_, err := io.WriteString(out, "OK\r\n")
			return err
		}
		return fmt.Errorf("unexpected verb: %v", req.Verb)
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest: manifest,
		FileID:   0,
		OutRoot:  outRoot,
	})
	if err != nil {
		t.Fatalf("DownloadFileFromManifest failed: %v", err)
	}
	if resp.Meta.TrailerMetadata == nil {
		t.Fatalf("expected trailer metadata")
	}
	if resp.Meta.TrailerMetadata.Mode != "0600" {
		t.Fatalf("expected trailer mode 0600, got %q", resp.Meta.TrailerMetadata.Mode)
	}
}

func TestDownloadFileFromManifestMetadataValidationIsCallerOwned(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txstrict",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	logical := []byte("hello")
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbSEND {
			frame := buildFXFrameWithTrailerTokens(t, 0, "none", 0, logical, nil, "meta:uid=not-a-number", "meta:gid=123")
			_, err := io.WriteString(out, frame)
			return err
		}
		if req.Verb == intftcp.VerbACK {
			_, err := io.WriteString(out, "OK\r\n")
			return err
		}
		return fmt.Errorf("unexpected verb: %v", req.Verb)
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest: manifest,
		FileID:   0,
		OutRoot:  outRoot,
	})
	if err != nil {
		t.Fatalf("expected metadata validation to be caller-owned, got %v", err)
	}
	if resp.Meta.TrailerMetadata == nil || resp.Meta.TrailerMetadata.UID != "not-a-number" {
		t.Fatalf("expected unvalidated trailer metadata in response, got %+v", resp.Meta.TrailerMetadata)
	}
}

func TestDownloadFileFromManifestMetadataBestEffort(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txbest",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	logical := []byte("hello")
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbSEND {
			frame := buildFXFrameWithTrailerTokens(t, 0, "none", 0, logical, nil, "meta:uid=not-a-number", "meta:gid=123")
			_, err := io.WriteString(out, frame)
			return err
		}
		if req.Verb == intftcp.VerbACK {
			_, err := io.WriteString(out, "OK\r\n")
			return err
		}
		return fmt.Errorf("unexpected verb: %v", req.Verb)
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest: manifest,
		FileID:   0,
		OutRoot:  outRoot,
	})
	if err != nil {
		t.Fatalf("expected success with caller-owned metadata apply, got %v", err)
	}
}

func TestDownloadFileFromManifestVerifiesBeforeMetadataApply(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txverify",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	logical := []byte("hello")
	const badFileHash = "xxh128:00000000000000000000000000000000"
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbSEND {
			frame := buildFXFrameWithTrailerTokens(t, 0, "none", 0, logical, nil, "file-hash="+badFileHash, "meta:uid=not-a-number", "meta:gid=123")
			_, err := io.WriteString(out, frame)
			return err
		}
		if req.Verb == intftcp.VerbACK {
			_, err := io.WriteString(out, "OK\r\n")
			return err
		}
		return fmt.Errorf("unexpected verb: %v", req.Verb)
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest: manifest,
		FileID:   0,
		OutRoot:  outRoot,
	})
	if err == nil || !strings.Contains(err.Error(), "window hash mismatch") {
		t.Fatalf("expected hash verification failure before metadata apply, got %v", err)
	}
}

func TestDownloadFileFromManifestProgressUpdatesIncludeACK(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txack",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	logical := []byte("hello")
	progressUpdates := make(chan DownloadProgressUpdate, 32)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbSEND {
			frame := buildFXFrame(t, 0, "none", 0, logical, nil)
			_, err := io.WriteString(out, frame)
			return err
		}
		if req.Verb == intftcp.VerbACK {
			_, err := io.WriteString(out, "OK\r\n")
			return err
		}
		return fmt.Errorf("unexpected verb: %v", req.Verb)
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest:        manifest,
		FileID:          0,
		OutRoot:         outRoot,
		AckEveryBytes:   1,
		ProgressUpdates: progressUpdates,
	})
	if err != nil {
		t.Fatalf("DownloadFileFromManifest failed: %v", err)
	}
	var lastProgress DownloadProgressUpdate
	var sawProgress bool
	var lastAck DownloadProgressUpdate
	var sawAck bool
	for {
		select {
		case update := <-progressUpdates:
			if update.CopiedBytes > 0 {
				lastProgress = update
				sawProgress = true
			}
			if update.AckBytes > 0 {
				lastAck = update
				sawAck = true
			}
		default:
			goto doneProgress
		}
	}
doneProgress:
	if !sawProgress {
		t.Fatalf("expected at least one file progress event")
	}
	if lastProgress.CopiedBytes != 5 || lastProgress.TargetBytes != 5 {
		t.Fatalf("unexpected final progress event: %+v", lastProgress)
	}
	if !sawAck {
		t.Fatalf("expected at least one ack progress event")
	}
	if lastAck.AckBytes != 5 || lastAck.TargetBytes != 5 {
		t.Fatalf("unexpected final ack progress event: %+v", lastAck)
	}
}

func TestDownloadFileFromManifestUsesSingleBatchACK(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txackpool",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 10, Path: "a.txt"},
		},
	}
	logical := []byte("helloworld")

	var acks []map[string]string
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			if got := req.Params[1]["offset"]; got != "" && got != "0" {
				return fmt.Errorf("expected offset 0, got %q", got)
			}
			if got := req.Params[1]["size"]; got != "10" {
				return fmt.Errorf("expected size 10, got %q", got)
			}
			_, err := io.WriteString(out, buildFXFrame(t, 0, "none", 0, logical, nil))
			return err
		case intftcp.VerbACK:
			item := map[string]string{}
			for k, v := range req.Params[0] {
				item[k] = v
			}
			acks = append(acks, item)
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL, WithFileRequestWindowBytes(5))
	_, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest:      manifest,
		FileID:        0,
		OutRoot:       outRoot,
		AckEveryBytes: 10,
		NoSync:        true,
	})
	if err != nil {
		t.Fatalf("DownloadFileFromManifest failed: %v", err)
	}
	if len(acks) != 1 {
		t.Fatalf("expected one ACK, got %d", len(acks))
	}
	ack := acks[0]
	expectedAck := "10@1001@xxh128:" + xxh128HexTest(logical)
	if got := ack["ack-token"]; got != expectedAck {
		t.Fatalf("expected ack-token=%s, got %q", expectedAck, got)
	}
	if got := ack["delta-bytes"]; got != "10" {
		t.Fatalf("expected delta-bytes=10, got %q", got)
	}
}

func TestDownloadFileFromManifestResumesFromOffsetWithoutTruncatingTail(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txresume",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 10, Path: "a.txt"},
		},
	}
	destPath := filepath.Join(outRoot, "a.txt")
	if err := os.WriteFile(destPath, []byte("helloSTALETAIL"), 0o644); err != nil {
		t.Fatalf("write stale output file: %v", err)
	}
	partB := []byte("world")

	var sawSend bool
	var sawAck bool
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			sawSend = true
			if got := req.Params[1]["offset"]; got != "5" {
				return fmt.Errorf("expected resume offset 5, got %q", got)
			}
			if got := req.Params[1]["size"]; got != "5" {
				return fmt.Errorf("expected resume size 5, got %q", got)
			}
			_, err := io.WriteString(out, buildFXFrame(t, 0, "none", 5, partB, nil))
			return err
		case intftcp.VerbACK:
			sawAck = true
			expectedAck := "10@1006@xxh128:" + xxh128HexTest(partB)
			if got := req.Params[0]["ack-token"]; got != expectedAck {
				return fmt.Errorf("expected ack-token=%s, got %q", expectedAck, got)
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest:        manifest,
		FileID:          0,
		OutRoot:         outRoot,
		ResumeFromBytes: 5,
		AckEveryBytes:   1,
		NoSync:          true,
		ProgressUpdates: make(chan DownloadProgressUpdate, 1),
	})
	if err != nil {
		t.Fatalf("DownloadFileFromManifest failed: %v", err)
	}
	if !sawSend {
		t.Fatalf("expected SEND request")
	}
	if !sawAck {
		t.Fatalf("expected ACK request")
	}

	got, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read resumed output: %v", err)
	}
	if string(got) != "helloworldTAIL" {
		t.Fatalf("unexpected resumed output: %q", got)
	}
}

func TestGetFilesSplitsLargeSingleFileWindows(t *testing.T) {
	outRoot := t.TempDir()
	destPath := filepath.Join(outRoot, "big.bin")
	if err := os.WriteFile(destPath, nil, 0o644); err != nil {
		t.Fatalf("create destination: %v", err)
	}

	manifest := &Manifest{
		TransferID: "txsplit",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 10, Path: "big.bin"},
		},
	}
	payload := []byte("abcdefghij")

	var (
		mu            sync.Mutex
		sendOffsets   []int64
		sendSizes     []int64
		writerOffsets []int64
		ackBytes      []int64
	)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			sendParam := req.Params[len(req.Params)-1]
			offset := int64(0)
			if rawOffset := sendParam["offset"]; rawOffset != "" {
				parsedOffset, err := strconv.ParseInt(rawOffset, 10, 64)
				if err != nil {
					return fmt.Errorf("parse offset: %w", err)
				}
				offset = parsedOffset
			}
			size, err := strconv.ParseInt(sendParam["size"], 10, 64)
			if err != nil {
				return fmt.Errorf("parse size: %w", err)
			}
			mu.Lock()
			sendOffsets = append(sendOffsets, offset)
			sendSizes = append(sendSizes, size)
			mu.Unlock()
			if offset == 0 {
				time.Sleep(200 * time.Millisecond)
			}
			_, err = io.WriteString(out, buildFXFrame(t, 0, "none", offset, payload[offset:offset+size], nil))
			return err
		case intftcp.VerbACK:
			mu.Lock()
			for _, item := range req.Params {
				rawAck := item["ack-token"]
				ackPrefix, _, ok := strings.Cut(rawAck, "@")
				if !ok {
					mu.Unlock()
					return fmt.Errorf("malformed ack-token: %q", rawAck)
				}
				ackByte, err := strconv.ParseInt(ackPrefix, 10, 64)
				if err != nil {
					mu.Unlock()
					return fmt.Errorf("parse ack-token prefix: %w", err)
				}
				ackBytes = append(ackBytes, ackByte)
			}
			mu.Unlock()
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL, WithFileRequestWindowBytes(12))
	progressUpdates := make(chan DownloadProgressUpdate, 32)

	done := make(chan struct{})
	var (
		resp GetFilesResponse
		err  error
	)
	go func() {
		defer close(done)
		resp, err = client.GetFiles(context.Background(), GetFilesRequest{
			Manifest:      manifest,
			FileIDs:       []uint64{0},
			BatchMaxBytes: 4,
			OutputWriter: func(entry ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
				mu.Lock()
				writerOffsets = append(writerOffsets, offset)
				mu.Unlock()
				fd, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE, 0o644)
				if err != nil {
					return nil, nil, err
				}
				if _, err := fd.Seek(offset, io.SeekStart); err != nil {
					_ = fd.Close()
					return nil, nil, err
				}
				return fd, func() error { return nil }, nil
			},
			ProgressUpdates: progressUpdates,
		})
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for split download completion")
	}

	if err != nil {
		t.Fatalf("GetFiles failed: %v", err)
	}
	if len(resp.Files) != 1 {
		t.Fatalf("expected one aggregated file result, got %d", len(resp.Files))
	}
	if resp.Files[0].Meta.Size != int64(len(payload)) {
		t.Fatalf("unexpected aggregated size: %d", resp.Files[0].Meta.Size)
	}
	if resp.Files[0].Meta.FileHashToken != "" || resp.Files[0].LocalFileHash != "" {
		t.Fatalf("expected split aggregate hashes to be cleared, got meta=%q local=%q", resp.Files[0].Meta.FileHashToken, resp.Files[0].LocalFileHash)
	}

	got, readErr := os.ReadFile(destPath)
	if readErr != nil {
		t.Fatalf("read output: %v", readErr)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("unexpected split output: %q", got)
	}

	mu.Lock()
	sendByOffset := make(map[int64]int64, len(sendOffsets))
	for i := range sendOffsets {
		sendByOffset[sendOffsets[i]] = sendSizes[i]
	}
	gotSendOffsets := append([]int64(nil), sendOffsets...)
	gotWriterOffsets := append([]int64(nil), writerOffsets...)
	gotAckBytes := append([]int64(nil), ackBytes...)
	mu.Unlock()
	sort.Slice(gotSendOffsets, func(i, j int) bool { return gotSendOffsets[i] < gotSendOffsets[j] })
	sort.Slice(gotWriterOffsets, func(i, j int) bool { return gotWriterOffsets[i] < gotWriterOffsets[j] })

	expectedOffsets := []int64{0, 4, 8}
	if fmt.Sprint(gotSendOffsets) != fmt.Sprint(expectedOffsets) {
		t.Fatalf("unexpected split offsets: got=%v want=%v", gotSendOffsets, expectedOffsets)
	}
	for _, offset := range expectedOffsets {
		expectedSize := int64(4)
		if offset == 8 {
			expectedSize = 2
		}
		if got := sendByOffset[offset]; got != expectedSize {
			t.Fatalf("unexpected split size at offset %d: got=%d want=%d", offset, got, expectedSize)
		}
	}
	if fmt.Sprint(gotWriterOffsets) != fmt.Sprint(expectedOffsets) {
		t.Fatalf("unexpected writer offsets: got=%v want=%v", gotWriterOffsets, expectedOffsets)
	}
	if fmt.Sprint(gotAckBytes) != fmt.Sprint([]int64{4, 8, 10}) {
		t.Fatalf("unexpected split ack bytes: %v", gotAckBytes)
	}

	var lastAck int64
	for {
		select {
		case update := <-progressUpdates:
			if update.AckBytes > lastAck {
				lastAck = update.AckBytes
			}
		default:
			if lastAck != 10 {
				t.Fatalf("unexpected final ack progress: %d", lastAck)
			}
			return
		}
	}
}

func TestGetFilesSplitWindowWorkersCapsConcurrency(t *testing.T) {
	outRoot := t.TempDir()
	destPath := filepath.Join(outRoot, "big.bin")
	if err := os.WriteFile(destPath, nil, 0o644); err != nil {
		t.Fatalf("create destination: %v", err)
	}

	manifest := &Manifest{
		TransferID: "txsplitcap",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 12, Path: "big.bin"},
		},
	}
	payload := []byte("abcdefghijkl")

	var (
		mu          sync.Mutex
		inFlight    int
		maxInFlight int
	)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			mu.Lock()
			inFlight++
			if inFlight > maxInFlight {
				maxInFlight = inFlight
			}
			mu.Unlock()
			defer func() {
				mu.Lock()
				inFlight--
				mu.Unlock()
			}()

			sendParam := req.Params[len(req.Params)-1]
			offset := int64(0)
			if rawOffset := sendParam["offset"]; rawOffset != "" {
				parsedOffset, err := strconv.ParseInt(rawOffset, 10, 64)
				if err != nil {
					return fmt.Errorf("parse offset: %w", err)
				}
				offset = parsedOffset
			}
			size, err := strconv.ParseInt(sendParam["size"], 10, 64)
			if err != nil {
				return fmt.Errorf("parse size: %w", err)
			}
			time.Sleep(50 * time.Millisecond)
			_, err = io.WriteString(out, buildFXFrame(t, 0, "none", offset, payload[offset:offset+size], nil))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL, WithFileRequestWindowBytes(12))
	_, err := client.GetFiles(context.Background(), GetFilesRequest{
		Manifest:           manifest,
		FileIDs:            []uint64{0},
		BatchMaxBytes:      4,
		SplitWindowWorkers: 1,
		ProgressUpdates:    make(chan DownloadProgressUpdate, 8),
		OutputWriter: func(entry ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
			fd, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE, 0o644)
			if err != nil {
				return nil, nil, err
			}
			if _, err := fd.Seek(offset, io.SeekStart); err != nil {
				_ = fd.Close()
				return nil, nil, err
			}
			return fd, func() error { return nil }, nil
		},
	})
	if err != nil {
		t.Fatalf("GetFiles failed: %v", err)
	}
	if maxInFlight != 1 {
		t.Fatalf("expected split-window concurrency cap of 1, got %d", maxInFlight)
	}
}

func TestGetFilesUsesMultiACK(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txbatchack",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
			{ID: 1, Size: 6, Path: "b.txt"},
		},
	}
	dataA := []byte("hello")
	dataB := []byte("world!")

	var ackRequests int
	var ackBlocks []map[string]string
	progressUpdates := make(chan DownloadProgressUpdate, 64)
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			if len(req.Params) < 2 {
				return fmt.Errorf("expected txfer header + at least 1 SEND item, got %d params", len(req.Params))
			}
			if got := req.Params[0]["txferid"]; got != manifest.TransferID {
				return fmt.Errorf("unexpected transfer id: %q", got)
			}
			for _, item := range req.Params[1:] {
				switch item["path"] {
				case "/remote/a.txt":
					if _, err := io.WriteString(out, buildFXFrame(t, 0, "none", 0, dataA, nil)); err != nil {
						return err
					}
				case "/remote/b.txt":
					if _, err := io.WriteString(out, buildFXFrame(t, 1, "none", 0, dataB, nil)); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected path in SEND: %q", item["path"])
				}
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			ackRequests++
			for _, p := range req.Params {
				item := map[string]string{}
				for k, v := range p {
					item[k] = v
				}
				ackBlocks = append(ackBlocks, item)
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.GetFiles(context.Background(), GetFilesRequest{
		Manifest: manifest,
		FileIDs:  []uint64{0, 1},
		OutputWriter: func(entry ManifestEntry, _ int64) (io.WriteCloser, func() error, error) {
			destPath := filepath.Join(outRoot, filepath.FromSlash(entry.Path))
			if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
				return nil, nil, err
			}
			fd, err := os.Create(destPath)
			if err != nil {
				return nil, nil, err
			}
			return fd, func() error { return nil }, nil
		},
		ProgressUpdates: progressUpdates,
	})
	if err != nil {
		t.Fatalf("GetFiles failed: %v", err)
	}
	if len(resp.Files) != 2 {
		t.Fatalf("expected two downloaded files, got %d", len(resp.Files))
	}
	if ackRequests != 1 {
		t.Fatalf("expected one ACK request, got %d", ackRequests)
	}
	if len(ackBlocks) != 2 {
		t.Fatalf("expected two ACK blocks, got %d", len(ackBlocks))
	}

	acksByFID := map[string]map[string]string{}
	for _, block := range ackBlocks {
		acksByFID[block["fid"]] = block
	}
	ack0, ok := acksByFID["0"]
	if !ok {
		t.Fatalf("missing ACK block for fid=0")
	}
	ack1, ok := acksByFID["1"]
	if !ok {
		t.Fatalf("missing ACK block for fid=1")
	}
	expectedAck0 := "5@1001@xxh128:" + xxh128HexTest(dataA)
	expectedAck1 := "6@1001@xxh128:" + xxh128HexTest(dataB)
	if got := ack0["ack-token"]; got != expectedAck0 {
		t.Fatalf("unexpected fid=0 ack-token: %q", got)
	}
	if got := ack1["ack-token"]; got != expectedAck1 {
		t.Fatalf("unexpected fid=1 ack-token: %q", got)
	}
	if got := ack0["delta-bytes"]; got != "5" {
		t.Fatalf("unexpected fid=0 delta-bytes: %q", got)
	}
	if got := ack1["delta-bytes"]; got != "6" {
		t.Fatalf("unexpected fid=1 delta-bytes: %q", got)
	}

	gotA, err := os.ReadFile(filepath.Join(outRoot, "a.txt"))
	if err != nil {
		t.Fatalf("read output a.txt: %v", err)
	}
	gotB, err := os.ReadFile(filepath.Join(outRoot, "b.txt"))
	if err != nil {
		t.Fatalf("read output b.txt: %v", err)
	}
	if !bytes.Equal(gotA, dataA) {
		t.Fatalf("unexpected output for a.txt: %q", gotA)
	}
	if !bytes.Equal(gotB, dataB) {
		t.Fatalf("unexpected output for b.txt: %q", gotB)
	}
	finalProgress := make(map[uint64]int64)
	for {
		select {
		case update := <-progressUpdates:
			if update.CopiedBytes > 0 {
				finalProgress[update.FileID] = update.CopiedBytes
			}
		default:
			goto doneBatchProgress
		}
	}
doneBatchProgress:
	if got := finalProgress[0]; got != int64(len(dataA)) {
		t.Fatalf("unexpected final progress for fid=0: %d", got)
	}
	if got := finalProgress[1]; got != int64(len(dataB)) {
		t.Fatalf("unexpected final progress for fid=1: %d", got)
	}
}

func TestStartFromManifestNoLongerProbes(t *testing.T) {
	manifest := &Manifest{
		TransferID:  "txstartprobe",
		Root:        "/remote",
		Mode:        LoadStrategyGentle,
		LinkMbps:    1000,
		Concurrency: 6,
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	payload := []byte("hello")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			if got := req.Params[1]["mode"]; got != LoadStrategyGentle {
				return fmt.Errorf("expected SEND mode gentle, got %q", got)
			}
			if _, err := io.WriteString(out, buildFXFrame(t, 0, "none", 0, payload, nil)); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL, WithLoadStrategy(LoadStrategyGentle))
	resp, err := client.StartFromManifest(context.Background(), StartFromManifestRequest{
		Manifest: manifest,
		OutputWriter: func(ManifestEntry, int64) (io.WriteCloser, func() error, error) {
			return noOpWriteCloser{Writer: io.Discard}, func() error { return nil }, nil
		},
		BatchMaxBytes: 1024,
	})
	if err != nil {
		t.Fatalf("StartFromManifest failed: %v", err)
	}
	if resp.Downloaded != 1 {
		t.Fatalf("expected one downloaded file, got %d", resp.Downloaded)
	}
}

func TestStartFromManifestRespectsExplicitConcurrency(t *testing.T) {
	manifest := &Manifest{
		TransferID:  "txstartoverride",
		Root:        "/remote",
		Mode:        LoadStrategyFast,
		LinkMbps:    1000,
		Concurrency: 4,
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	payload := []byte("hello")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			if _, err := io.WriteString(out, buildFXFrame(t, 0, "none", 0, payload, nil)); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.StartFromManifest(context.Background(), StartFromManifestRequest{
		Manifest: manifest,
		OutputWriter: func(ManifestEntry, int64) (io.WriteCloser, func() error, error) {
			return noOpWriteCloser{Writer: io.Discard}, func() error { return nil }, nil
		},
		BatchMaxBytes: 1024,
		Concurrency:   99,
	})
	if err != nil {
		t.Fatalf("StartFromManifest failed: %v", err)
	}
	if resp.Downloaded != 1 {
		t.Fatalf("expected one downloaded file, got %d", resp.Downloaded)
	}
}

func TestDownloadFileFromManifestMissingFileACKImmediate(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txmissingack",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "missing.txt"},
		},
	}
	var acks []map[string]string
	var requests int
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		requests++
		switch req.Verb {
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, "ERR NOT_FOUND file not found\r\n")
			return err
		case intftcp.VerbACK:
			item := map[string]string{}
			for k, v := range req.Params[0] {
				item[k] = v
			}
			acks = append(acks, item)
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest:      manifest,
		FileID:        0,
		OutRoot:       outRoot,
		AckEveryBytes: 1024,
	})
	if err == nil || !errors.Is(err, ErrFileMissing) {
		t.Fatalf("expected ErrFileMissing, got %v", err)
	}
	if requests != 2 {
		t.Fatalf("expected SEND + ACK requests, got %d", requests)
	}
	if len(acks) != 1 {
		t.Fatalf("expected exactly one missing ACK, got %d", len(acks))
	}
	if got := acks[0]["ack-token"]; got != "-1" {
		t.Fatalf("expected missing ack-token -1, got %q", got)
	}
}

func TestDownloadFileFromManifestAckTimeoutDoesNotHang(t *testing.T) {
	outRoot := t.TempDir()
	manifest := &Manifest{
		TransferID: "txacktimeout",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	logical := []byte("hello")
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbSEND {
			frame := buildFXFrame(t, 0, "none", 0, logical, nil)
			_, err := io.WriteString(out, frame)
			return err
		}
		if req.Verb == intftcp.VerbACK {
			_, err := io.WriteString(out, "ERR TIMEOUT timed out\r\n")
			return err
		}
		return fmt.Errorf("unexpected verb: %v", req.Verb)
	})
	defer srv.Close()

	client := NewClient(srv.URL, WithAckRequestTimeout(50*time.Millisecond))
	start := time.Now()
	_, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest: manifest,
		FileID:   0,
		OutRoot:  outRoot,
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected ack timeout failure")
	}
	if !strings.Contains(err.Error(), "acknowledge download failed") {
		t.Fatalf("expected acknowledge failure, got %v", err)
	}
	if elapsed > 4*time.Second {
		t.Fatalf("expected bounded ack timeout behavior, elapsed=%s", elapsed)
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
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb == intftcp.VerbACK {
			acked = true
			_, err := io.WriteString(out, "OK\r\n")
			return err
		}
		frame := buildFXFrame(t, 0, "none", 0, logical, nil)
		_, err := io.WriteString(out, frame)
		return err
	})
	defer srv.Close()

	var out bytes.Buffer
	client := NewClient(srv.URL)
	downloadResp, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest: manifest,
		FileID:   0,
		OutRoot:  ".",
		OutFile:  "-",
		Stdout:   &out,
	})
	if err != nil {
		t.Fatalf("DownloadFileFromManifest failed: %v", err)
	}
	if downloadResp.Meta.FileID != 0 {
		t.Fatalf("expected file id 0, got %d", downloadResp.Meta.FileID)
	}
	if out.String() != "hello" {
		t.Fatalf("unexpected stdout output: %q", out.String())
	}
	if !acked {
		t.Fatalf("expected ack request for stdout destination")
	}
}

func TestGetFilesRequiresOutputWriter(t *testing.T) {
	manifest := &Manifest{
		TransferID: "txmissingwriter",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	client := NewClient("127.0.0.1:1")
	_, err := client.GetFiles(context.Background(), GetFilesRequest{
		Manifest: manifest,
		FileIDs:  []uint64{0},
	})
	if err == nil || !strings.Contains(err.Error(), "missing output writer callback") {
		t.Fatalf("expected missing output writer error, got %v", err)
	}
}

func TestGetFilesRejectsNilWriterFromCallback(t *testing.T) {
	manifest := &Manifest{
		TransferID: "txnilwriter",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildFXFrame(t, 0, "none", 0, []byte("hello"), nil))
			return err
		case intftcp.VerbACK:
			return fmt.Errorf("unexpected ACK for nil writer callback test")
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.GetFiles(context.Background(), GetFilesRequest{
		Manifest: manifest,
		FileIDs:  []uint64{0},
		OutputWriter: func(ManifestEntry, int64) (io.WriteCloser, func() error, error) {
			return nil, nil, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "nil writer") {
		t.Fatalf("expected nil writer error, got %v", err)
	}
}

func TestGetFilesPropagatesSyncError(t *testing.T) {
	manifest := &Manifest{
		TransferID: "txsyncerr",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	acked := false
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildFXFrame(t, 0, "none", 0, []byte("hello"), nil))
			return err
		case intftcp.VerbACK:
			acked = true
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.GetFiles(context.Background(), GetFilesRequest{
		Manifest: manifest,
		FileIDs:  []uint64{0},
		OutputWriter: func(ManifestEntry, int64) (io.WriteCloser, func() error, error) {
			return noOpWriteCloser{Writer: io.Discard}, func() error { return errors.New("sync blew up") }, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "sync output for file 0") {
		t.Fatalf("expected sync output error, got %v", err)
	}
	if acked {
		t.Fatalf("did not expect ACK when sync fails")
	}
}

func TestGetFilesPropagatesCloseError(t *testing.T) {
	manifest := &Manifest{
		TransferID: "txcloseerr",
		Root:       "/remote",
		Entries: []ManifestEntry{
			{ID: 0, Size: 5, Path: "a.txt"},
		},
	}
	acked := false
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildFXFrame(t, 0, "none", 0, []byte("hello"), nil))
			return err
		case intftcp.VerbACK:
			acked = true
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.GetFiles(context.Background(), GetFilesRequest{
		Manifest: manifest,
		FileIDs:  []uint64{0},
		OutputWriter: func(ManifestEntry, int64) (io.WriteCloser, func() error, error) {
			return errCloseWriter{Writer: io.Discard, err: errors.New("close failed")}, func() error { return nil }, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "close output for file 0") {
		t.Fatalf("expected close output error, got %v", err)
	}
	if acked {
		t.Fatalf("did not expect ACK when close fails")
	}
}

func TestGetStatus(t *testing.T) {
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbSTATUS {
			return fmt.Errorf("expected STATUS, got %v", req.Verb)
		}
		if got := req.Params[0]["txferid"]; got != "tx123" {
			return fmt.Errorf("unexpected transfer id: %q", got)
		}
		_, err := io.WriteString(out, `OK {"transfer_id":"tx123","directory":"/r","num_files":10,"total_size":1000,"done":4,"done_size":300,"percent_files":40,"percent_bytes":30,"download_status":{"started":4,"running":2,"done":4,"missing":0}}`+"\r\n")
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	statusResp, err := client.GetStatus(context.Background(), GetStatusRequest{
		TransferID: "tx123",
	})
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	status := statusResp.Status
	if status.TransferID != "tx123" || status.DownloadStatus.Running != 2 {
		t.Fatalf("unexpected status payload: %+v", status)
	}
}

func TestClientUsesInjectedDialContext(t *testing.T) {
	var called bool
	dialContext := func(ctx context.Context, addr string) (net.Conn, error) {
		called = true
		if addr != "ignored:0" {
			t.Fatalf("unexpected addr: %q", addr)
		}
		serverConn, clientConn := net.Pipe()
		go func() {
			defer serverConn.Close()
			br := bufio.NewReader(serverConn)
			line, err := readCompatLine(br)
			if err != nil {
				return
			}
			req, err := intftcp.ParseRequest([]byte(line))
			if err != nil {
				return
			}
			if req.Verb != intftcp.VerbSTATUS {
				return
			}
			_, _ = io.WriteString(serverConn, "OK {\"transfer_id\":\"tx123\"}\r\n")
		}()
		return clientConn, nil
	}
	client := NewClient("ignored:0", WithContextDialer(dialContext))

	resp, err := client.GetStatus(context.Background(), GetStatusRequest{TransferID: "tx123"})
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if !called {
		t.Fatalf("expected injected DialContext to be used")
	}
	if resp.Status == nil || resp.Status.TransferID != "tx123" {
		t.Fatalf("unexpected status response: %+v", resp.Status)
	}
}

func TestSuggestBatchMaxBytes(t *testing.T) {
	const mib = int64(1 << 20)
	clientRmem := int64(utils.MaxSocketReadBufferBytes())

	tests := []struct {
		name                 string
		conc, winConc        int
		windowBytes, srvWmem int64
		linkMbps             int64
		want                 int64
	}{
		// Fast mode: 24 cpu * 8 io = 192 conc, winConc=4, perFile=48
		// 1GiB/48 ≈ 21.3 MiB → ceil pow2 = 32 MiB
		// linkMbps=0 → no bw cap
		{"fast 24cpu*8io", 192, 4, 1 << 30, 4 * mib, 0, 32 * mib},

		// Fast mode: 2 cpu * 8 io = 16, winConc=4, perFile=4
		// 1GiB/4 = 256 MiB
		{"fast 2cpu*8io", 16, 4, 1 << 30, 4 * mib, 0, 256 * mib},

		// Gentle 24 cpu: conc=6, 6/4=1 < 2 → winConc=6, perFile=1
		// batch = 1 GiB (capped at window)
		{"gentle 24cpu conc=6", 6, 4, 1 << 30, 4 * mib, 0, 1 << 30},

		// Gentle 32 cpu: conc=8, 8/4=2 → perFile=2
		// 1GiB/2 = 512 MiB
		{"gentle 32cpu conc=8", 8, 4, 1 << 30, 4 * mib, 0, 512 * mib},

		// Zero concurrency defaults to 1
		{"zero conc defaults", 0, 0, 1 << 30, 4 * mib, 0, 1 << 30},

		// Negative values default safely
		{"negative conc", -5, -2, 1 << 30, 4 * mib, 0, 1 << 30},

		// Socket buffer floor: 192 conc → 48 perFile → 1MiB batch
		// but floor at max(4MiB, clientRmem)
		{"batch floors at socket buf", 192, 4, 32 * mib, 4 * mib, 0, max(4*mib, clientRmem)},

		// Batch caps at window
		{"batch caps at window", 2, 1, 8 * mib, 4 * mib, 0, 8 * mib},

		// Small window (< 1 MiB) returns window directly
		{"sub-MiB window", 100, 4, 512 * 1024, 4 * mib, 0, 512 * 1024},

		// High CPU fast mode: 256 cpu * 8 io = 2048 conc, winConc=4, perFile=512
		// 1GiB/512 = 2 MiB
		{"256 cpus fast", 2048, 4, 1 << 30, 4 * mib, 0, max(4*mib, clientRmem)},

		// Bandwidth ceiling (500ms target): 8400 Mbps, conc=6
		// perWorker=175 MB/s, 500ms=87.5 MB → pow2 down = 64 MiB
		// Without bw cap: conc=6, 6/4<2 → winConc=6, perFile=1 → batch=1GiB
		// With bw cap: min(1GiB, 64MiB) = 64 MiB
		{"bw cap 8400mbps conc=6", 6, 4, 1 << 30, 4 * mib, 8400, 64 * mib},

		// Bandwidth ceiling (500ms target): 1000 Mbps, conc=6
		// perWorker=20.8 MB/s, 500ms=10.4 MB → pow2 down = 8 MiB
		// Without bw cap: batch=1GiB; with bw cap: 8 MiB; floor max(4MiB,rmem) applies
		{"bw cap 1000mbps conc=6", 6, 4, 1 << 30, 4 * mib, 1000, max(8*mib, clientRmem)},

		// Bandwidth ceiling: very high link
		// 100000 Mbps, conc=6 → perWorker≈2 GB/s, 500ms=1.04 GB → pow2 down = 512 MiB
		// concurrency batch = 1GiB → min(1GiB, 512MiB) = 512 MiB
		{"bw cap 100gbps", 6, 4, 1 << 30, 4 * mib, 100000, 512 * mib},

		// Bandwidth ceiling with high concurrency:
		// 8400 Mbps, conc=192 → perWorker=5.5 MB/s, 500ms=2.7 MB → pow2 down = 2 MiB
		// concurrency batch = 32 MiB → min(32, 2) = 2 MiB, floor raises it
		{"bw cap 8400mbps conc=192", 192, 4, 1 << 30, 4 * mib, 8400, max(4*mib, clientRmem)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SuggestBatchMaxBytes(tt.conc, tt.winConc, tt.windowBytes, tt.srvWmem, tt.linkMbps)
			if got != tt.want {
				t.Fatalf("SuggestBatchMaxBytes(%d, %d, %d, %d, %d) = %d, want %d",
					tt.conc, tt.winConc, tt.windowBytes, tt.srvWmem, tt.linkMbps, got, tt.want)
			}
		})
	}
}

func TestExplainBatchMaxBytesBandwidthCapKeepsSplitWindowWorkers(t *testing.T) {
	const mib = int64(1 << 20)
	plan := ExplainBatchMaxBytes(6, 4, 1<<30, 4*mib, 8400)
	if plan.BatchMaxBytes != 64*mib {
		t.Fatalf("expected batch size 64 MiB, got %d", plan.BatchMaxBytes)
	}
	if plan.PerFileWorkers != 1 {
		t.Fatalf("expected planned per-file workers to stay at 1, got %d", plan.PerFileWorkers)
	}
	if plan.SplitWindowWorkers != 1 {
		t.Fatalf("expected split-window workers to stay capped at 1, got %d", plan.SplitWindowWorkers)
	}
}

func FuzzSuggestBatchMaxBytes(f *testing.F) {
	f.Add(192, 4, int64(1<<30), int64(4<<20), int64(0))
	f.Add(0, 0, int64(1<<30), int64(4<<20), int64(0))
	f.Add(-10, -5, int64(1<<20), int64(0), int64(0))
	f.Add(256, 128, int64(512<<20), int64(16<<20), int64(0))
	f.Add(6, 4, int64(1<<30), int64(4<<20), int64(8400))
	f.Add(8, 4, int64(1<<30), int64(4<<20), int64(1000))
	f.Add(1, 1, int64(1), int64(0), int64(0))
	f.Add(2048, 4, int64(1<<30), int64(4<<20), int64(100000))
	f.Fuzz(func(t *testing.T, conc int, winConc int, windowBytes int64, serverWmem int64, linkMbps int64) {
		if windowBytes <= 0 {
			return
		}
		if serverWmem < 0 {
			serverWmem = 0
		}
		if linkMbps < 0 {
			linkMbps = 0
		}
		got := SuggestBatchMaxBytes(conc, winConc, windowBytes, serverWmem, linkMbps)
		const mib = int64(1 << 20)

		// Property 1: power of 2 MiB (when result >= 1 MiB)
		if got >= mib {
			if got%mib != 0 {
				t.Fatalf("not MiB-aligned: %d", got)
			}
			mibUnits := got / mib
			if mibUnits&(mibUnits-1) != 0 {
				t.Fatalf("not power-of-2 MiB: %d (%d MiB units)", got, mibUnits)
			}
		}

		// Property 2: <= windowBytes
		if got > windowBytes {
			t.Fatalf("exceeds window: %d > %d", got, windowBytes)
		}

		// Property 3: >= socket buffer floor (when floor <= window)
		clientRmem := int64(utils.MaxSocketReadBufferBytes())
		floor := max(serverWmem, clientRmem)
		if floor <= windowBytes && got < floor {
			t.Fatalf("below socket floor: %d < %d (serverWmem=%d clientRmem=%d)", got, floor, serverWmem, clientRmem)
		}
	})
}

func TestManifestSizeEmpty(t *testing.T) {
	var m *Manifest
	mem, disk := m.Size()
	if mem != 0 || disk != 0 {
		t.Errorf("nil manifest: got mem=%d disk=%d, want 0,0", mem, disk)
	}
	m2 := &Manifest{}
	mem, disk = m2.Size()
	if mem <= 0 {
		t.Errorf("empty manifest: got mem=%d, want > 0 (struct itself)", mem)
	}
	if disk != 0 {
		t.Errorf("empty manifest: got disk=%d, want 0 (never serialized)", disk)
	}
}

func TestManifestSizeCached(t *testing.T) {
	var b strings.Builder
	b.WriteString("FM/1 tx 7:/remote mode=fast link-mbps=1000 concurrency=1\n")
	b.WriteString("F1 10 0:100 0644 0:5:a.txt\n")
	m, err := parseManifest([]byte(b.String()))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	first, firstDisk := m.Size()
	if firstDisk <= 0 {
		t.Errorf("parsed manifest should have disk > 0, got %d", firstDisk)
	}
	// Mutate a Path — cache should be sticky.
	m.Entries[0].Path = "a-much-longer-path-than-before.txt"
	mem, disk := m.Size()
	if mem != first {
		t.Errorf("cached mem changed after mutation: got %d, want %d", mem, first)
	}
	if disk != firstDisk {
		t.Errorf("disk changed after mutation: got %d, want %d", disk, firstDisk)
	}
}

func TestManifestSizeScales(t *testing.T) {
	var b strings.Builder
	b.WriteString("FM/1 txmeasure 7:/remote mode=fast link-mbps=1000 concurrency=1\n")
	prevPath := ""
	for i := 0; i < 1000; i++ {
		path := fmt.Sprintf("data/images/2024/album_%03d/photo_%04d.jpg", i/100, i)
		b.WriteString(fmt.Sprintf("F%d 1024 0:%d 0644 %s\n",
			i, 1735771234567890123+int64(i),
			intencoding.EncodePathToken(prevPath, path)))
		prevPath = path
	}
	raw := []byte(b.String())
	m, err := parseManifest(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	mem, disk := m.Size()
	t.Logf("1000 entries: mem=%d bytes (~%d B/entry), disk=%d bytes (~%d B/entry)",
		mem, mem/1000, disk, disk/1000)
	if mem < 96*1000 {
		t.Errorf("mem too small: %d", mem)
	}
	if mem > 1<<20 {
		t.Errorf("mem too large: %d", mem)
	}
	if disk != int64(len(raw)) {
		t.Errorf("disk=%d, want %d", disk, len(raw))
	}
}
