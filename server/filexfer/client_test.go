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
	"strings"
	"sync"
	"testing"
	"time"

	"filippo.io/age"
	intencoding "github.com/jolynch/pinch/internal/filexfer/encoding"
	intftcp "github.com/jolynch/pinch/internal/filexfer/ftcp"
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
		if len(firstReq.Params) > 0 {
			blob := strings.TrimSpace(firstReq.Params[0]["blob"])
			if blob != "" {
				if recipient, parseErr := age.ParseX25519Recipient(blob); parseErr == nil {
					ew, encErr := age.Encrypt(conn, recipient)
					if encErr != nil {
						return
					}
					responseOut = ew
					closeResponse = ew.Close
				}
			}
		}
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
	t.Setenv("PINCH_FILE_SERVER_AGE_PUBLIC_KEY", "age1envvalue")
	dialer := func(context.Context, string) (net.Conn, error) { return nil, errors.New("unused") }

	client := NewClient(
		" 127.0.0.1:3453 ",
		WithServerAgePublicKey(""),
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
	if client.ServerAgePublicKey != "" {
		t.Fatalf("expected option to override env, got %q", client.ServerAgePublicKey)
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

func encodeSingleFramePayload(data []byte, comp string) ([]byte, error) {
	switch comp {
	case "none":
		return data, nil
	case EncodingZstd, EncodingLz4:
		var buf bytes.Buffer
		out, closeEncoded, _, err := intencoding.WrapCompressedWriter(&buf, comp)
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

type singleDownloadRequest struct {
	Manifest                *Manifest
	FileID                  uint64
	OutRoot                 string
	OutFile                 string
	Stdout                  io.Writer
	AgePublicKey            string
	AgeIdentity             string
	AckEveryBytes           int64
	ResumeFromBytes         int64
	NoSync                  bool
	MetadataApplyBestEffort bool
	ProgressUpdates         chan<- DownloadProgressUpdate
}

func downloadSingle(ctx context.Context, client *Client, req singleDownloadRequest) (DownloadFileResponse, error) {
	_ = req.AckEveryBytes
	if req.Manifest != nil && req.ResumeFromBytes > 0 {
		if req.Manifest.Progress == nil {
			req.Manifest.Progress = make(map[uint64]ManifestProgress)
		}
		progress := req.Manifest.Progress[req.FileID]
		progress.AckBytes = req.ResumeFromBytes
		req.Manifest.Progress[req.FileID] = progress
	}
	if req.ResumeFromBytes != 0 {
		// Keep backward-compatible helper signature, but resume is now sourced from manifest progress.
	}
	resp, err := client.DownloadFilesFromManifestBatch(ctx, DownloadBatchRequest{
		Manifest:                req.Manifest,
		FileIDs:                 []uint64{req.FileID},
		OutRoot:                 req.OutRoot,
		OutFile:                 req.OutFile,
		Stdout:                  req.Stdout,
		AgePublicKey:            req.AgePublicKey,
		AgeIdentity:             req.AgeIdentity,
		NoSync:                  req.NoSync,
		MetadataApplyBestEffort: req.MetadataApplyBestEffort,
		ProgressUpdates:         req.ProgressUpdates,
	})
	if err != nil {
		return DownloadFileResponse{}, err
	}
	if len(resp.Files) != 1 {
		return DownloadFileResponse{}, fmt.Errorf("expected one downloaded file, got %d", len(resp.Files))
	}
	return resp.Files[0], nil
}

func encryptAgeBlob(t *testing.T, plaintext []byte, recipient age.Recipient) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, recipient)
	if err != nil {
		t.Fatalf("age encrypt setup failed: %v", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		t.Fatalf("age encrypt write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("age encrypt close failed: %v", err)
	}
	return buf.Bytes()
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

func TestParseManifestSingleChunk(t *testing.T) {
	raw := strings.Join([]string{
		"FM/1 tx123 5:/root",
		"0 5 0:100 0644 0:5:a.txt",
		"1 7 2:1 0600 0:9:dir/b.txt",
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
	if manifest.Entries[1].Mode != 0o600 {
		t.Fatalf("expected parsed mode 0600, got %04o", manifest.Entries[1].Mode)
	}
}

func TestParseManifestMultiChunk(t *testing.T) {
	raw := strings.Join([]string{
		"FM/1 tx456 6:/root2",
		"0 5 0:100 0644 0:5:a.txt",
		"",
		"FM/1 tx456 6:/root2",
		"1 3 0:200 0600 0:5:b.txt",
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

func TestMarshalManifestRoundTrip(t *testing.T) {
	manifest := &Manifest{
		TransferID: "txrt",
		Root:       "/root",
		Entries: []ManifestEntry{
			{ID: 1, Size: 5, Mtime: 100, Mode: 0o644, Path: "a.txt"},
			{ID: 2, Size: 9, Mtime: 100, Mode: 0o600, Path: "dir/b.txt"},
		},
	}
	raw, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatalf("MarshalManifest failed: %v", err)
	}
	parsed, err := parseManifest(raw)
	if err != nil {
		t.Fatalf("parseManifest failed: %v", err)
	}
	if parsed.TransferID != manifest.TransferID {
		t.Fatalf("transfer id mismatch: got=%q want=%q", parsed.TransferID, manifest.TransferID)
	}
	if parsed.Root != manifest.Root {
		t.Fatalf("root mismatch: got=%q want=%q", parsed.Root, manifest.Root)
	}
	if len(parsed.Entries) != len(manifest.Entries) {
		t.Fatalf("entry count mismatch: got=%d want=%d", len(parsed.Entries), len(manifest.Entries))
	}
	for i := range parsed.Entries {
		if parsed.Entries[i] != manifest.Entries[i] {
			t.Fatalf("entry %d mismatch: got=%+v want=%+v", i, parsed.Entries[i], manifest.Entries[i])
		}
	}
}

func TestParseManifestMalformed(t *testing.T) {
	raw := strings.Join([]string{
		"FM/1 tx789 5:/root",
		"0 5 0:abc 0644 0:5:a.txt",
		"",
	}, "\n")
	if _, err := parseManifest([]byte(raw)); err == nil {
		t.Fatalf("expected parseManifest error")
	}
}

func TestParseManifestLegacyEntryRejected(t *testing.T) {
	raw := strings.Join([]string{
		"FM/1 tx789 5:/root",
		"0 5 0:100 0:5:a.txt",
		"",
	}, "\n")
	if _, err := parseManifest([]byte(raw)); err == nil {
		t.Fatalf("expected legacy manifest format to be rejected")
	}
}

func TestFetchManifestEncryptedWithAge(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate age identity: %v", err)
	}
	recipient := identity.Recipient().String()
	manifestRaw := strings.Join([]string{
		"FM/1 txenc 5:/root",
		"0 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbTXFER {
			return fmt.Errorf("expected TXFER, got %v", req.Verb)
		}
		if got := req.Params[0]["directory"]; got != "/root" {
			return fmt.Errorf("expected TXFER directory /root, got %q", got)
		}
		if _, err := io.WriteString(out, manifestRaw); err != nil {
			return err
		}
		_, err := io.WriteString(out, "OK\r\n")
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.FetchManifest(context.Background(), FetchManifestRequest{
		Directory:    "/root",
		AgePublicKey: recipient,
		AgeIdentity:  identity.String(),
	})
	if err != nil {
		t.Fatalf("FetchManifest failed: %v", err)
	}
	if resp.Manifest.TransferID != "txenc" || len(resp.Manifest.Entries) != 1 {
		t.Fatalf("unexpected manifest: %+v", resp.Manifest)
	}
}

func TestFetchFileDecodesByHeaderComp(t *testing.T) {
	logical := []byte("hello world")
	for _, comp := range []string{"none", EncodingZstd, EncodingLz4} {
		comp := comp
		t.Run(comp, func(t *testing.T) {
			frame := buildFXFrame(t, 7, comp, 0, logical, nil)

			srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
				if req.Verb != intftcp.VerbSEND {
					return fmt.Errorf("expected SEND, got %v", req.Verb)
				}
				if _, err := io.WriteString(out, frame); err != nil {
					return err
				}
				return nil
			})
			defer srv.Close()

			client := NewClient(srv.URL)
			resp, err := client.FetchFile(context.Background(), FetchFileRequest{
				TransferID: "tx",
				Files: []FetchFileTarget{{
					FileID:   7,
					FullPath: "/root/a.txt",
				}},
				AckBytes: -1,
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

func TestFetchFileDecryptsWholeResponseWithAge(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate age identity: %v", err)
	}
	recipient := identity.Recipient().String()
	logical := []byte("hello world")
	frame := buildFXFrame(t, 7, "none", 0, logical, nil)
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbSEND {
			return fmt.Errorf("expected SEND, got %v", req.Verb)
		}
		if _, err := io.WriteString(out, frame); err != nil {
			return err
		}
		return nil
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{
		TransferID: "tx",
		Files: []FetchFileTarget{{
			FileID:   7,
			FullPath: "/root/a.txt",
		}},
		AgePublicKey: recipient,
		AgeIdentity:  identity.String(),
		AckBytes:     -1,
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
}

func TestFetchFileIgnoresPerFrameChecksumMismatch(t *testing.T) {
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
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		_, err := io.WriteString(out, frame)
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", Files: []FetchFileTarget{{FileID: 0, FullPath: "/root/a.txt"}}, AckBytes: -1})
	if err != nil {
		t.Fatalf("FetchFile setup failed: %v", err)
	}
	got, err := readAndClose(t, resp.Reader)
	if err != nil {
		t.Fatalf("expected per-frame checksum mismatch to be ignored, got %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("unexpected payload %q", got)
	}
}

func TestFetchFileRejectsMalformedTrailer(t *testing.T) {
	logical := []byte("hello")
	frame := fmt.Sprintf(
		"FX/1 0 offset=0 size=%d wsize=%d comp=none enc=none hash=xxh128:%s ts=1000\n%sFXT/1 0 ts=1001 next=0\n",
		len(logical),
		len(logical),
		xxh128HexTest(logical),
		string(logical),
	)
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		_, err := io.WriteString(out, frame)
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", Files: []FetchFileTarget{{FileID: 0, FullPath: "/root/a.txt"}}, AckBytes: -1})
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
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		_, err := io.WriteString(out, frame)
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", Files: []FetchFileTarget{{FileID: 0, FullPath: "/root/a.txt"}}, AckBytes: -1})
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
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		_, err := io.WriteString(out, frame)
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	if _, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", Files: []FetchFileTarget{{FileID: 0, FullPath: "/root/a.txt"}}, AckBytes: -1}); err == nil {
		t.Fatalf("expected unsupported enc error")
	}
}

func TestFetchFileDecodesMultiFrameSequence(t *testing.T) {
	var body strings.Builder
	next := int64(5)
	body.WriteString(buildFXFrame(t, 0, "none", 0, []byte("hello"), &next))
	body.WriteString(buildFXFrame(t, 0, "none", 5, []byte(" world"), nil))

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		_, err := io.WriteString(out, body.String())
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", Files: []FetchFileTarget{{FileID: 0, FullPath: "/root/a.txt"}}, AckBytes: -1})
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
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		_, err := io.WriteString(out, frame)
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", Files: []FetchFileTarget{{FileID: 0, FullPath: "/root/a.txt"}}, AckBytes: -1})
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

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		_, err := io.WriteString(out, body.String())
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	resp, err := client.FetchFile(context.Background(), FetchFileRequest{TransferID: "tx", Files: []FetchFileTarget{{FileID: 0, FullPath: "/root/a.txt"}}, AckBytes: -1})
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
	downloadResp, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest: manifest,
		FileID:   0,
		OutRoot:  outRoot,
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

func TestDownloadFileFromManifestAppliesModeAfterVerify(t *testing.T) {
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
	info, err := os.Stat(resp.DestinationPath)
	if err != nil {
		t.Fatalf("stat output file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected mode 0600, got %04o", info.Mode().Perm())
	}
}

func TestDownloadFileFromManifestMetadataStrictFailure(t *testing.T) {
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
	_, err := downloadSingle(context.Background(), client, singleDownloadRequest{
		Manifest: manifest,
		FileID:   0,
		OutRoot:  outRoot,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid trailer uid") {
		t.Fatalf("expected strict metadata failure, got %v", err)
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
		Manifest:                manifest,
		FileID:                  0,
		OutRoot:                 outRoot,
		MetadataApplyBestEffort: true,
	})
	if err != nil {
		t.Fatalf("expected best-effort metadata apply success, got %v", err)
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

func TestDownloadFileFromManifestResumesFromOffsetAndTruncatesTail(t *testing.T) {
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
		Manifest:                manifest,
		FileID:                  0,
		OutRoot:                 outRoot,
		ResumeFromBytes:         5,
		AckEveryBytes:           1,
		NoSync:                  true,
		ProgressUpdates:         make(chan DownloadProgressUpdate, 1),
		AgePublicKey:            "",
		AgeIdentity:             "",
		MetadataApplyBestEffort: false,
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
	if string(got) != "helloworld" {
		t.Fatalf("unexpected resumed output: %q", got)
	}
}

func TestDownloadFilesFromManifestBatchUsesMultiACK(t *testing.T) {
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
			if len(req.Params) != 3 {
				return fmt.Errorf("expected txfer header + 2 SEND items, got %d params", len(req.Params))
			}
			if got := req.Params[0]["txferid"]; got != manifest.TransferID {
				return fmt.Errorf("unexpected transfer id: %q", got)
			}
			if got := req.Params[1]["path"]; got != "/remote/a.txt" {
				return fmt.Errorf("unexpected first path: %q", got)
			}
			if got := req.Params[2]["path"]; got != "/remote/b.txt" {
				return fmt.Errorf("unexpected second path: %q", got)
			}
			if _, err := io.WriteString(out, buildFXFrame(t, 0, "none", 0, dataA, nil)); err != nil {
				return err
			}
			if _, err := io.WriteString(out, buildFXFrame(t, 1, "none", 0, dataB, nil)); err != nil {
				return err
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
	resp, err := client.DownloadFilesFromManifestBatch(context.Background(), DownloadBatchRequest{
		Manifest:        manifest,
		FileIDs:         []uint64{0, 1},
		OutRoot:         outRoot,
		NoSync:          true,
		ProgressUpdates: progressUpdates,
	})
	if err != nil {
		t.Fatalf("DownloadFilesFromManifestBatch failed: %v", err)
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
	client := NewClient("ignored:0", WithContextDialer(dialContext), WithServerAgePublicKey(""))

	resp, err := client.GetTransferStatus(context.Background(), GetTransferStatusRequest{TransferID: "tx123"})
	if err != nil {
		t.Fatalf("GetTransferStatus failed: %v", err)
	}
	if !called {
		t.Fatalf("expected injected DialContext to be used")
	}
	if resp.Status == nil || resp.Status.TransferID != "tx123" {
		t.Fatalf("unexpected status response: %+v", resp.Status)
	}
}

func TestFetchFileMissingDoesNotAckInline(t *testing.T) {
	var requests int
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		requests++
		_, err := io.WriteString(out, "ERR NOT_FOUND file not found\r\n")
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.FetchFile(context.Background(), FetchFileRequest{
		TransferID: "tx",
		Files: []FetchFileTarget{{
			FileID:   0,
			FullPath: "/root/missing.txt",
		}},
		AckBytes: -1,
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
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		requests++
		_, err := io.WriteString(out, "ERR NOT_FOUND transfer not found\r\n")
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.FetchFile(context.Background(), FetchFileRequest{
		TransferID: "tx",
		Files: []FetchFileTarget{{
			FileID:   0,
			FullPath: "/root/missing.txt",
		}},
		AckBytes: -1,
	})
	if err == nil || !errors.Is(err, ErrFileMissing) {
		t.Fatalf("expected ErrFileMissing, got %v", err)
	}
	if requests != 1 {
		t.Fatalf("expected only one request without missing-ack retry, got %d", requests)
	}
}
