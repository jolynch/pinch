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
	intcodec "github.com/jolynch/pinch/internal/filexfer/codec"
	intftcp "github.com/jolynch/pinch/internal/filexfer/ftcp"
	"github.com/zeebo/xxh3"
)

const defaultCLIEncodings = "zstd,lz4,identity"

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

func encodeSingleFramePayload(data []byte, comp string) ([]byte, error) {
	switch comp {
	case "none":
		return data, nil
	case EncodingZstd, EncodingLz4:
		var buf bytes.Buffer
		out, closeEncoded, _, err := intcodec.WrapCompressedWriter(&buf, comp)
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
	return intcodec.FormatXXH64HashToken(h.Sum64())
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
		"FX/1 0 offset=0 size=%d wsize=%d comp=none enc=none hash=xxh128:%s ts=1000\n%sFXT/1 0 status=ok ts=1001\n",
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
	resp, err := client.DownloadFileFromManifest(context.Background(), DownloadFileRequest{
		Manifest:       manifest,
		FileID:         0,
		OutRoot:        outRoot,
		AcceptEncoding: defaultCLIEncodings,
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
	_, err := client.DownloadFileFromManifest(context.Background(), DownloadFileRequest{
		Manifest:       manifest,
		FileID:         0,
		OutRoot:        outRoot,
		AcceptEncoding: defaultCLIEncodings,
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
	_, err := client.DownloadFileFromManifest(context.Background(), DownloadFileRequest{
		Manifest:                manifest,
		FileID:                  0,
		OutRoot:                 outRoot,
		AcceptEncoding:          defaultCLIEncodings,
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
	_, err := client.DownloadFileFromManifest(context.Background(), DownloadFileRequest{
		Manifest:       manifest,
		FileID:         0,
		OutRoot:        outRoot,
		AcceptEncoding: defaultCLIEncodings,
	})
	if err == nil || !strings.Contains(err.Error(), "window hash mismatch") {
		t.Fatalf("expected hash verification failure before metadata apply, got %v", err)
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

	client := NewClient(srv.URL)
	client.AckRequestTimeout = 50 * time.Millisecond
	start := time.Now()
	_, err := client.DownloadFileFromManifest(context.Background(), DownloadFileRequest{
		Manifest:       manifest,
		FileID:         0,
		OutRoot:        outRoot,
		AcceptEncoding: defaultCLIEncodings,
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
	client := NewClient("ignored:0", WithContextDialer(dialContext))
	client.ServerAgePublicKey = ""

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
