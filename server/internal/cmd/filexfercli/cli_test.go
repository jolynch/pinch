package filexfercli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"filippo.io/age"
	. "github.com/jolynch/pinch/filexfer"
	"github.com/jolynch/pinch/internal/aead"
	"github.com/jolynch/pinch/internal/filexfer/encoding"
	intftcp "github.com/jolynch/pinch/internal/filexfer/ftcp"
	"github.com/zeebo/xxh3"
)

type ftcpTestServer struct {
	URL      string
	listener net.Listener
	wg       sync.WaitGroup
}

func (s *ftcpTestServer) Close() {
	if s == nil {
		return
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
}

func newFTCPTestServer(t *testing.T, handler func(intftcp.Request, io.Writer) error) *ftcpTestServer {
	return newFTCPTestServerWithIdentity(t, nil, handler)
}

func newFTCPTestServerWithIdentity(t *testing.T, serverID *age.X25519Identity, handler func(intftcp.Request, io.Writer) error) *ftcpTestServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	s := &ftcpTestServer{URL: ln.Addr().String(), listener: ln}
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
				serveFTCPConn(c, serverID, handler)
			}(conn)
		}
	}()
	return s
}

func serveFTCPConn(conn net.Conn, serverID *age.X25519Identity, handler func(intftcp.Request, io.Writer) error) {
	br := bufio.NewReader(conn)
	firstLine, err := readFTCPLine(br)
	if err != nil {
		return
	}
	req, err := intftcp.ParseRequest([]byte(firstLine))
	if err != nil {
		_, _ = io.WriteString(conn, "ERR BAD_REQUEST "+err.Error()+"\r\n")
		return
	}

	out := io.Writer(conn)
	closeOut := func() error { return nil }
	cmdReq := req
	if req.Verb == intftcp.VerbAUTH {
		if len(req.Params) == 0 {
			_, _ = io.WriteString(conn, "ERR BAD_AUTH missing protocol\r\n")
			return
		}
		protocol := req.Params[0]["protocol"]

		if protocol == "key" {
			// Key exchange: return recommended cipher and server public key.
			if serverID == nil {
				_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED no server identity\r\n")
				return
			}
			_, _ = io.WriteString(conn, "OK "+string(aead.RecommendedCipher())+" "+serverID.Recipient().String()+"\r\n")
			return
		}

		// aes/chacha20: decode and decrypt the blob to get the client's public key.
		blobRaw := req.Params[0]["blob"]
		if blobRaw == "" || serverID == nil {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}
		opts := aead.Options{}
		switch protocol {
		case "aes":
			opts.Algorithm = aead.AlgorithmAES
		case "chacha20":
			opts.Algorithm = aead.AlgorithmChaCha20
		default:
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}
		blobBytes, b64Err := base64.StdEncoding.DecodeString(strings.TrimSpace(blobRaw))
		if b64Err != nil {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED bad base64\r\n")
			return
		}
		dec, decErr := aead.DecryptWithOptions(bytes.NewReader(blobBytes), serverID, opts)
		if decErr != nil {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}
		plain, readErr := io.ReadAll(dec)
		if readErr != nil {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}
		recipient, parseErr := age.ParseX25519Recipient(strings.TrimSpace(string(plain)))
		if parseErr != nil {
			_, _ = io.WriteString(conn, "ERR NOT_AUTHORIZED\r\n")
			return
		}

		// Encrypt responses to client.
		ew, encErr := aead.Encrypt(conn, recipient, opts)
		if encErr != nil {
			return
		}
		out = ew
		closeOut = ew.Close

		// Decrypt the command from client.
		cmdReader, cmdDecErr := aead.DecryptWithOptions(br, serverID, opts)
		if cmdDecErr != nil {
			_, _ = io.WriteString(out, "ERR NOT_AUTHORIZED request decryption failed\r\n")
			_ = closeOut()
			return
		}
		br = bufio.NewReader(cmdReader)

		cmdLine, cmdErr := readFTCPLine(br)
		if cmdErr != nil {
			_, _ = io.WriteString(out, "ERR BAD_REQUEST missing command\r\n")
			_ = closeOut()
			return
		}
		cmdReq, err = intftcp.ParseRequest([]byte(cmdLine))
		if err != nil {
			_, _ = io.WriteString(out, "ERR BAD_REQUEST "+err.Error()+"\r\n")
			_ = closeOut()
			return
		}
	}
	if cmdReq.Verb == intftcp.VerbPROBE && len(cmdReq.Params) > 0 {
		n, convErr := strconv.ParseInt(strings.TrimSpace(cmdReq.Params[0]["probe-bytes"]), 10, 64)
		if convErr != nil || n < 0 {
			_, _ = io.WriteString(out, "ERR BAD_REQUEST invalid probe-bytes\r\n")
			_ = closeOut()
			return
		}
		if n > 0 {
			if _, drainErr := io.CopyN(io.Discard, br, n); drainErr != nil {
				_, _ = io.WriteString(out, "ERR BAD_REQUEST invalid probe payload\r\n")
				_ = closeOut()
				return
			}
		}
	}
	if cmdReq.Verb == intftcp.VerbSYNC {
		for {
			line, readErr := readFTCPLine(br)
			if readErr != nil {
				_, _ = io.WriteString(out, "ERR BAD_REQUEST invalid sync manifest\r\n")
				_ = closeOut()
				return
			}
			if strings.TrimSpace(line) == "" {
				break
			}
		}
	}

	if err := handler(cmdReq, out); err != nil {
		_, _ = io.WriteString(out, "ERR INTERNAL "+err.Error()+"\r\n")
	}
	_ = closeOut()
}

func readFTCPLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func xxh128HexCLI(data []byte) string {
	h := xxh3.Hash128(data).Bytes()
	return hex.EncodeToString(h[:])
}

func buildCLIFrame(fileID uint64, body []byte, offset int64) string {
	return buildCLIFrameWithMetadata(fileID, body, offset, nil)
}

func buildCLIFrameWithMetadata(fileID uint64, body []byte, offset int64, meta *FileTrailerMetadata) string {
	xsum := xxh128HexCLI(body)
	header := fmt.Sprintf(
		"FX/1 %d offset=%d size=%d wsize=%d comp=none hash=xxh128:%s ts=1000\n",
		fileID,
		offset,
		len(body),
		len(body),
		xsum,
	)
	trailerParts := []string{
		fmt.Sprintf("FXT/1 %d", fileID),
		"status=ok",
		"ts=1001",
		"next=0",
		fmt.Sprintf("file-hash=xxh128:%s", xsum),
	}
	if meta != nil {
		if meta.Size > 0 {
			trailerParts = append(trailerParts, fmt.Sprintf("meta:size=%d", meta.Size))
		}
		if meta.MtimeNS > 0 {
			trailerParts = append(trailerParts, fmt.Sprintf("meta:mtime_ns=%d", meta.MtimeNS))
		}
		if meta.Mode != "" {
			trailerParts = append(trailerParts, "meta:mode="+meta.Mode)
		}
		if meta.UID != "" {
			trailerParts = append(trailerParts, "meta:uid="+meta.UID)
		}
		if meta.GID != "" {
			trailerParts = append(trailerParts, "meta:gid="+meta.GID)
		}
	}
	trailerPrefix := strings.Join(trailerParts, " ")
	h := xxh3.New()
	_, _ = h.Write([]byte(header))
	_, _ = h.Write(body)
	_, _ = h.Write([]byte(trailerPrefix))
	return fmt.Sprintf("%s%s%s hash=xxh64:%016x\n", header, string(body), trailerPrefix, h.Sum64())
}

// setupPinchState creates the .pinch directory structure for tests and writes
// a manifest (and optional progress) file. Returns the target directory path.
func setupPinchState(t *testing.T, tmp string, manifestRaw string, progressRaw string) string {
	t.Helper()
	targetDir := filepath.Join(tmp, "dst")
	pinchDir := filepath.Join(tmp, ".pinch")
	if err := os.MkdirAll(pinchDir, 0o755); err != nil {
		t.Fatalf("mkdir .pinch: %v", err)
	}
	if manifestRaw != "" {
		// Write to manifest.server (the server-state file) so that start/get can use it.
		if err := os.WriteFile(filepath.Join(pinchDir, "manifest.server"), []byte(manifestRaw), 0o644); err != nil {
			t.Fatalf("write manifest.server: %v", err)
		}
	}
	if progressRaw != "" {
		if err := os.WriteFile(filepath.Join(pinchDir, "manifest.progress"), []byte(progressRaw), 0o644); err != nil {
			t.Fatalf("write progress: %v", err)
		}
	}
	return targetDir
}

func withSyncPromptTestInput(t *testing.T, input string, isTerminal bool) {
	t.Helper()
	prevInput := syncPromptInput
	prevIsTerminal := syncPromptIsTerminal
	syncPromptInput = strings.NewReader(input)
	syncPromptIsTerminal = func() bool { return isTerminal }
	t.Cleanup(func() {
		syncPromptInput = prevInput
		syncPromptIsTerminal = prevIsTerminal
	})
}

func writeCLIProbeResponse(req intftcp.Request, out io.Writer) error {
	cts0 := req.Params[0]["cts0"]
	n, err := strconv.Atoi(req.Params[0]["probe-bytes"])
	if err != nil || n < 0 {
		return fmt.Errorf("invalid probe-bytes: %q", req.Params[0]["probe-bytes"])
	}
	if _, err := io.WriteString(out, fmt.Sprintf("PROBE cpu=24 io-depth=1 cts0=%s sts0=10 sts1=11 probe-bytes=%d gentle-cpu-pct=25 gentle-bw-pct=25\n", cts0, n)); err != nil {
		return err
	}
	if n > 0 {
		if _, err := out.Write(make([]byte, n)); err != nil {
			return err
		}
	}
	_, err = io.WriteString(out, "OK\r\n")
	return err
}

func buildTestManifestRaw(transferID string, entries []string) string {
	root := "/remote"
	lines := []string{
		fmt.Sprintf("FM/1 %s %d:%s mode=fast link-mbps=1000 concurrency=1", transferID, len(root), root),
	}
	lines = append(lines, entries...)
	lines = append(lines, "")
	return strings.Join(lines, "\n")
}

func buildTestManifestEntry(id uint64, size int64, mtime int64, mode os.FileMode, path string) string {
	return fmt.Sprintf("%d %d 0:%d %s 0:%d:%s", id, size, mtime, encoding.FormatManifestMode(mode), len(path), path)
}

func buildTestManifestEntryFromDisk(t *testing.T, fullPath string, relPath string, id uint64) string {
	t.Helper()
	info, err := os.Stat(fullPath)
	if err != nil {
		t.Fatalf("stat %s: %v", fullPath, err)
	}
	return buildTestManifestEntry(id, info.Size(), info.ModTime().UnixNano(), info.Mode(), relPath)
}

func writeSyncResponse(out io.Writer, transferID string, entries []string, removedIDs []uint64) error {
	if _, err := io.WriteString(out, buildTestManifestRaw(transferID, entries)); err != nil {
		return err
	}
	for _, id := range removedIDs {
		if _, err := io.WriteString(out, fmt.Sprintf("RM %d\n", id)); err != nil {
			return err
		}
	}
	_, err := io.WriteString(out, "OK\r\n")
	return err
}

func TestRunCLITransferAndGet(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	manifestRaw := strings.Join([]string{
		"FM/1 txcli 7:/remote mode=fast link-mbps=1000 concurrency=8",
		"0 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	fileBody := []byte("hello")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			cts0 := req.Params[0]["cts0"]
			n, err := strconv.Atoi(req.Params[0]["probe-bytes"])
			if err != nil || n < 0 {
				return fmt.Errorf("invalid probe-bytes: %q", req.Params[0]["probe-bytes"])
			}
			if _, err := io.WriteString(out, fmt.Sprintf("PROBE cpu=24 io-depth=1 cts0=%s sts0=10 sts1=11 probe-bytes=%d gentle-cpu-pct=25 gentle-bw-pct=25\n", cts0, n)); err != nil {
				return err
			}
			if n > 0 {
				if _, err := out.Write(make([]byte, n)); err != nil {
					return err
				}
			}
			_, err = io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbTXFER:
			dir := req.Params[0]["directory"]
			switch dir {
			case "/remote":
				if got := req.Params[0]["mode"]; got != LoadStrategyFast {
					return fmt.Errorf("unexpected mode: %q", got)
				}
				if _, err := io.WriteString(out, manifestRaw); err != nil {
					return err
				}
			case "/remote/a.txt":
				singleManifest := "FM/1 txget 7:/remote mode=fast link-mbps=0 concurrency=8\n0 5 0:100 0644 0:5:a.txt\n"
				if _, err := io.WriteString(out, singleManifest); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unexpected directory: %q", dir)
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrame(0, fileBody, 0))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	serverManifestPath := filepath.Join(tmp, ".pinch", "manifest.server")
	code := runTransferCLI(srv.URL, []string{"-s", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("transfer: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(serverManifestPath); err != nil {
		t.Fatalf("manifest.server not written: %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	code = RunCLI([]string{srv.URL, "get", "--progress=false", "-a", "1KiB", "-o", filepath.Join(targetDir, "a.txt"), "/remote/a.txt"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("get: expected 0, got %d stderr=%s", code, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "a.txt"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("unexpected output: %q", string(got))
	}
}

func TestRunCLIGetSkipWriteDiscardsOutput(t *testing.T) {
	payload := []byte("hello")
	singleManifest := "FM/1 txdevnull 7:/remote mode=fast link-mbps=0 concurrency=8\n0 5 0:100 0644 0:5:a.txt\n"
	var sawAck atomic.Bool

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if _, err := io.WriteString(out, singleManifest); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrame(0, payload, 0))
			return err
		case intftcp.VerbACK:
			sawAck.Store(true)
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "get", "--skip-write", "--progress=false", "-a", "1KiB", "/remote/a.txt"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("get skip-write: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !sawAck.Load() {
		t.Fatalf("expected ACK request")
	}
	if !strings.Contains(stdout.String(), "  path: "+os.DevNull) {
		t.Fatalf("expected file metrics path %q, got: %s", os.DevNull, stdout.String())
	}
}

func TestRunCLITransferWithEncryptAuto(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	manifestRaw := strings.Join([]string{
		"FM/1 txenccli 7:/remote mode=fast link-mbps=1000 concurrency=8",
		"0 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")

	serverID, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate server identity: %v", err)
	}
	srv := newFTCPTestServerWithIdentity(t, serverID, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			cts0 := req.Params[0]["cts0"]
			n, err := strconv.Atoi(req.Params[0]["probe-bytes"])
			if err != nil || n < 0 {
				return fmt.Errorf("invalid probe-bytes: %q", req.Params[0]["probe-bytes"])
			}
			if _, err := io.WriteString(out, fmt.Sprintf("PROBE cpu=24 io-depth=1 cts0=%s sts0=10 sts1=11 probe-bytes=%d gentle-cpu-pct=25 gentle-bw-pct=25\n", cts0, n)); err != nil {
				return err
			}
			if n > 0 {
				if _, err := out.Write(make([]byte, n)); err != nil {
					return err
				}
			}
			_, err = io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbTXFER:
			if _, err := io.WriteString(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runTransferCLI(srv.URL, []string{"-s", "/remote", "--encrypt", "auto", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("transfer: expected 0, got %d stderr=%s", code, stderr.String())
	}
	serverManifestPath := filepath.Join(tmp, ".pinch", "manifest.server")
	raw, err := os.ReadFile(serverManifestPath)
	if err != nil {
		t.Fatalf("read manifest.server: %v", err)
	}
	if string(raw) != manifestRaw {
		t.Fatalf("unexpected decrypted manifest: %q", string(raw))
	}
}

func TestRunCLITransferWithEncryptAES(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	manifestRaw := strings.Join([]string{
		"FM/1 txaescli 7:/remote mode=fast link-mbps=1000 concurrency=8",
		"0 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")

	serverID, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate server identity: %v", err)
	}
	srv := newFTCPTestServerWithIdentity(t, serverID, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			cts0 := req.Params[0]["cts0"]
			n, err := strconv.Atoi(req.Params[0]["probe-bytes"])
			if err != nil || n < 0 {
				return fmt.Errorf("invalid probe-bytes: %q", req.Params[0]["probe-bytes"])
			}
			if _, err := io.WriteString(out, fmt.Sprintf("PROBE cpu=24 io-depth=1 cts0=%s sts0=10 sts1=11 probe-bytes=%d gentle-cpu-pct=25 gentle-bw-pct=25\n", cts0, n)); err != nil {
				return err
			}
			if n > 0 {
				if _, err := out.Write(make([]byte, n)); err != nil {
					return err
				}
			}
			_, err = io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbTXFER:
			if _, err := io.WriteString(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runTransferCLI(srv.URL, []string{"-s", "/remote", "--encrypt", "aes", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("transfer: expected 0, got %d stderr=%s", code, stderr.String())
	}
	serverManifestPath := filepath.Join(tmp, ".pinch", "manifest.server")
	raw, err := os.ReadFile(serverManifestPath)
	if err != nil {
		t.Fatalf("read manifest.server: %v", err)
	}
	if string(raw) != manifestRaw {
		t.Fatalf("unexpected decrypted manifest: %q", string(raw))
	}
}

func TestRunCLIStartDownloadsAll(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txstart 7:/remote mode=gentle link-mbps=700 concurrency=3",
		"0 5 0:100 0644 0:5:a.txt",
		"1 4 0:101 0644 0:5:b.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			for _, p := range req.Params[1:] {
				if got := p["mode"]; got != LoadStrategyGentle {
					return fmt.Errorf("expected SEND mode=%s, got %q", LoadStrategyGentle, got)
				}
			}
			for _, p := range req.Params[1:] {
				switch p["fid"] {
				case "0":
					if _, err := io.WriteString(out, buildCLIFrame(0, []byte("hello"), 0)); err != nil {
						return err
					}
				case "1":
					if _, err := io.WriteString(out, buildCLIFrame(1, []byte("test"), 0)); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unexpected fid: %q", p["fid"])
				}
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

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--concurrency", "2", "--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start: expected 0, got %d stderr=%s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "mode: [gentle]") ||
		!strings.Contains(out, "concurrency: 2 (override from --concurrency") ||
		!strings.Contains(out, "    window: ") ||
		!strings.Contains(out, "    batch-per-window: ") ||
		!strings.Contains(out, "server: 24 cpu, 1 io-depth") ||
		!strings.Contains(out, "25% gentle-bw") {
		t.Fatalf("missing start plan output: %s", out)
	}
	// After start, staging dir is renamed to target dir.
	for _, p := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(targetDir, p)); err != nil {
			t.Fatalf("missing output %s: %v", p, err)
		}
	}
}

func TestRunCLIStartUsesManifestConcurrencyDefault(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txstartdefault 7:/remote mode=fast link-mbps=1200 concurrency=5",
		"0 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSEND:
			if got := req.Params[1]["mode"]; got != LoadStrategyFast {
				return fmt.Errorf("expected SEND mode=%s, got %q", LoadStrategyFast, got)
			}
			if _, err := io.WriteString(out, buildCLIFrame(0, []byte("hello"), 0)); err != nil {
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

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start: expected 0, got %d stderr=%s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "mode: [fast]") ||
		!strings.Contains(out, "concurrency: 5") ||
		!strings.Contains(out, "    window: ") ||
		!strings.Contains(out, "    batch-per-window: ") ||
		!strings.Contains(out, "server: 24 cpu, 1 io-depth") {
		t.Fatalf("missing default start plan output: %s", out)
	}
}

func TestRunCLIStartDiscardSkipsTargetMutationAndLocalManifest(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txdiscard 7:/remote mode=fast link-mbps=700 concurrency=1",
		"0 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	keepPath := filepath.Join(targetDir, "keep.txt")
	if err := os.WriteFile(keepPath, []byte("keep"), 0o644); err != nil {
		t.Fatalf("write keep file: %v", err)
	}
	var sawAck atomic.Bool

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
			if _, err := io.WriteString(out, buildCLIFrame(0, []byte("hello"), 0)); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbACK:
			sawAck.Store(true)
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return nil
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{"--discard", "--progress=false", "--ack-every", "1KiB", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start --discard: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !sawAck.Load() {
		t.Fatalf("expected ACK request")
	}
	gotKeep, err := os.ReadFile(keepPath)
	if err != nil {
		t.Fatalf("read keep file: %v", err)
	}
	if string(gotKeep) != "keep" {
		t.Fatalf("unexpected keep file contents: %q", gotKeep)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected discarded output to be absent, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(tmp, ".pinch", "manifest")); !os.IsNotExist(err) {
		t.Fatalf("expected local manifest to be absent, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(tmp, ".pinch", "manifest.progress")); !os.IsNotExist(err) {
		t.Fatalf("expected progress state to be removed, stat err=%v", err)
	}
}

func TestRunCLIStartDiscardSkipsCompletedMetadataRefresh(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txdiscardrefresh 7:/remote mode=fast link-mbps=700 concurrency=1",
		"0 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	targetDir := setupPinchState(t, tmp, manifestRaw, "0 5 0\n")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		return fmt.Errorf("unexpected verb: %v", req.Verb)
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runStartCLI(srv.URL, []string{
		"--discard",
		"--progress=false",
		"--ack-every", "1KiB",
		targetDir,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start --discard completed refresh: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(tmp, ".pinch", "manifest.progress")); !os.IsNotExist(err) {
		t.Fatalf("expected progress state to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected discarded output to be absent, stat err=%v", err)
	}
}

func TestStartTransferProbeReporterIncludesTransferTelemetry(t *testing.T) {
	oldInterval := transferProbeRefreshInterval
	transferProbeRefreshInterval = 5 * time.Millisecond
	defer func() { transferProbeRefreshInterval = oldInterval }()

	var probeCount atomic.Int64
	var firstTransferID atomic.Value
	var firstObserved atomic.Int64

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbPROBE {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		probeCount.Add(1)
		firstTransferID.CompareAndSwap(nil, req.Params[0]["txferid"])
		if probeCount.Load() == 1 {
			obs, _ := strconv.ParseInt(strings.TrimSpace(req.Params[0]["obs-link-mbps"]), 10, 64)
			firstObserved.Store(obs)
		}
		cts0 := req.Params[0]["cts0"]
		n, err := strconv.Atoi(req.Params[0]["probe-bytes"])
		if err != nil {
			return err
		}
		if _, err := io.WriteString(out, fmt.Sprintf("PROBE cpu=24 io-depth=8 cts0=%s sts0=10 sts1=11 probe-bytes=%d wmem=4096 gentle-cpu-pct=25 gentle-bw-pct=25\n", cts0, n)); err != nil {
			return err
		}
		if n > 0 {
			if _, err := out.Write(make([]byte, n)); err != nil {
				return err
			}
		}
		_, err = io.WriteString(out, "OK\r\n")
		return err
	})
	defer srv.Close()

	client := NewClient(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	pr := startTransferProbeReporter(ctx, client, "txprobe", LoadStrategyFast, 1024, 700)
	defer pr.stop()
	defer cancel()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if probeCount.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if probeCount.Load() == 0 {
		t.Fatalf("expected at least one probe refresh")
	}
	gotTxferID, _ := firstTransferID.Load().(string)
	if gotTxferID != "txprobe" {
		t.Fatalf("unexpected transfer id: %q", gotTxferID)
	}
	if firstObserved.Load() != 700 {
		t.Fatalf("unexpected initial observed link mbps: %d", firstObserved.Load())
	}
	if got := pr.linkMbps.Load(); got <= 0 {
		t.Fatalf("expected probe reporter to retain link mbps, got %d", got)
	}
	if got := pr.lastProbeUnixS.Load(); got <= 0 {
		t.Fatalf("expected probe reporter to retain last probe timestamp, got %d", got)
	}
}

func TestFormatProbeRateSuffixUsesServerLimiter(t *testing.T) {
	now := time.Unix(200, 0)
	var probe probeReporter
	probe.limiterBps.Store(100 * 1024 * 1024)
	probe.linkMbps.Store(9000)
	probe.lastProbeUnixS.Store(now.Add(-2 * time.Second).Unix())

	got := formatProbeRateSuffix(now, 25*1024*1024, &probe)
	if got != " (25% of limit=100.00 MiB/s @  2s)" {
		t.Fatalf("unexpected limiter suffix: %q", got)
	}
}

func TestFormatProbeRateSuffixFallsBackToLinkBandwidth(t *testing.T) {
	now := time.Unix(300, 0)
	var probe probeReporter
	probe.linkMbps.Store(800)
	probe.lastProbeUnixS.Store(now.Add(-10 * time.Second).Unix())

	got := formatProbeRateSuffix(now, 50*1_000_000, &probe)
	if got != " (50% of link=95.37 MiB/s @ 10s)" {
		t.Fatalf("unexpected link suffix: %q", got)
	}
}

func TestFormatProbeRateSuffixClampsLinkFallbackTo100Pct(t *testing.T) {
	now := time.Unix(320, 0)
	var probe probeReporter
	probe.linkMbps.Store(800)
	probe.lastProbeUnixS.Store(now.Add(-3 * time.Second).Unix())

	got := formatProbeRateSuffix(now, 400*1_000_000, &probe)
	if got != " (100% of link=95.37 MiB/s @  3s)" {
		t.Fatalf("unexpected clamped link suffix: %q", got)
	}
}

func TestFormatStartBatchCause(t *testing.T) {
	const mib = int64(1 << 20)
	tests := []struct {
		name string
		plan BatchSizePlan
		want string
	}{
		{
			name: "window",
			plan: BatchSizePlan{
				BatchMaxBytes:  32 * mib,
				ConcBatchBytes: 32 * mib,
				FloorBytes:     16 * mib,
			},
			want: "window",
		},
		{
			name: "bw-probe",
			plan: BatchSizePlan{
				BatchMaxBytes:  4 * mib,
				ConcBatchBytes: 32 * mib,
				BwCeilBytes:    4 * mib,
				FloorBytes:     1 * mib,
			},
			want: "bw-probe",
		},
		{
			name: "bw-probe raised to socket size",
			plan: BatchSizePlan{
				BatchMaxBytes:  16 * mib,
				ConcBatchBytes: 32 * mib,
				BwCeilBytes:    4 * mib,
				FloorBytes:     16 * mib,
			},
			want: "bw-probe, raised to socket size",
		},
		{
			name: "floor",
			plan: BatchSizePlan{
				BatchMaxBytes:  16 * mib,
				ConcBatchBytes: 8 * mib,
				FloorBytes:     16 * mib,
			},
			want: "floor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatStartBatchCause(tt.plan); got != tt.want {
				t.Fatalf("formatStartBatchCause() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatStartBatchWindowLine(t *testing.T) {
	const mib = int64(1 << 20)
	got := formatStartBatchWindowLine(512*mib, BatchSizePlan{
		PerFileWorkers: 24,
		ConcBatchBytes: 32 * mib,
	})
	want := "    window: 512.00 MiB / 24 per-file-workers = 32.00 MiB"
	if got != want {
		t.Fatalf("formatStartBatchWindowLine() = %q, want %q", got, want)
	}
}

func TestFormatStartBatchProbeLine(t *testing.T) {
	const mib = int64(1 << 20)

	t.Run("active", func(t *testing.T) {
		got := formatStartBatchProbeLine(1001, 96, BatchSizePlan{
			ConcBatchBytes: 32 * mib,
			BwCeilBytes:    4 * mib,
		})
		want := "    bw-probe: 1001 MiB/s / 96 conc / 2 = 4.00 MiB"
		if got != want {
			t.Fatalf("formatStartBatchProbeLine() = %q, want %q", got, want)
		}
	})

	t.Run("inactive", func(t *testing.T) {
		got := formatStartBatchProbeLine(1001, 96, BatchSizePlan{
			ConcBatchBytes: 32 * mib,
			BwCeilBytes:    32 * mib,
		})
		if got != "" {
			t.Fatalf("expected inactive bw-probe line to be hidden, got %q", got)
		}
	})
}

func TestFixedWidthHumanDurationKeepsShortDurationsAligned(t *testing.T) {
	short := fixedWidthHumanDuration(2 * time.Second)
	longer := fixedWidthHumanDuration(10 * time.Second)
	minute := fixedWidthHumanDuration(62 * time.Second)

	if short != "  2s" {
		t.Fatalf("unexpected short duration: %q", short)
	}
	if longer != " 10s" {
		t.Fatalf("unexpected longer duration: %q", longer)
	}
	if minute != "1m2s" {
		t.Fatalf("unexpected minute duration: %q", minute)
	}
	if len(short) != len(longer) || len(longer) != len(minute) {
		t.Fatalf("expected fixed-width durations, got lens %d %d %d", len(short), len(longer), len(minute))
	}
}

func TestFixedWidthETAKeepsDurationsAligned(t *testing.T) {
	short := fixedWidthETA(57 * time.Second)
	minute := fixedWidthETA(74 * time.Second)

	if short != "  57s" {
		t.Fatalf("unexpected short eta: %q", short)
	}
	if minute != " 1.2m" {
		t.Fatalf("unexpected minute eta: %q", minute)
	}
	if len(short) != 5 || len(minute) != 5 {
		t.Fatalf("expected 5-char eta fields, got %d and %d", len(short), len(minute))
	}
}

func TestFixedWidthETANA(t *testing.T) {
	if got := fixedWidthETANA(); got != "  n/a" {
		t.Fatalf("unexpected n/a eta: %q", got)
	}
	if len(fixedWidthETANA()) != 5 {
		t.Fatalf("expected 5-char n/a eta field, got %d", len(fixedWidthETANA()))
	}
}

func TestCompactETAUsesFractionalUnitsEarly(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want string
	}{
		{in: 59 * time.Second, want: "59s"},
		{in: 74 * time.Second, want: "1.2m"},
		{in: 95 * time.Minute, want: "1.6h"},
		{in: 36 * time.Hour, want: "1.5d"},
		{in: 15 * 24 * time.Hour, want: "2.1w"},
	}
	for _, tt := range tests {
		if got := compactETA(tt.in); got != tt.want {
			t.Fatalf("compactETA(%s) = %q, want %q", tt.in, got, tt.want)
		}
		if len(tt.want) > 5 {
			t.Fatalf("test case %q exceeds 5 chars", tt.want)
		}
	}
}

func TestVerbosityFromFlags(t *testing.T) {
	tests := []struct {
		name     string
		progress bool
		verbose  bool
		want     int
	}{
		{name: "quiet", progress: false, verbose: false, want: 0},
		{name: "progress", progress: true, verbose: false, want: 1},
		{name: "verbose", progress: false, verbose: true, want: 2},
		{name: "verbose wins", progress: true, verbose: true, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := verbosityFromFlags(tt.progress, tt.verbose); got != tt.want {
				t.Fatalf("verbosityFromFlags(%v, %v) = %d, want %d", tt.progress, tt.verbose, got, tt.want)
			}
		})
	}
}

func TestPrintTransferErrors(t *testing.T) {
	buildErrors := func(n int) []error {
		errs := make([]error, 0, n)
		for i := 1; i <= n; i++ {
			errs = append(errs, fmt.Errorf("boom-%d", i))
		}
		return errs
	}

	t.Run("no errors", func(t *testing.T) {
		var buf bytes.Buffer
		printTransferErrors(&buf, "start", nil, 1)
		if got := buf.String(); got != "" {
			t.Fatalf("expected no output, got %q", got)
		}
	})

	t.Run("prints first five and summary when not verbose", func(t *testing.T) {
		var buf bytes.Buffer
		printTransferErrors(&buf, "start", buildErrors(7), 1)
		got := buf.String()
		for i := 1; i <= 5; i++ {
			want := fmt.Sprintf("start error: boom-%d\n", i)
			if !strings.Contains(got, want) {
				t.Fatalf("expected output to contain %q, got %q", want, got)
			}
		}
		if strings.Contains(got, "start error: boom-6\n") || strings.Contains(got, "start error: boom-7\n") {
			t.Fatalf("expected output to truncate after five errors, got %q", got)
		}
		if !strings.Contains(got, "start failed with 7 errors\n") {
			t.Fatalf("expected summary line, got %q", got)
		}
	})

	t.Run("prints all when verbose", func(t *testing.T) {
		var buf bytes.Buffer
		printTransferErrors(&buf, "sync", buildErrors(6), 2)
		got := buf.String()
		for i := 1; i <= 6; i++ {
			want := fmt.Sprintf("sync error: boom-%d\n", i)
			if !strings.Contains(got, want) {
				t.Fatalf("expected output to contain %q, got %q", want, got)
			}
		}
		if strings.Contains(got, "sync failed with 6 errors\n") {
			t.Fatalf("did not expect summary line in verbose mode, got %q", got)
		}
	})
}

func TestHumanBytesFixedWidthUsesPerValueUnits(t *testing.T) {
	zero := encoding.HumanBytesFixedWidth(0, fixedWidthProgressBytesWidth)
	mid := encoding.HumanBytesFixedWidth(492_340_000, fixedWidthProgressBytesWidth)
	done := encoding.HumanBytesFixedWidth(1_950_000_000, fixedWidthProgressBytesWidth)
	totalFormatted := encoding.HumanBytesFixedWidth(20_174_499_881, fixedWidthProgressBytesWidth)

	if zero != "       0 B" {
		t.Fatalf("unexpected zero progress bytes: %q", zero)
	}
	if mid != "469.53 MiB" || done != "  1.82 GiB" || totalFormatted != " 18.79 GiB" {
		t.Fatalf("unexpected fixed-width byte values: %q %q %q", mid, done, totalFormatted)
	}
}

func TestEffectiveModeLinkMbpsScalesGentleBandwidth(t *testing.T) {
	if got := effectiveModeLinkMbps(LoadStrategyGentle, 8400, 25); got != 2100 {
		t.Fatalf("expected gentle link mbps 2100, got %d", got)
	}
	if got := effectiveModeLinkMbps(LoadStrategyFast, 8400, 25); got != 8400 {
		t.Fatalf("expected fast link mbps 8400, got %d", got)
	}
}

func TestFormatProbeRateSuffixOmitsWhenNoProbeData(t *testing.T) {
	if got := formatProbeRateSuffix(time.Unix(400, 0), 10, &probeReporter{}); got != "" {
		t.Fatalf("expected empty suffix without probe data, got %q", got)
	}
}

func TestRunCLISyncNoOpSkipsPrompt(t *testing.T) {
	tmp := t.TempDir()
	targetDir := setupPinchState(t, tmp, "", "")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	destPath := filepath.Join(targetDir, "same.txt")
	if err := os.WriteFile(destPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write target file: %v", err)
	}
	info, err := os.Stat(destPath)
	if err != nil {
		t.Fatalf("stat target file: %v", err)
	}
	entry := buildTestManifestEntry(0, info.Size(), info.ModTime().UnixNano(), info.Mode(), "same.txt")
	manifestRaw := buildTestManifestRaw("txsyncnoop", []string{entry})
	if err := os.WriteFile(filepath.Join(tmp, ".pinch", "manifest.server"), []byte(manifestRaw), 0o644); err != nil {
		t.Fatalf("write manifest.server: %v", err)
	}
	withSyncPromptTestInput(t, "\n", true)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncnoop", []string{entry}, nil)
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync no-op: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "sync: converged, nothing to do") {
		t.Fatalf("expected converged output, got: %s", stdout.String())
	}
	if strings.Contains(stderr.String(), "proceed?") {
		t.Fatalf("did not expect prompt for no-op sync, got stderr=%s", stderr.String())
	}
}

func TestRunCLISyncDownloadPromptDefaultsYes(t *testing.T) {
	tmp := t.TempDir()
	targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncdownload", nil), "")
	withSyncPromptTestInput(t, "\n", true)

	payload := []byte("hello")
	entry := buildTestManifestEntry(0, int64(len(payload)), 100, 0o644, "new.txt")
	meta := &FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncdownload", []string{entry}, nil)
		case intftcp.VerbSEND:
			if got := req.Params[0]["txferid"]; got != "txsyncdownload" {
				return fmt.Errorf("unexpected transfer id: %q", got)
			}
			_, err := io.WriteString(out, buildCLIFrameWithMetadata(0, payload, 0, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", "--ack-every", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync download: expected 0, got %d stderr=%s", code, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "new.txt"))
	if err != nil {
		t.Fatalf("read synced file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected synced file: %q", string(got))
	}
	if !strings.Contains(stderr.String(), "proceed? [Y/n]: ") {
		t.Fatalf("expected [Y/n] prompt, got stderr=%s", stderr.String())
	}
}

func TestRunCLISyncDeletePromptDefaultsNo(t *testing.T) {
	tmp := t.TempDir()
	targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncdelete", nil), "")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	destPath := filepath.Join(targetDir, "old.txt")
	if err := os.WriteFile(destPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("write old file: %v", err)
	}
	withSyncPromptTestInput(t, "\n", true)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncdelete", nil, []uint64{0})
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync delete abort: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(destPath); err != nil {
		t.Fatalf("expected delete-only sync to abort before removing file: %v", err)
	}
	if !strings.Contains(stderr.String(), "proceed? [y/N]: ") {
		t.Fatalf("expected [y/N] prompt, got stderr=%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "aborted") {
		t.Fatalf("expected abort message, got stderr=%s", stderr.String())
	}
}

func TestRunCLISyncMixedPromptDefaultsNo(t *testing.T) {
	tmp := t.TempDir()
	targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncmixed", nil), "")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	oldPath := filepath.Join(targetDir, "old.txt")
	if err := os.WriteFile(oldPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("write old file: %v", err)
	}
	withSyncPromptTestInput(t, "\n", true)

	entry := buildTestManifestEntry(0, 5, 100, 0o644, "new.txt")
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncmixed", []string{entry}, []uint64{0})
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync mixed abort: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("expected mixed sync to abort before removing old file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "new.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected mixed sync to abort before downloading new file, err=%v", err)
	}
	if !strings.Contains(stderr.String(), "proceed? [y/N]: ") {
		t.Fatalf("expected [y/N] prompt, got stderr=%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "aborted") {
		t.Fatalf("expected abort message, got stderr=%s", stderr.String())
	}
}

func TestRunCLISyncPromptAcceptsExplicitYes(t *testing.T) {
	t.Run("download-default-yes", func(t *testing.T) {
		tmp := t.TempDir()
		targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncyesdownload", nil), "")
		withSyncPromptTestInput(t, "Y\n", true)

		payload := []byte("hello")
		entry := buildTestManifestEntry(0, int64(len(payload)), 100, 0o644, "new.txt")
		meta := &FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			switch req.Verb {
			case intftcp.VerbPROBE:
				return writeCLIProbeResponse(req, out)
			case intftcp.VerbSYNC:
				return writeSyncResponse(out, "txsyncyesdownload", []string{entry}, nil)
			case intftcp.VerbSEND:
				_, err := io.WriteString(out, buildCLIFrameWithMetadata(0, payload, 0, meta))
				return err
			case intftcp.VerbACK:
				_, err := io.WriteString(out, "OK\r\n")
				return err
			default:
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", "--ack-every", "1B", targetDir}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("sync explicit yes download: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if _, err := os.Stat(filepath.Join(targetDir, "new.txt")); err != nil {
			t.Fatalf("expected new file to download: %v", err)
		}
		if !strings.Contains(stderr.String(), "proceed? [Y/n]: ") {
			t.Fatalf("expected [Y/n] prompt, got stderr=%s", stderr.String())
		}
	})

	t.Run("delete-default-no", func(t *testing.T) {
		tmp := t.TempDir()
		targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncyesdelete", nil), "")
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			t.Fatalf("mkdir target: %v", err)
		}
		oldPath := filepath.Join(targetDir, "old.txt")
		if err := os.WriteFile(oldPath, []byte("old"), 0o644); err != nil {
			t.Fatalf("write old file: %v", err)
		}
		withSyncPromptTestInput(t, "y\n", true)

		syncCalls := 0
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			switch req.Verb {
			case intftcp.VerbPROBE:
				return writeCLIProbeResponse(req, out)
			case intftcp.VerbSYNC:
				syncCalls++
				if syncCalls == 1 {
					return writeSyncResponse(out, "txsyncyesdelete", nil, []uint64{0})
				}
				return writeSyncResponse(out, "txsyncyesdelete", nil, nil)
			default:
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", targetDir}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("sync explicit yes delete: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
			t.Fatalf("expected old file to be removed, err=%v", err)
		}
		if !strings.Contains(stderr.String(), "proceed? [y/N]: ") {
			t.Fatalf("expected [y/N] prompt, got stderr=%s", stderr.String())
		}
	})
}

func TestRunCLISyncNonTerminalSkipsPrompt(t *testing.T) {
	tmp := t.TempDir()
	targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncnonterm", nil), "")
	withSyncPromptTestInput(t, "", false)

	payload := []byte("hello")
	entry := buildTestManifestEntry(0, int64(len(payload)), 100, 0o644, "new.txt")
	meta := &FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbSYNC:
			return writeSyncResponse(out, "txsyncnonterm", []string{entry}, nil)
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrameWithMetadata(0, payload, 0, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runSyncCLI(srv.URL, []string{"--probe-size", "1B", "--ack-every", "1B", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sync non-terminal: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(targetDir, "new.txt")); err != nil {
		t.Fatalf("expected new file to download: %v", err)
	}
	if strings.Contains(stderr.String(), "proceed?") {
		t.Fatalf("did not expect prompt for non-terminal stdin, got stderr=%s", stderr.String())
	}
}

func TestRunCLISyncYesFlagBypassesPrompt(t *testing.T) {
	t.Run("delete-only", func(t *testing.T) {
		tmp := t.TempDir()
		targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncyesflagdelete", nil), "")
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			t.Fatalf("mkdir target: %v", err)
		}
		oldPath := filepath.Join(targetDir, "old.txt")
		if err := os.WriteFile(oldPath, []byte("old"), 0o644); err != nil {
			t.Fatalf("write old file: %v", err)
		}
		withSyncPromptTestInput(t, "\n", true)

		syncCalls := 0
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			switch req.Verb {
			case intftcp.VerbPROBE:
				return writeCLIProbeResponse(req, out)
			case intftcp.VerbSYNC:
				syncCalls++
				if syncCalls == 1 {
					return writeSyncResponse(out, "txsyncyesflagdelete", nil, []uint64{0})
				}
				return writeSyncResponse(out, "txsyncyesflagdelete", nil, nil)
			default:
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runSyncCLI(srv.URL, []string{"--yes", "--probe-size", "1B", targetDir}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("sync --yes delete-only: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
			t.Fatalf("expected old file to be removed, err=%v", err)
		}
		if strings.Contains(stderr.String(), "proceed?") {
			t.Fatalf("did not expect prompt with --yes, got stderr=%s", stderr.String())
		}
	})

	t.Run("mixed", func(t *testing.T) {
		tmp := t.TempDir()
		targetDir := setupPinchState(t, tmp, buildTestManifestRaw("txsyncyesflagmixed", nil), "")
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			t.Fatalf("mkdir target: %v", err)
		}
		oldPath := filepath.Join(targetDir, "old.txt")
		if err := os.WriteFile(oldPath, []byte("old"), 0o644); err != nil {
			t.Fatalf("write old file: %v", err)
		}
		withSyncPromptTestInput(t, "\n", true)

		payload := []byte("hello")
		entry := buildTestManifestEntry(0, int64(len(payload)), 100, 0o644, "new.txt")
		syncCalls := 0
		meta := &FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			switch req.Verb {
			case intftcp.VerbPROBE:
				return writeCLIProbeResponse(req, out)
			case intftcp.VerbSYNC:
				syncCalls++
				if syncCalls == 1 {
					return writeSyncResponse(out, "txsyncyesflagmixed", []string{entry}, []uint64{0})
				}
				return writeSyncResponse(out, "txsyncyesflagmixed", []string{entry}, nil)
			case intftcp.VerbSEND:
				_, err := io.WriteString(out, buildCLIFrameWithMetadata(0, payload, 0, meta))
				return err
			case intftcp.VerbACK:
				_, err := io.WriteString(out, "OK\r\n")
				return err
			default:
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runSyncCLI(srv.URL, []string{"--yes", "--probe-size", "1B", "--ack-every", "1B", targetDir}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("sync --yes mixed: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
			t.Fatalf("expected old file to be removed, err=%v", err)
		}
		got, err := os.ReadFile(filepath.Join(targetDir, "new.txt"))
		if err != nil {
			t.Fatalf("read new file: %v", err)
		}
		if string(got) != string(payload) {
			t.Fatalf("unexpected new file contents: %q", string(got))
		}
		if strings.Contains(stderr.String(), "proceed?") {
			t.Fatalf("did not expect prompt with --yes, got stderr=%s", stderr.String())
		}
	})
}

func TestRunCLICopyStartPath(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	payload := []byte("hello")
	manifestRaw := buildTestManifestRaw("txcopy-start", []string{
		buildTestManifestEntry(0, int64(len(payload)), 100, 0o644, "new.txt"),
	})
	meta := &FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if _, err := io.WriteString(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrameWithMetadata(0, payload, 0, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--progress=false", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy start path: expected 0, got %d stderr=%s", code, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "new.txt"))
	if err != nil {
		t.Fatalf("read copied file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected copied file: %q", got)
	}
	if _, err := os.Stat(filepath.Join(tmp, ".pinch")); !os.IsNotExist(err) {
		t.Fatalf("expected copy to remove state dir, stat err=%v", err)
	}
}

func TestRunCLICopySyncPath(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	payload := []byte("hello")
	entry := buildTestManifestEntry(0, int64(len(payload)), 100, 0o644, "new.txt")
	manifestRaw := buildTestManifestRaw("txcopy-sync", []string{entry})
	meta := &FileTrailerMetadata{Size: int64(len(payload)), MtimeNS: 100, Mode: "0644"}
	withSyncPromptTestInput(t, "", false)

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if _, err := io.WriteString(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSYNC:
			if got := req.Params[0]["directory"]; got != "/remote" {
				return fmt.Errorf("unexpected sync directory: %q", got)
			}
			return writeSyncResponse(out, "txcopy-sync", []string{entry}, nil)
		case intftcp.VerbSEND:
			_, err := io.WriteString(out, buildCLIFrameWithMetadata(0, payload, 0, meta))
			return err
		case intftcp.VerbACK:
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy sync path: expected 0, got %d stderr=%s", code, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "new.txt"))
	if err != nil {
		t.Fatalf("read synced file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected synced file: %q", got)
	}
	if _, err := os.Stat(filepath.Join(tmp, ".pinch")); !os.IsNotExist(err) {
		t.Fatalf("expected copy to remove state dir, stat err=%v", err)
	}
}

func TestRunCLICopySkipFetchVerifyMeta(t *testing.T) {
	tmp := t.TempDir()
	targetDir := filepath.Join(tmp, "dst")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	destPath := filepath.Join(targetDir, "same.txt")
	if err := os.WriteFile(destPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	if err := os.Chtimes(destPath, time.Unix(0, 100), time.Unix(0, 100)); err != nil {
		t.Fatalf("chtimes local file: %v", err)
	}
	info, err := os.Stat(destPath)
	if err != nil {
		t.Fatalf("stat local file: %v", err)
	}
	manifestRaw := buildTestManifestRaw("txcopy-verify", []string{
		buildTestManifestEntry(0, info.Size(), info.ModTime().UnixNano(), info.Mode(), "same.txt"),
	})

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbPROBE:
			return writeCLIProbeResponse(req, out)
		case intftcp.VerbTXFER:
			if _, err := io.WriteString(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		default:
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "copy", "--skip-fetch", "--verify-meta", "/remote", targetDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("copy skip-fetch verify-meta: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "copy-verify-meta: ok") {
		t.Fatalf("expected verify output, got stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(tmp, ".pinch")); err != nil {
		t.Fatalf("expected skip-fetch copy to preserve state dir, stat err=%v", err)
	}
}

func TestRunCLIStatus(t *testing.T) {
	t.Run("list-all", func(t *testing.T) {
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			if req.Verb != intftcp.VerbSTATUS {
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
			_, err := io.WriteString(out, "OK 1\r\n{\"transfer_id\":\"abc\",\"directory\":\"/r\",\"num_files\":10,\"total_size\":1000,\"done\":3,\"done_size\":200,\"percent_files\":30.0,\"percent_bytes\":20.0,\"download_status\":{\"started\":5,\"running\":2,\"done\":3,\"missing\":0}}\r\n")
			return err
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := RunCLI([]string{srv.URL, "status"}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("status list-all: expected 0, got %d stderr=%s", code, stderr.String())
		}
		output := stdout.String()
		if !strings.Contains(output, "[abc]") {
			t.Fatalf("expected transfer ID in output: %s", output)
		}
		if !strings.Contains(output, "source=[/r]") {
			t.Fatalf("expected source directory in output: %s", output)
		}
	})

	t.Run("poll-complete", func(t *testing.T) {
		srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
			if req.Verb != intftcp.VerbSTATUS {
				return fmt.Errorf("unexpected verb: %v", req.Verb)
			}
			_, err := io.WriteString(out, `OK {"transfer_id":"done1","directory":"/d","num_files":2,"total_size":500,"done":2,"done_size":500,"percent_files":100.0,"percent_bytes":100.0,"download_status":{"started":0,"running":0,"done":2,"missing":0}}`+"\r\n")
			return err
		})
		defer srv.Close()

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := RunCLI([]string{srv.URL, "status", "--tid", "done1"}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("status poll: expected 0, got %d stderr=%s", code, stderr.String())
		}
		if !strings.Contains(stdout.String(), "transfer complete:") {
			t.Fatalf("expected completion output: %s", stdout.String())
		}
	})
}

func TestRunCLIUsageErrors(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if code := RunCLI([]string{}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2, got %d", code)
	}
	if code := RunCLI([]string{"127.0.0.1:1", "bogus"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for unknown cmd, got %d", code)
	}
	// get requires exactly one REMOTE_PATH
	if code := RunCLI([]string{"127.0.0.1:1", "get"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for missing REMOTE_PATH on get, got %d", code)
	}
	// get requires REMOTE_PATH to be absolute
	if code := RunCLI([]string{"127.0.0.1:1", "get", "relative/path"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for relative REMOTE_PATH, got %d", code)
	}
	stderr.Reset()
	if code := RunCLI([]string{"127.0.0.1:1", "transfer", "--directory", "/tmp"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed transfer command, got %d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command: transfer") {
		t.Fatalf("expected unknown transfer command, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := RunCLI([]string{"127.0.0.1:1", "start", "--probe-size", "bad"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed start command, got %d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command: start") {
		t.Fatalf("expected unknown start command, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := RunCLI([]string{"127.0.0.1:1", "sync", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed sync command, got %d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command: sync") {
		t.Fatalf("expected unknown sync command, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected copy help exit 0, got %d", code)
	}
	copyHelp := stderr.String()
	if !strings.Contains(copyHelp, `--clean`) || !strings.Contains(copyHelp, `(default false)`) {
		t.Fatalf("expected bool default in copy help, got: %s", copyHelp)
	}
	if !strings.Contains(copyHelp, `--concurrency int`) || !strings.Contains(copyHelp, `(default 0)`) {
		t.Fatalf("expected int default in copy help, got: %s", copyHelp)
	}
	if !strings.Contains(copyHelp, `--progress-file-interval string`) || !strings.Contains(copyHelp, `(default "1s")`) {
		t.Fatalf("expected string default in copy help, got: %s", copyHelp)
	}
	lines := strings.Split(copyHelp, "\n")
	for _, line := range lines {
		if len(line) > 88 {
			t.Fatalf("expected copy help to wrap at 88 chars, got %d: %q", len(line), line)
		}
	}
	wrappedVerifySample := false
	for i, line := range lines {
		if !strings.Contains(line, "--verify-data-sample int") {
			continue
		}
		if i+1 >= len(lines) {
			break
		}
		next := lines[i+1]
		if len(next) > 0 && next[0] == ' ' && strings.Contains(next, "data verification") {
			wrappedVerifySample = true
		}
		break
	}
	if !wrappedVerifySample {
		t.Fatalf("expected wrapped help indentation for verify-data-sample, got: %s", copyHelp)
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--compress", "bogus", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid --compress, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid --compress: unsupported --compress value") {
		t.Fatalf("expected invalid --compress error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--comp", "lz4", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed --comp flag, got %d", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined: -comp") {
		t.Fatalf("expected removed --comp flag error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--per-file", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed --per-file flag, got %d", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined: -per-file") {
		t.Fatalf("expected removed --per-file flag error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--progress-path", "/tmp/pct", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed --progress-path flag, got %d", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined: -progress-path") {
		t.Fatalf("expected removed --progress-path flag error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--probe-bytes", "1B", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for removed --probe-bytes flag, got %d", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined: -probe-bytes") {
		t.Fatalf("expected removed --probe-bytes flag error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--probe-size", "bad", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid --probe-size, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid --probe-size") {
		t.Fatalf("expected invalid --probe-size error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--progress-file-interval", "bad", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid --progress-file-interval, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid --progress-file-interval") {
		t.Fatalf("expected invalid --progress-file-interval error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := runCopyCLI("127.0.0.1:1", []string{"--verify-data-sample", "5", "--skip-fetch", "/remote", "/tmp/dst"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid verify/skip-fetch combo, got %d", code)
	}
	if !strings.Contains(stderr.String(), "--verify-data-sample cannot be used with --skip-fetch or --skip-write") {
		t.Fatalf("expected invalid verify-data-sample error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := RunCLI([]string{"--tid", "tx", "get"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for missing server address, got %d", code)
	}
	if !strings.Contains(stderr.String(), "file-listener address") {
		t.Fatalf("expected explicit server address error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := RunCLI([]string{"help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected help exit 0, got %d", code)
	}
	if !strings.Contains(stderr.String(), "  copy") || strings.Contains(stderr.String(), "\n  transfer") || strings.Contains(stderr.String(), "\n  start") || strings.Contains(stderr.String(), "\n  sync") {
		t.Fatalf("expected top-level help to mention copy only, got: %s", stderr.String())
	}
}

func TestVerboseProgressReporterIncludesAckedBytes(t *testing.T) {
	var stderr bytes.Buffer
	reporter := newVerboseProgressReporter(&stderr)
	t0 := time.Unix(0, 0)

	reporter.ReportUpdate(DownloadProgressUpdate{
		FileID:      42,
		CopiedBytes: 20,
		TargetBytes: 100,
		UpdateTime:  t0,
	})
	reporter.ReportUpdate(DownloadProgressUpdate{
		FileID:      42,
		AckBytes:    10,
		TargetBytes: 100,
		UpdateTime:  t0.Add(500 * time.Millisecond),
	})
	reporter.ReportUpdate(DownloadProgressUpdate{
		FileID:      42,
		CopiedBytes: 40,
		TargetBytes: 100,
		UpdateTime:  t0.Add(1 * time.Second),
	})

	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 progress lines, got %d: %q", len(lines), stderr.String())
	}
	if got := lines[0]; !strings.Contains(got, "file progress[42]: 20% bytes=") || !strings.Contains(got, "20 B/") || !strings.Contains(got, "[       0 B]") {
		t.Fatalf("unexpected first progress line: %q", got)
	}
	if got := lines[1]; !strings.Contains(got, "file progress[42]: 40% bytes=") || !strings.Contains(got, "40 B/") || !strings.Contains(got, "[      10 B]") {
		t.Fatalf("unexpected second progress line: %q", got)
	}
	for _, line := range lines {
		if strings.Contains(line, "tid=") {
			t.Fatalf("progress line should not include tid: %q", line)
		}
	}
}

func TestVerboseProgressReporterTimeCadenceAndCompletion(t *testing.T) {
	var stderr bytes.Buffer
	reporter := newVerboseProgressReporter(&stderr)
	t0 := time.Unix(0, 0)

	reporter.ReportUpdate(DownloadProgressUpdate{
		FileID:      7,
		CopiedBytes: 5,
		TargetBytes: 100,
		UpdateTime:  t0,
	})
	reporter.ReportUpdate(DownloadProgressUpdate{
		FileID:      7,
		CopiedBytes: 10,
		TargetBytes: 100,
		UpdateTime:  t0.Add(2 * time.Second),
	})
	reporter.ReportUpdate(DownloadProgressUpdate{
		FileID:      7,
		CopiedBytes: 100,
		TargetBytes: 100,
		UpdateTime:  t0.Add(3 * time.Second),
	})

	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 progress lines, got %d: %q", len(lines), stderr.String())
	}
	if got := lines[0]; !strings.Contains(got, "file progress[7]: 10% ") {
		t.Fatalf("expected timed 10%% line, got %q", got)
	}
	if got := lines[1]; !strings.Contains(got, "file progress[7]: 100% ") {
		t.Fatalf("expected final 100%% line, got %q", got)
	}
}

func TestVerboseProgressReporterConcurrentUse(t *testing.T) {
	var stderr bytes.Buffer
	reporter := newVerboseProgressReporter(&stderr)
	start := time.Unix(0, 0)

	var wg sync.WaitGroup
	runFile := func(fileID uint64) {
		defer wg.Done()
		for pct := int64(20); pct <= 100; pct += 20 {
			copied := pct
			reporter.ReportUpdate(DownloadProgressUpdate{
				FileID:      fileID,
				CopiedBytes: copied,
				TargetBytes: 100,
				UpdateTime:  start.Add(time.Duration(copied) * time.Millisecond),
			})
			reporter.ReportUpdate(DownloadProgressUpdate{
				FileID:      fileID,
				AckBytes:    copied / 2,
				TargetBytes: 100,
				UpdateTime:  start.Add(time.Duration(copied)*time.Millisecond + 500*time.Microsecond),
			})
		}
	}

	wg.Add(2)
	go runFile(1)
	go runFile(2)
	wg.Wait()

	out := stderr.String()
	if !strings.Contains(out, "file progress[1]: ") {
		t.Fatalf("expected fd=1 progress lines, got %q", out)
	}
	if !strings.Contains(out, "file progress[2]: ") {
		t.Fatalf("expected fd=2 progress lines, got %q", out)
	}
}
