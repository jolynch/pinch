package filexfercli

import (
	"bufio"
	"bytes"
	"encoding/hex"
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
	. "github.com/jolynch/pinch/filexfer"
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
				serveFTCPConn(c, handler)
			}(conn)
		}
	}()
	return s
}

func serveFTCPConn(conn net.Conn, handler func(intftcp.Request, io.Writer) error) {
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
		if len(req.Params) > 0 {
			blob := strings.TrimSpace(req.Params[0]["blob"])
			if blob != "" {
				if recipient, parseErr := age.ParseX25519Recipient(blob); parseErr == nil {
					ew, encErr := age.Encrypt(conn, recipient)
					if encErr != nil {
						return
					}
					out = ew
					closeOut = ew.Close
				}
			}
		}
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
	xsum := xxh128HexCLI(body)
	header := fmt.Sprintf(
		"FX/1 %d offset=%d size=%d wsize=%d comp=none enc=none hash=xxh128:%s ts=1000\n",
		fileID,
		offset,
		len(body),
		len(body),
		xsum,
	)
	trailerPrefix := fmt.Sprintf("FXT/1 %d status=ok ts=1001 next=0 file-hash=xxh128:%s", fileID, xsum)
	h := xxh3.New()
	_, _ = h.Write([]byte(header))
	_, _ = h.Write(body)
	_, _ = h.Write([]byte(trailerPrefix))
	return fmt.Sprintf("%s%s%s hash=xxh64:%016x\n", header, string(body), trailerPrefix, h.Sum64())
}

func TestRunCLITransferAndGet(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txcli 7:/remote",
		"0 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	fileBody := []byte("hello")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbTXFER:
			if got := req.Params[0]["directory"]; got != "/remote" {
				return fmt.Errorf("unexpected directory: %q", got)
			}
			if _, err := io.WriteString(out, manifestRaw); err != nil {
				return err
			}
			_, err := io.WriteString(out, "OK\r\n")
			return err
		case intftcp.VerbSEND:
			if got := req.Params[0]["txferid"]; got != "txcli" {
				return fmt.Errorf("unexpected transfer id: %q", got)
			}
			_, err := io.WriteString(out, buildCLIFrame(0, fileBody, 0))
			return err
		case intftcp.VerbACK:
			ack := req.Params[0]["ack-token"]
			expectedAck := "5@1001@xxh128:" + xxh128HexCLI(fileBody)
			if ack != expectedAck {
				return fmt.Errorf("expected ack-token=%s, got %q", expectedAck, ack)
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
	manifestPath := filepath.Join(tmp, "txcli.fm1")
	code := RunCLI([]string{srv.URL, "transfer", "-s", "/remote", "-o", manifestPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("transfer: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("manifest not written: %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	outRoot := filepath.Join(tmp, "out")
	code = RunCLI([]string{srv.URL, "get", "--tid", "txcli", "--fd", "0", "--manifest", manifestPath, "--out-root", outRoot, "-A", "1KiB"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("get: expected 0, got %d stderr=%s", code, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(outRoot, "a.txt"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("unexpected output: %q", string(got))
	}
}

func TestRunCLIGetResumesFromProgressOffset(t *testing.T) {
	tmp := t.TempDir()
	manifestPath := filepath.Join(tmp, "txresume.fm1")
	manifestRaw := strings.Join([]string{
		"FM/1 txresume 7:/remote",
		"0 10 0:100 0644 0:5:a.txt",
		"",
	}, "\n")
	if err := os.WriteFile(manifestPath, []byte(manifestRaw), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath+".progress", []byte("0 5 0\n"), 0o644); err != nil {
		t.Fatalf("write progress: %v", err)
	}

	outRoot := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outRoot, 0o755); err != nil {
		t.Fatalf("mkdir out root: %v", err)
	}
	destPath := filepath.Join(outRoot, "a.txt")
	if err := os.WriteFile(destPath, []byte("helloSTALETAIL"), 0o644); err != nil {
		t.Fatalf("write stale destination: %v", err)
	}
	partB := []byte("world")

	var sawSend bool
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
			_, err := io.WriteString(out, buildCLIFrame(0, partB, 5))
			return err
		case intftcp.VerbACK:
			expectedAck := "10@1001@xxh128:" + xxh128HexCLI(partB)
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

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "get", "--tid", "txresume", "--fd", "0", "--manifest", manifestPath, "--out-root", outRoot, "-A", "1KiB"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("get resume: expected 0, got %d stderr=%s", code, stderr.String())
	}
	if !sawSend {
		t.Fatalf("expected SEND request")
	}
	got, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("read resumed output: %v", err)
	}
	if string(got) != "helloworld" {
		t.Fatalf("unexpected resumed output: %q", got)
	}
}

func TestRunCLITransferWithEncryptAge(t *testing.T) {
	tmp := t.TempDir()
	manifestRaw := strings.Join([]string{
		"FM/1 txenccli 7:/remote",
		"0 5 0:100 0644 0:5:a.txt",
		"",
	}, "\n")

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbTXFER {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		if _, err := io.WriteString(out, manifestRaw); err != nil {
			return err
		}
		_, err := io.WriteString(out, "OK\r\n")
		return err
	})
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	manifestPath := filepath.Join(tmp, "txenccli.fm1")
	code := RunCLI([]string{srv.URL, "transfer", "-s", "/remote", "--encrypt", "age", "-o", manifestPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("transfer: expected 0, got %d stderr=%s", code, stderr.String())
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if string(raw) != manifestRaw {
		t.Fatalf("unexpected decrypted manifest: %q", string(raw))
	}
}

func TestRunCLIStartDownloadsAll(t *testing.T) {
	tmp := t.TempDir()
	manifestPath := filepath.Join(tmp, "txstart.fm1")
	manifestRaw := strings.Join([]string{
		"FM/1 txstart 7:/remote",
		"0 5 0:100 0644 0:5:a.txt",
		"1 4 0:101 0644 0:5:b.txt",
		"",
	}, "\n")
	if err := os.WriteFile(manifestPath, []byte(manifestRaw), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		switch req.Verb {
		case intftcp.VerbSEND:
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

	outRoot := filepath.Join(tmp, "out")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunCLI([]string{srv.URL, "start", "--tid", "txstart", "--manifest", manifestPath, "--out-root", outRoot, "--concurrency", "2", "--ack-every", "1KiB"}, &stdout, &stderr)
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
	srv := newFTCPTestServer(t, func(req intftcp.Request, out io.Writer) error {
		if req.Verb != intftcp.VerbSTATUS {
			return fmt.Errorf("unexpected verb: %v", req.Verb)
		}
		_, err := io.WriteString(out, `OK {"transfer_id":"abc","directory":"/r","num_files":10,"total_size":1000,"done":3,"done_size":200,"percent_files":30,"percent_bytes":20,"download_status":{"started":5,"running":2,"done":3,"missing":0}}`+"\r\n")
		return err
	})
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
	if code := RunCLI([]string{"127.0.0.1:1", "bogus"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for unknown cmd, got %d", code)
	}
	if code := RunCLI([]string{"127.0.0.1:1", "get", "--tid", "t"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for missing --fd, got %d", code)
	}
	if code := RunCLI([]string{"127.0.0.1:1", "get", "--fd", "0"}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected runtime error when get has no --tid and no --manifest, got %d", code)
	}
	if code := RunCLI([]string{"127.0.0.1:1", "get", "--tid", "t", "--fd", "0", "--ack-every", "0"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid --ack-every, got %d", code)
	}
	stderr.Reset()
	if code := RunCLI([]string{"127.0.0.1:1", "get", "--tid", "t", "--fd", "0", "--ack-every", "bad"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for invalid --ack-every size, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid --ack-every") {
		t.Fatalf("expected invalid --ack-every message, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := RunCLI([]string{"127.0.0.1:1", "transfer", "--directory", "/tmp"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for legacy --directory flag, got %d", code)
	}
	stderr.Reset()
	if code := RunCLI([]string{"127.0.0.1:1", "transfer", "-s", "/tmp", "--encrypt", "aes"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for unsupported --encrypt mode, got %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid --encrypt") {
		t.Fatalf("expected invalid --encrypt error, got: %s", stderr.String())
	}
	stderr.Reset()
	if code := RunCLI([]string{"--tid", "tx", "get"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected usage exit 2 for missing server address, got %d", code)
	}
	if !strings.Contains(stderr.String(), "file-listener address") {
		t.Fatalf("expected explicit server address error, got: %s", stderr.String())
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
	if got := lines[0]; !strings.Contains(got, "progress: fd=42 20% bytes=20 B/100 B [0 B]") {
		t.Fatalf("unexpected first progress line: %q", got)
	}
	if got := lines[1]; !strings.Contains(got, "progress: fd=42 40% bytes=40 B/100 B [10 B]") {
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
	if got := lines[0]; !strings.Contains(got, "progress: fd=7 10% ") {
		t.Fatalf("expected timed 10%% line, got %q", got)
	}
	if got := lines[1]; !strings.Contains(got, "progress: fd=7 100% ") {
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
	if !strings.Contains(out, "progress: fd=1 ") {
		t.Fatalf("expected fd=1 progress lines, got %q", out)
	}
	if !strings.Contains(out, "progress: fd=2 ") {
		t.Fatalf("expected fd=2 progress lines, got %q", out)
	}
}
