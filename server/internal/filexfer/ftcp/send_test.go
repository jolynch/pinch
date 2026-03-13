package ftcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	intencoding "github.com/jolynch/pinch/internal/filexfer/encoding"
	"github.com/zeebo/xxh3"
)

type sendTestDeps struct {
	filePath       string
	windowHashEnd  int64
	windowHash     string
	setStateCalls  int
	setWindowCalls int
}

func (d *sendTestDeps) NewTransfer(string, int, int64) (Transfer, error) {
	return Transfer{}, errors.New("not implemented")
}

func (d *sendTestDeps) DeleteTransfer(string) bool { return false }

func (d *sendTestDeps) RegisterTransferFileState(string, <-chan TransferFileStateUpdate, uint8) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (d *sendTestDeps) ClipTransfer(string) bool { return false }

func (d *sendTestDeps) GetTransfer(string) (Transfer, bool) { return Transfer{}, false }

func (d *sendTestDeps) SetTransferHints(string, string, int64, int) bool { return true }

func (d *sendTestDeps) GetFile(txferID string, fileID uint64, fullPathRaw string) (*os.File, FileRef, error) {
	fd, err := os.Open(d.filePath)
	if err != nil {
		return nil, FileRef{}, err
	}
	info, err := fd.Stat()
	if err != nil {
		_ = fd.Close()
		return nil, FileRef{}, err
	}
	return fd, FileRef{
		TransferID: txferID,
		FileID:     fileID,
		Path:       d.filePath,
		Directory:  filepath.Dir(d.filePath),
		FileSize:   info.Size(),
	}, nil
}

func (d *sendTestDeps) GetFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error) {
	info, err := os.Stat(d.filePath)
	if err != nil {
		return FileRef{}, err
	}
	return FileRef{
		TransferID: txferID,
		FileID:     fileID,
		Path:       d.filePath,
		Directory:  filepath.Dir(d.filePath),
		FileSize:   info.Size(),
	}, nil
}

func (d *sendTestDeps) SetTransferFileState(string, uint64, uint8) bool {
	d.setStateCalls++
	return true
}

func (d *sendTestDeps) SetTransferFileWindowHash(_ string, _ uint64, endBytes int64, hashToken string) bool {
	d.setWindowCalls++
	d.windowHashEnd = endBytes
	d.windowHash = hashToken
	return true
}

func (d *sendTestDeps) VerifyTransferFileWindowHash(string, uint64, int64, string) bool { return false }

func (d *sendTestDeps) AcknowledgeTransferFile(string, uint64, int64) bool { return false }

func TestParseSENDRequestCompDefaultsAndModes(t *testing.T) {
	req, err := ParseRequest([]byte(`SEND tx1 fd=1 "/tmp/a.txt"`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err := parseSENDRequest(req)
	if err != nil {
		t.Fatalf("parseSENDRequest failed: %v", err)
	}
	if parsed.Items[0].Comp != "adapt" {
		t.Fatalf("expected default comp adapt, got %q", parsed.Items[0].Comp)
	}

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "none", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=none`, want: "none"},
		{name: "identity", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=identity`, want: "none"},
		{name: "lz4", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=lz4`, want: intencoding.EncodingLz4},
		{name: "zstd", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=zstd`, want: intencoding.EncodingZstd},
		{name: "adapt", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=adapt`, want: "adapt"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := ParseRequest([]byte(tc.raw))
			if err != nil {
				t.Fatalf("ParseRequest failed: %v", err)
			}
			parsed, err := parseSENDRequest(req)
			if err != nil {
				t.Fatalf("parseSENDRequest failed: %v", err)
			}
			if got := parsed.Items[0].Comp; got != tc.want {
				t.Fatalf("expected comp %q, got %q", tc.want, got)
			}
		})
	}

	req, err = ParseRequest([]byte(`SEND tx1 fd=1 "/tmp/a.txt" comp=snappy`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	_, err = parseSENDRequest(req)
	if err == nil {
		t.Fatalf("expected unsupported comp error")
	}
	var pe protocolErr
	if !errors.As(err, &pe) || pe.code != "UNSUPPORTED_COMP" {
		t.Fatalf("expected UNSUPPORTED_COMP, got %v", err)
	}
}

func TestParseSENDRequestModeDefaultsAndValidation(t *testing.T) {
	req, err := ParseRequest([]byte(`SEND tx1 fd=1 "/tmp/a.txt"`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err := parseSENDRequest(req)
	if err != nil {
		t.Fatalf("parseSENDRequest failed: %v", err)
	}
	if got := parsed.Items[0].Mode; got != loadStrategyFast {
		t.Fatalf("expected default mode %q, got %q", loadStrategyFast, got)
	}

	req, err = ParseRequest([]byte(`SEND tx1 fd=1 "/tmp/a.txt" mode=gentle`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err = parseSENDRequest(req)
	if err != nil {
		t.Fatalf("parseSENDRequest failed: %v", err)
	}
	if got := parsed.Items[0].Mode; got != loadStrategyGentle {
		t.Fatalf("expected mode %q, got %q", loadStrategyGentle, got)
	}

	req, err = ParseRequest([]byte(`SEND tx1 fd=1 "/tmp/a.txt" mode=slow`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	_, err = parseSENDRequest(req)
	if err == nil {
		t.Fatalf("expected mode validation error")
	}
}

func TestStreamSendItemRoundTripCompressionModes(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefghijklmnopqrstuvwxyz012345"), 8192)

	for _, comp := range []string{"none", intencoding.EncodingLz4, intencoding.EncodingZstd} {
		t.Run(comp, func(t *testing.T) {
			tmp := writeTempSendFile(t, data)
			deps := &sendTestDeps{filePath: tmp}
			var out bytes.Buffer

			err := streamSendItem(&out, deps, "tx1", sendItem{FileID: 7, Offset: 0, Size: 0, Comp: comp, Path: tmp})
			if err != nil {
				t.Fatalf("streamSendItem failed: %v", err)
			}

			frames, err := decodeFrameStream(out.Bytes())
			if err != nil {
				t.Fatalf("decodeFrameStream failed: %v", err)
			}
			if len(frames) != 1 {
				t.Fatalf("expected one frame, got %d", len(frames))
			}
			if frames[0].Header.Comp != comp {
				t.Fatalf("expected comp %q, got %q", comp, frames[0].Header.Comp)
			}
			if !bytes.Equal(frames[0].Logical, data) {
				t.Fatalf("decoded logical payload mismatch")
			}

			expectedHash := intencoding.FormatXXH128HashToken(xxh3.Hash128(data))
			if deps.windowHash != expectedHash {
				t.Fatalf("unexpected stored window hash: got=%q want=%q", deps.windowHash, expectedHash)
			}
			if deps.windowHashEnd != int64(len(data)) {
				t.Fatalf("unexpected window hash end: got=%d want=%d", deps.windowHashEnd, len(data))
			}
			if deps.setWindowCalls != 1 {
				t.Fatalf("expected one window hash set call, got %d", deps.setWindowCalls)
			}
		})
	}
}

func TestStreamSendItemAdaptiveUpgradesFromNone(t *testing.T) {
	size := (2 * defaultFileFrameLogicalSize) + 1
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}
	tmp := writeTempSendFile(t, data)
	deps := &sendTestDeps{filePath: tmp}

	var rawOut bytes.Buffer
	slowOut := delayedWriter{w: &rawOut, delay: 10 * time.Millisecond}
	err := streamSendItem(&slowOut, deps, "tx-adapt", sendItem{FileID: 9, Offset: 0, Size: 0, Comp: "adapt", Path: tmp})
	if err != nil {
		t.Fatalf("streamSendItem failed: %v", err)
	}

	comps, err := frameComps(rawOut.Bytes())
	if err != nil {
		t.Fatalf("frameComps failed: %v", err)
	}
	if len(comps) < 3 {
		t.Fatalf("expected >=3 frames for adaptive test, got %d", len(comps))
	}
	if comps[0] != "none" {
		t.Fatalf("expected first frame to start at none, got %q", comps[0])
	}
	sawCompressed := false
	for _, comp := range comps[1:] {
		if comp == intencoding.EncodingLz4 || comp == intencoding.EncodingZstd {
			sawCompressed = true
			break
		}
	}
	if !sawCompressed {
		t.Fatalf("expected adaptive mode to upgrade to a compressed frame, comps=%v", comps)
	}
}

type decodedFrame struct {
	Header  intencoding.FileFrameMeta
	Logical []byte
	Trailer intencoding.FrameTrailer
}

func decodeFrameStream(raw []byte) ([]decodedFrame, error) {
	br := bufio.NewReader(bytes.NewReader(raw))
	frames := make([]decodedFrame, 0, 4)
	for {
		headerLine, err := br.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}
		headerTrimmed := strings.TrimRight(headerLine, "\r\n")
		header, err := intencoding.ParseFXHeader(headerTrimmed)
		if err != nil {
			return nil, fmt.Errorf("parse header: %w", err)
		}
		if header.WireSize < 0 {
			return nil, errors.New("negative wire size")
		}
		payload := make([]byte, header.WireSize)
		if _, err := io.ReadFull(br, payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
		decodedReader, err := intencoding.DecodePayloadReaderByComp(bytes.NewReader(payload), header.Comp)
		if err != nil {
			return nil, fmt.Errorf("decode payload: %w", err)
		}
		logical, readErr := io.ReadAll(decodedReader)
		closeErr := decodedReader.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read decoded payload: %w", readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close decoded payload: %w", closeErr)
		}

		trailerLine, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read trailer: %w", err)
		}
		trailer, err := intencoding.ParseFXTrailer(strings.TrimRight(trailerLine, "\r\n"))
		if err != nil {
			return nil, fmt.Errorf("parse trailer: %w", err)
		}
		frames = append(frames, decodedFrame{Header: header, Logical: logical, Trailer: trailer})
	}
	return frames, nil
}

func frameComps(raw []byte) ([]string, error) {
	br := bufio.NewReader(bytes.NewReader(raw))
	comps := make([]string, 0, 8)
	for {
		headerLine, err := br.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		header, err := intencoding.ParseFXHeader(strings.TrimRight(headerLine, "\r\n"))
		if err != nil {
			return nil, err
		}
		comps = append(comps, header.Comp)
		if _, err := io.CopyN(io.Discard, br, header.WireSize); err != nil {
			return nil, err
		}
		if _, err := br.ReadString('\n'); err != nil {
			return nil, err
		}
	}
	return comps, nil
}

func writeTempSendFile(t *testing.T, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

type delayedWriter struct {
	w     io.Writer
	delay time.Duration
}

func (d *delayedWriter) Write(p []byte) (int, error) {
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	return d.w.Write(p)
}

func TestHandleSENDBasic(t *testing.T) {
	data := []byte("hello send")
	tmp := writeTempSendFile(t, data)
	deps := &sendTestDeps{filePath: tmp}
	payload := []byte(`SEND tx1 fd=1 ` + strconv.Quote(tmp))
	req, err := ParseRequest(payload)
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	var out bytes.Buffer
	if err := handleSEND(context.Background(), req, &out, deps); err != nil {
		t.Fatalf("handleSEND failed: %v", err)
	}
	frames, err := decodeFrameStream(out.Bytes())
	if err != nil {
		t.Fatalf("decodeFrameStream failed: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected one frame, got %d", len(frames))
	}
	if !bytes.Equal(frames[0].Logical, data) {
		t.Fatalf("unexpected logical bytes")
	}
}
