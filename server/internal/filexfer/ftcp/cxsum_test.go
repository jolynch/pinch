package ftcp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jolynch/pinch/internal/filexfer/encoding"
	"github.com/zeebo/xxh3"
)

func TestHandleCXSUMMultipleRanges(t *testing.T) {
	tmp := t.TempDir()
	filePath := filepath.Join(tmp, "cxsum.txt")
	if err := os.WriteFile(filePath, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	deps := &sendTestDeps{filePath: filePath}
	req, err := ParseRequest([]byte(`CXSUM tx1 fd=1 "/tmp/a.txt" offset=0 size=5 algo=xxh128 fd=1 "/tmp/a.txt" offset=6 size=5 algo=xxh64`))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}

	var out bytes.Buffer
	if err := handleCXSUM(context.Background(), req, &out, deps); err != nil {
		t.Fatalf("handleCXSUM err: %v", err)
	}

	raw := out.String()
	if got := strings.Count(raw, "FX/1 "); got != 2 {
		t.Fatalf("expected 2 frames, got %d output=%q", got, raw)
	}
	if !strings.Contains(raw, "offset=0 size=5 wsize=0") {
		t.Fatalf("missing first checksum range: %q", raw)
	}
	if !strings.Contains(raw, "offset=6 size=5 wsize=0") {
		t.Fatalf("missing second checksum range: %q", raw)
	}
	if !strings.Contains(raw, "file-hash="+encoding.FormatXXH128HashToken(xxh3.Hash128([]byte("hello")))) {
		t.Fatalf("missing xxh128 checksum token: %q", raw)
	}
	if !strings.Contains(raw, "file-hash="+encoding.FormatXXH64HashToken(xxh3.Hash([]byte("world")))) {
		t.Fatalf("missing xxh64 checksum token: %q", raw)
	}
	if got := strings.Count(raw, "next=0"); got != 2 {
		t.Fatalf("expected terminal trailers for each range, got %d output=%q", got, raw)
	}
}
