package ftcp

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/pinch/internal/filexfer/encoding"
)

type syncTestDeps struct {
	txferTestDeps
}

func TestParseSYNCRequest(t *testing.T) {
	good := fmt.Sprintf(`SYNC %q mode=fast link-mbps=900 concurrency=12`, "/tmp/test")
	req, err := ParseRequest([]byte(good))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err := parseSYNCRequest(req)
	if err != nil {
		t.Fatalf("parseSYNCRequest failed: %v", err)
	}
	if parsed.Directory != "/tmp/test" || parsed.Mode != "fast" || parsed.LinkMbps != 900 || parsed.Concurrency != 12 {
		t.Fatalf("unexpected parsed request: %+v", parsed)
	}

	bad := []string{
		`SYNC "/tmp" link-mbps=900 concurrency=12`,
		`SYNC "/tmp" mode=fast concurrency=12`,
		`SYNC "/tmp" mode=fast link-mbps=900`,
		`SYNC "/tmp" mode=slow link-mbps=900 concurrency=12`,
		`SYNC "/tmp" mode=fast link-mbps=-1 concurrency=12`,
		`SYNC "/tmp" mode=fast link-mbps=900 concurrency=0`,
	}
	for _, raw := range bad {
		t.Run(raw, func(t *testing.T) {
			req, err := ParseRequest([]byte(raw))
			if err != nil {
				t.Fatalf("ParseRequest failed: %v", err)
			}
			if _, err := parseSYNCRequest(req); err == nil {
				t.Fatalf("expected parseSYNCRequest to fail for %q", raw)
			}
		})
	}
}

func TestHandleSYNCEmptyOldManifest(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	writeTestFile(t, root, "b.txt", "world")

	newManifest, rmPaths := runSYNCTest(t, root, "")
	if len(rmPaths) != 0 {
		t.Fatalf("expected no removals, got %v", rmPaths)
	}
	if len(newManifest) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(newManifest))
	}
	paths := manifestPaths(newManifest)
	if paths[0] != "a.txt" || paths[1] != "b.txt" {
		t.Fatalf("unexpected paths: %v", paths)
	}
}

func TestHandleSYNCNoChanges(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	writeTestFile(t, root, "sub/b.txt", "world")

	// Get initial manifest via TXFER.
	initialManifest := runTXFERTest(t, root)

	// SYNC with that manifest — nothing should be removed.
	newManifest, rmPaths := runSYNCTest(t, root, initialManifest)
	if len(rmPaths) != 0 {
		t.Fatalf("expected no removals, got %v", rmPaths)
	}
	if len(newManifest) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(newManifest))
	}
}

func TestHandleSYNCAllRemoved(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt", "hello")
	writeTestFile(t, root, "b.txt", "world")

	initialManifest := runTXFERTest(t, root)

	// Remove all files.
	os.Remove(filepath.Join(root, "a.txt"))
	os.Remove(filepath.Join(root, "b.txt"))

	newManifest, rmPaths := runSYNCTest(t, root, initialManifest)
	if len(newManifest) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(newManifest))
	}
	sort.Strings(rmPaths)
	if len(rmPaths) != 2 || rmPaths[0] != "a.txt" || rmPaths[1] != "b.txt" {
		t.Fatalf("unexpected removals: %v", rmPaths)
	}
}

func TestHandleSYNCMixedDelta(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "keep.txt", "unchanged")
	writeTestFile(t, root, "stale.txt", "old content")
	writeTestFile(t, root, "remove.txt", "will be removed")

	initialManifest := runTXFERTest(t, root)

	// Modify stale.txt (change content and mtime).
	time.Sleep(10 * time.Millisecond)
	writeTestFile(t, root, "stale.txt", "new content!!!")

	// Remove remove.txt.
	os.Remove(filepath.Join(root, "remove.txt"))

	// Add new.txt.
	writeTestFile(t, root, "new.txt", "brand new")

	newManifest, rmPaths := runSYNCTest(t, root, initialManifest)

	// Should have keep.txt, new.txt, stale.txt in manifest.
	if len(newManifest) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(newManifest))
	}
	paths := manifestPaths(newManifest)
	// Walk order is alphabetical.
	if paths[0] != "keep.txt" || paths[1] != "new.txt" || paths[2] != "stale.txt" {
		t.Fatalf("unexpected paths: %v", paths)
	}

	// remove.txt should be in removals.
	if len(rmPaths) != 1 || rmPaths[0] != "remove.txt" {
		t.Fatalf("unexpected removals: %v", rmPaths)
	}
}

// --- Test helpers ---

func writeTestFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// runTXFERTest runs TXFER and returns the raw FM/1 manifest output.
func runTXFERTest(t *testing.T, root string) string {
	t.Helper()
	reqRaw := fmt.Sprintf(`TXFER %q mode=fast link-mbps=1000 concurrency=8`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	deps := &syncTestDeps{}
	var out bytes.Buffer
	if err := handleTXFER(context.Background(), req, &out, deps); err != nil {
		t.Fatalf("handleTXFER: %v", err)
	}
	return out.String()
}

// runSYNCTest runs SYNC with the given old manifest body and returns parsed entries + RM paths.
// RM fileIDs in the server response are resolved to paths using oldManifest.
func runSYNCTest(t *testing.T, root string, oldManifest string) ([]encoding.ManifestEntry, []string) {
	t.Helper()

	// Build ID→path index from old manifest for RM resolution.
	oldByID := buildOldByID(oldManifest)

	// Build SYNC request.
	reqRaw := fmt.Sprintf(`SYNC %q mode=fast link-mbps=1000 concurrency=8`, root)
	req, err := ParseRequest([]byte(reqRaw))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}

	// Create input: old manifest body + blank line.
	var input bytes.Buffer
	input.WriteString(oldManifest)
	if oldManifest != "" && !strings.HasSuffix(oldManifest, "\n") {
		input.WriteByte('\n')
	}
	input.WriteByte('\n') // blank line terminator

	deps := &syncTestDeps{}
	var out bytes.Buffer
	if err := handleSYNCWithInput(context.Background(), req, &input, &out, deps, nil); err != nil {
		t.Fatalf("handleSYNCWithInput: %v", err)
	}

	return parseSYNCResponse(t, out.String(), oldByID)
}

// buildOldByID parses a raw FM/1 manifest and returns a fileID→path map.
func buildOldByID(rawManifest string) map[uint64]string {
	entries, _ := parseSYNCResponseEntries(rawManifest, nil)
	m := make(map[uint64]string, len(entries))
	for _, e := range entries {
		m[e.ID] = e.Path
	}
	return m
}

// parseSYNCResponse parses the SYNC output into manifest entries and RM paths.
// oldByID maps old manifest fileIDs to paths for RM resolution.
func parseSYNCResponse(t *testing.T, raw string, oldByID map[uint64]string) ([]encoding.ManifestEntry, []string) {
	t.Helper()
	entries, rmIDs := parseSYNCResponseEntries(raw, nil)
	rmPaths := make([]string, 0, len(rmIDs))
	for _, id := range rmIDs {
		path, ok := oldByID[id]
		if !ok {
			t.Fatalf("RM fileID %d not in old manifest", id)
		}
		rmPaths = append(rmPaths, path)
	}
	return entries, rmPaths
}

func manifestPaths(entries []encoding.ManifestEntry) []string {
	paths := make([]string, len(entries))
	for i, e := range entries {
		paths[i] = e.Path
	}
	sort.Strings(paths)
	return paths
}

// --- Fuzz test ---

func FuzzSync(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{0xFF, 0x01, 0x80, 0x42, 0x10, 0x99, 0xAB, 0xCD, 0xEF, 0x12})

	f.Fuzz(func(t *testing.T, seed []byte) {
		rng := deterministicRNG(seed)
		tmpDir := t.TempDir()

		// Round 1: Create initial files and get manifest via TXFER.
		numFiles := rng.Intn(101) // 0-100
		files := fuzzCreateRandomFiles(t, tmpDir, numFiles, rng)

		oldManifest := runTXFERTest(t, tmpDir)
		if oldManifest == "" && numFiles > 0 {
			t.Fatal("expected non-empty manifest")
		}

		// Round 2: Modify filesystem and run SYNC.
		fuzzApplyRandomModifications(t, tmpDir, &files, rng)

		newEntries, rmPaths := runSYNCTest(t, tmpDir, oldManifest)
		fuzzVerifySyncResult(t, tmpDir, oldManifest, newEntries, rmPaths)

		// Round 3: Chain — use SYNC result as old manifest, modify again, SYNC again.
		if len(newEntries) == 0 {
			return
		}
		chainManifest := fuzzBuildManifestFromEntries(t, tmpDir, newEntries)
		fuzzApplyRandomModifications(t, tmpDir, &files, rng)
		newEntries2, rmPaths2 := runSYNCTest(t, tmpDir, chainManifest)
		fuzzVerifySyncResult(t, tmpDir, chainManifest, newEntries2, rmPaths2)
	})
}

func deterministicRNG(seed []byte) *rand.Rand {
	var s int64
	if len(seed) >= 8 {
		s = int64(binary.LittleEndian.Uint64(seed[:8]))
	} else {
		for i, b := range seed {
			s |= int64(b) << (uint(i) * 8)
		}
	}
	return rand.New(rand.NewSource(s))
}

func fuzzCreateRandomFiles(t *testing.T, dir string, n int, rng *rand.Rand) []string {
	t.Helper()
	files := make([]string, 0, n)
	for i := range n {
		rel := fmt.Sprintf("file_%04d.dat", i)
		if rng.Intn(3) == 0 {
			rel = fmt.Sprintf("sub%d/file_%04d.dat", rng.Intn(5), i)
		}
		size := fuzzRandomFileSize(rng)
		data := make([]byte, size)
		rng.Read(data)
		writeTestFile(t, dir, rel, string(data))
		files = append(files, rel)
	}
	return files
}

func fuzzRandomFileSize(rng *rand.Rand) int {
	switch rng.Intn(5) {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		return 64 + rng.Intn(4000)
	case 3:
		return 4096 + rng.Intn(60000)
	default:
		return 64*1024 + rng.Intn(192*1024)
	}
}

func fuzzApplyRandomModifications(t *testing.T, dir string, files *[]string, rng *rand.Rand) {
	t.Helper()
	// Remove some files (0-30%).
	var kept []string
	for _, f := range *files {
		if rng.Intn(100) < 30 {
			os.Remove(filepath.Join(dir, f))
		} else {
			kept = append(kept, f)
		}
	}
	// Change some files (0-20%).
	for _, f := range kept {
		if rng.Intn(100) < 20 {
			size := fuzzRandomFileSize(rng)
			data := make([]byte, size)
			rng.Read(data)
			writeTestFile(t, dir, f, string(data))
		}
	}
	// Add new files (0-20).
	numNew := rng.Intn(21)
	base := len(kept) + 1000
	for i := range numNew {
		rel := fmt.Sprintf("new_%04d.dat", base+i)
		if rng.Intn(3) == 0 {
			rel = fmt.Sprintf("newsub%d/new_%04d.dat", rng.Intn(3), base+i)
		}
		size := fuzzRandomFileSize(rng)
		data := make([]byte, size)
		rng.Read(data)
		writeTestFile(t, dir, rel, string(data))
		kept = append(kept, rel)
	}
	*files = kept
}

func fuzzVerifySyncResult(t *testing.T, dir string, oldManifestRaw string, newEntries []encoding.ManifestEntry, rmPaths []string) {
	t.Helper()

	// Walk the filesystem to get ground truth.
	onDisk := make(map[string]struct{})
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		onDisk[filepath.ToSlash(rel)] = struct{}{}
		return nil
	})

	// Every file on disk must appear in newEntries.
	inManifest := make(map[string]struct{}, len(newEntries))
	for _, e := range newEntries {
		inManifest[e.Path] = struct{}{}
	}
	for path := range onDisk {
		if _, ok := inManifest[path]; !ok {
			t.Errorf("file on disk %q not in new manifest", path)
		}
	}
	// Every new manifest entry must exist on disk.
	for _, e := range newEntries {
		if _, ok := onDisk[e.Path]; !ok {
			t.Errorf("manifest entry %q not on disk", e.Path)
		}
		// Verify size matches.
		info, err := os.Stat(filepath.Join(dir, filepath.FromSlash(e.Path)))
		if err != nil {
			t.Errorf("stat %q: %v", e.Path, err)
			continue
		}
		if info.Size() != e.Size {
			t.Errorf("size mismatch for %q: manifest=%d disk=%d", e.Path, e.Size, info.Size())
		}
	}

	// Parse old manifest paths.
	oldPaths := make(map[string]struct{})
	if oldManifestRaw != "" {
		oldEntries, _ := parseSYNCResponseEntries(oldManifestRaw, nil)
		for _, e := range oldEntries {
			oldPaths[e.Path] = struct{}{}
		}
	}

	// rmPaths must be exactly: old paths not on disk.
	rmSet := make(map[string]struct{}, len(rmPaths))
	for _, p := range rmPaths {
		if _, dup := rmSet[p]; dup {
			t.Errorf("duplicate RM path: %q", p)
		}
		rmSet[p] = struct{}{}
	}
	for oldPath := range oldPaths {
		_, stillOnDisk := onDisk[oldPath]
		_, inRM := rmSet[oldPath]
		if !stillOnDisk && !inRM {
			t.Errorf("old path %q not on disk and not in RM", oldPath)
		}
		if stillOnDisk && inRM {
			t.Errorf("old path %q still on disk but in RM", oldPath)
		}
	}
	for rmPath := range rmSet {
		if _, ok := oldPaths[rmPath]; !ok {
			t.Errorf("RM path %q not in old manifest", rmPath)
		}
	}
}

// parseSYNCResponseEntries is a non-fatal version for fuzz/helper use.
// Returns manifest entries and raw RM fileIDs (caller resolves IDs to paths).
// oldByID is unused here but accepted for API symmetry with parseSYNCResponse.
func parseSYNCResponseEntries(raw string, _ map[uint64]string) ([]encoding.ManifestEntry, []uint64) {
	var entries []encoding.ManifestEntry
	var rmIDs []uint64
	prevPath := ""
	prevMtime := ""
	seenHeader := false

	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "FM/1 ") {
			seenHeader = true
			prevPath = ""
			prevMtime = ""
			continue
		}
		if strings.HasPrefix(trimmed, "RM ") {
			id, err := strconv.ParseUint(trimmed[3:], 10, 64)
			if err == nil {
				rmIDs = append(rmIDs, id)
			}
			continue
		}
		if !seenHeader {
			continue
		}
		entry, nextPath, nextMtime, err := encoding.ParseManifestEntry(trimmed, prevPath, prevMtime)
		if err != nil {
			continue
		}
		entries = append(entries, entry)
		prevPath = nextPath
		prevMtime = nextMtime
	}
	return entries, rmIDs
}

// fuzzBuildManifestFromEntries creates a raw FM/1 manifest string from entries.
func fuzzBuildManifestFromEntries(t *testing.T, root string, entries []encoding.ManifestEntry) string {
	t.Helper()
	var b strings.Builder
	hdr := encoding.FormatManifestHeader(encoding.ManifestHeader{
		TransferID:  "fuzz-tx",
		Root:        root,
		Mode:        "fast",
		LinkMbps:    1000,
		Concurrency: 8,
	})
	b.WriteString(hdr)
	b.WriteByte('\n')
	prevPath := ""
	prevMtime := ""
	for _, e := range entries {
		line, nextPath, nextMtime, err := encoding.MarshalManifestEntry(e, prevPath, prevMtime)
		if err != nil {
			t.Fatalf("marshal entry: %v", err)
		}
		b.WriteString(line)
		b.WriteByte('\n')
		prevPath = nextPath
		prevMtime = nextMtime
	}
	return b.String()
}
