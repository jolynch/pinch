package encoding

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMarshalParseManifestEntryFile(t *testing.T) {
	entry := ManifestEntry{
		Type:       EntryTypeFile,
		ID:         0,
		Size:       1024,
		Mtime:      1735771234567890123,
		Mode:       0o644,
		Path:       "data/a.txt",
		LinkTarget: -1,
	}
	line, path, mtime, err := MarshalManifestEntry(entry, "", "")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if path != "data/a.txt" {
		t.Fatalf("unexpected path: %q", path)
	}

	parsed, parsedPath, parsedMtime, err := ParseManifestEntry(line, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Type != EntryTypeFile {
		t.Errorf("type: got %c, want F", parsed.Type)
	}
	if parsed.ID != 0 {
		t.Errorf("id: got %d, want 0", parsed.ID)
	}
	if parsed.Size != 1024 {
		t.Errorf("size: got %d, want 1024", parsed.Size)
	}
	if parsed.Mtime != 1735771234567890123 {
		t.Errorf("mtime: got %d, want 1735771234567890123", parsed.Mtime)
	}
	if parsed.Mode != 0o644 {
		t.Errorf("mode: got %o, want 0644", parsed.Mode)
	}
	if parsedPath != "data/a.txt" {
		t.Errorf("path: got %q, want %q", parsedPath, "data/a.txt")
	}
	if parsedMtime != mtime {
		t.Errorf("mtime string: got %q, want %q", parsedMtime, mtime)
	}
	if parsed.LinkTarget != -1 {
		t.Errorf("link target: got %d, want -1", parsed.LinkTarget)
	}
}

func TestMarshalParseManifestEntryHardlink(t *testing.T) {
	entry := ManifestEntry{
		Type:       EntryTypeHard,
		ID:         2,
		Size:       0,
		Mode:       0o644,
		Path:       "b.txt",
		LinkTarget: 0,
	}
	line, _, mtime, err := MarshalManifestEntry(entry, "", "")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	parsed, _, parsedMtime, err := ParseManifestEntry(line, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Type != EntryTypeHard {
		t.Errorf("type: got %c, want H", parsed.Type)
	}
	if parsed.ID != 2 {
		t.Errorf("id: got %d, want 2", parsed.ID)
	}
	if parsed.Size != 0 {
		t.Errorf("size: got %d, want 0", parsed.Size)
	}
	if parsed.LinkTarget != 0 {
		t.Errorf("link target: got %d, want 0", parsed.LinkTarget)
	}
	if parsed.Mtime != 0 {
		t.Errorf("mtime: got %d, want 0", parsed.Mtime)
	}
	if parsedMtime != mtime {
		t.Errorf("mtime string: got %q, want %q", parsedMtime, mtime)
	}
}

func TestMarshalParseManifestEntryDir(t *testing.T) {
	entry := ManifestEntry{
		Type:       EntryTypeDir,
		ID:         0,
		Size:       0,
		Mtime:      1735771234567890123,
		Mode:       0o750,
		Path:       "subdir",
		LinkTarget: -1,
	}
	line, _, _, err := MarshalManifestEntry(entry, "", "")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	parsed, _, _, err := ParseManifestEntry(line, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Type != EntryTypeDir {
		t.Errorf("type: got %c, want D", parsed.Type)
	}
	if parsed.Size != 0 {
		t.Errorf("size: got %d, want 0", parsed.Size)
	}
	if parsed.Mode != 0o750 {
		t.Errorf("mode: got %o, want 0750", parsed.Mode)
	}
}

func TestMarshalParseManifestEntrySymlink(t *testing.T) {
	entry := ManifestEntry{
		Type:       EntryTypeSymlink,
		ID:         3,
		Size:       0,
		Mtime:      1735771234567890123,
		Mode:       0o777,
		Path:       "link.txt",
		LinkTarget: -1,
		LinkPath:   "../target/file.txt",
	}
	line, _, _, err := MarshalManifestEntry(entry, "", "")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	parsed, _, _, err := ParseManifestEntry(line, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Type != EntryTypeSymlink {
		t.Errorf("type: got %c, want S", parsed.Type)
	}
	if parsed.LinkPath != "../target/file.txt" {
		t.Errorf("link path: got %q, want %q", parsed.LinkPath, "../target/file.txt")
	}
	if parsed.Size != 0 {
		t.Errorf("size: got %d, want 0", parsed.Size)
	}
}

func TestParseManifestEntryBackwardCompat(t *testing.T) {
	// Old format: no type prefix, bare numeric ID.
	line := "5 1024 0:1735771234567890123 0644 0:5:a.txt"
	parsed, _, _, err := ParseManifestEntry(line, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Type != EntryTypeFile {
		t.Errorf("type: got %c, want F", parsed.Type)
	}
	if parsed.ID != 5 {
		t.Errorf("id: got %d, want 5", parsed.ID)
	}
}

func TestDecodePathTokenPrefixTrailing(t *testing.T) {
	// Path token "0:5:a.txt" with trailing " 3:foo" after it.
	token := "0:5:a.txt 3:foo"
	decoded, consumed, err := DecodePathTokenPrefix("", token)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded != "a.txt" {
		t.Errorf("decoded: got %q, want %q", decoded, "a.txt")
	}
	if consumed != 9 { // "0:5:a.txt" = 9 chars
		t.Errorf("consumed: got %d, want 9", consumed)
	}
	trailing := token[consumed:]
	if trailing != " 3:foo" {
		t.Errorf("trailing: got %q, want %q", trailing, " 3:foo")
	}
}

func TestDecodePathTokenRejectsTrailing(t *testing.T) {
	// DecodePathToken (not Prefix) must reject trailing content.
	_, err := DecodePathToken("", "0:5:a.txt extra")
	if err == nil {
		t.Fatal("expected error for trailing content")
	}
}

func TestMarshalParseManifestEntryFrontCodingAcrossTypes(t *testing.T) {
	// Verify front-coding works across D, F, H, S entries.
	entries := []ManifestEntry{
		{Type: EntryTypeDir, ID: 0, Mtime: 100, Mode: 0o755, Path: "sub", LinkTarget: -1},
		{Type: EntryTypeFile, ID: 1, Size: 1024, Mtime: 1735771234567890123, Mode: 0o644, Path: "sub/a.txt", LinkTarget: -1},
		{Type: EntryTypeHard, ID: 2, Mode: 0o644, Path: "sub/b.txt", LinkTarget: 1},
		{Type: EntryTypeSymlink, ID: 3, Mtime: 1735771234567890123, Mode: 0o777, Path: "sub/link.txt", LinkTarget: -1, LinkPath: "a.txt"},
	}

	prevPath := ""
	prevMtime := ""
	var lines []string
	for _, e := range entries {
		line, p, m, err := MarshalManifestEntry(e, prevPath, prevMtime)
		if err != nil {
			t.Fatalf("marshal id=%d: %v", e.ID, err)
		}
		lines = append(lines, line)
		prevPath = p
		prevMtime = m
	}

	// Parse back.
	prevPath = ""
	prevMtime = ""
	for i, line := range lines {
		parsed, p, m, err := ParseManifestEntry(line, prevPath, prevMtime)
		if err != nil {
			t.Fatalf("parse line %d: %v (line=%q)", i, err, line)
		}
		if parsed.Type != entries[i].Type {
			t.Errorf("entry %d: type got %c, want %c", i, parsed.Type, entries[i].Type)
		}
		if parsed.ID != entries[i].ID {
			t.Errorf("entry %d: id got %d, want %d", i, parsed.ID, entries[i].ID)
		}
		if parsed.Path != entries[i].Path {
			t.Errorf("entry %d: path got %q, want %q", i, parsed.Path, entries[i].Path)
		}
		if parsed.LinkTarget != entries[i].LinkTarget {
			t.Errorf("entry %d: link target got %d, want %d", i, parsed.LinkTarget, entries[i].LinkTarget)
		}
		if parsed.LinkPath != entries[i].LinkPath {
			t.Errorf("entry %d: link path got %q, want %q", i, parsed.LinkPath, entries[i].LinkPath)
		}
		prevPath = p
		prevMtime = m
	}
}

func TestNormalizeManifestMode(t *testing.T) {
	mode := os.ModeDir | os.ModeSetgid | os.ModeSticky | 0o750
	got := NormalizeManifestMode(mode)
	want := os.FileMode(0o3750)
	if got != want {
		t.Fatalf("NormalizeManifestMode(%o) = %o, want %o", mode, got, want)
	}
}

func TestWalkManifestEntriesMixedTypes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	if err := os.Link(filepath.Join(root, "a.txt"), filepath.Join(root, "b.txt")); err != nil {
		t.Fatalf("hardlink b.txt: %v", err)
	}
	if err := os.Symlink("a.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Fatalf("symlink link.txt: %v", err)
	}
	subDir := filepath.Join(root, "sub")
	if err := os.Mkdir(subDir, 0o750); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "c.txt"), []byte("world"), 0o640); err != nil {
		t.Fatalf("write sub/c.txt: %v", err)
	}

	var results []WalkResult
	if err := WalkManifestEntries(root, func(result WalkResult) error {
		results = append(results, result)
		return nil
	}); err != nil {
		t.Fatalf("WalkManifestEntries: %v", err)
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(results))
	}

	wantPaths := []string{"a.txt", "b.txt", "link.txt", "sub", "sub/c.txt"}
	wantTypes := []byte{EntryTypeFile, EntryTypeHard, EntryTypeSymlink, EntryTypeDir, EntryTypeFile}
	for i, result := range results {
		if result.Entry.ID != uint64(i) {
			t.Errorf("entry %d id: got %d, want %d", i, result.Entry.ID, i)
		}
		if result.Entry.Path != wantPaths[i] {
			t.Errorf("entry %d path: got %q, want %q", i, result.Entry.Path, wantPaths[i])
		}
		if result.Entry.Type != wantTypes[i] {
			t.Errorf("entry %d type: got %c, want %c", i, result.Entry.Type, wantTypes[i])
		}
		if result.FullPath != filepath.Clean(filepath.Join(root, wantPaths[i])) {
			t.Errorf("entry %d full path: got %q", i, result.FullPath)
		}
	}

	if results[0].Entry.Size != 5 {
		t.Errorf("a.txt size: got %d, want 5", results[0].Entry.Size)
	}
	if results[1].Entry.LinkTarget != 0 {
		t.Errorf("b.txt link target: got %d, want 0", results[1].Entry.LinkTarget)
	}
	if results[1].Entry.Size != 0 || results[1].Entry.Mtime != 0 {
		t.Errorf("b.txt hardlink entry: size=%d mtime=%d, want 0/0", results[1].Entry.Size, results[1].Entry.Mtime)
	}
	if results[2].Entry.LinkPath != "a.txt" {
		t.Errorf("link.txt target: got %q, want %q", results[2].Entry.LinkPath, "a.txt")
	}
	if results[3].Entry.Mode != 0o750 {
		t.Errorf("sub mode: got %o, want 0750", results[3].Entry.Mode)
	}
}
