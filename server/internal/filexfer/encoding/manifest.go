package encoding

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/jolynch/pinch/utils"
)

// Entry type constants for the manifest wire format.
const (
	EntryTypeFile    byte = 'F' // regular file
	EntryTypeHard    byte = 'H' // hardlink (mtime carries target file ID)
	EntryTypeDir     byte = 'D' // directory
	EntryTypeSymlink byte = 'S' // symlink (link target after path token)
)

// ManifestEntry is the internal representation of one FM/1 manifest line.
type ManifestEntry struct {
	Type       byte // 'F', 'H', 'D', 'S'; zero treated as 'F'
	ID         uint64
	Size       int64
	Mtime      int64
	Mode       os.FileMode
	Path       string
	LinkTarget int64  // H: target file ID; -1 otherwise
	LinkPath   string // S: symlink target path; "" otherwise
}

// ManifestHeader is the parsed FM/1 header line.
type ManifestHeader struct {
	TransferID  string
	Root        string
	Mode        string
	LinkMbps    int64
	Concurrency int
	DeadlineMS  int64 // optional; 0 = no deadline
}

// WalkResult is one canonical filesystem-derived manifest entry.
type WalkResult struct {
	Entry    ManifestEntry
	FullPath string
}

// EncodePathToken front-codes current against prev and returns a "prefix:suffixLen:suffix" token.
func EncodePathToken(prev string, current string) string {
	prefixLen := utils.CommonPrefixLen(prev, current)
	suffix := current[prefixLen:]
	return strconv.Itoa(prefixLen) + ":" + strconv.Itoa(len(suffix)) + ":" + suffix
}

// DecodePathTokenPrefix decodes a "prefix:suffixLen:suffix" token against prev,
// consuming exactly suffixLen bytes from the suffix. Returns the decoded path
// and the number of bytes consumed from token, allowing trailing content.
func DecodePathTokenPrefix(prev string, token string) (string, int, error) {
	first := strings.IndexByte(token, ':')
	if first < 0 {
		return "", 0, errors.New("invalid path token")
	}
	second := strings.IndexByte(token[first+1:], ':')
	if second < 0 {
		return "", 0, errors.New("invalid path token")
	}
	second += first + 1
	prefixLen, err := strconv.Atoi(token[:first])
	if err != nil || prefixLen < 0 {
		return "", 0, errors.New("invalid path prefix length")
	}
	if prefixLen > len(prev) {
		return "", 0, errors.New("path prefix length exceeds previous value")
	}
	suffixLen, err := strconv.Atoi(token[first+1 : second])
	if err != nil || suffixLen < 0 {
		return "", 0, errors.New("invalid path suffix length")
	}
	suffixStart := second + 1
	suffixEnd := suffixStart + suffixLen
	if suffixEnd > len(token) {
		return "", 0, errors.New("path suffix length exceeds token")
	}
	suffix := token[suffixStart:suffixEnd]
	if prev == "" && prefixLen != 0 {
		return "", 0, errors.New("first path prefix length must be zero")
	}
	return prev[:prefixLen] + suffix, suffixEnd, nil
}

// DecodePathToken decodes a "prefix:suffixLen:suffix" token against prev.
// The entire token must be consumed (no trailing content).
func DecodePathToken(prev string, token string) (string, error) {
	decoded, consumed, err := DecodePathTokenPrefix(prev, token)
	if err != nil {
		return "", err
	}
	if consumed != len(token) {
		return "", errors.New("path suffix length mismatch")
	}
	return decoded, nil
}

// EncodeMtimeToken front-codes current against prev and returns a "prefix:suffix" token.
func EncodeMtimeToken(prev string, current string) (string, error) {
	if current == "" {
		return "", errors.New("empty mtime")
	}
	for _, ch := range current {
		if ch < '0' || ch > '9' {
			return "", errors.New("mtime must be decimal digits")
		}
	}
	prefixLen := utils.CommonPrefixLen(prev, current)
	suffix := current[prefixLen:]
	if suffix == "" {
		if len(current) == 0 {
			return "", errors.New("mtime cannot be empty")
		}
		prefixLen = len(current) - 1
		suffix = current[prefixLen:]
	}
	return strconv.Itoa(prefixLen) + ":" + suffix, nil
}

// DecodeMtimeToken decodes a "prefix:suffix" token against prev.
func DecodeMtimeToken(prev string, token string) (string, error) {
	head, suffix, ok := strings.Cut(token, ":")
	if !ok {
		return "", errors.New("invalid mtime token")
	}
	prefixLen, err := strconv.Atoi(head)
	if err != nil || prefixLen < 0 {
		return "", errors.New("invalid mtime prefix length")
	}
	if prefixLen > len(prev) {
		return "", errors.New("mtime prefix length exceeds previous value")
	}
	if suffix == "" {
		return "", errors.New("empty mtime suffix")
	}
	for _, ch := range suffix {
		if ch < '0' || ch > '9' {
			return "", errors.New("mtime suffix must be decimal digits")
		}
	}
	if prev == "" && prefixLen != 0 {
		return "", errors.New("first mtime prefix length must be zero")
	}
	return prev[:prefixLen] + suffix, nil
}

// NormalizeManifestMode strips file-type bits and keeps only manifest-relevant mode bits.
func NormalizeManifestMode(mode os.FileMode) os.FileMode {
	bits := mode.Perm()
	if mode&os.ModeSetuid != 0 {
		bits |= 0o4000
	}
	if mode&os.ModeSetgid != 0 {
		bits |= 0o2000
	}
	if mode&os.ModeSticky != 0 {
		bits |= 0o1000
	}
	return bits
}

// FormatManifestMode formats a file mode as a 4-digit octal string.
func FormatManifestMode(mode os.FileMode) string {
	bits := NormalizeManifestMode(mode)
	return fmt.Sprintf("%04o", bits)
}

// ParseManifestModeToken parses a 4-digit octal mode string.
func ParseManifestModeToken(raw string) (os.FileMode, error) {
	if raw == "" {
		return 0, errors.New("manifest mode is required")
	}
	for _, ch := range raw {
		if ch < '0' || ch > '7' {
			return 0, errors.New("manifest mode must be octal")
		}
	}
	v, err := strconv.ParseUint(raw, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid manifest mode: %w", err)
	}
	if v > 0o7777 {
		return 0, errors.New("manifest mode must be <= 07777")
	}
	return os.FileMode(v), nil
}

// ParseManifestEntry parses a single FM/1 manifest entry line.
// Returns the parsed entry, the resolved path, and the resolved mtime string.
//
// The mtime string returned is always the raw resolved mtime token (even for H
// entries where the mtime field carries a target file ID). This preserves
// front-coding continuity across entry types.
func ParseManifestEntry(line string, prevPath string, prevMtime string) (ManifestEntry, string, string, error) {
	first := strings.IndexByte(line, ' ')
	if first <= 0 {
		return ManifestEntry{}, "", "", errors.New("invalid manifest entry")
	}
	second := strings.IndexByte(line[first+1:], ' ')
	if second < 0 {
		return ManifestEntry{}, "", "", errors.New("invalid manifest entry")
	}
	second += first + 1
	third := strings.IndexByte(line[second+1:], ' ')
	if third < 0 {
		return ManifestEntry{}, "", "", errors.New("invalid manifest entry")
	}
	third += second + 1
	fourth := strings.IndexByte(line[third+1:], ' ')
	if fourth < 0 {
		return ManifestEntry{}, "", "", errors.New("invalid manifest entry")
	}
	fourth += third + 1

	idRaw := line[:first]
	sizeRaw := line[first+1 : second]
	mtimeToken := line[second+1 : third]
	modeRaw := line[third+1 : fourth]
	pathAndTrailing := line[fourth+1:]

	// Parse type prefix: F, H, D, S before the numeric ID. If the first
	// character is a digit, treat as legacy format (type F).
	entryType := EntryTypeFile
	idStr := idRaw
	if len(idRaw) > 0 && (idRaw[0] < '0' || idRaw[0] > '9') {
		entryType = idRaw[0]
		idStr = idRaw[1:]
		switch entryType {
		case EntryTypeFile, EntryTypeHard, EntryTypeDir, EntryTypeSymlink:
		default:
			return ManifestEntry{}, "", "", fmt.Errorf("unknown manifest entry type: %c", entryType)
		}
	}

	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return ManifestEntry{}, "", "", fmt.Errorf("invalid manifest id: %w", err)
	}
	sizeU, err := strconv.ParseUint(sizeRaw, 10, 64)
	if err != nil {
		return ManifestEntry{}, "", "", fmt.Errorf("invalid manifest size: %w", err)
	}
	if sizeU > uint64(^uint64(0)>>1) {
		return ManifestEntry{}, "", "", errors.New("manifest size overflows int64")
	}

	mtimeResolved, err := DecodeMtimeToken(prevMtime, mtimeToken)
	if err != nil {
		return ManifestEntry{}, "", "", err
	}
	mode, err := ParseManifestModeToken(modeRaw)
	if err != nil {
		return ManifestEntry{}, "", "", err
	}

	// For S entries, use DecodePathTokenPrefix (path token is self-delimiting,
	// trailing content is the symlink target). For other types, consume all.
	var pathResolved string
	var linkPath string
	if entryType == EntryTypeSymlink {
		consumed := 0
		pathResolved, consumed, err = DecodePathTokenPrefix(prevPath, pathAndTrailing)
		if err != nil {
			return ManifestEntry{}, "", "", err
		}
		trailing := strings.TrimSpace(pathAndTrailing[consumed:])
		if trailing == "" {
			return ManifestEntry{}, "", "", errors.New("S entry missing symlink target")
		}
		linkPath, _, err = ParseLenPrefixedPrefix(trailing)
		if err != nil {
			return ManifestEntry{}, "", "", fmt.Errorf("invalid symlink target: %w", err)
		}
	} else {
		pathResolved, err = DecodePathToken(prevPath, pathAndTrailing)
		if err != nil {
			return ManifestEntry{}, "", "", err
		}
	}

	if strings.Contains(pathResolved, `\`) {
		return ManifestEntry{}, "", "", errors.New("manifest path contains backslash")
	}
	if strings.HasPrefix(pathResolved, "/") {
		return ManifestEntry{}, "", "", errors.New("manifest path must be relative")
	}
	cleanPath := filepath.Clean(filepath.FromSlash(pathResolved))
	if cleanPath == "." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) || cleanPath == ".." {
		return ManifestEntry{}, "", "", errors.New("manifest path traversal is not allowed")
	}

	entry := ManifestEntry{
		Type:       entryType,
		ID:         id,
		Size:       int64(sizeU),
		Mode:       mode,
		Path:       pathResolved,
		LinkTarget: -1,
		LinkPath:   linkPath,
	}

	// Interpret mtime field based on type.
	switch entryType {
	case EntryTypeHard:
		// Mtime field carries the target file ID.
		targetID, pErr := strconv.ParseUint(mtimeResolved, 10, 64)
		if pErr != nil {
			return ManifestEntry{}, "", "", fmt.Errorf("invalid H entry target id: %w", pErr)
		}
		entry.LinkTarget = int64(targetID)
		entry.Mtime = 0
	default:
		mtimeNanos, pErr := strconv.ParseUint(mtimeResolved, 10, 64)
		if pErr != nil {
			return ManifestEntry{}, "", "", fmt.Errorf("invalid manifest mtime value: %w", pErr)
		}
		if mtimeNanos > uint64(^uint64(0)>>1) {
			return ManifestEntry{}, "", "", errors.New("manifest mtime overflows int64")
		}
		entry.Mtime = int64(mtimeNanos)
	}

	return entry, pathResolved, mtimeResolved, nil
}

// WalkManifestEntries streams filesystem-derived manifest entries in walk order.
func WalkManifestEntries(root string, fn func(WalkResult) error) error {
	root = filepath.Clean(root)
	var fileID uint64
	inodeSeen := make(map[uint64]map[uint64]uint64, 4)

	return filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		entry := ManifestEntry{
			Type:       EntryTypeFile,
			ID:         fileID,
			Size:       info.Size(),
			Mtime:      info.ModTime().UnixNano(),
			Mode:       NormalizeManifestMode(info.Mode()),
			Path:       filepath.ToSlash(rel),
			LinkTarget: -1,
		}

		switch {
		case d.IsDir():
			entry.Type = EntryTypeDir
			entry.Size = 0
		case d.Type()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			entry.Type = EntryTypeSymlink
			entry.Size = 0
			entry.LinkPath = target
		default:
			if st, ok := info.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
				dev := uint64(st.Dev)
				ino := st.Ino
				devMap, ok := inodeSeen[dev]
				if !ok {
					devMap = make(map[uint64]uint64)
					inodeSeen[dev] = devMap
				}
				if firstID, seen := devMap[ino]; seen {
					entry.Type = EntryTypeHard
					entry.Size = 0
					entry.Mtime = 0
					entry.LinkTarget = int64(firstID)
				} else {
					devMap[ino] = fileID
				}
			}
		}

		if err := fn(WalkResult{
			Entry:    entry,
			FullPath: filepath.Clean(path),
		}); err != nil {
			return err
		}
		fileID++
		return nil
	})
}

// MarshalManifestEntry serializes a single manifest entry using front-coding
// against the previous path and mtime. Returns the line (without trailing newline),
// the current path, and the current mtime string.
func MarshalManifestEntry(entry ManifestEntry, prevPath string, prevMtime string) (string, string, string, error) {
	if entry.Size < 0 {
		return "", "", "", fmt.Errorf("manifest size must be >= 0 for id=%d", entry.ID)
	}
	if strings.Contains(entry.Path, `\`) {
		return "", "", "", fmt.Errorf("manifest path contains backslash: %q", entry.Path)
	}
	if strings.HasPrefix(entry.Path, "/") {
		return "", "", "", fmt.Errorf("manifest path must be relative: %q", entry.Path)
	}
	cleanPath := filepath.Clean(filepath.FromSlash(entry.Path))
	if cleanPath == "." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) || cleanPath == ".." {
		return "", "", "", fmt.Errorf("manifest path traversal is not allowed: %q", entry.Path)
	}

	entryType := entry.Type
	if entryType == 0 {
		entryType = EntryTypeFile
	}

	// For H entries, the mtime field carries the target file ID.
	var mtimeRaw string
	switch entryType {
	case EntryTypeHard:
		if entry.LinkTarget < 0 {
			return "", "", "", fmt.Errorf("H entry id=%d missing link target", entry.ID)
		}
		mtimeRaw = strconv.FormatInt(entry.LinkTarget, 10)
	default:
		mtimeRaw = strconv.FormatInt(entry.Mtime, 10)
	}

	modeToken := FormatManifestMode(entry.Mode)
	mtimeToken, err := EncodeMtimeToken(prevMtime, mtimeRaw)
	if err != nil {
		return "", "", "", fmt.Errorf("encode manifest mtime id=%d: %w", entry.ID, err)
	}
	pathToken := EncodePathToken(prevPath, entry.Path)
	line := fmt.Sprintf("%c%d %d %s %s %s", entryType, entry.ID, entry.Size, mtimeToken, modeToken, pathToken)

	// For S entries, append the symlink target as a len-prefixed token.
	if entryType == EntryTypeSymlink {
		line += fmt.Sprintf(" %d:%s", len(entry.LinkPath), entry.LinkPath)
	}

	return line, entry.Path, mtimeRaw, nil
}

// ParseManifestHeader parses an FM/1 header line and returns the header fields.
func ParseManifestHeader(line string) (ManifestHeader, error) {
	rest := strings.TrimPrefix(line, "FM/1 ")
	sep := strings.IndexByte(rest, ' ')
	if sep <= 0 || sep == len(rest)-1 {
		return ManifestHeader{}, errors.New("invalid manifest header")
	}
	txferID := rest[:sep]
	rootRaw := rest[sep+1:]
	root, consumed, err := ParseLenPrefixedPrefix(rootRaw)
	if err != nil {
		return ManifestHeader{}, fmt.Errorf("invalid manifest root token: %w", err)
	}
	optionsRaw := strings.TrimSpace(rootRaw[consumed:])
	if optionsRaw == "" {
		return ManifestHeader{}, errors.New("manifest header missing metadata options")
	}
	options := strings.Fields(optionsRaw)
	var (
		hdr      ManifestHeader
		seenMode bool
		seenLink bool
		seenConc bool
	)
	hdr.TransferID = txferID
	hdr.Root = root
	for _, option := range options {
		key, value, ok := strings.Cut(option, "=")
		if !ok {
			return ManifestHeader{}, errors.New("invalid manifest header option")
		}
		switch key {
		case "mode":
			value = strings.ToLower(strings.TrimSpace(value))
			if value != "fast" && value != "gentle" {
				return ManifestHeader{}, errors.New("invalid manifest mode")
			}
			hdr.Mode = value
			seenMode = true
		case "link-mbps":
			hdr.LinkMbps, err = strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err != nil || hdr.LinkMbps < 0 {
				return ManifestHeader{}, errors.New("invalid manifest link-mbps")
			}
			seenLink = true
		case "concurrency":
			hdr.Concurrency, err = strconv.Atoi(strings.TrimSpace(value))
			if err != nil || hdr.Concurrency <= 0 {
				return ManifestHeader{}, errors.New("invalid manifest concurrency")
			}
			seenConc = true
		case "deadline-ms":
			hdr.DeadlineMS, err = strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err != nil || hdr.DeadlineMS < 0 {
				return ManifestHeader{}, errors.New("invalid manifest deadline-ms")
			}
		default:
			return ManifestHeader{}, errors.New("unknown manifest header option")
		}
	}
	if !seenMode || !seenLink || !seenConc {
		return ManifestHeader{}, errors.New("manifest header missing required metadata")
	}
	return hdr, nil
}

// FormatManifestHeader formats an FM/1 header line (without trailing newline).
func FormatManifestHeader(hdr ManifestHeader) string {
	s := fmt.Sprintf(
		"FM/1 %s %d:%s mode=%s link-mbps=%d concurrency=%d",
		hdr.TransferID,
		len(hdr.Root),
		hdr.Root,
		hdr.Mode,
		hdr.LinkMbps,
		hdr.Concurrency,
	)
	if hdr.DeadlineMS > 0 {
		s += fmt.Sprintf(" deadline-ms=%d", hdr.DeadlineMS)
	}
	return s
}

// ParseLenPrefixedPrefix parses a "len:value" token at the start of raw.
// Returns the value, the number of bytes consumed, and any error.
func ParseLenPrefixedPrefix(raw string) (string, int, error) {
	sep := strings.IndexByte(raw, ':')
	if sep <= 0 {
		return "", 0, errors.New("invalid len-prefixed token")
	}
	n, err := strconv.Atoi(raw[:sep])
	if err != nil || n < 0 {
		return "", 0, errors.New("invalid len prefix")
	}
	start := sep + 1
	end := start + n
	if end > len(raw) {
		return "", 0, errors.New("len prefix mismatch")
	}
	return raw[start:end], end, nil
}
