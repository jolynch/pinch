package encoding

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jolynch/pinch/utils"
)

// ManifestEntry is the internal representation of one FM/2 manifest line.
type ManifestEntry struct {
	ID    uint64
	Size  int64
	Mtime int64
	Mode  os.FileMode
	Path  string
}

// ManifestHeader is the parsed FM/2 header line.
type ManifestHeader struct {
	TransferID  string
	Root        string
	Mode        string
	LinkMbps    int64
	Concurrency int
}

// EncodePathToken front-codes current against prev and returns a "prefix:suffixLen:suffix" token.
func EncodePathToken(prev string, current string) string {
	prefixLen := utils.CommonPrefixLen(prev, current)
	suffix := current[prefixLen:]
	return strconv.Itoa(prefixLen) + ":" + strconv.Itoa(len(suffix)) + ":" + suffix
}

// DecodePathToken decodes a "prefix:suffixLen:suffix" token against prev.
func DecodePathToken(prev string, token string) (string, error) {
	first := strings.IndexByte(token, ':')
	if first < 0 {
		return "", errors.New("invalid path token")
	}
	second := strings.IndexByte(token[first+1:], ':')
	if second < 0 {
		return "", errors.New("invalid path token")
	}
	second += first + 1
	prefixLen, err := strconv.Atoi(token[:first])
	if err != nil || prefixLen < 0 {
		return "", errors.New("invalid path prefix length")
	}
	if prefixLen > len(prev) {
		return "", errors.New("path prefix length exceeds previous value")
	}
	suffixLen, err := strconv.Atoi(token[first+1 : second])
	if err != nil || suffixLen < 0 {
		return "", errors.New("invalid path suffix length")
	}
	suffix := token[second+1:]
	if len(suffix) != suffixLen {
		return "", errors.New("path suffix length mismatch")
	}
	if prev == "" && prefixLen != 0 {
		return "", errors.New("first path prefix length must be zero")
	}
	return prev[:prefixLen] + suffix, nil
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

// FormatManifestMode formats a file mode as a 4-digit octal string.
func FormatManifestMode(mode os.FileMode) string {
	bits := mode.Perm() | (mode & (os.ModeSetuid | os.ModeSetgid | os.ModeSticky))
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

// ParseManifestEntry parses a single FM/2 manifest entry line.
// Returns the parsed entry, the resolved path, and the resolved mtime string.
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
	pathToken := line[fourth+1:]

	id, err := strconv.ParseUint(idRaw, 10, 64)
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
	mtimeNanos, err := strconv.ParseUint(mtimeResolved, 10, 64)
	if err != nil {
		return ManifestEntry{}, "", "", fmt.Errorf("invalid manifest mtime value: %w", err)
	}
	if mtimeNanos > uint64(^uint64(0)>>1) {
		return ManifestEntry{}, "", "", errors.New("manifest mtime overflows int64")
	}
	mode, err := ParseManifestModeToken(modeRaw)
	if err != nil {
		return ManifestEntry{}, "", "", err
	}

	pathResolved, err := DecodePathToken(prevPath, pathToken)
	if err != nil {
		return ManifestEntry{}, "", "", err
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
		ID:    id,
		Size:  int64(sizeU),
		Mtime: int64(mtimeNanos),
		Mode:  mode,
		Path:  pathResolved,
	}
	return entry, pathResolved, mtimeResolved, nil
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
	modeToken := FormatManifestMode(entry.Mode)
	mtimeRaw := strconv.FormatInt(entry.Mtime, 10)
	mtimeToken, err := EncodeMtimeToken(prevMtime, mtimeRaw)
	if err != nil {
		return "", "", "", fmt.Errorf("encode manifest mtime id=%d: %w", entry.ID, err)
	}
	pathToken := EncodePathToken(prevPath, entry.Path)
	line := fmt.Sprintf("%d %d %s %s %s", entry.ID, entry.Size, mtimeToken, modeToken, pathToken)
	return line, entry.Path, mtimeRaw, nil
}

// ParseManifestHeader parses an FM/2 header line and returns the header fields.
func ParseManifestHeader(line string) (ManifestHeader, error) {
	rest := strings.TrimPrefix(line, "FM/2 ")
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
		default:
			return ManifestHeader{}, errors.New("unknown manifest header option")
		}
	}
	if !seenMode || !seenLink || !seenConc {
		return ManifestHeader{}, errors.New("manifest header missing required metadata")
	}
	return hdr, nil
}

// FormatManifestHeader formats an FM/2 header line (without trailing newline).
func FormatManifestHeader(hdr ManifestHeader) string {
	return fmt.Sprintf(
		"FM/2 %s %d:%s mode=%s link-mbps=%d concurrency=%d",
		hdr.TransferID,
		len(hdr.Root),
		hdr.Root,
		hdr.Mode,
		hdr.LinkMbps,
		hdr.Concurrency,
	)
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
