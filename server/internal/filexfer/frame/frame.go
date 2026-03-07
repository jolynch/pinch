package frame

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jolynch/pinch/internal/filexfer/codec"
	"github.com/zeebo/xxh3"
)

type FileFrameMetadata struct {
	Size    int64
	MtimeNS int64
	Mode    string
	UID     string
	GID     string
	User    string
	Group   string
}

type fileFrameMetadata = FileFrameMetadata

func CollectFileFrameMetadata(path string, info os.FileInfo) FileFrameMetadata {
	meta := FileFrameMetadata{
		Size:    info.Size(),
		MtimeNS: info.ModTime().UnixNano(),
		Mode:    fmt.Sprintf("%04o", info.Mode().Perm()|(info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky))),
		UID:     "unknown",
		GID:     "unknown",
		User:    "unknown",
		Group:   "unknown",
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		meta.UID = strconv.FormatUint(uint64(st.Uid), 10)
		meta.GID = strconv.FormatUint(uint64(st.Gid), 10)
		if u, err := user.LookupId(meta.UID); err == nil {
			meta.User = u.Username
		}
		if g, err := user.LookupGroupId(meta.GID); err == nil {
			meta.Group = g.Name
		}
	}
	_ = path
	return meta
}

func collectFileFrameMetadata(path string, info os.FileInfo) fileFrameMetadata {
	return CollectFileFrameMetadata(path, info)
}

func (m FileFrameMetadata) trailerTokens() []string {
	return []string{
		fmt.Sprintf("meta:size=%d", m.Size),
		fmt.Sprintf("meta:mtime_ns=%d", m.MtimeNS),
		fmt.Sprintf("meta:mode=%s", m.Mode),
		fmt.Sprintf("meta:uid=%s", m.UID),
		fmt.Sprintf("meta:gid=%s", m.GID),
		fmt.Sprintf("meta:user=%s", strings.ReplaceAll(m.User, " ", "_")),
		fmt.Sprintf("meta:group=%s", strings.ReplaceAll(m.Group, " ", "_")),
	}
}

type WriteArgs struct {
	FileID       uint64
	Offset       int64
	Size         int64
	WSize        int64
	Comp         string
	Enc          string
	HeaderHash   string
	MaxWSizeHint *int64
	HeaderTS     int64
	Payload      []byte
	TrailerTS    int64
	HashTokens   []string
	FileHashes   []string
	Next         int64
	Metadata     *FileFrameMetadata
}

type frameWriteArgs = WriteArgs

type WriteStats struct {
	WriteLatency      time.Duration
	WireThroughputBps float64
}

type frameWriteStats = WriteStats

func WriteFrame(w http.ResponseWriter, args WriteArgs) (WriteStats, error) {
	start := time.Now()
	header := ""
	if args.MaxWSizeHint != nil {
		header = fmt.Sprintf(
			"FX/1 %d offset=%d size=%d wsize=%d comp=%s enc=%s hash=%s max-wsize=%d ts=%d\n",
			args.FileID, args.Offset, args.Size, args.WSize, args.Comp, args.Enc, args.HeaderHash, *args.MaxWSizeHint, args.HeaderTS,
		)
	} else {
		header = fmt.Sprintf(
			"FX/1 %d offset=%d size=%d wsize=%d comp=%s enc=%s hash=%s ts=%d\n",
			args.FileID, args.Offset, args.Size, args.WSize, args.Comp, args.Enc, args.HeaderHash, args.HeaderTS,
		)
	}
	if _, err := w.Write([]byte(header)); err != nil {
		return WriteStats{}, err
	}

	if len(args.Payload) > 0 {
		if _, err := w.Write(args.Payload); err != nil {
			return WriteStats{}, err
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "FXT/1 %d status=ok ts=%d", args.FileID, args.TrailerTS)
	for _, token := range args.FileHashes {
		if token != "" {
			b.WriteString(" file-hash=")
			b.WriteString(token)
		}
	}
	b.WriteString(" next=")
	b.WriteString(strconv.FormatInt(args.Next, 10))
	if args.Metadata != nil {
		for _, token := range args.Metadata.trailerTokens() {
			b.WriteString(" ")
			b.WriteString(token)
		}
	}
	trailerPrefix := b.String()
	frameHasher := xxh3.New()
	_, _ = frameHasher.Write([]byte(header))
	if len(args.Payload) > 0 {
		_, _ = frameHasher.Write(args.Payload)
	}
	_, _ = frameHasher.Write([]byte(trailerPrefix))
	frameHashToken := fmt.Sprintf("xxh64:%016x", frameHasher.Sum64())
	trailer := trailerPrefix + " hash=" + frameHashToken + "\n"
	if _, err := w.Write([]byte(trailer)); err != nil {
		return WriteStats{}, err
	}

	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
	writeLatency := time.Since(start)
	wireBps := 0.0
	if writeLatency > 0 && args.WSize > 0 {
		wireBps = float64(args.WSize) / writeLatency.Seconds()
	}
	return WriteStats{
		WriteLatency:      writeLatency,
		WireThroughputBps: wireBps,
	}, nil
}

func writeFrame(w http.ResponseWriter, args frameWriteArgs) (frameWriteStats, error) {
	return WriteFrame(w, args)
}

type FileFrameMeta struct {
	FileID          uint64
	Comp            string
	Enc             string
	Offset          int64
	Size            int64
	WireSize        int64
	MaxWireSizeHint int64
	HeaderTS        int64
}

type FrameTrailer struct {
	FileID         uint64
	TS             int64
	HashToken      string
	FileHashToken  string
	ChecksumPrefix string
	Next           *int64
}

func ParseFXHeader(line string) (FileFrameMeta, error) {
	fields := strings.Fields(line)
	if len(fields) < 3 || fields[0] != "FX/1" {
		return FileFrameMeta{}, errors.New("invalid FX/1 header")
	}
	fileID, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return FileFrameMeta{}, fmt.Errorf("invalid header file id: %w", err)
	}
	props := make(map[string]string, len(fields)-2)
	for _, token := range fields[2:] {
		parts := strings.SplitN(token, "=", 2)
		if len(parts) != 2 {
			continue
		}
		props[parts[0]] = parts[1]
	}

	comp := props["comp"]
	enc := props["enc"]
	offset, err := parseHeaderInt(props["offset"], "offset")
	if err != nil {
		return FileFrameMeta{}, err
	}
	size, err := parseHeaderInt(props["size"], "size")
	if err != nil {
		return FileFrameMeta{}, err
	}
	wsize, err := parseHeaderInt(props["wsize"], "wsize")
	if err != nil {
		return FileFrameMeta{}, err
	}
	ts, err := parseHeaderInt(props["ts"], "ts")
	if err != nil {
		return FileFrameMeta{}, err
	}
	maxWSizeHint := int64(0)
	if raw, ok := props["max-wsize"]; ok {
		maxWSizeHint, err = parseHeaderInt(raw, "max-wsize")
		if err != nil {
			return FileFrameMeta{}, err
		}
		if maxWSizeHint <= 0 {
			return FileFrameMeta{}, errors.New("invalid header max-wsize")
		}
	}
	if ts < 0 {
		return FileFrameMeta{}, errors.New("invalid header ts")
	}
	if comp == "" || enc == "" {
		return FileFrameMeta{}, errors.New("missing required frame properties")
	}
	return FileFrameMeta{
		FileID:          fileID,
		Comp:            comp,
		Enc:             enc,
		Offset:          offset,
		Size:            size,
		WireSize:        wsize,
		MaxWireSizeHint: maxWSizeHint,
		HeaderTS:        ts,
	}, nil
}

func parseHeaderInt(raw string, key string) (int64, error) {
	if raw == "" {
		return 0, fmt.Errorf("missing header property: %s", key)
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid header property %s: %w", key, err)
	}
	return v, nil
}

func ParseFXTrailer(line string) (FrameTrailer, error) {
	prefix, hashToken, err := splitTrailerPrefixAndHash(line)
	if err != nil {
		return FrameTrailer{}, err
	}
	fields := strings.Fields(prefix)
	if len(fields) < 3 || fields[0] != "FXT/1" {
		return FrameTrailer{}, errors.New("invalid FXT/1 trailer")
	}
	fileID, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return FrameTrailer{}, fmt.Errorf("invalid trailer file id: %w", err)
	}
	status := ""
	var fileHashToken string
	var ts int64 = -1
	var nextOffset *int64
	for _, token := range fields[2:] {
		if strings.HasPrefix(token, "status=") {
			status = strings.TrimPrefix(token, "status=")
		}
		if strings.HasPrefix(token, "ts=") {
			tsRaw := strings.TrimPrefix(token, "ts=")
			parsedTS, parseErr := strconv.ParseInt(tsRaw, 10, 64)
			if parseErr != nil || parsedTS < 0 {
				return FrameTrailer{}, errors.New("invalid trailer ts")
			}
			ts = parsedTS
		}
		if strings.HasPrefix(token, "file-hash=") {
			fileHashToken = strings.TrimPrefix(token, "file-hash=")
		}
		if strings.HasPrefix(token, "next=") {
			nextRaw := strings.TrimPrefix(token, "next=")
			nextValue, parseErr := strconv.ParseInt(nextRaw, 10, 64)
			if parseErr != nil || nextValue < 0 {
				return FrameTrailer{}, errors.New("invalid trailer next offset")
			}
			nextOffset = &nextValue
		}
	}
	if status != "ok" {
		return FrameTrailer{}, fmt.Errorf("trailer status not ok: %s", status)
	}
	if ts < 0 {
		return FrameTrailer{}, errors.New("trailer missing ts")
	}
	if !ValidHashToken(hashToken) {
		return FrameTrailer{}, errors.New("trailer missing or invalid hash token")
	}
	if fileHashToken != "" && !ValidHashToken(fileHashToken) {
		return FrameTrailer{}, errors.New("trailer invalid file hash token")
	}
	return FrameTrailer{
		FileID:         fileID,
		TS:             ts,
		HashToken:      hashToken,
		FileHashToken:  fileHashToken,
		ChecksumPrefix: prefix,
		Next:           nextOffset,
	}, nil
}

func splitTrailerPrefixAndHash(line string) (string, string, error) {
	idx := strings.LastIndex(line, " hash=")
	if idx <= 0 {
		return "", "", errors.New("trailer missing hash token")
	}
	prefix := line[:idx]
	hashToken := strings.TrimSpace(line[idx+len(" hash="):])
	if !ValidHashToken(hashToken) {
		return "", "", errors.New("trailer missing or invalid hash token")
	}
	return prefix, hashToken, nil
}

func ValidHashToken(raw string) bool {
	if raw == "" {
		return false
	}
	parts := strings.SplitN(raw, ":", 2)
	return len(parts) == 2 && parts[0] != "" && parts[1] != ""
}

func DecodePayloadReaderByComp(payload io.Reader, comp string) (io.ReadCloser, error) {
	switch comp {
	case "none":
		return io.NopCloser(payload), nil
	case codec.EncodingZstd, codec.EncodingLz4:
		reader, err := codec.WrapDecompressedReader(payload, comp)
		if err != nil {
			return nil, err
		}
		return reader, nil
	default:
		return nil, fmt.Errorf("unsupported compression mode: %s", comp)
	}
}
