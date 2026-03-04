package filexfer

import (
	"fmt"
	"net/http"
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zeebo/xxh3"
)

type fileFrameMetadata struct {
	Size    int64
	MtimeNS int64
	Mode    string
	UID     string
	GID     string
	User    string
	Group   string
}

func collectFileFrameMetadata(path string, info os.FileInfo) fileFrameMetadata {
	meta := fileFrameMetadata{
		Size:    info.Size(),
		MtimeNS: info.ModTime().UnixNano(),
		Mode:    info.Mode().String(),
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

func (m fileFrameMetadata) trailerTokens() []string {
	return []string{
		fmt.Sprintf("meta:size=%d", m.Size),
		fmt.Sprintf("meta:mtime_ns=%d", m.MtimeNS),
		fmt.Sprintf("meta:mode=%s", strings.ReplaceAll(m.Mode, " ", "_")),
		fmt.Sprintf("meta:uid=%s", m.UID),
		fmt.Sprintf("meta:gid=%s", m.GID),
		fmt.Sprintf("meta:user=%s", strings.ReplaceAll(m.User, " ", "_")),
		fmt.Sprintf("meta:group=%s", strings.ReplaceAll(m.Group, " ", "_")),
	}
}

type frameWriteArgs struct {
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
	Metadata     *fileFrameMetadata
}

type frameWriteStats struct {
	WriteLatency      time.Duration
	WireThroughputBps float64
}

func writeFrame(w http.ResponseWriter, args frameWriteArgs) (frameWriteStats, error) {
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
		return frameWriteStats{}, err
	}

	if len(args.Payload) > 0 {
		if _, err := w.Write(args.Payload); err != nil {
			return frameWriteStats{}, err
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
		return frameWriteStats{}, err
	}

	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
	writeLatency := time.Since(start)
	wireBps := 0.0
	if writeLatency > 0 && args.WSize > 0 {
		wireBps = float64(args.WSize) / writeLatency.Seconds()
	}
	return frameWriteStats{
		WriteLatency:      writeLatency,
		WireThroughputBps: wireBps,
	}, nil
}
