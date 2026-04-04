package ftcp

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/jolynch/pinch/internal/filexfer/encoding"
	"github.com/zeebo/xxh3"
)

const checksumReadBufferSize int64 = 1 * 1024 * 1024

type cxsumItem struct {
	FileID     uint64
	Path       string
	Offset     int64
	Size       int64
	HasSize    bool
	Algorithms []string
}

type cxsumRequest struct {
	TransferID string
	Items      []cxsumItem
}

func parseCXSUMRequest(req Request) (cxsumRequest, error) {
	if req.Verb != VerbCXSUM {
		return cxsumRequest{}, protocolErr{code: "BAD_COMMAND", message: "not CXSUM"}
	}
	if len(req.Params) < 2 {
		return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid CXSUM arguments"}
	}
	txferID := req.Params[0]["txferid"]
	if txferID == "" {
		return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing transfer id"}
	}
	items := make([]cxsumItem, 0, len(req.Params)-1)
	for _, p := range req.Params[1:] {
		fileID, err := strconv.ParseUint(p["fid"], 10, 64)
		if err != nil {
			return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid file id"}
		}
		path := p["path"]
		if path == "" {
			return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing path"}
		}
		offset := int64(0)
		if raw := strings.TrimSpace(p["offset"]); raw != "" {
			offset, err = strconv.ParseInt(raw, 10, 64)
			if err != nil || offset < 0 {
				return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid checksum offset"}
			}
		}
		size := int64(0)
		hasSize := false
		if raw := strings.TrimSpace(p["size"]); raw != "" {
			size, err = strconv.ParseInt(raw, 10, 64)
			if err != nil || size < 0 {
				return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid checksum size"}
			}
			hasSize = true
		}
		algorithms, err := parseRequestedChecksums([]string{p["algo"]})
		if err != nil {
			return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid checksum parameter"}
		}
		items = append(items, cxsumItem{
			FileID:     fileID,
			Path:       path,
			Offset:     offset,
			Size:       size,
			HasSize:    hasSize,
			Algorithms: algorithms,
		})
	}
	return cxsumRequest{
		TransferID: txferID,
		Items:      items,
	}, nil
}

func handleCXSUM(_ context.Context, req Request, out io.Writer, deps Deps) error {
	parsed, err := parseCXSUMRequest(req)
	if err != nil {
		return err
	}
	buf := make([]byte, checksumReadBufferSize)
	for _, item := range parsed.Items {
		fd, fileRef, err := deps.GetFile(parsed.TransferID, item.FileID, item.Path)
		if err != nil {
			return mapLookupError(err)
		}

		fileInfo, statErr := fd.Stat()
		if statErr != nil {
			_ = fd.Close()
			return protocolErr{code: "INTERNAL", message: "failed to stat file"}
		}
		fileSize := fileInfo.Size()
		if item.Offset > fileSize {
			_ = fd.Close()
			return protocolErr{code: "BAD_REQUEST", message: "checksum offset beyond eof"}
		}
		rangeSize := fileSize - item.Offset
		if item.HasSize {
			if item.Offset+item.Size > fileSize {
				_ = fd.Close()
				return protocolErr{code: "BAD_REQUEST", message: "checksum range beyond eof"}
			}
			rangeSize = item.Size
		}
		fileHashes, hashErr := hashChecksumRange(fd, item.Offset, rangeSize, item.Algorithms, buf)
		_ = fd.Close()
		if hashErr != nil {
			return protocolErr{code: "INTERNAL", message: "failed to checksum file range"}
		}

		headerHash := "none:0"
		if len(fileHashes) > 0 {
			headerHash = fileHashes[0]
		}
		metadata := encoding.CollectFileFrameMetadata(fileRef.Path, fileInfo)
		headerTS := time.Now().UnixMilli()
		trailerTS := time.Now().UnixMilli()
		if _, err := encoding.WriteFrame(out, encoding.WriteArgs{
			FileID:     item.FileID,
			Offset:     item.Offset,
			Size:       rangeSize,
			WSize:      0,
			Comp:       "none",
			HeaderHash: headerHash,
			HeaderTS:   headerTS,
			TrailerTS:  trailerTS,
			FileHashes: fileHashes,
			Next:       0,
			Metadata:   &metadata,
		}); err != nil {
			return err
		}
	}
	return nil
}

func hashChecksumRange(fd io.ReaderAt, offset int64, size int64, algorithms []string, buf []byte) ([]string, error) {
	full128 := xxh3.New128()
	full64 := xxh3.New()
	if size == 0 {
		return finalChecksumTokens(algorithms, full128, full64), nil
	}
	if len(buf) == 0 {
		buf = make([]byte, checksumReadBufferSize)
	}
	reader := io.NewSectionReader(fd, offset, size)
	remaining := size
	for remaining > 0 {
		chunk := int64(len(buf))
		if remaining < chunk {
			chunk = remaining
		}
		n, err := io.ReadFull(reader, buf[:chunk])
		if n > 0 {
			part := buf[:n]
			_, _ = full128.Write(part)
			_, _ = full64.Write(part)
			remaining -= int64(n)
		}
		if err == nil {
			continue
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		return nil, err
	}
	if remaining != 0 {
		return nil, fmt.Errorf("short checksum read")
	}
	return finalChecksumTokens(algorithms, full128, full64), nil
}

func parseRequestedChecksums(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return []string{"xxh128"}, nil
	}
	set := make(map[string]struct{})
	for _, token := range raw {
		for _, part := range strings.Split(token, ",") {
			name := strings.ToLower(strings.TrimSpace(part))
			if name == "" {
				continue
			}
			switch name {
			case "none", "xxh128", "xxh64":
				set[name] = struct{}{}
			default:
				return nil, fmt.Errorf("unsupported checksum")
			}
		}
	}
	if len(set) == 0 {
		return []string{"xxh128"}, nil
	}
	if _, ok := set["none"]; ok && len(set) == 1 {
		return nil, nil
	}
	delete(set, "none")

	out := make([]string, 0, len(set))
	if _, ok := set["xxh128"]; ok {
		out = append(out, "xxh128")
	}
	if _, ok := set["xxh64"]; ok {
		out = append(out, "xxh64")
	}
	return out, nil
}

func finalChecksumTokens(algorithms []string, full128 *xxh3.Hasher128, full64 *xxh3.Hasher) []string {
	tokens := make([]string, 0, len(algorithms))
	for _, name := range algorithms {
		switch name {
		case "xxh128":
			tokens = append(tokens, encoding.FormatXXH128HashToken(full128.Sum128()))
		case "xxh64":
			tokens = append(tokens, encoding.FormatXXH64HashToken(full64.Sum64()))
		}
	}
	return tokens
}
