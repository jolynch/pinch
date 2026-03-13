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

const defaultChecksumWindowSize int64 = 64 * 1024 * 1024
const checksumReadBufferSize int64 = 1 * 1024 * 1024

type cxsumRequest struct {
	TransferID   string
	FileID       uint64
	WindowSize   int64
	ChecksumsCSV string
	Path         string
}

func parseCXSUMRequest(req Request) (cxsumRequest, error) {
	if req.Verb != VerbCXSUM {
		return cxsumRequest{}, protocolErr{code: "BAD_COMMAND", message: "not CXSUM"}
	}
	if len(req.Params) != 1 {
		return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid CXSUM arguments"}
	}
	p := req.Params[0]
	txferID := p["txferid"]
	if txferID == "" {
		return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing transfer id"}
	}
	fileID, err := strconv.ParseUint(p["fid"], 10, 64)
	if err != nil {
		return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid file id"}
	}
	windowSize, err := strconv.ParseInt(p["window-size"], 10, 64)
	if err != nil || windowSize <= 0 {
		return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid window size"}
	}
	path := p["path"]
	if path == "" {
		return cxsumRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing path"}
	}
	return cxsumRequest{
		TransferID:   txferID,
		FileID:       fileID,
		WindowSize:   windowSize,
		ChecksumsCSV: p["checksums-csv"],
		Path:         path,
	}, nil
}

func handleCXSUM(_ context.Context, req Request, out io.Writer, deps Deps) error {
	parsed, err := parseCXSUMRequest(req)
	if err != nil {
		return err
	}
	fd, fileRef, err := deps.GetFile(parsed.TransferID, parsed.FileID, parsed.Path)
	if err != nil {
		return mapLookupError(err)
	}
	defer fd.Close()

	fileInfo, err := fd.Stat()
	if err != nil {
		return protocolErr{code: "INTERNAL", message: "failed to stat file"}
	}
	fileSize := fileInfo.Size()
	if fileSize < 0 {
		return protocolErr{code: "INTERNAL", message: "invalid file size"}
	}
	windowSize, err := parseChecksumWindowSize(strconv.FormatInt(parsed.WindowSize, 10), fileSize)
	if err != nil {
		return protocolErr{code: "BAD_REQUEST", message: "invalid window size"}
	}
	algorithms, err := parseRequestedChecksums([]string{parsed.ChecksumsCSV})
	if err != nil {
		return protocolErr{code: "BAD_REQUEST", message: "invalid checksum parameter"}
	}

	metadata := encoding.CollectFileFrameMetadata(fileRef.Path, fileInfo)
	full128 := xxh3.New128()
	full64 := xxh3.New()

	if fileSize == 0 {
		fileHashes := finalChecksumTokens(algorithms, full128, full64)
		headerTS := time.Now().UnixMilli()
		trailerTS := time.Now().UnixMilli()
		headerHash := "none:0"
		if len(fileHashes) > 0 {
			headerHash = fileHashes[0]
		}
		_, err := encoding.WriteFrame(out, encoding.WriteArgs{
			FileID:     parsed.FileID,
			Offset:     0,
			Size:       0,
			WSize:      0,
			Comp:       "none",
			Enc:        "none",
			HeaderHash: headerHash,
			HeaderTS:   headerTS,
			TrailerTS:  trailerTS,
			FileHashes: fileHashes,
			Next:       0,
			Metadata:   &metadata,
		})
		if err != nil {
			return err
		}
		return nil
	}

	bufSize := checksumReadBufferSize
	if windowSize < bufSize {
		bufSize = windowSize
	}
	if bufSize <= 0 {
		bufSize = checksumReadBufferSize
	}
	buf := make([]byte, bufSize)
	cursor := int64(0)
	for cursor < fileSize {
		chunkSize := fileSize - cursor
		if chunkSize > windowSize {
			chunkSize = windowSize
		}

		reader := io.NewSectionReader(fd, cursor, chunkSize)
		remaining := chunkSize
		for remaining > 0 {
			n, readErr := reader.Read(buf)
			if n > 0 {
				part := buf[:n]
				_, _ = full128.Write(part)
				_, _ = full64.Write(part)
				remaining -= int64(n)
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return protocolErr{code: "INTERNAL", message: "failed to read file chunk"}
			}
		}
		if remaining != 0 {
			return protocolErr{code: "INTERNAL", message: "failed to read complete file chunk"}
		}

		rollingFileHashes := finalChecksumTokens(algorithms, full128, full64)
		nextOffset := cursor + chunkSize
		isTerminal := nextOffset == fileSize
		nextValue := nextOffset
		fileHashes := rollingFileHashes
		var meta *encoding.FileFrameMetadata
		if isTerminal {
			nextValue = 0
			meta = &metadata
		}
		headerHash := "none:0"
		if len(fileHashes) > 0 {
			headerHash = fileHashes[0]
		}

		headerTS := time.Now().UnixMilli()
		trailerTS := time.Now().UnixMilli()
		_, err := encoding.WriteFrame(out, encoding.WriteArgs{
			FileID:     parsed.FileID,
			Offset:     cursor,
			Size:       chunkSize,
			WSize:      0,
			Comp:       "none",
			Enc:        "none",
			HeaderHash: headerHash,
			HeaderTS:   headerTS,
			TrailerTS:  trailerTS,
			FileHashes: fileHashes,
			Next:       nextValue,
			Metadata:   meta,
		})
		if err != nil {
			return err
		}
		cursor = nextOffset
	}
	return nil
}

func parseChecksumWindowSize(raw string, fileSize int64) (int64, error) {
	if raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v <= 0 {
			return 0, fmt.Errorf("invalid window size")
		}
		return v, nil
	}
	if fileSize <= 0 {
		return defaultChecksumWindowSize, nil
	}
	if fileSize < defaultChecksumWindowSize {
		return fileSize, nil
	}
	return defaultChecksumWindowSize, nil
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
