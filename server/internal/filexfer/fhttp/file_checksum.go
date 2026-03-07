package fhttp

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/xxh3"
)

const defaultChecksumWindowSize int64 = 64 * 1024 * 1024
const checksumReadBufferSize int64 = 1 * 1024 * 1024

func FileChecksumHandler(w http.ResponseWriter, req *http.Request) {
	txferID := req.PathValue("txferid")
	if txferID == "" {
		http.Error(w, "missing required path parameter: txferid", http.StatusBadRequest)
		return
	}
	fileIDRaw := req.PathValue("fid")
	if fileIDRaw == "" {
		http.Error(w, "missing required path parameter: fid", http.StatusBadRequest)
		return
	}
	fileID, err := strconv.ParseUint(fileIDRaw, 10, 64)
	if err != nil {
		http.Error(w, "invalid path parameter: fid", http.StatusBadRequest)
		return
	}
	fullPathRaw := req.URL.Query().Get("path")
	if fullPathRaw == "" {
		http.Error(w, "missing required query parameter: path", http.StatusBadRequest)
		return
	}

	fd, fileRef, err := GetFile(txferID, fileID, fullPathRaw)
	if err != nil {
		writeLookupErr(w, err)
		return
	}
	defer fd.Close()

	fileInfo, err := fd.Stat()
	if err != nil {
		http.Error(w, "failed to stat file", http.StatusInternalServerError)
		return
	}
	fileSize := fileInfo.Size()
	if fileSize < 0 {
		http.Error(w, "invalid file size", http.StatusInternalServerError)
		return
	}

	windowSize, err := parseChecksumWindowSize(req.URL.Query().Get("window-size"), fileSize)
	if err != nil {
		http.Error(w, "invalid query parameter: window-size", http.StatusBadRequest)
		return
	}
	algorithms, err := parseRequestedChecksums(req.URL.Query()["checksum"])
	if err != nil {
		http.Error(w, "invalid query parameter: checksum", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Add("Vary", "Accept-Encoding")

	metadata := collectFileFrameMetadata(fileRef.Path, fileInfo)
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
		if _, err := writeFrame(w, frameWriteArgs{
			FileID:     fileID,
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
		}); err != nil {
			return
		}
		return
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
			n, err := reader.Read(buf)
			if n > 0 {
				part := buf[:n]
				_, _ = full128.Write(part)
				_, _ = full64.Write(part)
				remaining -= int64(n)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, "failed to read file chunk", http.StatusInternalServerError)
				return
			}
		}
		if remaining != 0 {
			http.Error(w, "failed to read complete file chunk", http.StatusInternalServerError)
			return
		}

		rollingFileHashes := finalChecksumTokens(algorithms, full128, full64)
		nextOffset := cursor + chunkSize
		isTerminal := nextOffset == fileSize
		nextValue := nextOffset
		fileHashes := rollingFileHashes
		var meta *fileFrameMetadata
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
		if _, err := writeFrame(w, frameWriteArgs{
			FileID:     fileID,
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
		}); err != nil {
			return
		}
		cursor = nextOffset
	}
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
			tokens = append(tokens, formatXXH128HashToken(full128.Sum128()))
		case "xxh64":
			tokens = append(tokens, formatXXH64HashToken(full64.Sum64()))
		}
	}
	return tokens
}
