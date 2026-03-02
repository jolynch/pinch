package filexfer

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/zeebo/xxh3"
)

const defaultFileFrameLogicalSize int64 = 8 * 1024 * 1024
const zstdMinCompressionRatio = 0.9

func FileHandler(w http.ResponseWriter, req *http.Request) {
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

	ackRaw := req.URL.Query().Get("ack-bytes")
	ackBytes, ackTS, ackHashToken, ackProvided, err := parseAckBytesQuery(ackRaw)
	if err != nil {
		http.Error(w, "invalid query parameter: ack-bytes", http.StatusBadRequest)
		return
	}

	fullPathRaw := req.URL.Query().Get("path")
	if fullPathRaw == "" {
		http.Error(w, "missing required query parameter: path", http.StatusBadRequest)
		return
	}
	offset, logicalSizeLimit, err := parseChunkWindow(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	transfer, ok := GetTransfer(txferID)
	if !ok {
		http.Error(w, "transfer not found", http.StatusNotFound)
		return
	}
	if fileID >= uint64(len(transfer.State)) {
		http.Error(w, "file id out of range", http.StatusNotFound)
		return
	}

	if ackProvided {
		maxAck := transfer.FileSize[fileID]
		ackTarget := ackBytes
		if ackTarget > maxAck {
			ackTarget = maxAck
		}
		if ackTarget < 0 {
			ackTarget = 0
		}
		if ackBytes >= 0 && ackTarget == maxAck {
			if ackHashToken == "" {
				http.Error(w, "missing final ack hash token", http.StatusBadRequest)
				return
			}
			if !VerifyTransferFileHash(txferID, fileID, maxAck, ackHashToken) {
				http.Error(w, "final ack hash token mismatch", http.StatusConflict)
				return
			}
		}
		if ok := AcknowledgeTransferFile(txferID, fileID, ackBytes); !ok {
			http.Error(w, "failed to acknowledge file progress", http.StatusInternalServerError)
			return
		}
		if ackBytes >= 0 {
			nowTS := time.Now().UnixMilli()
			lagMS := nowTS - ackTS
			throughput := 0.0
			if lagMS > 0 {
				throughput = float64(ackBytes) / (float64(lagMS) / 1000.0)
			}
			log.Printf(
				"filexfer ack tid=%s fid=%d ack_bytes=%d ack_hash=%s acked_server_ts=%d ack_recv_ts=%d lag_ms=%d ack_throughput=%s",
				txferID,
				fileID,
				ackBytes,
				ackHashToken,
				ackTS,
				nowTS,
				lagMS,
				humanRate(throughput),
			)
		}
		if ackBytes == -1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if offset == 0 && logicalSizeLimit == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}

	fullPath := filepath.Clean(fullPathRaw)
	if !filepath.IsAbs(fullPath) {
		http.Error(w, "path must be absolute", http.StatusBadRequest)
		return
	}
	if !pathWithinRoot(transfer.Directory, fullPath) {
		http.Error(w, "path must be within transfer root", http.StatusForbidden)
		return
	}

	computedDigest := xxh3.Hash128([]byte(fullPath))
	if transfer.PathHash[fileID] != computedDigest {
		http.Error(w, "file path digest mismatch", http.StatusForbidden)
		return
	}

	_ = SetTransferFileState(txferID, fileID, TransferStateRunning)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "file not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to stat file", http.StatusInternalServerError)
		return
	}
	windowLen := fileInfo.Size()
	if windowLen < 0 {
		http.Error(w, "invalid file size", http.StatusInternalServerError)
		return
	}
	if offset > windowLen {
		http.Error(w, "offset out of range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	windowLen -= offset
	if logicalSizeLimit >= 0 && logicalSizeLimit < windowLen {
		windowLen = logicalSizeLimit
	}

	comp := SelectEncoding(req.Header.Get("Accept-Encoding"))
	if comp == EncodingIdentity {
		comp = "none"
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Add("Vary", "Accept-Encoding")
	if comp == EncodingZstd || comp == EncodingLz4 {
		w.Header().Set("Content-Encoding", comp)
	}

	fd, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "file not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to open file", http.StatusInternalServerError)
		return
	}
	defer fd.Close()

	compressors, err := newFrameCompressor()
	if err != nil {
		http.Error(w, "failed to initialize compressors", http.StatusInternalServerError)
		return
	}
	defer compressors.Close()

	activeComp := comp
	maxWSizeHint, err := maxFrameWireSizeHintBytes(activeComp, defaultFileFrameLogicalSize)
	if err != nil {
		http.Error(w, "failed to compute frame wire-size hint", http.StatusInternalServerError)
		return
	}
	cursor := offset
	if !UpdateTransferFileHash(txferID, fileID, cursor, nil) {
		http.Error(w, "invalid file stream offset for file hash", http.StatusConflict)
		return
	}
	firstFrame := true
	for remaining := windowLen; remaining > 0; {
		chunkLogical := remaining
		if chunkLogical > defaultFileFrameLogicalSize {
			chunkLogical = defaultFileFrameLogicalSize
		}

		logicalChunk, framePayload, wireSize, chunkHash, err := prepareFramePayload(fd, compressors, activeComp, cursor, chunkLogical)
		if err != nil {
			http.Error(w, "failed to prepare frame payload", http.StatusInternalServerError)
			return
		}
		if !UpdateTransferFileHash(txferID, fileID, cursor, logicalChunk) {
			http.Error(w, "failed to update file hash state", http.StatusInternalServerError)
			return
		}

		headerTS := time.Now().UnixMilli()
		hash128Bytes := chunkHash.Bytes()
		hashHex := hex.EncodeToString(hash128Bytes[:])
		headerHash := "xxh128:" + hashHex
		nextOffset := cursor + chunkLogical
		isTerminalResponseChunk := remaining == chunkLogical
		isTerminalFileChunk := nextOffset == fileInfo.Size()
		nextValue := nextOffset
		if isTerminalResponseChunk {
			nextValue = 0
		}
		trailerTS := time.Now().UnixMilli()
		var (
			fileHashes []string
			terminalMD *fileFrameMetadata
		)
		if isTerminalFileChunk {
			fileHashToken, ok := FinalizeTransferFileHash(txferID, fileID)
			if !ok {
				http.Error(w, "failed to finalize file hash state", http.StatusInternalServerError)
				return
			}
			fileHashes = []string{fileHashToken}
		}
		if isTerminalResponseChunk {
			md := collectFileFrameMetadata(fullPath, fileInfo)
			terminalMD = &md
		}
		var maxHint *int64
		if firstFrame {
			maxHint = &maxWSizeHint
		}
		if err := writeFrame(w, frameWriteArgs{
			FileID:       fileID,
			Offset:       cursor,
			Size:         chunkLogical,
			WSize:        wireSize,
			Comp:         activeComp,
			Enc:          "none",
			HeaderHash:   headerHash,
			MaxWSizeHint: maxHint,
			HeaderTS:     headerTS,
			Payload:      framePayload,
			TrailerTS:    trailerTS,
			HashTokens:   []string{headerHash},
			FileHashes:   fileHashes,
			Next:         nextValue,
			Metadata:     terminalMD,
		}); err != nil {
			return
		}
		frameMS := trailerTS - headerTS
		logicalBps := 0.0
		wireBps := 0.0
		if frameMS > 0 {
			seconds := float64(frameMS) / 1000.0
			logicalBps = float64(chunkLogical) / seconds
			wireBps = float64(wireSize) / seconds
		}
		log.Printf(
			"filexfer frame tid=%s fid=%d comp=%s offset=%d size=%d wsize=%d ts0=%d ts1=%d frame_ms=%d logical=%s wire=%s",
			txferID,
			fileID,
			activeComp,
			cursor,
			chunkLogical,
			wireSize,
			headerTS,
			trailerTS,
			frameMS,
			humanRate(logicalBps),
			humanRate(wireBps),
		)
		if shouldFallbackToNone(activeComp, chunkLogical, wireSize) {
			log.Printf(
				"filexfer frame tid=%s fid=%d switching compression %s->none ratio=%.3f threshold=%.3f",
				txferID,
				fileID,
				activeComp,
				compressionRatio(chunkLogical, wireSize),
				zstdMinCompressionRatio,
			)
			activeComp = "none"
		}

		cursor = nextOffset
		remaining -= chunkLogical
		firstFrame = false
	}
}

func compressionRatio(logicalSize int64, wireSize int64) float64 {
	if wireSize <= 0 {
		return 0
	}
	return float64(logicalSize) / float64(wireSize)
}

func shouldFallbackToNone(comp string, logicalSize int64, wireSize int64) bool {
	if comp != EncodingZstd {
		return false
	}
	return compressionRatio(logicalSize, wireSize) < zstdMinCompressionRatio
}

func parseAckBytesQuery(raw string) (ackBytes int64, ackTS int64, ackHashToken string, provided bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, "", false, nil
	}
	if raw == "-1" {
		return -1, 0, "", true, nil
	}
	parts := strings.SplitN(raw, "@", 3)
	if len(parts) < 2 {
		return 0, 0, "", true, errors.New("invalid ack format")
	}
	ackBytes, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ackBytes < 0 {
		return 0, 0, "", true, errors.New("invalid ack bytes")
	}
	ackTS, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || ackTS < 0 {
		return 0, 0, "", true, errors.New("invalid ack timestamp")
	}
	if len(parts) == 3 {
		ackHashToken = strings.TrimSpace(parts[2])
		if !validHashToken(ackHashToken) {
			return 0, 0, "", true, errors.New("invalid ack hash token")
		}
	}
	return ackBytes, ackTS, ackHashToken, true, nil
}

func prepareFramePayload(fd *os.File, compressors *frameCompressor, comp string, offset int64, logicalSize int64) ([]byte, []byte, int64, xxh3.Uint128, error) {
	if logicalSize < 0 {
		return nil, nil, 0, xxh3.Uint128{}, errors.New("negative logical size")
	}
	if compressors == nil {
		return nil, nil, 0, xxh3.Uint128{}, errors.New("nil compressor context")
	}

	if _, err := fd.Seek(offset, io.SeekStart); err != nil {
		return nil, nil, 0, xxh3.Uint128{}, err
	}
	logical := make([]byte, logicalSize)
	if _, err := io.ReadFull(fd, logical); err != nil {
		return nil, nil, 0, xxh3.Uint128{}, err
	}
	hash128 := xxh3.Hash128(logical)

	payload, err := compressors.Compress(logical, comp)
	if err != nil {
		return nil, nil, 0, xxh3.Uint128{}, err
	}
	return logical, payload, int64(len(payload)), hash128, nil
}

type frameCompressor struct {
	buf     bytes.Buffer
	zstdEnc *zstd.Encoder
	lz4Enc  *lz4.Writer
}

func newFrameCompressor() (*frameCompressor, error) {
	return &frameCompressor{}, nil
}

func (c *frameCompressor) Close() error {
	if c == nil || c.zstdEnc == nil {
		return nil
	}
	return c.zstdEnc.Close()
}

func (c *frameCompressor) Compress(data []byte, comp string) ([]byte, error) {
	switch comp {
	case "none":
		return data, nil
	case EncodingZstd:
		c.buf.Reset()
		if c.zstdEnc == nil {
			enc, err := zstd.NewWriter(&c.buf)
			if err != nil {
				return nil, err
			}
			c.zstdEnc = enc
		} else {
			c.zstdEnc.Reset(&c.buf)
		}
		if _, err := c.zstdEnc.Write(data); err != nil {
			return nil, err
		}
		if err := c.zstdEnc.Close(); err != nil {
			return nil, err
		}
		return c.buf.Bytes(), nil
	case EncodingLz4:
		c.buf.Reset()
		if c.lz4Enc == nil {
			c.lz4Enc = lz4.NewWriter(&c.buf)
		} else {
			c.lz4Enc.Reset(&c.buf)
		}
		if _, err := c.lz4Enc.Write(data); err != nil {
			return nil, err
		}
		if err := c.lz4Enc.Close(); err != nil {
			return nil, err
		}
		return c.buf.Bytes(), nil
	default:
		return nil, errors.New("unsupported compression mode")
	}
}

func parseChunkWindow(req *http.Request) (int64, int64, error) {
	offset := int64(0)
	if raw := req.URL.Query().Get("offset"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			return 0, 0, errors.New("invalid query parameter: offset")
		}
		offset = v
	}

	size := int64(-1)
	if raw := req.URL.Query().Get("size"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			return 0, 0, errors.New("invalid query parameter: size")
		}
		size = v
	}
	return offset, size, nil
}

func encodeSingleFramePayload(data []byte, comp string) ([]byte, error) {
	switch comp {
	case "none":
		return data, nil
	case EncodingZstd, EncodingLz4:
		var buf bytes.Buffer
		out, closeEncoded, _, err := WrapCompressedWriter(&buf, comp)
		if err != nil {
			return nil, err
		}
		if _, err := out.Write(data); err != nil {
			return nil, err
		}
		if err := closeEncoded(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, errors.New("unsupported compression mode")
	}
}

func pathWithinRoot(root string, p string) bool {
	root = filepath.Clean(root)
	p = filepath.Clean(p)
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return false
	}
	return !filepath.IsAbs(rel)
}
