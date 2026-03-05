package filexfer

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/zeebo/xxh3"
)

const defaultFileFrameLogicalSize int64 = 8 * 1024 * 1024

var logicalBufferPools sync.Map   // map[int]*sync.Pool
var frameCompressorPools sync.Map // map[int64]*sync.Pool

func FileHandler(w http.ResponseWriter, req *http.Request) {
	rawWriter := w
	limitState := currentFileStreamLimitState()
	limitedWriter := wrapLimitedResponseWriter(w, req.Context(), limitState)
	w = limitedWriter

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

	if req.URL.Query().Get("ack-bytes") != "" {
		http.Error(w, "ack-bytes is only supported on PUT /fs/file/{txferid}/{fid}/ack", http.StatusBadRequest)
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

	fd, fileRef, err := GetFile(txferID, fileID, fullPathRaw)
	if err != nil {
		writeLookupErr(w, err)
		return
	}
	defer fd.Close()

	_ = SetTransferFileState(txferID, fileID, TransferStateRunning)

	fileInfo, err := fd.Stat()
	if err != nil {
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

	comp, err := parseRequestedComp(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	requestChunkMax := min(windowLen, defaultFileFrameLogicalSize)
	logicalBufferSize := logicalBufferBucketSize(requestChunkMax)
	logicalBuffer, releaseLogicalBuffer, err := acquireLogicalBuffer(logicalBufferSize)
	if err != nil {
		http.Error(w, "failed to acquire logical frame buffer", http.StatusInternalServerError)
		return
	}
	defer releaseLogicalBuffer()

	adaptiveComp := comp == "adapt"
	activeMode := initialCompressionMode(comp)
	if mode, ok := GetTransferFileCompressionMode(txferID, fileID); ok {
		activeMode = clampCompressionModeForAccept(mode, comp)
	}
	activeComp := frameCompTokenForMode(activeMode)
	maxWSizeHint, err := maxRequestWireSizeHintBytes(comp, int64(logicalBufferSize))
	if err != nil {
		http.Error(w, "failed to compute frame wire-size hint", http.StatusInternalServerError)
		return
	}
	compressors, releaseCompressors, err := acquireFrameCompressor(maxWSizeHint)
	if err != nil {
		http.Error(w, "failed to initialize compressors", http.StatusInternalServerError)
		return
	}
	defer releaseCompressors()

	policy := NewCompressionPolicy()
	cursor := offset
	windowStart := offset
	windowWireTotal := int64(0)
	windowLogicalTotal := int64(0)
	windowFrames := 0
	windowTS0 := time.Now().UnixMilli()
	if !UpdateTransferFileHash(txferID, fileID, cursor, nil) {
		http.Error(w, "invalid file stream offset for file hash", http.StatusConflict)
		return
	}
	firstFrame := true
	for remaining := windowLen; remaining > 0; {
		chunkLogical := min(remaining, defaultFileFrameLogicalSize)

		prepareStart := time.Now()
		logicalChunk, framePayload, wireSize, chunkHash, err := prepareFramePayload(fd, compressors, activeMode, logicalBuffer, cursor, chunkLogical)
		if err != nil {
			http.Error(w, "failed to prepare frame payload", http.StatusInternalServerError)
			return
		}
		prepareLatency := time.Since(prepareStart)
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
			md := collectFileFrameMetadata(fileRef.Path, fileInfo)
			terminalMD = &md
		}
		var maxHint *int64
		if firstFrame {
			maxHint = &maxWSizeHint
		}
		writeStats, err := writeFrame(w, frameWriteArgs{
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
			FileHashes:   fileHashes,
			Next:         nextValue,
			Metadata:     terminalMD,
		})
		if err != nil {
			if errors.Is(err, errFileStreamTimeLimitExceeded) && !limitedWriter.wroteAnyBody() {
				http.Error(rawWriter, "file stream time limit exceeded", http.StatusGatewayTimeout)
			}
			return
		}
		windowFrames++
		windowLogicalTotal += chunkLogical
		windowWireTotal += wireSize
		if adaptiveComp {
			decision := policy.Decide(activeMode, CompressionMetrics{
				LogicalSize:    chunkLogical,
				WireSize:       wireSize,
				PrepareLatency: prepareLatency,
				WriteLatency:   writeStats.WriteLatency,
			})
			nextMode := clampCompressionModeForAccept(decision.Next, comp)
			if nextMode != activeMode {
				prevComp := frameCompTokenForMode(activeMode)
				nextComp := frameCompTokenForMode(nextMode)
				log.Printf(
					"filexfer frame tid=%s fid=%d switching compression %s->%s reason=%s ratio=%.3f prepare_over_write=%.3f",
					txferID,
					fileID,
					prevComp,
					nextComp,
					decision.Reason,
					decision.Ratio,
					decision.PrepareOverWrite,
				)
				activeMode = nextMode
				_ = SetTransferFileCompressionMode(txferID, fileID, activeMode)
				activeComp = frameCompTokenForMode(activeMode)
			}
		}

		cursor = nextOffset
		remaining -= chunkLogical
		firstFrame = false
	}
	windowTS1 := time.Now().UnixMilli()
	windowMS := windowTS1 - windowTS0
	logicalBps := 0.0
	wireBps := 0.0
	if windowMS > 0 {
		seconds := float64(windowMS) / 1000.0
		logicalBps = float64(windowLogicalTotal) / seconds
		wireBps = float64(windowWireTotal) / seconds
	}
	log.Printf(
		"filexfer window tid=%s fid=%d frames=%d offset=%d size=%d wsize=%d ts0=%d ts1=%d window_ms=%d logical=%s wire=%s",
		txferID,
		fileID,
		windowFrames,
		windowStart,
		windowLogicalTotal,
		windowWireTotal,
		windowTS0,
		windowTS1,
		windowMS,
		humanRate(logicalBps),
		humanRate(wireBps),
	)
}

func FileAckHandler(w http.ResponseWriter, req *http.Request) {
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
	ackBytes, ackTS, ackHashToken, ackProvided, err := parseAckBytesQuery(req.URL.Query().Get("ack-bytes"))
	if err != nil || !ackProvided {
		http.Error(w, "invalid query parameter: ack-bytes", http.StatusBadRequest)
		return
	}
	ackDeltaBytes, ackRecvMS, ackSyncMS, err := parseAckTelemetryQuery(req.URL.Query(), ackBytes)
	if err != nil {
		http.Error(w, "invalid query parameters: delta-bytes/recv-ms/sync-ms", http.StatusBadRequest)
		return
	}

	fileRef, err := GetFileRef(txferID, fileID, fullPathRaw)
	if err != nil {
		writeLookupErr(w, err)
		return
	}

	maxAck := fileRef.FileSize
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
		receiverMS := ackRecvMS + ackSyncMS
		receiverBps := 0.0
		recvBps := 0.0
		syncBps := 0.0
		if ackDeltaBytes > 0 {
			if receiverMS > 0 {
				receiverBps = float64(ackDeltaBytes) / (float64(receiverMS) / 1000.0)
			}
			if ackRecvMS > 0 {
				recvBps = float64(ackDeltaBytes) / (float64(ackRecvMS) / 1000.0)
			}
			if ackSyncMS > 0 {
				syncBps = float64(ackDeltaBytes) / (float64(ackSyncMS) / 1000.0)
			}
		}
		log.Printf(
			"filexfer ack tid=%s fid=%d ack_bytes=%d ack_hash=%s acked_server_ts=%d ack_recv_ts=%d lag_ms=%d delta_bytes=%d recv_ms=%d sync_ms=%d receiver_ms=%d receiver_throughput=%s recv_throughput=%s sync_throughput=%s",
			txferID,
			fileID,
			ackBytes,
			ackHashToken,
			ackTS,
			nowTS,
			lagMS,
			ackDeltaBytes,
			ackRecvMS,
			ackSyncMS,
			receiverMS,
			humanRate(receiverBps),
			humanRate(recvBps),
			humanRate(syncBps),
		)
	}
	w.WriteHeader(http.StatusNoContent)
}

func compressionRatio(logicalSize int64, wireSize int64) float64 {
	if wireSize <= 0 {
		return 0
	}
	return float64(logicalSize) / float64(wireSize)
}

func initialCompressionMode(comp string) CompressionMode {
	switch comp {
	case EncodingZstd:
		return CompressionModeZstdLevel1
	case EncodingLz4:
		return CompressionModeLz4
	case "none", EncodingIdentity:
		return CompressionModeNone
	default:
		return CompressionModeNone
	}
}

func parseRequestedComp(req *http.Request) (string, error) {
	raw := strings.ToLower(strings.TrimSpace(req.URL.Query().Get("comp")))
	switch raw {
	case "":
		// Backward compatibility for older clients.
		raw = SelectEncoding(req.Header.Get("Accept-Encoding"))
	}
	switch raw {
	case "", "adapt":
		return "adapt", nil
	case EncodingZstd:
		return EncodingZstd, nil
	case EncodingLz4:
		return EncodingLz4, nil
	case "none", EncodingIdentity:
		return "none", nil
	default:
		return "", errors.New("invalid query parameter: comp")
	}
}

func clampCompressionModeForAccept(mode CompressionMode, acceptedComp string) CompressionMode {
	switch acceptedComp {
	case EncodingZstd:
		return CompressionModeZstdLevel1
	case EncodingLz4:
		if mode == CompressionModeNone {
			return CompressionModeNone
		}
		return CompressionModeLz4
	case "none", EncodingIdentity:
		return CompressionModeNone
	default:
		return mode
	}
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

func parseAckTelemetryQuery(values url.Values, ackBytes int64) (deltaBytes int64, recvMS int64, syncMS int64, err error) {
	if ackBytes < 0 {
		return 0, 0, 0, nil
	}
	deltaRaw := strings.TrimSpace(values.Get("delta-bytes"))
	recvRaw := strings.TrimSpace(values.Get("recv-ms"))
	syncRaw := strings.TrimSpace(values.Get("sync-ms"))
	if deltaRaw == "" || recvRaw == "" || syncRaw == "" {
		return 0, 0, 0, errors.New("missing required ack telemetry")
	}
	deltaBytes, err = strconv.ParseInt(deltaRaw, 10, 64)
	if err != nil || deltaBytes < 0 {
		return 0, 0, 0, errors.New("invalid delta-bytes")
	}
	recvMS, err = strconv.ParseInt(recvRaw, 10, 64)
	if err != nil || recvMS < 0 {
		return 0, 0, 0, errors.New("invalid recv-ms")
	}
	syncMS, err = strconv.ParseInt(syncRaw, 10, 64)
	if err != nil || syncMS < 0 {
		return 0, 0, 0, errors.New("invalid sync-ms")
	}
	return deltaBytes, recvMS, syncMS, nil
}

func prepareFramePayload(fd *os.File, compressors *frameCompressor, mode CompressionMode, logicalBuffer []byte, offset int64, logicalSize int64) ([]byte, []byte, int64, xxh3.Uint128, error) {
	if logicalSize < 0 {
		return nil, nil, 0, xxh3.Uint128{}, errors.New("negative logical size")
	}
	if compressors == nil {
		return nil, nil, 0, xxh3.Uint128{}, errors.New("nil compressor context")
	}
	if logicalSize > int64(len(logicalBuffer)) {
		return nil, nil, 0, xxh3.Uint128{}, errors.New("logical frame size exceeds reusable buffer")
	}

	if _, err := fd.Seek(offset, io.SeekStart); err != nil {
		return nil, nil, 0, xxh3.Uint128{}, err
	}
	logical := logicalBuffer[:logicalSize]
	if _, err := io.ReadFull(fd, logical); err != nil {
		return nil, nil, 0, xxh3.Uint128{}, err
	}
	hash128 := xxh3.Hash128(logical)

	payload, err := compressors.Compress(logical, mode)
	if err != nil {
		return nil, nil, 0, xxh3.Uint128{}, err
	}
	return logical, payload, int64(len(payload)), hash128, nil
}

func acquireLogicalBuffer(size int) ([]byte, func(), error) {
	if size <= 0 {
		return nil, nil, errors.New("invalid logical buffer size")
	}
	pool := logicalBufferPool(size)
	raw := pool.Get()
	buf, ok := raw.([]byte)
	if !ok {
		return nil, nil, errors.New("logical buffer pool returned invalid type")
	}
	if cap(buf) < size {
		buf = make([]byte, size)
	}
	buf = buf[:size]
	return buf, func() {
		pool.Put(buf[:size])
	}, nil
}

func logicalBufferPool(size int) *sync.Pool {
	if existing, ok := logicalBufferPools.Load(size); ok {
		return existing.(*sync.Pool)
	}
	sz := size
	created := &sync.Pool{
		New: func() any {
			return make([]byte, sz)
		},
	}
	actual, _ := logicalBufferPools.LoadOrStore(size, created)
	return actual.(*sync.Pool)
}

func logicalBufferBucketSize(maxChunk int64) int {
	if maxChunk <= 4*1024 {
		return 4 * 1024
	}
	for _, bucket := range []int{
		16 * 1024,
		64 * 1024,
		256 * 1024,
		1 * 1024 * 1024,
		2 * 1024 * 1024,
		4 * 1024 * 1024,
		8 * 1024 * 1024,
	} {
		if maxChunk <= int64(bucket) {
			return bucket
		}
	}
	return 8 * 1024 * 1024
}

type frameCompressor struct {
	buf         bytes.Buffer
	zstdEnc     *zstd.Encoder
	zstdFastEnc *zstd.Encoder
	lz4Enc      *lz4.Writer
	poolKey     int64
}

func acquireFrameCompressor(maxWireSizeHint int64) (*frameCompressor, func(), error) {
	poolKey := compressorPoolKey(maxWireSizeHint)
	pool := frameCompressorPool(poolKey)
	raw := pool.Get()
	c, ok := raw.(*frameCompressor)
	if !ok || c == nil {
		c = &frameCompressor{}
	}
	c.poolKey = poolKey
	c.buf.Reset()
	if poolKey > 0 && poolKey <= int64(^uint(0)>>1) {
		wantCap := int(poolKey)
		if c.buf.Cap() < wantCap {
			c.buf.Grow(wantCap - c.buf.Cap())
		}
	}
	release := func() {
		c.resetForPool()
		pool.Put(c)
	}
	return c, release, nil
}

func compressorPoolKey(maxWireSizeHint int64) int64 {
	if maxWireSizeHint <= 0 {
		return 0
	}
	return ceilingMaxWSizeBucketBytes(maxWireSizeHint)
}

func frameCompressorPool(poolKey int64) *sync.Pool {
	if existing, ok := frameCompressorPools.Load(poolKey); ok {
		return existing.(*sync.Pool)
	}
	key := poolKey
	created := &sync.Pool{
		New: func() any {
			c := &frameCompressor{poolKey: key}
			if key > 0 && key <= int64(^uint(0)>>1) {
				c.buf.Grow(int(key))
			}
			return c
		},
	}
	actual, _ := frameCompressorPools.LoadOrStore(poolKey, created)
	return actual.(*sync.Pool)
}

func (c *frameCompressor) resetForPool() {
	if c == nil {
		return
	}
	c.buf.Reset()
}

func (c *frameCompressor) Compress(data []byte, mode CompressionMode) ([]byte, error) {
	switch mode {
	case CompressionModeNone:
		return data, nil
	case CompressionModeZstdDefault:
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
	case CompressionModeZstdLevel1:
		c.buf.Reset()
		if c.zstdFastEnc == nil {
			enc, err := zstd.NewWriter(&c.buf, zstd.WithEncoderLevel(zstd.SpeedFastest))
			if err != nil {
				return nil, err
			}
			c.zstdFastEnc = enc
		} else {
			c.zstdFastEnc.Reset(&c.buf)
		}
		if _, err := c.zstdFastEnc.Write(data); err != nil {
			return nil, err
		}
		if err := c.zstdFastEnc.Close(); err != nil {
			return nil, err
		}
		return c.buf.Bytes(), nil
	case CompressionModeLz4:
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

func maxRequestWireSizeHintBytes(requestedComp string, logicalSize int64) (int64, error) {
	if requestedComp != "adapt" {
		return maxFrameWireSizeHintBytes(requestedComp, logicalSize)
	}
	maxWire := int64(0)
	for _, comp := range []string{"none", EncodingLz4, EncodingZstd} {
		size, err := maxEncodedFrameSizeBytes(comp, logicalSize)
		if err != nil {
			return 0, err
		}
		if size > maxWire {
			maxWire = size
		}
	}
	return ceilingMaxWSizeBucketBytes(maxWire), nil
}
