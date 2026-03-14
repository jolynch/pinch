package ftcp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jolynch/pinch/internal/filexfer/encoding"
	"github.com/jolynch/pinch/internal/filexfer/limit"
	"github.com/jolynch/pinch/internal/filexfer/policy"
	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"
)

const defaultFileFrameLogicalSize int64 = 8 * 1024 * 1024
const defaultMaxLinuxPipeSizeBytes int64 = 1 * 1024 * 1024
const defaultCompressedFrameBufferBytes = 8 * 1024 * 1024
const maxCompressedFrameBufferPoolBytes = 32 * 1024 * 1024
const placeholderHeaderHashToken = "xxh128:00000000000000000000000000000000"
const loadStrategyFast = "fast"
const loadStrategyGentle = "gentle"

var logicalBufferPools sync.Map
var compressedFrameBufferPool sync.Pool
var pipeMaxSizeOnce sync.Once
var pipeMaxSizeBytes int64 = defaultMaxLinuxPipeSizeBytes

type sendItem struct {
	FileID uint64
	Offset int64
	Size   int64
	Comp   string
	Path   string
	Mode   string
}

type sendRequest struct {
	TransferID string
	Items      []sendItem
}

type frameStreamArgs struct {
	FileID        uint64
	Offset        int64
	FrameSize     int64
	Comp          string
	MaxWSizeHint  *int64
	HeaderTS      int64
	Next          int64
	IsTerminal    bool
	TerminalMD    *encoding.FileFrameMetadata
	WindowHasher  *xxh3.Hasher128
	Output        io.Writer
	PipeSizeBytes int
	DirectIO      bool
}

type frameStreamStats struct {
	LogicalSize     int64
	WireSize        int64
	PrepareLatency  time.Duration
	WriteLatency    time.Duration
	NextOffset      int64
	WindowHashToken string
}

func parseSENDRequest(req Request) (sendRequest, error) {
	if req.Verb != VerbSEND {
		return sendRequest{}, protocolErr{code: "BAD_COMMAND", message: "not SEND"}
	}
	if len(req.Params) < 2 {
		return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "SEND requires at least one item"}
	}
	header := req.Params[0]
	txferID := strings.TrimSpace(header["txferid"])
	if txferID == "" {
		return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing transfer id"}
	}

	items := make([]sendItem, 0, len(req.Params)-1)
	for _, p := range req.Params[1:] {
		fid, err := strconv.ParseUint(p["fid"], 10, 64)
		if err != nil {
			return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND file id"}
		}
		offset := int64(0)
		if raw := strings.TrimSpace(p["offset"]); raw != "" {
			offset, err = strconv.ParseInt(raw, 10, 64)
			if err != nil || offset < 0 {
				return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND offset"}
			}
		}
		size := int64(0)
		if raw := strings.TrimSpace(p["size"]); raw != "" {
			size, err = strconv.ParseInt(raw, 10, 64)
			if err != nil || size < 0 {
				return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND size"}
			}
		}
		comp := strings.ToLower(strings.TrimSpace(p["comp"]))
		if comp == "" {
			comp = "adapt"
		}
		if comp == encoding.EncodingIdentity {
			comp = "none"
		}
		switch comp {
		case "adapt", "none", encoding.EncodingLz4, encoding.EncodingZstd:
		default:
			return sendRequest{}, protocolErr{code: "UNSUPPORTED_COMP", message: "supported comp values: adapt, none, lz4, zstd"}
		}
		path := p["path"]
		if path == "" {
			return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND path"}
		}
		mode := strings.ToLower(strings.TrimSpace(p["mode"]))
		if mode == "" {
			mode = loadStrategyFast
		}
		switch mode {
		case loadStrategyFast, loadStrategyGentle:
		default:
			return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "unsupported SEND mode"}
		}
		items = append(items, sendItem{FileID: fid, Offset: offset, Size: size, Comp: comp, Path: path, Mode: mode})
	}
	return sendRequest{TransferID: txferID, Items: items}, nil
}

func handleSEND(ctx context.Context, req Request, out io.Writer, deps Deps) error {
	return handleSENDWithOptions(ctx, req, out, deps, nil)
}

func handleSENDWithOptions(ctx context.Context, req Request, out io.Writer, deps Deps, limiter *limit.Limiter) error {
	parsed, err := parseSENDRequest(req)
	if err != nil {
		return err
	}
	for _, item := range parsed.Items {
		itemOut := out
		if limiter != nil && item.Mode == loadStrategyGentle {
			itemOut = limiter.WrapRateLimitedWriter(out, ctx)
		}
		if err := streamSendItem(itemOut, deps, parsed.TransferID, item); err != nil {
			return err
		}
	}
	return nil
}

func streamSendItem(out io.Writer, deps Deps, txferID string, item sendItem) error {
	fd, fileRef, usedDirectOpen, err := openSendFile(deps, txferID, item)
	if err != nil {
		return mapLookupError(err)
	}
	defer func() {
		if fd != nil {
			_ = fd.Close()
		}
	}()

	_ = deps.SetTransferFileState(txferID, item.FileID, TransferStateRunning)

	fileInfo, err := fd.Stat()
	if err != nil {
		return protocolErr{code: "INTERNAL", message: "failed to stat file"}
	}
	windowLen := fileInfo.Size()
	if windowLen < 0 {
		return protocolErr{code: "INTERNAL", message: "invalid file size"}
	}
	if item.Offset > windowLen {
		return protocolErr{code: "RANGE", message: "offset out of range"}
	}
	windowLen -= item.Offset
	if item.Size > 0 && item.Size < windowLen {
		windowLen = item.Size
	}
	if item.Mode == loadStrategyFast {
		tryReadAheadWindow(fd, item.Offset, windowLen)
	}

	cursor := item.Offset
	windowStart := item.Offset
	windowWireTotal := int64(0)
	windowLogicalTotal := int64(0)
	windowFrames := 0
	windowTS0 := time.Now().UnixMilli()

	firstFrameLogical := min(windowLen, defaultFileFrameLogicalSize)
	maxWSizeHint, err := maxFrameWireHint(item.Comp, firstFrameLogical)
	if err != nil {
		return protocolErr{code: "INTERNAL", message: "failed to compute max frame size hint"}
	}
	pipeSizeBytes := desiredPipeSizeBytes(windowLen, firstFrameLogical)
	useLinuxSplice := runtime.GOOS == "linux" && item.Mode == loadStrategyFast
	firstFrame := true

	adaptive := item.Comp == "adapt"
	currentMode := initialCompressionMode(item.Comp)
	compressPolicy := policy.NewCompressionPolicy()
	windowHasher := xxh3.New128()

	for remaining := windowLen; remaining > 0; {
		frameSize := min(remaining, defaultFileFrameLogicalSize)
		nextOffset := cursor + frameSize
		isTerminal := nextOffset == item.Offset+windowLen
		nextValue := nextOffset
		if isTerminal {
			nextValue = 0
		}
		var maxHint *int64
		if firstFrame && maxWSizeHint > 0 {
			maxHint = &maxWSizeHint
		}

		frameComp := policy.FrameCompTokenForMode(currentMode)
		var terminalMD *encoding.FileFrameMetadata
		if isTerminal {
			md := encoding.CollectFileFrameMetadata(fileRef.Path, fileInfo)
			terminalMD = &md
		}

		frameArgs := frameStreamArgs{
			FileID:        item.FileID,
			Offset:        cursor,
			FrameSize:     frameSize,
			Comp:          frameComp,
			MaxWSizeHint:  maxHint,
			HeaderTS:      time.Now().UnixMilli(),
			Next:          nextValue,
			IsTerminal:    isTerminal,
			TerminalMD:    terminalMD,
			WindowHasher:  windowHasher,
			Output:        out,
			PipeSizeBytes: pipeSizeBytes,
			DirectIO:      usedDirectOpen,
		}

		frameOffset := cursor
		var stats frameStreamStats
		if useLinuxSplice {
			stats, err = streamFramePayloadLinuxSplice(fd, &frameOffset, frameArgs)
		} else {
			stats, err = streamFramePayloadBuffered(fd, &frameOffset, frameArgs)
		}
		if err != nil {
			if usedDirectOpen && isDirectIOReadError(err) {
				if frameOffset != cursor {
					_ = fd.Close()
					return err
				}
				_ = fd.Close()
				fd = nil
				fd, fileRef, err = deps.GetFile(txferID, item.FileID, item.Path)
				if err != nil {
					return mapLookupError(err)
				}
				usedDirectOpen = false
				useLinuxSplice = runtime.GOOS == "linux" && item.Mode == loadStrategyFast
				remaining = item.Offset + windowLen - cursor
				if remaining < 0 {
					return protocolErr{code: "INTERNAL", message: "invalid resume offset after direct I/O retry"}
				}
				if cursor == item.Offset {
					if item.Mode == loadStrategyFast {
						tryReadAheadWindow(fd, item.Offset, windowLen)
					}
					windowWireTotal = 0
					windowLogicalTotal = 0
					windowFrames = 0
					windowTS0 = time.Now().UnixMilli()
					firstFrame = true
					currentMode = initialCompressionMode(item.Comp)
					compressPolicy = policy.NewCompressionPolicy()
					windowHasher = xxh3.New128()
				} else {
					if item.Mode == loadStrategyFast {
						tryReadAheadWindow(fd, cursor, remaining)
					}
				}
				continue
			}
			return err
		}

		cursor = stats.NextOffset
		remaining -= frameSize
		firstFrame = false
		windowFrames++
		windowLogicalTotal += stats.LogicalSize
		windowWireTotal += stats.WireSize

		if isTerminal {
			if stats.WindowHashToken == "" {
				return protocolErr{code: "INTERNAL", message: "failed to finalize window hash"}
			}
			if !deps.SetTransferFileWindowHash(txferID, item.FileID, cursor, stats.WindowHashToken) {
				return protocolErr{code: "INTERNAL", message: "failed to store window hash state"}
			}
		}

		if adaptive {
			decision := compressPolicy.Decide(currentMode, policy.CompressionMetrics{
				LogicalSize:    stats.LogicalSize,
				WireSize:       stats.WireSize,
				PrepareLatency: stats.PrepareLatency,
				WriteLatency:   stats.WriteLatency,
			})
			if decision.Next != currentMode {
				prevComp := policy.FrameCompTokenForMode(currentMode)
				nextComp := policy.FrameCompTokenForMode(decision.Next)
				log.Printf(
					"filexfer frame tid=%s fid=%d switching compression %s->%s reason=%s ratio=%.3f read_over_write=%.3f",
					txferID,
					item.FileID,
					prevComp,
					nextComp,
					decision.Reason,
					decision.Ratio,
					decision.ReadOverWrite,
				)
				currentMode = decision.Next
			}
		}
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
		item.FileID,
		windowFrames,
		windowStart,
		windowLogicalTotal,
		windowWireTotal,
		windowTS0,
		windowTS1,
		windowMS,
		encoding.HumanRate(logicalBps),
		encoding.HumanRate(wireBps),
	)
	return nil
}

func openSendFile(deps Deps, txferID string, item sendItem) (*os.File, FileRef, bool, error) {
	if item.Mode != loadStrategyGentle {
		fd, fileRef, err := deps.GetFile(txferID, item.FileID, item.Path)
		return fd, fileRef, false, err
	}
	fileRef, err := deps.GetFileRef(txferID, item.FileID, item.Path)
	if err != nil {
		return nil, FileRef{}, false, err
	}
	fd, err := os.OpenFile(fileRef.Path, os.O_RDONLY|unix.O_DIRECT, 0)
	if err != nil {
		fd, fileRef, err = deps.GetFile(txferID, item.FileID, item.Path)
		return fd, fileRef, false, err
	}
	return fd, fileRef, true, nil
}

func isDirectIOReadError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, syscall.EINVAL) || errors.Is(err, unix.EINVAL)
}

func initialCompressionMode(comp string) policy.CompressionMode {
	switch comp {
	case encoding.EncodingLz4:
		return policy.CompressionModeLz4
	case encoding.EncodingZstd:
		return policy.CompressionModeZstdLevel1
	default:
		return policy.CompressionModeNone
	}
}

func maxFrameWireHint(comp string, logicalSize int64) (int64, error) {
	if logicalSize <= 0 {
		return 0, nil
	}
	switch comp {
	case "adapt":
		maxHint := int64(0)
		for _, candidate := range []string{"none", encoding.EncodingLz4, encoding.EncodingZstd} {
			hint, err := encoding.MaxFrameWireSizeHintBytes(candidate, logicalSize)
			if err != nil {
				return 0, err
			}
			if hint > maxHint {
				maxHint = hint
			}
		}
		return maxHint, nil
	default:
		return encoding.MaxFrameWireSizeHintBytes(comp, logicalSize)
	}
}

func desiredPipeSizeBytes(windowLen int64, frameSize int64) int {
	maxPipeSize := bestEffortPipeMaxSizeBytes()
	const minPipeSize = 64 * 1024
	if frameSize <= 0 {
		return minPipeSize
	}
	if windowLen > maxPipeSize {
		return int(maxPipeSize)
	}
	return int(max(minPipeSize, min(frameSize, maxPipeSize)))
}

func bestEffortPipeMaxSizeBytes() int64 {
	pipeMaxSizeOnce.Do(func() {
		raw, err := os.ReadFile("/proc/sys/fs/pipe-max-size")
		if err != nil {
			return
		}
		value, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if err != nil || value <= 0 {
			return
		}
		pipeMaxSizeBytes = value
	})
	return pipeMaxSizeBytes
}

func growPipeBestEffort(pipeFD *os.File, sizeBytes int) {
	if sizeBytes <= 0 {
		return
	}
	_, _ = unix.FcntlInt(pipeFD.Fd(), unix.F_SETPIPE_SZ, sizeBytes)
}

func buildFrameHeaderLine(fileID uint64, offset int64, size int64, wireSize int64, comp string, maxWSizeHint *int64, ts int64) string {
	if maxWSizeHint != nil {
		return "FX/1 " + strconv.FormatUint(fileID, 10) +
			" offset=" + strconv.FormatInt(offset, 10) +
			" size=" + strconv.FormatInt(size, 10) +
			" wsize=" + strconv.FormatInt(wireSize, 10) +
			" comp=" + comp + " enc=none hash=" + placeholderHeaderHashToken +
			" max-wsize=" + strconv.FormatInt(*maxWSizeHint, 10) +
			" ts=" + strconv.FormatInt(ts, 10) + "\n"
	}
	return "FX/1 " + strconv.FormatUint(fileID, 10) +
		" offset=" + strconv.FormatInt(offset, 10) +
		" size=" + strconv.FormatInt(size, 10) +
		" wsize=" + strconv.FormatInt(wireSize, 10) +
		" comp=" + comp + " enc=none hash=" + placeholderHeaderHashToken +
		" ts=" + strconv.FormatInt(ts, 10) + "\n"
}

func buildFrameTrailerLine(fileID uint64, ts int64, next int64, windowHashToken string, metadata *encoding.FileFrameMetadata) string {
	var b strings.Builder
	b.WriteString("FXT/1 ")
	b.WriteString(strconv.FormatUint(fileID, 10))
	b.WriteString(" status=ok ts=")
	b.WriteString(strconv.FormatInt(ts, 10))
	if windowHashToken != "" {
		b.WriteString(" file-hash=")
		b.WriteString(windowHashToken)
	}
	b.WriteString(" next=")
	b.WriteString(strconv.FormatInt(next, 10))
	if metadata != nil {
		for _, token := range metadataTrailerTokens(metadata) {
			b.WriteString(" ")
			b.WriteString(token)
		}
	}
	b.WriteString("\n")
	return b.String()
}

func metadataTrailerTokens(metadata *encoding.FileFrameMetadata) []string {
	if metadata == nil {
		return nil
	}
	return []string{
		"meta:size=" + strconv.FormatInt(metadata.Size, 10),
		"meta:mtime_ns=" + strconv.FormatInt(metadata.MtimeNS, 10),
		"meta:mode=" + metadata.Mode,
		"meta:uid=" + metadata.UID,
		"meta:gid=" + metadata.GID,
		"meta:user=" + strings.ReplaceAll(metadata.User, " ", "_"),
		"meta:group=" + strings.ReplaceAll(metadata.Group, " ", "_"),
	}
}

func streamFramePayloadBuffered(fd *os.File, fileOffset *int64, args frameStreamArgs) (frameStreamStats, error) {
	readBuf, releaseRead, err := acquireLogicalBuffer(logicalBufferBucketSize(args.FrameSize))
	if err != nil {
		return frameStreamStats{}, err
	}
	defer releaseRead()

	writeLatency := time.Duration(0)
	isCompressed := args.Comp != "none"

	// For compressed: collect into frameBuf so wireSize is known before writing the header.
	// For none: normally stream directly, but when using direct I/O retry path we stage
	// payload in-memory until the read succeeds to avoid writing partial frame headers.
	payloadWriter := io.Writer(args.Output)
	var frameBuf *bytes.Buffer
	var closeCompWriter func() error
	var stagingBuf *bytes.Buffer
	stagedDirectWrite := args.DirectIO && !isCompressed

	if isCompressed {
		frameBuf = acquireCompressedFrameBuffer(args.FrameSize, args.Comp)
		defer releaseCompressedFrameBuffer(frameBuf)
		var compWriter io.Writer
		var selected string
		compWriter, closeCompWriter, selected, err = encoding.WrapCompressedWriter(frameBuf, args.Comp)
		if err != nil {
			return frameStreamStats{}, err
		}
		if selected != args.Comp {
			return frameStreamStats{}, errors.New("compression mode negotiation mismatch")
		}
		payloadWriter = compWriter
	} else {
		if stagedDirectWrite {
			stagingBuf = bytes.NewBuffer(make([]byte, 0, int(args.FrameSize)))
			payloadWriter = stagingBuf
		} else {
			if err := writeFrameHeader(args.Output, args, args.FrameSize, &writeLatency); err != nil {
				return frameStreamStats{}, err
			}
		}
	}

	prepareLatency, err := streamBufferedRead(fd, fileOffset, args.FrameSize, readBuf, isCompressed, func(chunk []byte) error {
		_, _ = args.WindowHasher.Write(chunk)
		writeStart := time.Now()
		written, writeErr := payloadWriter.Write(chunk)
		if !isCompressed && !stagedDirectWrite {
			writeLatency += time.Since(writeStart)
		}
		if writeErr != nil {
			return writeErr
		}
		if written != len(chunk) {
			return io.ErrShortWrite
		}
		return nil
	})
	if err != nil {
		return frameStreamStats{}, err
	}

	wireSize := args.FrameSize
	if isCompressed {
		if err := closeCompWriter(); err != nil {
			return frameStreamStats{}, err
		}
		wireSize = int64(frameBuf.Len())
		if err := writeFrameHeader(args.Output, args, wireSize, &writeLatency); err != nil {
			return frameStreamStats{}, err
		}
		writeStart := time.Now()
		if _, err := args.Output.Write(frameBuf.Bytes()); err != nil {
			return frameStreamStats{}, err
		}
		writeLatency += time.Since(writeStart)
	} else if stagedDirectWrite {
		if err := writeFrameHeader(args.Output, args, wireSize, &writeLatency); err != nil {
			return frameStreamStats{}, err
		}
		writeStart := time.Now()
		if _, err := args.Output.Write(stagingBuf.Bytes()); err != nil {
			return frameStreamStats{}, err
		}
		writeLatency += time.Since(writeStart)
	}

	windowHashToken, err := writeFrameTrailer(args.Output, args, &writeLatency)
	if err != nil {
		return frameStreamStats{}, err
	}

	return frameStreamStats{
		LogicalSize:     args.FrameSize,
		WireSize:        wireSize,
		PrepareLatency:  prepareLatency,
		WriteLatency:    writeLatency,
		NextOffset:      *fileOffset,
		WindowHashToken: windowHashToken,
	}, nil
}

func streamFramePayloadLinuxSplice(fd *os.File, fileOffset *int64, args frameStreamArgs) (frameStreamStats, error) {
	if args.Comp == "none" && args.DirectIO {
		return streamFramePayloadBuffered(fd, fileOffset, args)
	}

	srcR, srcW, err := os.Pipe()
	if err != nil {
		return frameStreamStats{}, err
	}
	defer srcR.Close()
	defer srcW.Close()

	growPipeBestEffort(srcR, args.PipeSizeBytes)
	growPipeBestEffort(srcW, args.PipeSizeBytes)

	copyBufSize := logicalBufferBucketSize(int64(args.PipeSizeBytes))
	copyBuf, releaseCopyBuf, err := acquireLogicalBuffer(copyBufSize)
	if err != nil {
		return frameStreamStats{}, err
	}
	defer releaseCopyBuf()

	writeLatency := time.Duration(0)
	isCompressed := args.Comp != "none"

	// For compressed: buffer payload into frameBuf so wireSize is known before writing the header.
	// For none: wireSize == frameSize, write the header immediately and splice directly.
	payloadWriter := io.Writer(args.Output)
	var frameBuf *bytes.Buffer
	var closeCompWriter func() error

	if isCompressed {
		frameBuf = acquireCompressedFrameBuffer(args.FrameSize, args.Comp)
		defer releaseCompressedFrameBuffer(frameBuf)
		var compWriter io.Writer
		var selected string
		compWriter, closeCompWriter, selected, err = encoding.WrapCompressedWriter(frameBuf, args.Comp)
		if err != nil {
			return frameStreamStats{}, err
		}
		if selected != args.Comp {
			return frameStreamStats{}, errors.New("compression mode negotiation mismatch")
		}
		payloadWriter = compWriter
	} else {
		if err := writeFrameHeader(args.Output, args, args.FrameSize, &writeLatency); err != nil {
			return frameStreamStats{}, err
		}
	}

	prepareLatency := time.Duration(0)
	remaining := args.FrameSize
	for remaining > 0 {
		step := remaining
		if step > int64(args.PipeSizeBytes) {
			step = int64(args.PipeSizeBytes)
		}
		spliceStart := time.Now()
		splicedIn, spliceErr := unix.Splice(int(fd.Fd()), fileOffset, int(srcW.Fd()), nil, int(step), unix.SPLICE_F_MOVE)
		prepareLatency += time.Since(spliceStart)
		if spliceErr != nil {
			return frameStreamStats{}, spliceErr
		}
		if splicedIn <= 0 {
			return frameStreamStats{}, io.ErrUnexpectedEOF
		}

		sourceRemaining := int64(splicedIn)
		for sourceRemaining > 0 {
			chunk := sourceRemaining
			if chunk > int64(len(copyBuf)) {
				chunk = int64(len(copyBuf))
			}
			readStart := time.Now()
			n, readErr := io.ReadFull(srcR, copyBuf[:chunk])
			prepareLatency += time.Since(readStart)
			if n > 0 {
				_, _ = args.WindowHasher.Write(copyBuf[:n])
				writeStart := time.Now()
				written, writeErr := payloadWriter.Write(copyBuf[:n])
				if !isCompressed {
					writeLatency += time.Since(writeStart)
				} else {
					prepareLatency += time.Since(writeStart)
				}
				if writeErr != nil {
					return frameStreamStats{}, writeErr
				}
				if written != n {
					return frameStreamStats{}, io.ErrShortWrite
				}
				sourceRemaining -= int64(n)
			}
			if readErr != nil {
				return frameStreamStats{}, readErr
			}
		}
		remaining -= int64(splicedIn)
	}

	wireSize := args.FrameSize
	if isCompressed {
		if err := closeCompWriter(); err != nil {
			return frameStreamStats{}, err
		}
		wireSize = int64(frameBuf.Len())
		if err := writeFrameHeader(args.Output, args, wireSize, &writeLatency); err != nil {
			return frameStreamStats{}, err
		}
		writeStart := time.Now()
		if _, err := args.Output.Write(frameBuf.Bytes()); err != nil {
			return frameStreamStats{}, err
		}
		writeLatency += time.Since(writeStart)
	}

	windowHashToken, err := writeFrameTrailer(args.Output, args, &writeLatency)
	if err != nil {
		return frameStreamStats{}, err
	}

	return frameStreamStats{
		LogicalSize:     args.FrameSize,
		WireSize:        wireSize,
		PrepareLatency:  prepareLatency,
		WriteLatency:    writeLatency,
		NextOffset:      *fileOffset,
		WindowHashToken: windowHashToken,
	}, nil
}

func writeFrameHeader(out io.Writer, args frameStreamArgs, wireSize int64, writeLatency *time.Duration) error {
	headerLine := buildFrameHeaderLine(args.FileID, args.Offset, args.FrameSize, wireSize, args.Comp, args.MaxWSizeHint, args.HeaderTS)
	writeStart := time.Now()
	if _, err := io.WriteString(out, headerLine); err != nil {
		return err
	}
	if writeLatency != nil {
		*writeLatency += time.Since(writeStart)
	}
	return nil
}

func writeFrameTrailer(out io.Writer, args frameStreamArgs, writeLatency *time.Duration) (string, error) {
	windowHashToken := ""
	if args.IsTerminal {
		windowHashToken = encoding.FormatXXH128HashToken(args.WindowHasher.Sum128())
	}
	trailerLine := buildFrameTrailerLine(args.FileID, time.Now().UnixMilli(), args.Next, windowHashToken, args.TerminalMD)
	writeStart := time.Now()
	if _, err := io.WriteString(out, trailerLine); err != nil {
		return "", err
	}
	if writeLatency != nil {
		*writeLatency += time.Since(writeStart)
	}
	return windowHashToken, nil
}

func streamBufferedRead(
	fd *os.File,
	fileOffset *int64,
	frameSize int64,
	buf []byte,
	includeHandleInPrepare bool,
	handle func([]byte) error,
) (time.Duration, error) {
	prepareLatency := time.Duration(0)
	remaining := frameSize
	for remaining > 0 {
		readSize := len(buf)
		if int64(readSize) > remaining {
			readSize = int(remaining)
		}
		readStart := time.Now()
		n, readErr := fd.ReadAt(buf[:readSize], *fileOffset)
		prepareLatency += time.Since(readStart)
		if n > 0 {
			*fileOffset += int64(n)
			remaining -= int64(n)
			handleStart := time.Now()
			if err := handle(buf[:n]); err != nil {
				return 0, err
			}
			if includeHandleInPrepare {
				prepareLatency += time.Since(handleStart)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && remaining == 0 {
				break
			}
			return 0, readErr
		}
	}
	if remaining != 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return prepareLatency, nil
}


func acquireCompressedFrameBuffer(logicalSize int64, comp string) *bytes.Buffer {
	maxEncoded, err := encoding.MaxEncodedFrameSizeBytes(comp, logicalSize)
	if err != nil || maxEncoded <= 0 {
		maxEncoded = defaultCompressedFrameBufferBytes
	}
	capacity := int(maxEncoded)
	if capacity <= 0 {
		capacity = defaultCompressedFrameBufferBytes
	}

	if raw := compressedFrameBufferPool.Get(); raw != nil {
		if buf, ok := raw.(*bytes.Buffer); ok && buf != nil {
			buf.Reset()
			if buf.Cap() >= capacity {
				return buf
			}
			return bytes.NewBuffer(make([]byte, 0, capacity))
		}
	}
	return bytes.NewBuffer(make([]byte, 0, capacity))
}

func releaseCompressedFrameBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > maxCompressedFrameBufferPoolBytes {
		return
	}
	buf.Reset()
	compressedFrameBufferPool.Put(buf)
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
	return buf, func() { pool.Put(buf[:size]) }, nil
}

func logicalBufferPool(size int) *sync.Pool {
	if existing, ok := logicalBufferPools.Load(size); ok {
		return existing.(*sync.Pool)
	}
	sz := size
	created := &sync.Pool{New: func() any { return make([]byte, sz) }}
	actual, _ := logicalBufferPools.LoadOrStore(size, created)
	return actual.(*sync.Pool)
}

func logicalBufferBucketSize(maxChunk int64) int {
	const (
		KiB   = 1024
		MiB   = 1024 * KiB
		bucket4KiB = 4 * KiB
		bucket16KiB = 16 * KiB
		bucket64KiB = 64 * KiB
		bucket256KiB = 256 * KiB
		bucket1MiB = 1 * MiB
		bucket2MiB = 2 * MiB
		bucket4MiB = 4 * MiB
		bucket8MiB = 8 * MiB
	)
	if maxChunk <= int64(bucket4KiB) {
		return bucket4KiB
	}
	for _, bucket := range []int{bucket16KiB, bucket64KiB, bucket256KiB, bucket1MiB, bucket2MiB, bucket4MiB, bucket8MiB} {
		if maxChunk <= int64(bucket) {
			return bucket
		}
	}
	return bucket8MiB
}

func tryReadAheadWindow(fd *os.File, offset int64, length int64) {
	if fd == nil || offset < 0 || length <= 0 {
		return
	}
	_ = unix.Fadvise(int(fd.Fd()), offset, length, unix.FADV_SEQUENTIAL)
}
