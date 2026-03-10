package ftcp

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	//"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	intcodec "github.com/jolynch/pinch/internal/filexfer/codec"
	intframe "github.com/jolynch/pinch/internal/filexfer/frame"
	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"
)

const defaultFileFrameLogicalSize int64 = 8 * 1024 * 1024
const defaultMaxLinuxPipeSizeBytes int64 = 1 * 1024 * 1024
const hashWorkerBufferSize = 256 * 1024
const placeholderHeaderHashToken = "xxh128:00000000000000000000000000000000"
const placeholderTrailerHashToken = "xxh64:0000000000000000"

var logicalBufferPools sync.Map
var pipeMaxSizeOnce sync.Once
var pipeMaxSizeBytes int64 = defaultMaxLinuxPipeSizeBytes

type sendItem struct {
	FileID uint64
	Offset int64
	Size   int64
	Path   string
}

type sendRequest struct {
	TransferID string
	Comp       string
	Items      []sendItem
}

type windowHashResult struct {
	token string
	err   error
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
	comp := strings.ToLower(strings.TrimSpace(header["comp"]))
	if comp == "" {
		comp = "none"
	}
	if comp == "identity" {
		comp = "none"
	}
	if comp != "none" {
		return sendRequest{}, protocolErr{code: "UNSUPPORTED_COMP", message: "only comp=none is supported"}
	}

	items := make([]sendItem, 0, len(req.Params)-1)
	for _, p := range req.Params[1:] {
		fid, err := strconv.ParseUint(p["fid"], 10, 64)
		if err != nil {
			return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND file id"}
		}
		offset, err := strconv.ParseInt(p["offset"], 10, 64)
		if err != nil || offset < 0 {
			return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND offset"}
		}
		size, err := strconv.ParseInt(p["size"], 10, 64)
		if err != nil || size < 0 {
			return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND size"}
		}
		path := p["path"]
		if path == "" {
			return sendRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND path"}
		}
		items = append(items, sendItem{FileID: fid, Offset: offset, Size: size, Path: path})
	}
	return sendRequest{TransferID: txferID, Comp: comp, Items: items}, nil
}

func handleSEND(_ context.Context, req Request, out io.Writer, deps Deps) error {
	parsed, err := parseSENDRequest(req)
	if err != nil {
		return err
	}
	for _, item := range parsed.Items {
		if err := streamSendItem(out, deps, parsed.TransferID, item); err != nil {
			return err
		}
	}
	return nil
}

func streamSendItem(out io.Writer, deps Deps, txferID string, item sendItem) error {
	fd, fileRef, err := deps.GetFile(txferID, item.FileID, item.Path)
	if err != nil {
		return mapLookupError(err)
	}
	defer fd.Close()

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
	maybeFadviseSequential(fd, item.Offset, windowLen)

	cursor := item.Offset
	windowStart := item.Offset
	windowWireTotal := int64(0)
	windowLogicalTotal := int64(0)
	windowFrames := 0
	windowTS0 := time.Now().UnixMilli()

	maxWSizeHint := min(windowLen, defaultFileFrameLogicalSize)
	pipeSizeBytes := desiredPipeSizeBytes(windowLen, defaultFileFrameLogicalSize)
	hashPipeR, hashPipeW, err := os.Pipe()
	if err != nil {
		return protocolErr{code: "INTERNAL", message: "failed to initialize window hash pipe"}
	}
	hashDone := startWindowHashWorker(hashPipeR)
	useLinuxSplice := false
	//:= runtime.GOOS == "linux"
	firstFrame := true

	for remaining := windowLen; remaining > 0; {
		frameSize := min(remaining, defaultFileFrameLogicalSize)
		headerTS := time.Now().UnixMilli()
		var maxHint *int64
		if firstFrame && maxWSizeHint > 0 {
			maxHint = &maxWSizeHint
		}
		frameOffset := cursor
		wireSize := frameSize
		headerLine := buildFrameHeaderLine(item.FileID, cursor, frameSize, wireSize, "none", maxHint, headerTS)
		if _, err := io.WriteString(out, headerLine); err != nil {
			_ = hashPipeW.Close()
			<-hashDone
			return err
		}

		var payloadErr error
		if useLinuxSplice {
			payloadErr = streamFramePayloadLinuxSplice(fd, &frameOffset, frameSize, out, hashPipeW, pipeSizeBytes)
		} else {
			payloadErr = streamFramePayloadBuffered(fd, &frameOffset, frameSize, out, hashPipeW)
		}
		if payloadErr != nil {
			_ = hashPipeW.Close()
			<-hashDone
			return payloadErr
		}

		cursor = frameOffset
		remaining -= frameSize
		firstFrame = false
		windowFrames++
		windowLogicalTotal += frameSize
		windowWireTotal += wireSize

		isTerminal := remaining == 0
		nextValue := cursor
		if isTerminal {
			nextValue = 0
		}

		trailerTS := time.Now().UnixMilli()
		windowHashToken := ""
		var terminalMD *intframe.FileFrameMetadata
		if isTerminal {
			_ = hashPipeW.Close()
			result := <-hashDone
			if result.err != nil {
				return protocolErr{code: "INTERNAL", message: "failed to finalize window hash"}
			}
			windowHashToken = result.token
			if !deps.SetTransferFileWindowHash(txferID, item.FileID, cursor, windowHashToken) {
				return protocolErr{code: "INTERNAL", message: "failed to store window hash state"}
			}
			md := intframe.CollectFileFrameMetadata(fileRef.Path, fileInfo)
			terminalMD = &md
		}
		trailerLine := buildFrameTrailerLine(item.FileID, trailerTS, nextValue, windowHashToken, terminalMD)
		if _, err := io.WriteString(out, trailerLine); err != nil {
			return err
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
		humanRate(logicalBps),
		humanRate(wireBps),
	)
	return nil
}

func startWindowHashWorker(pipeR *os.File) <-chan windowHashResult {
	done := make(chan windowHashResult, 1)
	go func() {
		defer close(done)
		defer pipeR.Close()
		hasher := xxh3.New128()
		buf, release, err := acquireLogicalBuffer(hashWorkerBufferSize)
		if err != nil {
			done <- windowHashResult{err: err}
			return
		}
		defer release()
		if _, err := io.CopyBuffer(hasher, pipeR, buf); err != nil {
			done <- windowHashResult{err: err}
			return
		}
		done <- windowHashResult{token: intcodec.FormatXXH128HashToken(hasher.Sum128())}
	}()
	return done
}

func desiredPipeSizeBytes(windowLen int64, frameSize int64) int {
	maxPipeSize := bestEffortPipeMaxSizeBytes()
	if windowLen > maxPipeSize {
		return int(maxPipeSize)
	}
	if frameSize <= 0 {
		return 64 * 1024
	}
	target := frameSize
	if target > maxPipeSize {
		target = maxPipeSize
	}
	if target < 64*1024 {
		target = 64 * 1024
	}
	return int(target)
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

func buildFrameTrailerLine(fileID uint64, ts int64, next int64, windowHashToken string, metadata *intframe.FileFrameMetadata) string {
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
	b.WriteString(" hash=")
	b.WriteString(placeholderTrailerHashToken)
	b.WriteString("\n")
	return b.String()
}

func metadataTrailerTokens(metadata *intframe.FileFrameMetadata) []string {
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

func streamFramePayloadBuffered(fd *os.File, fileOffset *int64, frameSize int64, frameWriter io.Writer, hashPipeW *os.File) error {
	section := io.NewSectionReader(fd, *fileOffset, frameSize)
	buf, release, err := acquireLogicalBuffer(1024 * 1024)
	if err != nil {
		return err
	}
	defer release()
	written, err := io.CopyBuffer(io.MultiWriter(frameWriter, hashPipeW), section, buf)
	*fileOffset += written
	if err != nil {
		return err
	}
	if written != frameSize {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func streamFramePayloadLinuxSplice(fd *os.File, fileOffset *int64, frameSize int64, frameWriter io.Writer, hashPipeW *os.File, pipeSize int) error {
	srcR, srcW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer srcR.Close()
	defer srcW.Close()

	growPipeBestEffort(srcR, pipeSize)
	growPipeBestEffort(srcW, pipeSize)
	growPipeBestEffort(hashPipeW, pipeSize)

	copyBufSize := logicalBufferBucketSize(int64(pipeSize))
	copyBuf, releaseCopyBuf, err := acquireLogicalBuffer(copyBufSize)
	if err != nil {
		return err
	}
	defer releaseCopyBuf()

	remaining := frameSize
	for remaining > 0 {
		step := remaining
		if step > int64(pipeSize) {
			step = int64(pipeSize)
		}
		splicedIn, spliceErr := unix.Splice(int(fd.Fd()), fileOffset, int(srcW.Fd()), nil, int(step), unix.SPLICE_F_MOVE)
		if spliceErr != nil {
			return spliceErr
		}
		if splicedIn <= 0 {
			return io.ErrUnexpectedEOF
		}
		sourceRemaining := splicedIn
		for sourceRemaining > 0 {
			teed, teeErr := unix.Tee(int(srcR.Fd()), int(hashPipeW.Fd()), int(sourceRemaining), 0)
			if teeErr != nil {
				return teeErr
			}
			if teed <= 0 {
				return io.ErrUnexpectedEOF
			}
			limited := &io.LimitedReader{R: srcR, N: int64(teed)}
			written, copyErr := io.CopyBuffer(frameWriter, limited, copyBuf)
			if copyErr != nil {
				return copyErr
			}
			if written != int64(teed) {
				return io.ErrUnexpectedEOF
			}
			sourceRemaining -= teed
		}
		remaining -= int64(splicedIn)
	}
	return nil
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
	if maxChunk <= 4*1024 {
		return 4 * 1024
	}
	for _, bucket := range []int{16 * 1024, 64 * 1024, 256 * 1024, 1 * 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024} {
		if maxChunk <= int64(bucket) {
			return bucket
		}
	}
	return 8 * 1024 * 1024
}

func maybeFadviseSequential(fd *os.File, offset int64, length int64) {
	if length <= 0 {
		return
	}
	_ = unix.Fadvise(int(fd.Fd()), offset, length, unix.FADV_SEQUENTIAL)
}
