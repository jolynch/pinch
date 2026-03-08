package fhttp

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"
)

const defaultFileFrameLogicalSize int64 = 8 * 1024 * 1024
const zeroCopyChunkSize int64 = 8 * 1024 * 1024
const maxLinuxPipeSizeBytes = 1 * 1024 * 1024
const placeholderHeaderHashToken = "xxh128:00000000000000000000000000000000"
const placeholderTrailerHashToken = "xxh64:0000000000000000"
const maxConcurrentFramePrepares = 8

var logicalBufferPools sync.Map   // map[int]*sync.Pool
var frameCompressorPools sync.Map // map[int64]*sync.Pool

type framePrepareTask struct {
	Index       int
	Offset      int64
	LogicalSize int64
	Mode        CompressionMode
}

type preparedFrame struct {
	Task           framePrepareTask
	LogicalChunk   []byte
	FramePayload   []byte
	WireSize       int64
	ChunkHash      xxh3.Uint128
	PrepareLatency time.Duration
	Err            error
	release        func()
}

type windowHashResult struct {
	token string
	err   error
}

type readerOnly struct {
	io.Reader
}

func startWindowHashWorker(pipeR *os.File) <-chan windowHashResult {
	done := make(chan windowHashResult, 1)
	go func() {
		defer close(done)
		if pipeR == nil {
			done <- windowHashResult{err: errors.New("nil hash pipe reader")}
			return
		}
		defer pipeR.Close()
		hasher := xxh3.New128()
		buf, release, err := acquireLogicalBuffer(256 * 1024)
		if err != nil {
			done <- windowHashResult{err: err}
			return
		}
		defer release()
		if _, err := io.CopyBuffer(hasher, readerOnly{Reader: pipeR}, buf); err != nil {
			done <- windowHashResult{err: err}
			return
		}
		done <- windowHashResult{token: formatXXH128HashToken(hasher.Sum128())}
	}()
	return done
}

func desiredPipeSizeBytes(windowLen int64, frameSize int64) int {
	if windowLen > maxLinuxPipeSizeBytes {
		return maxLinuxPipeSizeBytes
	}
	if frameSize <= 0 {
		return 64 * 1024
	}
	target := frameSize
	if target > maxLinuxPipeSizeBytes {
		target = maxLinuxPipeSizeBytes
	}
	if target < 64*1024 {
		target = 64 * 1024
	}
	return int(target)
}

func growPipeBestEffort(pipeFD *os.File, sizeBytes int) {
	if pipeFD == nil || sizeBytes <= 0 {
		return
	}
	_, _ = unix.FcntlInt(pipeFD.Fd(), unix.F_SETPIPE_SZ, sizeBytes)
}

func buildFrameHeaderLine(fileID uint64, offset int64, size int64, maxWSizeHint *int64, ts int64) string {
	if maxWSizeHint != nil {
		return "FX/1 " + strconv.FormatUint(fileID, 10) +
			" offset=" + strconv.FormatInt(offset, 10) +
			" size=" + strconv.FormatInt(size, 10) +
			" wsize=" + strconv.FormatInt(size, 10) +
			" comp=none enc=none hash=" + placeholderHeaderHashToken +
			" max-wsize=" + strconv.FormatInt(*maxWSizeHint, 10) +
			" ts=" + strconv.FormatInt(ts, 10) + "\n"
	}
	return "FX/1 " + strconv.FormatUint(fileID, 10) +
		" offset=" + strconv.FormatInt(offset, 10) +
		" size=" + strconv.FormatInt(size, 10) +
		" wsize=" + strconv.FormatInt(size, 10) +
		" comp=none enc=none hash=" + placeholderHeaderHashToken +
		" ts=" + strconv.FormatInt(ts, 10) + "\n"
}

func buildFrameTrailerLine(fileID uint64, ts int64, next int64, windowHashToken string, metadata *fileFrameMetadata) string {
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
		for _, token := range fileFrameMetadataTokens(metadata) {
			b.WriteString(" ")
			b.WriteString(token)
		}
	}
	b.WriteString(" hash=")
	b.WriteString(placeholderTrailerHashToken)
	b.WriteString("\n")
	return b.String()
}

func fileFrameMetadataTokens(metadata *fileFrameMetadata) []string {
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
	if fd == nil || fileOffset == nil || frameWriter == nil || hashPipeW == nil {
		return errors.New("invalid buffered payload stream arguments")
	}
	section := io.NewSectionReader(fd, *fileOffset, frameSize)
	buf, release, err := acquireLogicalBuffer(256 * 1024)
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
	return streamFramePayloadLinuxSpliceToOutput(fd, fileOffset, frameSize, frameWriter, hashPipeW, pipeSize, -1)
}

func streamFramePayloadLinuxSpliceToOutput(fd *os.File, fileOffset *int64, frameSize int64, frameWriter io.Writer, hashPipeW *os.File, pipeSize int, outFD int) error {
	if fd == nil || fileOffset == nil || frameWriter == nil || hashPipeW == nil {
		return errors.New("invalid splice payload stream arguments")
	}
	srcR, srcW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer srcR.Close()
	defer srcW.Close()

	growPipeBestEffort(srcR, pipeSize)
	growPipeBestEffort(srcW, pipeSize)
	growPipeBestEffort(hashPipeW, pipeSize)
	var (
		copyBuf        []byte
		releaseCopyBuf func()
	)
	if outFD < 0 {
		copyBufSize := logicalBufferBucketSize(int64(pipeSize))
		copyBuf, releaseCopyBuf, err = acquireLogicalBuffer(copyBufSize)
		if err != nil {
			return err
		}
		defer releaseCopyBuf()
	}

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

		// tee(2) does not consume src bytes. If tee returns a partial duplicate,
		// we must consume exactly that amount from src before calling tee again,
		// otherwise we'd duplicate the same prefix repeatedly.
		sourceRemaining := splicedIn
		for sourceRemaining > 0 {
			teed, teeErr := unix.Tee(int(srcR.Fd()), int(hashPipeW.Fd()), int(sourceRemaining), 0)
			if teeErr != nil {
				return teeErr
			}
			if teed <= 0 {
				return io.ErrUnexpectedEOF
			}
			if outFD >= 0 {
				moveRemaining := teed
				for moveRemaining > 0 {
					moved, moveErr := unix.Splice(int(srcR.Fd()), nil, outFD, nil, int(moveRemaining), unix.SPLICE_F_MOVE)
					if moveErr != nil {
						if errors.Is(moveErr, unix.EINTR) {
							continue
						}
						if errors.Is(moveErr, unix.EAGAIN) {
							if waitErr := waitSocketWritable(outFD); waitErr != nil {
								return waitErr
							}
							continue
						}
						return moveErr
					}
					if moved <= 0 {
						return io.ErrUnexpectedEOF
					}
					moveRemaining -= moved
				}
			} else {
				limited := &io.LimitedReader{R: readerOnly{Reader: srcR}, N: int64(teed)}
				written, copyErr := io.CopyBuffer(frameWriter, limited, copyBuf)
				if copyErr != nil {
					return copyErr
				}
				if written != int64(teed) {
					return io.ErrUnexpectedEOF
				}
			}
			sourceRemaining -= teed
		}
		remaining -= int64(splicedIn)
	}
	return nil
}

func waitSocketWritable(fd int) error {
	if fd < 0 {
		return errors.New("invalid socket fd")
	}
	pollFDs := []unix.PollFd{{
		Fd:     int32(fd),
		Events: unix.POLLOUT,
	}}
	for {
		_, err := unix.Poll(pollFDs, -1)
		if err == nil {
			return nil
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return err
	}
}

func dupConnFD(conn net.Conn) (int, error) {
	sysConn, ok := conn.(syscall.Conn)
	if !ok {
		return -1, errors.New("connection does not implement syscall.Conn")
	}
	raw, err := sysConn.SyscallConn()
	if err != nil {
		return -1, err
	}
	dupFD := -1
	var dupErr error
	if err := raw.Control(func(fd uintptr) {
		dupFD, dupErr = unix.Dup(int(fd))
	}); err != nil {
		return -1, err
	}
	if dupErr != nil {
		return -1, dupErr
	}
	if dupFD < 0 {
		return -1, errors.New("failed to duplicate connection fd")
	}
	return dupFD, nil
}

func hijackStreamingConn(w http.ResponseWriter) (net.Conn, int, error) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, -1, errors.New("response writer does not support hijack")
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, -1, err
	}
	if _, err := io.WriteString(rw, "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n"); err != nil {
		_ = conn.Close()
		return nil, -1, err
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return nil, -1, err
	}
	outFD, err := dupConnFD(conn)
	if err != nil {
		_ = conn.Close()
		return nil, -1, err
	}
	return conn, outFD, nil
}

func ZeroCopyFileHandler(w http.ResponseWriter, req *http.Request) {
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
	offset, logicalSizeLimit, err := parseChunkWindow(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fd, _, err := GetFile(txferID, fileID, fullPathRaw)
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

	maybeFadviseSequential(fd, offset, windowLen)
	if _, err := fd.Seek(offset, io.SeekStart); err != nil {
		http.Error(w, "failed to seek file", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(windowLen, 10))
	flusher, _ := w.(http.Flusher)

	remaining := windowLen
	for remaining > 0 {
		chunkLen := min(remaining, zeroCopyChunkSize)
		written, copyErr := io.CopyN(w, fd, chunkLen)
		remaining -= written
		if copyErr != nil {
			if errors.Is(copyErr, io.EOF) || errors.Is(copyErr, io.ErrUnexpectedEOF) {
				log.Printf("filexfer zerocopy: short read tid=%s fid=%d want=%d wrote=%d err=%v", txferID, fileID, chunkLen, written, copyErr)
				return
			}
			log.Printf("filexfer zerocopy: stream write failed tid=%s fid=%d wrote=%d err=%v", txferID, fileID, written, copyErr)
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
}

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
	// Try to hint to the kernel we might want to read this data
	// in short order.
	maybeFadviseSequential(fd, offset, windowLen)

	ageRecipient, err := parseAgePublicKey(req.URL.Query().Get("age-public-key"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rawSpliceFD := strings.ToLower(strings.TrimSpace(req.URL.Query().Get("splice-fd")))
	explicitSocketSplice := rawSpliceFD != ""
	socketSpliceEnabled := true
	switch rawSpliceFD {
	case "", "1", "true", "on", "yes":
		// default-enabled unless explicitly disabled
	case "0", "false", "off", "no":
		socketSpliceEnabled = false
	default:
		http.Error(w, "invalid query parameter: splice-fd", http.StatusBadRequest)
		return
	}
	if ageRecipient != nil {
		if explicitSocketSplice && socketSpliceEnabled {
			http.Error(w, "splice-fd cannot be combined with age-public-key", http.StatusBadRequest)
			return
		}
		socketSpliceEnabled = false
	}
	experimentalSocketSplice := socketSpliceEnabled && runtime.GOOS == "linux"
	frameWriter := io.Writer(w)
	socketSpliceFD := -1
	var hijackedConn net.Conn
	if experimentalSocketSplice {
		conn, outFD, hijackErr := hijackStreamingConn(rawWriter)
		if hijackErr != nil {
			if explicitSocketSplice {
				http.Error(w, "failed to initialize hijacked streaming response", http.StatusInternalServerError)
				return
			}
			// Best-effort default: fall back to regular writer path when hijack is unavailable.
			experimentalSocketSplice = false
			log.Printf("filexfer: splice-fd fallback tid=%s fid=%d err=%v", txferID, fileID, hijackErr)
		} else {
			hijackedConn = conn
			socketSpliceFD = outFD
			frameWriter = conn
			defer func() {
				if socketSpliceFD >= 0 {
					_ = unix.Close(socketSpliceFD)
				}
				_ = hijackedConn.Close()
			}()
		}
	}
	if ageRecipient != nil {
		w.Header().Set("X-Filexfer-Enc", "age")
		encryptedRW, closeEncrypted, encErr := wrapAgeEncryptedResponseWriter(w, ageRecipient)
		if encErr != nil {
			http.Error(w, "failed to initialize encrypted response", http.StatusInternalServerError)
			return
		}
		defer func() {
			if cerr := closeEncrypted(); cerr != nil {
				log.Printf("filexfer frame: failed to finalize encrypted stream tid=%s fid=%d err=%v", txferID, fileID, cerr)
			}
		}()
		frameWriter = encryptedRW
	}

	if !experimentalSocketSplice {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	maxWSizeHint := min(windowLen, defaultFileFrameLogicalSize)
	cursor := offset
	windowStart := offset
	windowWireTotal := int64(0)    // no compression currently; equal to logical
	windowLogicalTotal := int64(0) // bytes served for this request window
	windowFrames := 0
	windowTS0 := time.Now().UnixMilli()
	pipeSizeBytes := desiredPipeSizeBytes(windowLen, defaultFileFrameLogicalSize)
	hashPipeR, hashPipeW, err := os.Pipe()
	if err != nil {
		http.Error(w, "failed to initialize window hash pipe", http.StatusInternalServerError)
		return
	}
	defer hashPipeR.Close()
	hashDone := startWindowHashWorker(hashPipeR)
	defer func() {
		if hashPipeW != nil {
			_ = hashPipeW.Close()
		}
		if hashDone != nil {
			<-hashDone
		}
	}()
	useLinuxSplice := runtime.GOOS == "linux"
	firstFrame := true
	for remaining := windowLen; remaining > 0; {
		frameSize := min(remaining, defaultFileFrameLogicalSize)
		headerTS := time.Now().UnixMilli()
		var maxHint *int64
		if firstFrame && maxWSizeHint > 0 {
			maxHint = &maxWSizeHint
		}
		headerLine := buildFrameHeaderLine(fileID, cursor, frameSize, maxHint, headerTS)
		if _, err := io.WriteString(frameWriter, headerLine); err != nil {
			if errors.Is(err, errFileStreamTimeLimitExceeded) && !limitedWriter.wroteAnyBody() {
				http.Error(rawWriter, "file stream time limit exceeded", http.StatusGatewayTimeout)
				return
			}
			return
		}

		frameOffset := cursor
		var payloadErr error
		if useLinuxSplice {
			payloadErr = streamFramePayloadLinuxSpliceToOutput(fd, &frameOffset, frameSize, frameWriter, hashPipeW, pipeSizeBytes, socketSpliceFD)
		} else {
			payloadErr = streamFramePayloadBuffered(fd, &frameOffset, frameSize, frameWriter, hashPipeW)
		}
		if payloadErr != nil {
			if errors.Is(payloadErr, errFileStreamTimeLimitExceeded) && !limitedWriter.wroteAnyBody() {
				http.Error(rawWriter, "file stream time limit exceeded", http.StatusGatewayTimeout)
				return
			}
			return
		}

		cursor = frameOffset
		remaining -= frameSize
		firstFrame = false
		windowFrames++
		windowLogicalTotal += frameSize
		windowWireTotal += frameSize

		isTerminalResponseChunk := remaining == 0
		nextValue := cursor
		if isTerminalResponseChunk {
			nextValue = 0
		}
		trailerTS := time.Now().UnixMilli()
		windowHashToken := ""
		var terminalMD *fileFrameMetadata
		if isTerminalResponseChunk {
			if hashPipeW != nil {
				_ = hashPipeW.Close()
				hashPipeW = nil
			}
			if hashDone == nil {
				if socketSpliceFD < 0 {
					http.Error(w, "failed to finalize window hash state", http.StatusInternalServerError)
				}
				return
			}
			result := <-hashDone
			hashDone = nil
			if result.err != nil {
				if socketSpliceFD < 0 {
					http.Error(w, "failed to finalize window hash", http.StatusInternalServerError)
				}
				return
			}
			windowHashToken = result.token
			if !SetTransferFileWindowHash(txferID, fileID, cursor, windowHashToken) {
				if socketSpliceFD < 0 {
					http.Error(w, "failed to store window hash state", http.StatusInternalServerError)
				}
				return
			}
			md := collectFileFrameMetadata(fileRef.Path, fileInfo)
			terminalMD = &md
		}
		trailerLine := buildFrameTrailerLine(fileID, trailerTS, nextValue, windowHashToken, terminalMD)
		if _, err := io.WriteString(frameWriter, trailerLine); err != nil {
			if errors.Is(err, errFileStreamTimeLimitExceeded) && !limitedWriter.wroteAnyBody() {
				http.Error(rawWriter, "file stream time limit exceeded", http.StatusGatewayTimeout)
				return
			}
			return
		}
		if fl, ok := frameWriter.(http.Flusher); ok {
			fl.Flush()
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
	if ackBytes >= 0 {
		if ackHashToken == "" {
			http.Error(w, "missing window ack hash token", http.StatusBadRequest)
			return
		}
		if !VerifyTransferFileWindowHash(txferID, fileID, ackTarget, ackHashToken) {
			http.Error(w, "window ack hash token mismatch", http.StatusConflict)
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
	case "", "adapt":
		return CompressionModeNone
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

func prepareFramePayloadAt(fd *os.File, compressors *frameCompressor, mode CompressionMode, logicalBuffer []byte, offset int64, logicalSize int64) ([]byte, []byte, int64, xxh3.Uint128, error) {
	if logicalSize < 0 {
		return nil, nil, 0, xxh3.Uint128{}, errors.New("negative logical size")
	}
	if compressors == nil {
		return nil, nil, 0, xxh3.Uint128{}, errors.New("nil compressor context")
	}
	if logicalSize > int64(len(logicalBuffer)) {
		return nil, nil, 0, xxh3.Uint128{}, errors.New("logical frame size exceeds reusable buffer")
	}
	if fd == nil {
		return nil, nil, 0, xxh3.Uint128{}, errors.New("nil file descriptor")
	}

	logical := logicalBuffer[:logicalSize]
	if _, err := fd.ReadAt(logical, offset); err != nil {
		return nil, nil, 0, xxh3.Uint128{}, err
	}
	hash128 := xxh3.Hash128(logical)

	payload, err := compressors.Compress(logical, mode)
	if err != nil {
		return nil, nil, 0, xxh3.Uint128{}, err
	}
	return logical, payload, int64(len(payload)), hash128, nil
}

func releasePreparedFrames(prepared []preparedFrame) {
	for i := range prepared {
		if prepared[i].release != nil {
			prepared[i].release()
			prepared[i].release = nil
		}
	}
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
			enc, err := zstd.NewWriter(&c.buf, zstd.WithEncoderConcurrency(1))
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
			enc, err := zstd.NewWriter(&c.buf, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
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

func maybeFadviseSequential(fd *os.File, offset int64, length int64) {
	if fd == nil || length <= 0 {
		return
	}
	_ = unix.Fadvise(int(fd.Fd()), offset, length, unix.FADV_SEQUENTIAL)
}
