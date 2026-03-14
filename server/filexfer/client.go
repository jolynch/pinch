package filexfer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"filippo.io/age"
	intencoding "github.com/jolynch/pinch/internal/filexfer/encoding"
	"github.com/jolynch/pinch/utils"
	"github.com/zeebo/xxh3"
)

const (
	EncodingIdentity = "identity"
	EncodingZstd     = "zstd"
	EncodingLz4      = "lz4"
)

const (
	LoadStrategyFast   = "fast"
	LoadStrategyGentle = "gentle"
)

type DownloadStatus struct {
	Started int `json:"started"`
	Running int `json:"running"`
	Done    int `json:"done"`
	Missing int `json:"missing"`
}

type TransferStatus struct {
	TransferID     string         `json:"transfer_id"`
	Directory      string         `json:"directory"`
	NumFiles       int            `json:"num_files"`
	TotalSize      int64          `json:"total_size"`
	Done           uint64         `json:"done"`
	DoneSize       int64          `json:"done_size"`
	PercentFiles   float64        `json:"percent_files"`
	PercentBytes   float64        `json:"percent_bytes"`
	DownloadStatus DownloadStatus `json:"download_status"`
}

type ClientOption interface {
	apply(*Client)
}

type clientOptionFunc func(*Client)

func (f clientOptionFunc) apply(c *Client) {
	f(c)
}

func WithContextDialer(dialer func(context.Context, string) (net.Conn, error)) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.contextDialer = dialer
	})
}

func WithServerAgePublicKey(publicKey string) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.ServerAgePublicKey = strings.TrimSpace(publicKey)
	})
}

func WithFileRequestWindowBytes(windowBytes int64) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.FileRequestWindowBytes = windowBytes
	})
}

func WithBatchMaxBytes(batchMaxBytes int64) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.BatchMaxBytes = batchMaxBytes
	})
}

func WithFrameBufferBytes(bufferBytes int) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.FrameBufferBytes = bufferBytes
	})
}

func WithMaxFrameReadBufferBytes(bufferBytes int) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.MaxFrameReadBufferBytes = bufferBytes
	})
}

func WithAckRequestTimeout(timeout time.Duration) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.AckRequestTimeout = timeout
	})
}

func WithSocketReadBufferBytes(bufferBytes int) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.SocketReadBufferBytes = bufferBytes
	})
}

func WithLoadStrategy(strategy string) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.LoadStrategy = normalizeLoadStrategy(strategy)
	})
}

type Client struct {
	FileAddr                string
	ServerAgePublicKey      string
	FileRequestWindowBytes  int64
	BatchMaxBytes           int64
	FrameBufferBytes        int
	MaxFrameReadBufferBytes int
	AckRequestTimeout       time.Duration
	SocketReadBufferBytes   int
	LoadStrategy            string

	// Context dialer allows clients to setup custom connections
	// For example injecting TLS
	contextDialer func(context.Context, string) (net.Conn, error)

	// bufferPool caches reusable frame-read buffers keyed by bucketed size.
	bufferPool sync.Map // map[int]*sync.Pool

	// scratchBufferPool caches reusable temporary byte buffers.
	scratchBufferPool sync.Pool
}

type Manifest struct {
	TransferID  string
	Root        string
	Mode        string
	LinkMbps    int64
	Concurrency int
	Entries     []ManifestEntry
}

type ManifestEntry struct {
	ID       uint64
	Size     int64
	Mtime    int64
	Mode     os.FileMode
	Path     string
	Progress ManifestProgress
}

type ManifestProgress struct {
	AckBytes     int64
	MetadataDone bool
}

type FileTrailerMetadata struct {
	Size    int64
	MtimeNS int64
	Mode    string
	UID     string
	GID     string
	User    string
	Group   string
}

type FileFrameMeta struct {
	FileID          uint64
	Comp            string
	CompCounts      map[string]uint64
	Enc             string
	Offset          int64
	Size            int64
	WireSize        int64
	MaxWireSizeHint int64
	HeaderTS        int64
	TrailerTS       int64
	HashToken       string
	FileHashToken   string
	TrailerMetadata *FileTrailerMetadata
}

type DownloadProgressUpdate struct {
	TransferID  string
	FileID      uint64
	CopiedBytes int64
	TargetBytes int64
	AckBytes    int64
	UpdateTime  time.Time
}

type DownloadFileResponse struct {
	Meta          FileFrameMeta
	LocalFileHash string
}

type DownloadBatchRequest struct {
	Manifest *Manifest
	FileIDs  []uint64
	OutputWriter func(ManifestEntry, int64) (io.WriteCloser, func() error, error)
	// BatchMaxBytes is the unit of parallel work for a single large file: when a file
	// exceeds BatchMaxBytes, it is split into windows of this size and downloaded
	// concurrently. The number of concurrent windows is bounded by
	// FileRequestWindowBytes/BatchMaxBytes — e.g. a 1 MiB batch with a 10 MiB window
	// allows up to 10 concurrent requests against that file, while a 10 MiB batch allows
	// only one. BatchMaxBytes must be <= Client.FileRequestWindowBytes.
	BatchMaxBytes   int64
	AgePublicKey    string
	AgeIdentity     string
	ProgressUpdates chan<- DownloadProgressUpdate
}

type DownloadBatchResponse struct {
	Files []DownloadFileResponse
}

type StartFromManifestRequest struct {
	Manifest     *Manifest
	Entries      []ManifestEntry
	OutputWriter func(ManifestEntry, int64) (io.WriteCloser, func() error, error)
	AgePublicKey string
	AgeIdentity  string
	Concurrency  int
	// BatchMaxBytes is the unit of parallel work per file. See DownloadBatchRequest.BatchMaxBytes.
	BatchMaxBytes   int64
	ProgressUpdates chan<- DownloadProgressUpdate
	OnFileDone      func(StartFileDoneEvent)
}

type StartFileDoneEvent struct {
	File    DownloadFileResponse
	Elapsed time.Duration
}

type StartFromManifestResponse struct {
	Requested        int
	Downloaded       int
	Failed           int
	TransferredBytes int64
	Errors           []error
}

type FetchManifestRequest struct {
	Directory    string
	Verbose      bool
	MaxChunkSize int
	Mode         string
	LinkMbps     int64
	Concurrency  int
	AgePublicKey string
	AgeIdentity  string
}

type ProbeRequest struct {
	Samples      int
	ProbeBytes   int64
	LoadStrategy string
	AgePublicKey string
	AgeIdentity  string
}

type ProbeResponse struct {
	ServerCPU            int
	AvgLatencyMS         int64
	LinkMbps             int64
	SuggestedConcurrency int
}

type FetchManifestResponse struct {
	Manifest *Manifest
}

type FetchFileRequest struct {
	TransferID   string
	Files        []FetchFileTarget
	AgePublicKey string
	AgeIdentity  string
	AckBytes     int64
}

type FetchFileTarget struct {
	FileID   uint64
	FullPath string
	Offset   int64
	Size     int64
}

type FetchFileResponse struct {
	Reader io.ReadCloser
	Meta   *FileFrameMeta
}

type FetchChecksumStreamRequest struct {
	TransferID   string
	FileID       uint64
	FullPath     string
	WindowSize   int64
	ChecksumsCSV string
	AgePublicKey string
	AgeIdentity  string
}

type FetchChecksumStreamResponse struct {
	Reader io.ReadCloser
}

type AcknowledgeFileProgressRequest struct {
	TransferID string
	FileID     uint64
	FullPath   string
	AckBytes   int64
	ServerTS   int64
	HashToken  string
	DeltaBytes int64
	RecvMS     int64
	SyncMS     int64
}

type AcknowledgeFileProgressResponse struct{}

type GetTransferStatusRequest struct {
	TransferID string
}

type GetTransferStatusResponse struct {
	Status *TransferStatus
}

type fileMissingError struct {
	Status int
	Body   string
}

func (e *fileMissingError) Error() string {
	return fmt.Sprintf("status=%d body=%s", e.Status, e.Body)
}

var ErrFileMissing = errors.New("file missing")

const (
	// Window and batch sizes. The window is max in-flight bytes per file; the batch is
	// the unit of parallel work. parallelism = window / batch.
	defaultClientRequestWindowBytes      int64 = 1024 * 1024 * 1024
	defaultClientMaxFrameReadBufferBytes int   = 64 * 1024 * 1024 // also the fallback BatchMaxBytes

	// Frame read-buffer bounds, scaled per stream based on file/frame size.
	defaultClientFrameBufferBytes int = 8 * 1024 * 1024
	minClientFrameReadBufferBytes int = 32 * 1024

	// Scratch buffers used for request/response payloads (headers, manifests, etc.).
	defaultClientScratchBufferBytes int = 64 * 1024
	maxClientScratchBufferPoolBytes int = 16 * 1024 * 1024

	defaultClientAckRequestTimeout = 15 * time.Second
	defaultClientLoadStrategy      = LoadStrategyFast

	// Probe parameters for server capability detection and concurrency selection.
	defaultClientProbeBytes   int64 = 1 * 1024 * 1024
	defaultClientProbeSamples       = 3

	// Concurrency bounds for StartFromManifest auto-selection.
	minAutoStartConcurrency = 2
	maxAutoStartConcurrency = 256
)

func NewClient(fileAddr string, opts ...ClientOption) *Client {
	trimmed := strings.TrimSpace(fileAddr)
	c := &Client{
		FileAddr:                trimmed,
		ServerAgePublicKey:      strings.TrimSpace(os.Getenv("PINCH_FILE_SERVER_AGE_PUBLIC_KEY")),
		FileRequestWindowBytes:  defaultClientRequestWindowBytes,
		FrameBufferBytes:        defaultClientFrameBufferBytes,
		MaxFrameReadBufferBytes: defaultClientMaxFrameReadBufferBytes,
		AckRequestTimeout:       defaultClientAckRequestTimeout,
		SocketReadBufferBytes:   utils.MaxSocketReadBufferBytes(),
		LoadStrategy:            defaultClientLoadStrategy,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt.apply(c)
	}
	c.bufferPool = sync.Map{}
	c.scratchBufferPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, defaultClientScratchBufferBytes))
		},
	}
	return c
}

func (c *Client) FetchManifest(ctx context.Context, request FetchManifestRequest) (FetchManifestResponse, error) {
	if c == nil {
		return FetchManifestResponse{}, errors.New("nil client")
	}
	if request.Directory == "" {
		return FetchManifestResponse{}, errors.New("missing directory")
	}
	request.Mode = strings.ToLower(strings.TrimSpace(request.Mode))
	if request.Mode != LoadStrategyFast && request.Mode != LoadStrategyGentle {
		return FetchManifestResponse{}, errors.New("invalid mode")
	}
	if request.LinkMbps < 0 {
		return FetchManifestResponse{}, errors.New("link mbps must be >= 0")
	}
	if request.Concurrency <= 0 {
		return FetchManifestResponse{}, errors.New("concurrency must be > 0")
	}
	return c.fetchManifestTCP(ctx, request)
}

func DefaultClientConcurrency() int {
	n := runtime.NumCPU() * 2
	if n < 1 {
		return 1
	}
	return n
}

func normalizeLoadStrategy(strategy string) string {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case LoadStrategyGentle:
		return LoadStrategyGentle
	default:
		return LoadStrategyFast
	}
}

func SaveManifest(path string, manifest *Manifest) error {
	if manifest == nil {
		return errors.New("nil manifest")
	}
	if path == "" {
		return errors.New("missing path")
	}
	raw, err := marshalManifest(manifest)
	if err != nil {
		return err
	}
	parent := filepath.Dir(path)
	if parent != "." && parent != "" {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("create manifest parent directory: %w", err)
		}
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

func MarshalManifest(manifest *Manifest) ([]byte, error) {
	return marshalManifest(manifest)
}

func LoadManifest(path string) (*Manifest, error) {
	if path == "" {
		return nil, errors.New("missing path")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	return parseManifest(raw)
}

func (m *Manifest) EntryByID(id uint64) (ManifestEntry, bool) {
	if m == nil {
		return ManifestEntry{}, false
	}
	for _, entry := range m.Entries {
		if entry.ID == id {
			return entry, true
		}
	}
	return ManifestEntry{}, false
}

func (c *Client) FetchFile(ctx context.Context, request FetchFileRequest) (FetchFileResponse, error) {
	_ = request.AckBytes
	if len(request.Files) != 1 {
		return FetchFileResponse{}, errors.New("FetchFile requires exactly one file target")
	}
	target := request.Files[0]
	reader, meta, err := c.fetchFileWindow(
		ctx,
		request.TransferID,
		target.FileID,
		target.FullPath,
		request.AgePublicKey,
		request.AgeIdentity,
		0,
		-1,
	)
	if err != nil {
		return FetchFileResponse{}, err
	}
	return FetchFileResponse{
		Reader: reader,
		Meta:   meta,
	}, nil
}

func (c *Client) fetchFileWindow(
	ctx context.Context,
	txferID string,
	fileID uint64,
	fullPath string,
	agePublicKey string,
	ageIdentity string,
	offset int64,
	size int64,
) (io.ReadCloser, *FileFrameMeta, error) {
	if c == nil {
		return nil, nil, errors.New("nil client")
	}
	if txferID == "" {
		return nil, nil, errors.New("missing transfer id")
	}
	if fullPath == "" {
		return nil, nil, errors.New("missing full path")
	}
	target := FetchFileTarget{
		FileID:   fileID,
		FullPath: fullPath,
		Offset:   offset,
	}
	if size > 0 {
		target.Size = size
	}
	stream, err := c.fetchFileBatchTCP(
		ctx,
		txferID,
		[]FetchFileTarget{target},
		agePublicKey,
		ageIdentity,
	)
	if err != nil {
		return nil, nil, err
	}
	fileStream, meta, err := c.newFileStream(stream, "", target.Size)
	if err != nil {
		_ = stream.Close()
		return nil, nil, err
	}
	if meta.FileID != fileID {
		_ = fileStream.Close()
		return nil, nil, fmt.Errorf("file id mismatch: expected %d got %d", fileID, meta.FileID)
	}
	return fileStream, meta, nil
}

func (c *Client) FetchChecksumStream(ctx context.Context, request FetchChecksumStreamRequest) (FetchChecksumStreamResponse, error) {
	if c == nil {
		return FetchChecksumStreamResponse{}, errors.New("nil client")
	}
	if request.TransferID == "" {
		return FetchChecksumStreamResponse{}, errors.New("missing transfer id")
	}
	if request.FullPath == "" {
		return FetchChecksumStreamResponse{}, errors.New("missing full path")
	}

	reader, err := c.fetchChecksumStreamTCP(ctx, request)
	if err != nil {
		return FetchChecksumStreamResponse{}, err
	}
	return FetchChecksumStreamResponse{Reader: reader}, nil
}

func (c *Client) acknowledgeMissingFile(ctx context.Context, transferID string, fileID uint64, fullPath string) error {
	ackTimeout := c.AckRequestTimeout
	if ackTimeout <= 0 {
		ackTimeout = defaultClientAckRequestTimeout
	}
	return retryAck(ctx, func(callCtx context.Context) error {
		ackCtx, cancel := context.WithTimeout(callCtx, ackTimeout)
		defer cancel()
		_, err := c.AcknowledgeFileProgress(ackCtx, AcknowledgeFileProgressRequest{
			TransferID: transferID,
			FileID:     fileID,
			FullPath:   fullPath,
			AckBytes:   -1,
		})
		return err
	})
}

func (c *Client) GetTransferStatus(ctx context.Context, request GetTransferStatusRequest) (GetTransferStatusResponse, error) {
	if c == nil {
		return GetTransferStatusResponse{}, errors.New("nil client")
	}
	if request.TransferID == "" {
		return GetTransferStatusResponse{}, errors.New("missing transfer id")
	}
	return c.getTransferStatusTCP(ctx, request)
}

type downloadBatchPlan struct {
	entry      ManifestEntry
	serverPath string
	resumeFrom int64
}

type splitWindow struct {
	start int64
	size  int64
	end   int64
}

type splitWindowResult struct {
	window   splitWindow
	response DownloadFileResponse
	ack      AcknowledgeFileProgressRequest
}

func (c *Client) DownloadFilesFromManifestBatch(ctx context.Context, req DownloadBatchRequest) (DownloadBatchResponse, error) {
	if req.Manifest == nil {
		return DownloadBatchResponse{}, errors.New("nil manifest")
	}
	if len(req.FileIDs) == 0 {
		return DownloadBatchResponse{}, errors.New("empty file batch")
	}
	if req.OutputWriter == nil {
		return DownloadBatchResponse{}, errors.New("missing output writer callback")
	}
	explicitBatch := req.BatchMaxBytes > 0 || (c != nil && c.BatchMaxBytes > 0)
	req.BatchMaxBytes = c.effectiveBatchMaxBytes(req.BatchMaxBytes)
	windowBytes := c.FileRequestWindowBytes
	if windowBytes <= 0 {
		windowBytes = defaultClientRequestWindowBytes
	}
	if explicitBatch && req.BatchMaxBytes > windowBytes {
		return DownloadBatchResponse{}, fmt.Errorf(
			"batch size %d exceeds window size %d: batch is the unit of parallel work per file, window is the max outstanding bytes across all concurrent requests for that file",
			req.BatchMaxBytes, windowBytes,
		)
	}

	plans := make([]downloadBatchPlan, 0, len(req.FileIDs))
	targets := make([]FetchFileTarget, 0, len(req.FileIDs))
	for _, fileID := range req.FileIDs {
		entry, serverPath, err := resolveManifestEntryPath(req.Manifest, fileID)
		if err != nil {
			return DownloadBatchResponse{}, err
		}
		resumeFrom := entry.Progress.AckBytes
		if resumeFrom < 0 {
			return DownloadBatchResponse{}, fmt.Errorf("file %d resume offset must be >= 0", fileID)
		}
		if entry.Size >= 0 && resumeFrom > entry.Size {
			return DownloadBatchResponse{}, fmt.Errorf("file %d resume offset %d exceeds file size %d", fileID, resumeFrom, entry.Size)
		}
		plans = append(plans, downloadBatchPlan{
			entry:      entry,
			serverPath: serverPath,
			resumeFrom: resumeFrom,
		})
		target := FetchFileTarget{FileID: fileID, FullPath: serverPath, Offset: resumeFrom}
		if entry.Size > resumeFrom {
			target.Size = entry.Size - resumeFrom
		}
		targets = append(targets, target)
	}

	if shouldSplitSingleFileBatch(req, plans) {
		return c.downloadManifestBatchWindows(ctx, req, plans[0])
	}
	return c.downloadManifestBatchSequential(ctx, req, plans, targets)
}

func shouldSplitSingleFileBatch(req DownloadBatchRequest, plans []downloadBatchPlan) bool {
	if len(plans) != 1 || req.BatchMaxBytes <= 0 {
		return false
	}
	entry := plans[0].entry
	if entry.Size < 0 {
		return false
	}
	return entry.Size-plans[0].resumeFrom > req.BatchMaxBytes
}

func (c *Client) downloadManifestBatchSequential(
	ctx context.Context,
	req DownloadBatchRequest,
	plans []downloadBatchPlan,
	targets []FetchFileTarget,
) (DownloadBatchResponse, error) {
	stream, err := c.fetchFileBatchTCP(ctx, req.Manifest.TransferID, targets, req.AgePublicKey, req.AgeIdentity)
	if err != nil {
		var missingErr *fileMissingError
		if len(plans) == 1 && errors.Is(err, ErrFileMissing) && errors.As(err, &missingErr) && shouldAcknowledgeMissing404(missingErr.Body) {
			if ackErr := c.acknowledgeMissingFile(ctx, req.Manifest.TransferID, plans[0].entry.ID, plans[0].serverPath); ackErr != nil {
				return DownloadBatchResponse{}, fmt.Errorf("%w (failed to ack missing: %v)", err, ackErr)
			}
		}
		return DownloadBatchResponse{}, err
	}
	defer stream.Close()
	br := bufio.NewReader(stream)

	results := make([]DownloadFileResponse, 0, len(plans))
	type ackProgress struct {
		fileID      uint64
		ackBytes    int64
		targetBytes int64
	}
	pendingAcks := make([]AcknowledgeFileProgressRequest, 0, len(plans))
	ackProgresses := make([]ackProgress, 0, len(plans))
	var (
		frameBuf        []byte
		releaseFrameBuf func()
	)
	defer func() {
		if releaseFrameBuf != nil {
			releaseFrameBuf()
		}
	}()
	ackTimeout := c.AckRequestTimeout
	if ackTimeout <= 0 {
		ackTimeout = defaultClientAckRequestTimeout
	}
	emitProgressUpdate := func(update DownloadProgressUpdate) {
		if req.ProgressUpdates == nil {
			return
		}
		select {
		case req.ProgressUpdates <- update:
		default:
		}
	}

	for _, plan := range plans {
		writer, syncOutput, err := req.OutputWriter(plan.entry, plan.resumeFrom)
		if err != nil {
			return DownloadBatchResponse{}, fmt.Errorf("create output writer for file %d: %w", plan.entry.ID, err)
		}
		if writer == nil {
			return DownloadBatchResponse{}, fmt.Errorf("create output writer for file %d: nil writer", plan.entry.ID)
		}
		if syncOutput == nil {
			syncOutput = func() error { return nil }
		}
		writerClosed := false
		closeWriter := func() error {
			if writerClosed {
				return nil
			}
			writerClosed = true
			return writer.Close()
		}

		fileHasher := xxh3.New128()
		if plan.resumeFrom > 0 {
			emitProgressUpdate(DownloadProgressUpdate{
				TransferID:  req.Manifest.TransferID,
				FileID:      plan.entry.ID,
				CopiedBytes: plan.resumeFrom,
				TargetBytes: plan.entry.Size,
				UpdateTime:  time.Now(),
			})
		}

		fileStart := time.Now()
		meta := FileFrameMeta{
			FileID: plan.entry.ID,
			Comp:   "none",
			Enc:    "none",
			Offset: plan.resumeFrom,
		}
		offset := plan.resumeFrom
		lastTrailerTS := int64(0)
		var lastMetadata *FileTrailerMetadata
		serverHash := ""
		windowHasher := xxh3.New128()

		for {
			headerLine, readErr := br.ReadString('\n')
			if readErr != nil {
				_ = closeWriter()
				return DownloadBatchResponse{}, fmt.Errorf("read frame header: %w", readErr)
			}
			headerTrimmed := strings.TrimRight(headerLine, "\r\n")
			if isStatusLine(headerTrimmed) {
				_ = closeWriter()
				return DownloadBatchResponse{}, fmt.Errorf("unexpected status line before file complete: %s", headerTrimmed)
			}
			frameMeta, parseErr := parseFXHeader(headerTrimmed)
			if parseErr != nil {
				_ = closeWriter()
				return DownloadBatchResponse{}, parseErr
			}
			if frameMeta.FileID != plan.entry.ID {
				_ = closeWriter()
				return DownloadBatchResponse{}, fmt.Errorf("batched file id mismatch: expected=%d got=%d", plan.entry.ID, frameMeta.FileID)
			}
			if frameMeta.Offset != offset {
				_ = closeWriter()
				return DownloadBatchResponse{}, fmt.Errorf("batched offset mismatch: expected=%d got=%d", offset, frameMeta.Offset)
			}
			if frameBuf == nil {
				frameBuf, releaseFrameBuf = c.acquireFrameReadBuffer(frameMeta.MaxWireSizeHint)
			}

			payloadReader := io.LimitReader(br, frameMeta.WireSize)
			logicalReader, decodeErr := decodePayloadReader(payloadReader, frameMeta.Comp, frameMeta.Enc, nil)
			if decodeErr != nil {
				_ = closeWriter()
				return DownloadBatchResponse{}, fmt.Errorf("decode payload reader: %w", decodeErr)
			}
			frameStartOffset := offset
			copyErr := copyStreamWithProgress(io.MultiWriter(writer, fileHasher, windowHasher), logicalReader, frameBuf, nil, func(written int64) error {
				emitProgressUpdate(DownloadProgressUpdate{
					TransferID:  req.Manifest.TransferID,
					FileID:      plan.entry.ID,
					CopiedBytes: frameStartOffset + written,
					TargetBytes: plan.entry.Size,
					UpdateTime:  time.Now(),
				})
				return nil
			})
			closeLogicalErr := logicalReader.Close()
			if copyErr != nil {
				_ = closeWriter()
				return DownloadBatchResponse{}, fmt.Errorf("stream output file: %w", copyErr)
			}
			if closeLogicalErr != nil {
				_ = closeWriter()
				return DownloadBatchResponse{}, closeLogicalErr
			}
			meta.Size += frameMeta.Size
			meta.WireSize += frameMeta.WireSize
			meta.Comp = frameMeta.Comp
			meta.Enc = frameMeta.Enc
			offset += frameMeta.Size

			trailerLine, trailerReadErr := br.ReadString('\n')
			if trailerReadErr != nil {
				_ = closeWriter()
				return DownloadBatchResponse{}, fmt.Errorf("read frame trailer: %w", trailerReadErr)
			}
			trailer, trailerErr := parseFXTrailer(strings.TrimRight(trailerLine, "\r\n"))
			if trailerErr != nil {
				_ = closeWriter()
				return DownloadBatchResponse{}, trailerErr
			}
			if trailer.FileID != plan.entry.ID {
				_ = closeWriter()
				return DownloadBatchResponse{}, fmt.Errorf("trailer file id mismatch: expected=%d got=%d", plan.entry.ID, trailer.FileID)
			}
			lastTrailerTS = trailer.TS
			if trailer.Metadata != nil {
				lastMetadata = cloneTrailerMetadata(trailer.Metadata)
			}
			if trailer.Next == nil {
				serverHash = trailer.FileHashToken
				break
			}
			if *trailer.Next == 0 {
				serverHash = trailer.FileHashToken
				break
			}
			if *trailer.Next != offset {
				_ = closeWriter()
				return DownloadBatchResponse{}, fmt.Errorf("invalid trailer next offset: expected=%d got=%d", offset, *trailer.Next)
			}
		}
		if offset != plan.entry.Size {
			_ = closeWriter()
			return DownloadBatchResponse{}, fmt.Errorf("batched file size mismatch: expected=%d got=%d", plan.entry.Size, offset)
		}

		windowHash := intencoding.FormatXXH128HashToken(windowHasher.Sum128())
		if serverHash == "" {
			_ = closeWriter()
			return DownloadBatchResponse{}, errors.New("window hash missing from trailer")
		}
		if !strings.EqualFold(serverHash, windowHash) {
			_ = closeWriter()
			return DownloadBatchResponse{}, fmt.Errorf("window hash mismatch: server=%s client=%s", serverHash, windowHash)
		}

		syncMS := int64(0)
		syncStart := time.Now()
		if err := syncOutput(); err != nil {
			_ = closeWriter()
			return DownloadBatchResponse{}, fmt.Errorf("sync output for file %d: %w", plan.entry.ID, err)
		}
		syncMS = time.Since(syncStart).Milliseconds()
		if err := closeWriter(); err != nil {
			return DownloadBatchResponse{}, fmt.Errorf("close output for file %d: %w", plan.entry.ID, err)
		}

		localHash := intencoding.FormatXXH128HashToken(fileHasher.Sum128())
		recvMS := time.Since(fileStart).Milliseconds()
		deltaBytes := offset - plan.resumeFrom
		pendingAcks = append(pendingAcks, AcknowledgeFileProgressRequest{
			TransferID: req.Manifest.TransferID,
			FileID:     plan.entry.ID,
			FullPath:   plan.serverPath,
			AckBytes:   offset,
			ServerTS:   lastTrailerTS,
			HashToken:  windowHash,
			DeltaBytes: deltaBytes,
			RecvMS:     recvMS,
			SyncMS:     syncMS,
		})
		ackProgresses = append(ackProgresses, ackProgress{
			fileID:      plan.entry.ID,
			ackBytes:    offset,
			targetBytes: plan.entry.Size,
		})

		meta.TrailerTS = lastTrailerTS
		meta.FileHashToken = windowHash
		meta.TrailerMetadata = lastMetadata
		results = append(results, DownloadFileResponse{
			Meta:          meta,
			LocalFileHash: localHash,
		})
	}

	statusLine, err := readTCPLine(br, maxTCPLineBytes)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return DownloadBatchResponse{}, fmt.Errorf("read batch terminal status: %w", err)
		}
	} else {
		if err := parseErrControlFrame(statusLine); err != nil {
			return DownloadBatchResponse{}, err
		}
		if _, ok := parseOKStatusLine(statusLine); !ok {
			return DownloadBatchResponse{}, fmt.Errorf("unexpected batch terminal response: %s", statusLine)
		}
	}

	if len(pendingAcks) > 0 {
		ackErr := retryAck(ctx, func(callCtx context.Context) error {
			ackCtx, cancel := context.WithTimeout(callCtx, ackTimeout)
			defer cancel()
			_, err := c.acknowledgeFileProgressBatch(ackCtx, pendingAcks)
			return err
		})
		if ackErr != nil {
			return DownloadBatchResponse{}, fmt.Errorf("acknowledge download failed: %w", ackErr)
		}
	}

	for _, progress := range ackProgresses {
		emitProgressUpdate(DownloadProgressUpdate{
			TransferID:  req.Manifest.TransferID,
			FileID:      progress.fileID,
			TargetBytes: progress.targetBytes,
			AckBytes:    progress.ackBytes,
			UpdateTime:  time.Now(),
		})
	}
	return DownloadBatchResponse{Files: results}, nil
}

func (c *Client) downloadManifestBatchWindows(
	ctx context.Context,
	req DownloadBatchRequest,
	plan downloadBatchPlan,
) (DownloadBatchResponse, error) {
	requestCtx := ctx
	windows := buildSplitWindows(plan.resumeFrom, plan.entry.Size, req.BatchMaxBytes)
	if len(windows) == 0 {
		return DownloadBatchResponse{}, errors.New("empty split window plan")
	}

	ackTimeout := c.AckRequestTimeout
	if ackTimeout <= 0 {
		ackTimeout = defaultClientAckRequestTimeout
	}
	emitProgressUpdate := func(update DownloadProgressUpdate) {
		if req.ProgressUpdates == nil {
			return
		}
		select {
		case req.ProgressUpdates <- update:
		default:
		}
	}
	if plan.resumeFrom > 0 {
		emitProgressUpdate(DownloadProgressUpdate{
			TransferID:  req.Manifest.TransferID,
			FileID:      plan.entry.ID,
			CopiedBytes: plan.resumeFrom,
			TargetBytes: plan.entry.Size,
			UpdateTime:  time.Now(),
		})
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var initialWriter io.WriteCloser
	var initialSync func() error
	if plan.resumeFrom == 0 {
		initialWriterResult, initialSyncResult, initialErr := req.OutputWriter(plan.entry, 0)
		if initialErr != nil {
			return DownloadBatchResponse{}, fmt.Errorf("create output writer for file %d: %w", plan.entry.ID, initialErr)
		}
		initialWriter, initialSync = initialWriterResult, initialSyncResult
		if initialWriter == nil {
			return DownloadBatchResponse{}, fmt.Errorf("create output writer for file %d: nil writer", plan.entry.ID)
		}
		if initialSync == nil {
			initialSync = func() error { return nil }
		}
	}

	maxWorkers := maxSplitWindowWorkers(c.FileRequestWindowBytes, req.BatchMaxBytes, len(windows))
	limiter := make(chan struct{}, maxWorkers)
	results := make(chan splitWindowResult, len(windows))
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	setErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	// windows[0].start == plan.resumeFrom always, so initialWriter is only ever used by i==0.
	for i, window := range windows {
		w, s := io.WriteCloser(nil), func() error { return nil }
		if i == 0 && initialWriter != nil {
			w, s = initialWriter, initialSync
		}
		wg.Add(1)
		go func(window splitWindow, w io.WriteCloser, s func() error) {
			defer wg.Done()
			select {
			case limiter <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-limiter }()

			result, err := c.downloadSplitWindow(ctx, req, plan, window, w, s, emitProgressUpdate)
			if err != nil {
				setErr(err)
				return
			}
			select {
			case results <- result:
			case <-ctx.Done():
			}
		}(window, w, s)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	completions := make(map[int64]splitWindowResult, len(windows))
	pending := make(map[int64]splitWindowResult, len(windows))
	nextAckOffset := plan.resumeFrom

	for result := range results {
		completions[result.window.start] = result
		pending[result.window.start] = result

		// Collect the contiguous chain of completed windows starting at nextAckOffset.
		var ackBatch []AcknowledgeFileProgressRequest
		for next := nextAckOffset; ; {
			ready, ok := pending[next]
			if !ok {
				break
			}
			ackBatch = append(ackBatch, ready.ack)
			next = ready.window.end
		}
		if len(ackBatch) == 0 {
			continue
		}
		ackErr := retryAck(requestCtx, func(callCtx context.Context) error {
			ackCtx, cancelAck := context.WithTimeout(callCtx, ackTimeout)
			defer cancelAck()
			_, err := c.acknowledgeFileProgressBatch(ackCtx, ackBatch)
			return err
		})
		if ackErr != nil {
			setErr(fmt.Errorf("acknowledge download failed: %w", ackErr))
			break
		}
		for next := nextAckOffset; ; {
			ready, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, ready.window.start)
			nextAckOffset = ready.window.end
			emitProgressUpdate(DownloadProgressUpdate{
				TransferID:  req.Manifest.TransferID,
				FileID:      plan.entry.ID,
				TargetBytes: plan.entry.Size,
				AckBytes:    ready.ack.AckBytes,
				UpdateTime:  time.Now(),
			})
			next = nextAckOffset
		}
	}

	if firstErr != nil {
		var missingErr *fileMissingError
		if errors.Is(firstErr, ErrFileMissing) && errors.As(firstErr, &missingErr) && shouldAcknowledgeMissing404(missingErr.Body) {
			if ackErr := c.acknowledgeMissingFile(requestCtx, req.Manifest.TransferID, plan.entry.ID, plan.serverPath); ackErr != nil {
				return DownloadBatchResponse{}, fmt.Errorf("%w (failed to ack missing: %v)", firstErr, ackErr)
			}
		}
		return DownloadBatchResponse{}, firstErr
	}
	if err := ctx.Err(); err != nil {
		return DownloadBatchResponse{}, err
	}
	if nextAckOffset != plan.entry.Size {
		return DownloadBatchResponse{}, fmt.Errorf("split ack offset mismatch: expected=%d got=%d", plan.entry.Size, nextAckOffset)
	}

	aggregate, err := aggregateSplitWindowResults(plan, windows, completions)
	if err != nil {
		return DownloadBatchResponse{}, err
	}
	return DownloadBatchResponse{Files: []DownloadFileResponse{aggregate}}, nil
}

func buildSplitWindows(start int64, end int64, maxBytes int64) []splitWindow {
	if maxBytes <= 0 || end <= start {
		return nil
	}
	windows := make([]splitWindow, 0, int((end-start+maxBytes-1)/maxBytes))
	for offset := start; offset < end; {
		size := min(maxBytes, end-offset)
		windows = append(windows, splitWindow{
			start: offset,
			size:  size,
			end:   offset + size,
		})
		offset += size
	}
	return windows
}

func maxSplitWindowWorkers(windowBytes int64, batchBytes int64, numWindows int) int {
	if numWindows < 1 || batchBytes <= 0 {
		return 1
	}
	if windowBytes <= 0 {
		windowBytes = defaultClientRequestWindowBytes
	}
	return max(1, min(numWindows, int(windowBytes/batchBytes)))
}

func (c *Client) downloadSplitWindow(
	ctx context.Context,
	req DownloadBatchRequest,
	plan downloadBatchPlan,
	window splitWindow,
	writer io.WriteCloser,
	syncOutput func() error,
	emitProgressUpdate func(DownloadProgressUpdate),
) (splitWindowResult, error) {
	start := time.Now()
	reader, meta, err := c.fetchFileWindow(
		ctx,
		req.Manifest.TransferID,
		plan.entry.ID,
		plan.serverPath,
		req.AgePublicKey,
		req.AgeIdentity,
		window.start,
		window.size,
	)
	if err != nil {
		if writer != nil {
			_ = writer.Close()
		}
		return splitWindowResult{}, err
	}

	if writer == nil {
		writer, syncOutput, err = req.OutputWriter(plan.entry, window.start)
		if err != nil {
			_ = reader.Close()
			return splitWindowResult{}, fmt.Errorf("create output writer for file %d: %w", plan.entry.ID, err)
		}
		if writer == nil {
			_ = reader.Close()
			return splitWindowResult{}, fmt.Errorf("create output writer for file %d: nil writer", plan.entry.ID)
		}
	}
	if syncOutput == nil {
		syncOutput = func() error { return nil }
	}

	writerClosed := false
	closeWriter := func() error {
		if writerClosed {
			return nil
		}
		writerClosed = true
		return writer.Close()
	}

	frameBuf, releaseFrameBuf := c.acquireFrameReadBuffer(meta.MaxWireSizeHint)
	defer releaseFrameBuf()

	windowHasher := xxh3.New128()
	copyErr := copyStreamWithProgress(io.MultiWriter(writer, windowHasher), reader, frameBuf, nil, func(written int64) error {
		emitProgressUpdate(DownloadProgressUpdate{
			TransferID:  req.Manifest.TransferID,
			FileID:      plan.entry.ID,
			CopiedBytes: window.start + written,
			TargetBytes: plan.entry.Size,
			UpdateTime:  time.Now(),
		})
		return nil
	})
	closeReadErr := reader.Close()
	if copyErr != nil {
		_ = closeWriter()
		return splitWindowResult{}, fmt.Errorf("stream output file: %w", copyErr)
	}
	if closeReadErr != nil {
		_ = closeWriter()
		return splitWindowResult{}, closeReadErr
	}
	if meta.Offset != window.start {
		_ = closeWriter()
		return splitWindowResult{}, fmt.Errorf("split window offset mismatch: expected=%d got=%d", window.start, meta.Offset)
	}
	if meta.Size != window.size {
		_ = closeWriter()
		return splitWindowResult{}, fmt.Errorf("split window size mismatch: expected=%d got=%d", window.size, meta.Size)
	}

	syncStart := time.Now()
	if err := syncOutput(); err != nil {
		_ = closeWriter()
		return splitWindowResult{}, fmt.Errorf("sync output for file %d: %w", plan.entry.ID, err)
	}
	syncMS := time.Since(syncStart).Milliseconds()
	if err := closeWriter(); err != nil {
		return splitWindowResult{}, fmt.Errorf("close output for file %d: %w", plan.entry.ID, err)
	}

	localHash := intencoding.FormatXXH128HashToken(windowHasher.Sum128())
	if meta.FileHashToken == "" {
		return splitWindowResult{}, errors.New("window hash missing from trailer")
	}
	if !strings.EqualFold(meta.FileHashToken, localHash) {
		return splitWindowResult{}, fmt.Errorf("window hash mismatch: server=%s client=%s", meta.FileHashToken, localHash)
	}

	recvMS := time.Since(start).Milliseconds()
	return splitWindowResult{
		window: window,
		response: DownloadFileResponse{
			Meta:          *meta,
			LocalFileHash: localHash,
		},
		ack: AcknowledgeFileProgressRequest{
			TransferID: req.Manifest.TransferID,
			FileID:     plan.entry.ID,
			FullPath:   plan.serverPath,
			AckBytes:   window.end,
			ServerTS:   meta.TrailerTS,
			HashToken:  meta.FileHashToken,
			DeltaBytes: window.size,
			RecvMS:     recvMS,
			SyncMS:     syncMS,
		},
	}, nil
}

func aggregateSplitWindowResults(
	plan downloadBatchPlan,
	windows []splitWindow,
	results map[int64]splitWindowResult,
) (DownloadFileResponse, error) {
	if len(windows) == 0 {
		return DownloadFileResponse{}, errors.New("missing split windows")
	}
	aggregate := DownloadFileResponse{
		Meta: FileFrameMeta{
			FileID: plan.entry.ID,
			Comp:   "none",
			Enc:    "none",
			Offset: plan.resumeFrom,
		},
	}
	for idx, window := range windows {
		result, ok := results[window.start]
		if !ok {
			return DownloadFileResponse{}, fmt.Errorf("missing split window result at offset %d", window.start)
		}
		meta := result.response.Meta
		if meta.HeaderTS > 0 {
			if aggregate.Meta.HeaderTS == 0 || meta.HeaderTS < aggregate.Meta.HeaderTS {
				aggregate.Meta.HeaderTS = meta.HeaderTS
			}
		}
		aggregate.Meta.Size += meta.Size
		aggregate.Meta.WireSize += meta.WireSize
		if idx == 0 {
			aggregate.Meta.Comp = meta.Comp
			aggregate.Meta.Enc = meta.Enc
		}
		if len(meta.CompCounts) > 0 {
			if aggregate.Meta.CompCounts == nil {
				aggregate.Meta.CompCounts = copyCompCounts(meta.CompCounts)
			} else {
				mergeCompCounts(aggregate.Meta.CompCounts, meta.CompCounts)
			}
		} else if meta.Comp != "" {
			if aggregate.Meta.CompCounts == nil {
				aggregate.Meta.CompCounts = make(map[string]uint64, len(windows))
			}
			aggregate.Meta.CompCounts[meta.Comp]++
		}
		if idx == len(windows)-1 {
			aggregate.Meta.TrailerTS = meta.TrailerTS
			aggregate.Meta.TrailerMetadata = cloneTrailerMetadata(meta.TrailerMetadata)
		}
	}
	if len(aggregate.Meta.CompCounts) == 1 {
		for comp := range aggregate.Meta.CompCounts {
			aggregate.Meta.Comp = comp
		}
	}
	aggregate.Meta.FileHashToken = ""
	aggregate.LocalFileHash = ""
	return aggregate, nil
}

func (c *Client) StartFromManifest(ctx context.Context, req StartFromManifestRequest) (StartFromManifestResponse, error) {
	if c == nil {
		return StartFromManifestResponse{}, errors.New("nil client")
	}
	if req.Manifest == nil {
		return StartFromManifestResponse{}, errors.New("nil manifest")
	}
	entries := req.Entries
	if entries == nil {
		entries = req.Manifest.Entries
	}
	resp := StartFromManifestResponse{
		Requested: len(entries),
	}
	if len(entries) == 0 {
		return resp, nil
	}
	if req.OutputWriter == nil {
		return StartFromManifestResponse{}, errors.New("missing output writer callback")
	}
	if req.Concurrency <= 0 {
		if req.Manifest.Concurrency > 0 {
			req.Concurrency = req.Manifest.Concurrency
		} else {
			req.Concurrency = DefaultClientConcurrency()
		}
	}
	req.Concurrency = clampConcurrency(req.Concurrency)
	batchMaxBytes := c.effectiveBatchMaxBytes(req.BatchMaxBytes)
	batches := buildManifestBatchesByBytes(entries, batchMaxBytes)
	workCh := make(chan []ManifestEntry)
	errCh := make(chan error, len(entries))
	var wg sync.WaitGroup
	var downloaded atomic.Int64
	var transferred atomic.Int64

	worker := func() {
		defer wg.Done()
		for batch := range workCh {
			if len(batch) == 0 {
				continue
			}
			fileIDs := make([]uint64, 0, len(batch))
			for _, entry := range batch {
				fileIDs = append(fileIDs, entry.ID)
			}
			startOne := time.Now()
			downloadBatchResp, err := c.DownloadFilesFromManifestBatch(ctx, DownloadBatchRequest{
				Manifest:        req.Manifest,
				FileIDs:         fileIDs,
				OutputWriter:    req.OutputWriter,
				BatchMaxBytes:   batchMaxBytes,
				AgePublicKey:    req.AgePublicKey,
				AgeIdentity:     req.AgeIdentity,
				ProgressUpdates: req.ProgressUpdates,
			})
			if err != nil {
				errCh <- fmt.Errorf("batch first-id=%d count=%d: %w", batch[0].ID, len(batch), err)
				continue
			}
			elapsedBatch := time.Since(startOne)
			for _, downloadResp := range downloadBatchResp.Files {
				downloaded.Add(1)
				transferred.Add(downloadResp.Meta.Size)
				if req.OnFileDone != nil {
					req.OnFileDone(StartFileDoneEvent{
						File:    downloadResp,
						Elapsed: elapsedBatch,
					})
				}
			}
		}
	}

	for i := 0; i < req.Concurrency; i++ {
		wg.Add(1)
		go worker()
	}
	submitBatch := func(batch []ManifestEntry) bool {
		select {
		case <-ctx.Done():
			return false
		case workCh <- batch:
			return true
		}
	}
	for _, batch := range batches {
		if !submitBatch(batch) {
			break
		}
	}
	close(workCh)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		resp.Errors = append(resp.Errors, err)
	}
	resp.Downloaded = int(downloaded.Load())
	resp.TransferredBytes = transferred.Load()
	resp.Failed = len(resp.Errors)
	return resp, nil
}

func (c *Client) effectiveBatchMaxBytes(reqBatchMaxBytes int64) int64 {
	if reqBatchMaxBytes > 0 {
		return reqBatchMaxBytes
	}
	if c != nil && c.BatchMaxBytes > 0 {
		return c.BatchMaxBytes
	}
	return int64(defaultClientMaxFrameReadBufferBytes)
}

func suggestedConcurrencyFromProbe(serverCPU int, strategy string) int {
	if serverCPU <= 0 {
		serverCPU = runtime.NumCPU()
	}
	strategy = normalizeLoadStrategy(strategy)
	if strategy == LoadStrategyGentle {
		value := serverCPU / 4
		if serverCPU%4 != 0 {
			value++
		}
		return value
	}
	return serverCPU * 2
}

func clampConcurrency(v int) int {
	if v < minAutoStartConcurrency {
		return minAutoStartConcurrency
	}
	if v > maxAutoStartConcurrency {
		return maxAutoStartConcurrency
	}
	return v
}

func (c *Client) ProbeLink(ctx context.Context, req ProbeRequest) (ProbeResponse, error) {
	if c == nil {
		return ProbeResponse{}, errors.New("nil client")
	}
	samples := req.Samples
	if samples <= 0 {
		samples = defaultClientProbeSamples
	}
	probeBytes := req.ProbeBytes
	if probeBytes <= 0 {
		probeBytes = defaultClientProbeBytes
	}
	loadStrategy := req.LoadStrategy
	if strings.TrimSpace(loadStrategy) == "" {
		loadStrategy = c.LoadStrategy
	}
	loadStrategy = normalizeLoadStrategy(loadStrategy)

	probeResults := make([]probeResponse, 0, samples)
	for i := 0; i < samples; i++ {
		result, err := c.probeTCP(ctx, probeBytes, req.AgePublicKey, req.AgeIdentity)
		if err != nil {
			return ProbeResponse{}, fmt.Errorf("probe %d failed: %w", i+1, err)
		}
		probeResults = append(probeResults, result)
	}
	response := summarizeProbeSamples(probeResults, probeBytes)
	response.SuggestedConcurrency = clampConcurrency(suggestedConcurrencyFromProbe(response.ServerCPU, loadStrategy))
	return response, nil
}

func summarizeProbeSamples(results []probeResponse, probeBytes int64) ProbeResponse {
	if len(results) == 0 {
		return ProbeResponse{}
	}
	totalIntervalMS := int64(0)
	serverCPU := 0
	for _, result := range results {
		serverCPU = result.ServerCPU
		intervalMS := max(int64(1), (result.CTS1-result.CTS0)-(result.STS1-result.STS0))
		totalIntervalMS += intervalMS
	}
	avgMS := max(int64(1), totalIntervalMS/int64(len(results)))
	mbps := ((probeBytes * 8 * 1000) / avgMS) / 1_000_000
	roundedMbps := ((mbps + 50) / 100) * 100
	return ProbeResponse{
		ServerCPU:    serverCPU,
		AvgLatencyMS: avgMS,
		LinkMbps:     roundedMbps,
	}
}

func buildManifestBatchesByBytes(entries []ManifestEntry, maxBytes int64) [][]ManifestEntry {
	if len(entries) == 0 {
		return nil
	}
	if maxBytes <= 0 {
		maxBytes = int64(defaultClientMaxFrameReadBufferBytes)
	}
	batches := make([][]ManifestEntry, 0, len(entries))
	current := make([]ManifestEntry, 0, 8)
	var currentBytes int64
	for _, entry := range entries {
		size := max(int64(0), entry.Size)
		if len(current) > 0 && currentBytes+size > maxBytes {
			batches = append(batches, current)
			current = make([]ManifestEntry, 0, 8)
			currentBytes = 0
		}
		current = append(current, entry)
		currentBytes += size
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}

func shouldAcknowledgeMissing404(body string) bool {
	return strings.EqualFold(strings.TrimSpace(body), "file not found")
}

type acknowledgeFileProgressCommand struct {
	request  AcknowledgeFileProgressRequest
	ackToken string
}

func buildAcknowledgeFileProgressCommand(request AcknowledgeFileProgressRequest) (acknowledgeFileProgressCommand, error) {
	if request.TransferID == "" {
		return acknowledgeFileProgressCommand{}, errors.New("missing transfer id")
	}
	if request.FullPath == "" {
		return acknowledgeFileProgressCommand{}, errors.New("missing full path")
	}
	ackToken, err := buildAckToken(request.AckBytes, request.ServerTS, request.HashToken)
	if err != nil {
		return acknowledgeFileProgressCommand{}, err
	}
	if request.AckBytes >= 0 {
		if request.DeltaBytes < 0 {
			return acknowledgeFileProgressCommand{}, errors.New("ack delta bytes must be >= 0")
		}
		if request.RecvMS < 0 {
			return acknowledgeFileProgressCommand{}, errors.New("ack recv-ms must be >= 0")
		}
		if request.SyncMS < 0 {
			return acknowledgeFileProgressCommand{}, errors.New("ack sync-ms must be >= 0")
		}
	}
	return acknowledgeFileProgressCommand{request: request, ackToken: ackToken}, nil
}

func (c *Client) acknowledgeFileProgressBatch(ctx context.Context, requests []AcknowledgeFileProgressRequest) (AcknowledgeFileProgressResponse, error) {
	if len(requests) == 0 {
		return AcknowledgeFileProgressResponse{}, errors.New("missing ack requests")
	}
	commands := make([]acknowledgeFileProgressCommand, 0, len(requests))
	for _, request := range requests {
		cmd, err := buildAcknowledgeFileProgressCommand(request)
		if err != nil {
			return AcknowledgeFileProgressResponse{}, err
		}
		commands = append(commands, cmd)
	}
	return c.acknowledgeFileProgressBatchTCP(ctx, commands)
}

func (c *Client) AcknowledgeFileProgress(ctx context.Context, request AcknowledgeFileProgressRequest) (AcknowledgeFileProgressResponse, error) {
	return c.acknowledgeFileProgressBatch(ctx, []AcknowledgeFileProgressRequest{request})
}

func buildAckToken(ackBytes int64, serverTS int64, hashToken string) (string, error) {
	if ackBytes == -1 {
		return "-1", nil
	}
	if ackBytes < 0 {
		return "", errors.New("ack bytes must be -1 or non-negative")
	}
	if serverTS < 0 {
		return "", errors.New("ack server timestamp must be non-negative")
	}
	token := fmt.Sprintf("%d@%d", ackBytes, serverTS)
	if hashToken != "" {
		if !validHashToken(hashToken) {
			return "", errors.New("invalid ack hash token")
		}
		token += "@" + hashToken
	}
	return token, nil
}

func retryAck(ctx context.Context, fn func(context.Context) error) error {
	const maxAttempts = 5
	backoff := 100 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := fn(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == maxAttempts {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
	}
	return lastErr
}


func copyCompCounts(src map[string]uint64) map[string]uint64 {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(src))
	maps.Copy(out, src)
	return out
}

func mergeCompCounts(dst map[string]uint64, src map[string]uint64) {
	if len(src) == 0 {
		return
	}
	if dst == nil {
		return
	}
	for k, v := range src {
		dst[k] += v
	}
}

func copyStreamWithProgress(dst io.Writer, src io.Reader, buf []byte, hash *xxh3.Hasher128, onWrite func(written int64) error) error {
	var written int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}
			if hash != nil {
				if _, err := hash.Write(buf[:n]); err != nil {
					return err
				}
			}
			written += int64(n)
			if onWrite != nil {
				if err := onWrite(written); err != nil {
					return err
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func effectiveFrameReadBufferSize(baseSize int, maxWireHint int64, capSize int) int {
	if baseSize <= 0 {
		baseSize = defaultClientFrameBufferBytes
	}
	if capSize <= 0 {
		capSize = defaultClientMaxFrameReadBufferBytes
	}
	if capSize < minClientFrameReadBufferBytes {
		capSize = minClientFrameReadBufferBytes
	}

	target := baseSize
	if maxWireHint > 0 {
		if maxWireHint > int64(capSize) {
			target = capSize
		} else {
			target = int(maxWireHint)
		}
	}
	target = max(minClientFrameReadBufferBytes, min(capSize, frameReadBucketSize(target)))
	return target
}

func frameReadBucketSize(target int) int {
	const mib = 1024 * 1024
	for _, bucket := range []int{
		1 * mib,
		2 * mib,
		4 * mib,
		8 * mib,
		16 * mib,
		32 * mib,
		64 * mib,
	} {
		if target <= bucket {
			return bucket
		}
	}
	return 64 * mib
}

func (c *Client) acquireFrameReadBuffer(maxWireHint int64) ([]byte, func()) {
	size := effectiveFrameReadBufferSize(c.FrameBufferBytes, maxWireHint, c.MaxFrameReadBufferBytes)
	pool := (*sync.Pool)(nil)
	if existing, ok := c.bufferPool.Load(size); ok {
		pool = existing.(*sync.Pool)
	} else {
		sz := size
		created := &sync.Pool{
			New: func() any {
				return make([]byte, sz)
			},
		}
		actual, _ := c.bufferPool.LoadOrStore(size, created)
		pool = actual.(*sync.Pool)
	}
	raw := pool.Get()
	buf, ok := raw.([]byte)
	if !ok || cap(buf) < size {
		buf = make([]byte, size)
	}
	buf = buf[:size]
	return buf, func() {
		pool.Put(buf[:size])
	}
}

func (c *Client) acquireScratchBuffer(sizeHint int) *bytes.Buffer {
	if sizeHint <= 0 {
		sizeHint = defaultClientScratchBufferBytes
	}
	if c == nil {
		return bytes.NewBuffer(make([]byte, 0, sizeHint))
	}
	raw := c.scratchBufferPool.Get()
	if buf, ok := raw.(*bytes.Buffer); ok && buf != nil {
		if buf.Cap() >= sizeHint {
			buf.Reset()
			return buf
		}
		c.releaseScratchBuffer(buf)
	}
	return bytes.NewBuffer(make([]byte, 0, sizeHint))
}

func (c *Client) releaseScratchBuffer(buf *bytes.Buffer) {
	if c == nil || buf == nil {
		return
	}
	if buf.Cap() > maxClientScratchBufferPoolBytes {
		return
	}
	buf.Reset()
	c.scratchBufferPool.Put(buf)
}

func parseManifest(raw []byte) (*Manifest, error) {
	reader := bufio.NewReader(bytes.NewReader(raw))
	manifest := &Manifest{}
	seenHeader := false
	seenIDs := make(map[uint64]struct{})
	var prevPath string
	var prevMtime string
	var lastID uint64
	haveLastID := false

	for {
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read manifest line: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			if strings.HasPrefix(trimmed, "FM/2 ") {
				txferID, root, mode, linkMbps, concurrency, parseErr := parseManifestHeader(trimmed)
				if parseErr != nil {
					return nil, parseErr
				}
				if !seenHeader {
					manifest.TransferID = txferID
					manifest.Root = root
					manifest.Mode = mode
					manifest.LinkMbps = linkMbps
					manifest.Concurrency = concurrency
					seenHeader = true
				} else if manifest.TransferID != txferID || manifest.Root != root || manifest.Mode != mode || manifest.LinkMbps != linkMbps || manifest.Concurrency != concurrency {
					return nil, errors.New("manifest chunk header mismatch")
				}
				prevPath = ""
				prevMtime = ""
			} else {
				if !seenHeader {
					return nil, errors.New("manifest entry before header")
				}
				entry, nextPath, nextMtime, parseErr := parseManifestEntry(trimmed, prevPath, prevMtime)
				if parseErr != nil {
					return nil, parseErr
				}
				if _, exists := seenIDs[entry.ID]; exists {
					return nil, fmt.Errorf("duplicate manifest id: %d", entry.ID)
				}
				if haveLastID && entry.ID <= lastID {
					return nil, fmt.Errorf("manifest ids must be increasing: prev=%d curr=%d", lastID, entry.ID)
				}
				seenIDs[entry.ID] = struct{}{}
				lastID = entry.ID
				haveLastID = true
				manifest.Entries = append(manifest.Entries, entry)
				prevPath = nextPath
				prevMtime = nextMtime
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}

	if !seenHeader {
		return nil, errors.New("manifest missing header")
	}

	sort.Slice(manifest.Entries, func(i, j int) bool { return manifest.Entries[i].ID < manifest.Entries[j].ID })
	return manifest, nil
}

func marshalManifest(manifest *Manifest) ([]byte, error) {
	if manifest == nil {
		return nil, errors.New("nil manifest")
	}
	if strings.TrimSpace(manifest.TransferID) == "" {
		return nil, errors.New("manifest transfer id is required")
	}
	if strings.TrimSpace(manifest.Root) == "" {
		return nil, errors.New("manifest root is required")
	}
	entries := append([]ManifestEntry(nil), manifest.Entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	var b strings.Builder
	mode := strings.ToLower(strings.TrimSpace(manifest.Mode))
	if mode != LoadStrategyFast && mode != LoadStrategyGentle {
		return nil, errors.New("manifest mode must be fast or gentle")
	}
	if manifest.LinkMbps < 0 {
		return nil, errors.New("manifest link mbps must be >= 0")
	}
	if manifest.Concurrency <= 0 {
		return nil, errors.New("manifest concurrency must be > 0")
	}
	fmt.Fprintf(
		&b,
		"FM/2 %s %d:%s mode=%s link-mbps=%d concurrency=%d\n",
		manifest.TransferID,
		len(manifest.Root),
		manifest.Root,
		mode,
		manifest.LinkMbps,
		manifest.Concurrency,
	)
	prevPath := ""
	prevMtime := ""
	seenIDs := make(map[uint64]struct{}, len(entries))
	for i, entry := range entries {
		if _, exists := seenIDs[entry.ID]; exists {
			return nil, fmt.Errorf("duplicate manifest id: %d", entry.ID)
		}
		seenIDs[entry.ID] = struct{}{}
		if i > 0 && entry.ID <= entries[i-1].ID {
			return nil, fmt.Errorf("manifest ids must be increasing: prev=%d curr=%d", entries[i-1].ID, entry.ID)
		}
		if entry.Size < 0 {
			return nil, fmt.Errorf("manifest size must be >= 0 for id=%d", entry.ID)
		}
		if strings.Contains(entry.Path, `\`) {
			return nil, fmt.Errorf("manifest path contains backslash: %q", entry.Path)
		}
		if strings.HasPrefix(entry.Path, "/") {
			return nil, fmt.Errorf("manifest path must be relative: %q", entry.Path)
		}
		cleanPath := filepath.Clean(filepath.FromSlash(entry.Path))
		if cleanPath == "." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) || cleanPath == ".." {
			return nil, fmt.Errorf("manifest path traversal is not allowed: %q", entry.Path)
		}
		modeToken := fmt.Sprintf("%04o", uint32(entry.Mode&0o7777))
		mtimeRaw := strconv.FormatInt(entry.Mtime, 10)
		mtimeToken, err := encodeMtimeToken(prevMtime, mtimeRaw)
		if err != nil {
			return nil, fmt.Errorf("encode manifest mtime id=%d: %w", entry.ID, err)
		}
		pathToken := encodePathToken(prevPath, entry.Path)
		fmt.Fprintf(&b, "%d %d %s %s %s\n", entry.ID, entry.Size, mtimeToken, modeToken, pathToken)
		prevPath = entry.Path
		prevMtime = mtimeRaw
	}
	return []byte(b.String()), nil
}

func encodeMtimeToken(prev string, current string) (string, error) {
	if current == "" {
		return "", errors.New("empty mtime")
	}
	for _, ch := range current {
		if ch < '0' || ch > '9' {
			return "", errors.New("mtime must be decimal digits")
		}
	}
	prefixLen := commonPrefixLen(prev, current)
	suffix := current[prefixLen:]
	if suffix == "" {
		if len(current) == 0 {
			return "", errors.New("mtime cannot be empty")
		}
		prefixLen = len(current) - 1
		suffix = current[prefixLen:]
	}
	return strconv.Itoa(prefixLen) + ":" + suffix, nil
}

func encodePathToken(prev string, current string) string {
	prefixLen := commonPrefixLen(prev, current)
	suffix := current[prefixLen:]
	return strconv.Itoa(prefixLen) + ":" + strconv.Itoa(len(suffix)) + ":" + suffix
}

func commonPrefixLen(a string, b string) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func resolveManifestEntryPath(manifest *Manifest, fileID uint64) (ManifestEntry, string, error) {
	entry, ok := manifest.EntryByID(fileID)
	if !ok {
		return ManifestEntry{}, "", fmt.Errorf("file id %d not in manifest", fileID)
	}
	serverPath := filepath.Clean(filepath.Join(manifest.Root, filepath.FromSlash(entry.Path)))
	if !filepath.IsAbs(serverPath) {
		return ManifestEntry{}, "", fmt.Errorf("resolved file path is not absolute: %s", serverPath)
	}
	return entry, serverPath, nil
}

func cloneTrailerMetadata(meta *FileTrailerMetadata) *FileTrailerMetadata {
	if meta == nil {
		return nil
	}
	cloned := *meta
	return &cloned
}

func parseManifestHeader(line string) (string, string, string, int64, int, error) {
	rest := strings.TrimPrefix(line, "FM/2 ")
	sep := strings.IndexByte(rest, ' ')
	if sep <= 0 || sep == len(rest)-1 {
		return "", "", "", 0, 0, errors.New("invalid manifest header")
	}
	txferID := rest[:sep]
	rootRaw := rest[sep+1:]
	root, consumed, err := parseLenPrefixedPrefix(rootRaw)
	if err != nil {
		return "", "", "", 0, 0, fmt.Errorf("invalid manifest root token: %w", err)
	}
	optionsRaw := strings.TrimSpace(rootRaw[consumed:])
	if optionsRaw == "" {
		return "", "", "", 0, 0, errors.New("manifest header missing metadata options")
	}
	options := strings.Fields(optionsRaw)
	var (
		mode        string
		linkMbps    int64
		concurrency int
		seenMode    bool
		seenLink    bool
		seenConc    bool
	)
	for _, option := range options {
		key, value, ok := strings.Cut(option, "=")
		if !ok {
			return "", "", "", 0, 0, errors.New("invalid manifest header option")
		}
		switch key {
		case "mode":
			value = strings.ToLower(strings.TrimSpace(value))
			if value != LoadStrategyFast && value != LoadStrategyGentle {
				return "", "", "", 0, 0, errors.New("invalid manifest mode")
			}
			mode = value
			seenMode = true
		case "link-mbps":
			linkMbps, err = strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err != nil || linkMbps < 0 {
				return "", "", "", 0, 0, errors.New("invalid manifest link-mbps")
			}
			seenLink = true
		case "concurrency":
			concurrency, err = strconv.Atoi(strings.TrimSpace(value))
			if err != nil || concurrency <= 0 {
				return "", "", "", 0, 0, errors.New("invalid manifest concurrency")
			}
			seenConc = true
		default:
			return "", "", "", 0, 0, errors.New("unknown manifest header option")
		}
	}
	if !seenMode || !seenLink || !seenConc {
		return "", "", "", 0, 0, errors.New("manifest header missing required metadata")
	}
	return txferID, root, mode, linkMbps, concurrency, nil
}

func parseManifestEntry(line string, prevPath string, prevMtime string) (ManifestEntry, string, string, error) {
	first := strings.IndexByte(line, ' ')
	if first <= 0 {
		return ManifestEntry{}, "", "", errors.New("invalid manifest entry")
	}
	second := strings.IndexByte(line[first+1:], ' ')
	if second < 0 {
		return ManifestEntry{}, "", "", errors.New("invalid manifest entry")
	}
	second += first + 1
	third := strings.IndexByte(line[second+1:], ' ')
	if third < 0 {
		return ManifestEntry{}, "", "", errors.New("invalid manifest entry")
	}
	third += second + 1
	fourth := strings.IndexByte(line[third+1:], ' ')
	if fourth < 0 {
		return ManifestEntry{}, "", "", errors.New("invalid manifest entry")
	}
	fourth += third + 1

	idRaw := line[:first]
	sizeRaw := line[first+1 : second]
	mtimeToken := line[second+1 : third]
	modeRaw := line[third+1 : fourth]
	pathToken := line[fourth+1:]

	id, err := strconv.ParseUint(idRaw, 10, 64)
	if err != nil {
		return ManifestEntry{}, "", "", fmt.Errorf("invalid manifest id: %w", err)
	}
	sizeU, err := strconv.ParseUint(sizeRaw, 10, 64)
	if err != nil {
		return ManifestEntry{}, "", "", fmt.Errorf("invalid manifest size: %w", err)
	}
	if sizeU > uint64(^uint64(0)>>1) {
		return ManifestEntry{}, "", "", errors.New("manifest size overflows int64")
	}

	mtimeResolved, err := decodeMtimeToken(prevMtime, mtimeToken)
	if err != nil {
		return ManifestEntry{}, "", "", err
	}
	mtimeNanos, err := strconv.ParseUint(mtimeResolved, 10, 64)
	if err != nil {
		return ManifestEntry{}, "", "", fmt.Errorf("invalid manifest mtime value: %w", err)
	}
	if mtimeNanos > uint64(^uint64(0)>>1) {
		return ManifestEntry{}, "", "", errors.New("manifest mtime overflows int64")
	}
	mode, err := parseManifestModeToken(modeRaw)
	if err != nil {
		return ManifestEntry{}, "", "", err
	}

	pathResolved, err := decodePathToken(prevPath, pathToken)
	if err != nil {
		return ManifestEntry{}, "", "", err
	}
	if strings.Contains(pathResolved, `\`) {
		return ManifestEntry{}, "", "", errors.New("manifest path contains backslash")
	}
	if strings.HasPrefix(pathResolved, "/") {
		return ManifestEntry{}, "", "", errors.New("manifest path must be relative")
	}
	cleanPath := filepath.Clean(filepath.FromSlash(pathResolved))
	if cleanPath == "." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) || cleanPath == ".." {
		return ManifestEntry{}, "", "", errors.New("manifest path traversal is not allowed")
	}

	entry := ManifestEntry{
		ID:    id,
		Size:  int64(sizeU),
		Mtime: int64(mtimeNanos),
		Mode:  mode,
		Path:  pathResolved,
	}
	return entry, pathResolved, mtimeResolved, nil
}

func parseManifestModeToken(raw string) (os.FileMode, error) {
	if raw == "" {
		return 0, errors.New("manifest mode is required")
	}
	for _, ch := range raw {
		if ch < '0' || ch > '7' {
			return 0, errors.New("manifest mode must be octal")
		}
	}
	v, err := strconv.ParseUint(raw, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid manifest mode: %w", err)
	}
	if v > 0o7777 {
		return 0, errors.New("manifest mode must be <= 07777")
	}
	return os.FileMode(v), nil
}


func parseLenPrefixedPrefix(raw string) (string, int, error) {
	sep := strings.IndexByte(raw, ':')
	if sep <= 0 {
		return "", 0, errors.New("invalid len-prefixed token")
	}
	n, err := strconv.Atoi(raw[:sep])
	if err != nil || n < 0 {
		return "", 0, errors.New("invalid len prefix")
	}
	start := sep + 1
	end := start + n
	if end > len(raw) {
		return "", 0, errors.New("len prefix mismatch")
	}
	return raw[start:end], end, nil
}

func decodeMtimeToken(prev string, token string) (string, error) {
	head, suffix, ok := strings.Cut(token, ":")
	if !ok {
		return "", errors.New("invalid mtime token")
	}
	prefixLen, err := strconv.Atoi(head)
	if err != nil || prefixLen < 0 {
		return "", errors.New("invalid mtime prefix length")
	}
	if prefixLen > len(prev) {
		return "", errors.New("mtime prefix length exceeds previous value")
	}
	if suffix == "" {
		return "", errors.New("empty mtime suffix")
	}
	for _, ch := range suffix {
		if ch < '0' || ch > '9' {
			return "", errors.New("mtime suffix must be decimal digits")
		}
	}
	if prev == "" && prefixLen != 0 {
		return "", errors.New("first mtime prefix length must be zero")
	}
	return prev[:prefixLen] + suffix, nil
}

func decodePathToken(prev string, token string) (string, error) {
	first := strings.IndexByte(token, ':')
	if first < 0 {
		return "", errors.New("invalid path token")
	}
	second := strings.IndexByte(token[first+1:], ':')
	if second < 0 {
		return "", errors.New("invalid path token")
	}
	second += first + 1
	prefixLen, err := strconv.Atoi(token[:first])
	if err != nil || prefixLen < 0 {
		return "", errors.New("invalid path prefix length")
	}
	if prefixLen > len(prev) {
		return "", errors.New("path prefix length exceeds previous value")
	}
	suffixLen, err := strconv.Atoi(token[first+1 : second])
	if err != nil || suffixLen < 0 {
		return "", errors.New("invalid path suffix length")
	}
	suffix := token[second+1:]
	if len(suffix) != suffixLen {
		return "", errors.New("path suffix length mismatch")
	}
	if prev == "" && prefixLen != 0 {
		return "", errors.New("first path prefix length must be zero")
	}
	return prev[:prefixLen] + suffix, nil
}

type fileStream struct {
	respBody io.Closer
	br       frameLineReader
	identity age.Identity

	meta *FileFrameMeta

	frameMeta   FileFrameMeta
	logical     io.ReadCloser
	logicalRead int64

	expectOffset    bool
	expectedOffset  int64
	expectNextFrame bool

	pendingErr error
	finished   bool
	closed     bool
	pending    *FileFrameMeta
	releaseBr  func()
}

type readerWithCloser struct {
	io.Reader
	io.Closer
}

type frameLineReader interface {
	io.Reader
	ReadString(delim byte) (string, error)
}

type pooledLineReader struct {
	reader io.Reader
	buf    []byte
	off    int
	n      int
}

func newPooledLineReader(reader io.Reader, buf []byte) *pooledLineReader {
	if len(buf) == 0 {
		buf = make([]byte, 4096)
	}
	return &pooledLineReader{
		reader: reader,
		buf:    buf,
	}
}

func (r *pooledLineReader) Read(p []byte) (int, error) {
	if r == nil {
		return 0, errors.New("nil pooled line reader")
	}
	for {
		if r.off < r.n {
			n := copy(p, r.buf[r.off:r.n])
			r.off += n
			return n, nil
		}
		if err := r.fill(); err != nil {
			return 0, err
		}
	}
}

func (r *pooledLineReader) fill() error {
	for {
		n, err := r.reader.Read(r.buf)
		if n > 0 {
			r.off = 0
			r.n = n
			return nil
		}
		if err != nil {
			r.off = 0
			r.n = 0
			return err
		}
	}
}

func (r *pooledLineReader) ReadString(delim byte) (string, error) {
	if r == nil {
		return "", errors.New("nil pooled line reader")
	}
	out := make([]byte, 0, 128)
	for {
		if r.off >= r.n {
			if err := r.fill(); err != nil {
				if err == io.EOF && len(out) > 0 {
					return string(out), io.EOF
				}
				return string(out), err
			}
		}
		chunk := r.buf[r.off:r.n]
		if idx := bytes.IndexByte(chunk, delim); idx >= 0 {
			idx++
			out = append(out, chunk[:idx]...)
			r.off += idx
			return string(out), nil
		}
		out = append(out, chunk...)
		r.off = r.n
	}
}

func (s *fileStream) LastTrailerTS() int64 {
	if s == nil || s.meta == nil {
		return 0
	}
	return s.meta.TrailerTS
}

func (c *Client) fileStreamBufferHint(fileSizeHint int64, firstFrameSize int64) int64 {
	if fileSizeHint <= 0 {
		fileSizeHint = firstFrameSize
	}
	return int64(effectiveFrameReadBufferSize(c.FrameBufferBytes, fileSizeHint, c.MaxFrameReadBufferBytes))
}

func (c *Client) newFileStream(respBody io.ReadCloser, ageIdentity string, fileSizeHint int64) (io.ReadCloser, *FileFrameMeta, error) {
	identity, err := parseAgeIdentity(ageIdentity)
	if err != nil {
		return nil, nil, err
	}
	probe := bufio.NewReader(respBody)
	headerLine, err := probe.ReadString('\n')
	if err != nil {
		return nil, nil, fmt.Errorf("read frame header: %w", err)
	}
	firstMeta, err := parseFXHeader(strings.TrimRight(headerLine, "\r\n"))
	if err != nil {
		return nil, nil, err
	}
	if firstMeta.Size < 0 || firstMeta.WireSize < 0 {
		return nil, nil, errors.New("negative frame size")
	}
	if firstMeta.Offset < 0 {
		return nil, nil, errors.New("negative frame offset")
	}

	bufferHint := c.fileStreamBufferHint(fileSizeHint, firstMeta.Size)
	readBuf, release := c.acquireFrameReadBuffer(bufferHint)
	stream := &fileStream{
		respBody: respBody,
		br:       newPooledLineReader(probe, readBuf),
		identity: identity,
		meta:     &FileFrameMeta{},
		pending:  &firstMeta,
		releaseBr: release,
	}
	if err := stream.openNextFrame(); err != nil {
		release()
		return nil, nil, err
	}
	return stream, stream.meta, nil
}

func (s *fileStream) Read(p []byte) (int, error) {
	if s.pendingErr != nil {
		err := s.pendingErr
		s.pendingErr = nil
		return 0, err
	}
	if s.finished {
		return 0, io.EOF
	}

	for {
		if s.logical == nil {
			if err := s.openNextFrame(); err != nil {
				if errors.Is(err, io.EOF) {
					s.finished = true
					return 0, io.EOF
				}
				return 0, err
			}
		}

		n, err := s.logical.Read(p)
		if n > 0 {
			s.logicalRead += int64(n)
		}
		if err == nil {
			return n, nil
		}
		if !errors.Is(err, io.EOF) {
			return n, err
		}

		frameErr := s.finishFrame()
		if n > 0 {
			s.pendingErr = frameErr
			return n, nil
		}
		if frameErr != nil {
			return 0, frameErr
		}
	}
}

func (s *fileStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	if !s.finished {
		_, _ = io.Copy(io.Discard, s)
	}
	logicalErr := error(nil)
	if s.logical != nil {
		logicalErr = s.logical.Close()
	}
	if s.releaseBr != nil {
		s.releaseBr()
		s.releaseBr = nil
	}
	bodyErr := s.respBody.Close()
	if logicalErr != nil {
		return logicalErr
	}
	if bodyErr != nil {
		return bodyErr
	}
	return nil
}

func (s *fileStream) openNextFrame() error {
	var meta FileFrameMeta
	if s.pending != nil {
		meta = *s.pending
		s.pending = nil
	} else {
		headerLine, err := s.br.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && headerLine == "" {
				if s.expectNextFrame {
					return errors.New("missing next frame after trailer next offset")
				}
				return io.EOF
			}
			return fmt.Errorf("read frame header: %w", err)
		}
		trimmedHeader := strings.TrimRight(headerLine, "\r\n")
		if s.expectOffset && !s.expectNextFrame && isStatusLine(trimmedHeader) {
			if err := parseErrControlFrame(trimmedHeader); err != nil {
				return err
			}
			if _, ok := parseOKStatusLine(trimmedHeader); ok {
				return io.EOF
			}
			return errors.New("unexpected terminal status line")
		}
		if s.expectOffset && !s.expectNextFrame {
			return errors.New("unexpected extra frame after terminal trailer")
		}

		meta, err = parseFXHeader(trimmedHeader)
		if err != nil {
			return err
		}
	}
	if meta.Size < 0 || meta.WireSize < 0 {
		return errors.New("negative frame size")
	}
	if meta.Offset < 0 {
		return errors.New("negative frame offset")
	}
	if s.expectOffset && meta.Offset != s.expectedOffset {
		return fmt.Errorf("non-contiguous frame offset: expected=%d got=%d", s.expectedOffset, meta.Offset)
	}
	if s.meta.FileID == 0 && s.meta.Comp == "" && s.meta.Enc == "" && s.meta.Size == 0 && s.meta.WireSize == 0 {
		s.meta.FileID = meta.FileID
		s.meta.Comp = meta.Comp
		s.meta.Enc = meta.Enc
		s.meta.Offset = meta.Offset
		s.meta.MaxWireSizeHint = meta.MaxWireSizeHint
		s.meta.HeaderTS = meta.HeaderTS
	} else {
		if meta.FileID != s.meta.FileID {
			return fmt.Errorf("file id mismatch across frames: expected=%d got=%d", s.meta.FileID, meta.FileID)
		}
		if meta.Enc != s.meta.Enc {
			return fmt.Errorf("encryption mode mismatch across frames: expected=%s got=%s", s.meta.Enc, meta.Enc)
		}
	}

	payloadReader := io.LimitReader(s.br, meta.WireSize)
	logicalReader, err := decodePayloadReader(payloadReader, meta.Comp, meta.Enc, s.identity)
	if err != nil {
		return fmt.Errorf("decode payload reader: %w", err)
	}
	s.frameMeta = meta
	s.logical = logicalReader
	s.logicalRead = 0
	s.expectOffset = false
	s.expectNextFrame = false
	return nil
}

func (s *fileStream) finishFrame() error {
	if s.logicalRead != s.frameMeta.Size {
		return fmt.Errorf("logical size mismatch: declared=%d actual=%d", s.frameMeta.Size, s.logicalRead)
	}
	trailerLine, err := s.br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read frame trailer: %w", err)
	}
	trailer, err := parseFXTrailer(strings.TrimRight(trailerLine, "\r\n"))
	if err != nil {
		return err
	}
	if trailer.FileID != s.frameMeta.FileID {
		return fmt.Errorf("trailer file id mismatch: header=%d trailer=%d", s.frameMeta.FileID, trailer.FileID)
	}

	nextOffset := s.frameMeta.Offset + s.frameMeta.Size
	if trailer.Next != nil {
		if *trailer.Next == 0 {
			s.expectNextFrame = false
		} else {
			if *trailer.Next != nextOffset {
				return fmt.Errorf("invalid trailer next offset: expected=%d got=%d", nextOffset, *trailer.Next)
			}
			s.expectNextFrame = true
		}
	}
	s.expectOffset = true
	s.expectedOffset = nextOffset

	s.meta.Size += s.frameMeta.Size
	s.meta.WireSize += s.frameMeta.WireSize
	if s.meta.CompCounts == nil {
		s.meta.CompCounts = make(map[string]uint64, 3)
	}
	s.meta.CompCounts[s.frameMeta.Comp]++
	if s.meta.Comp != s.frameMeta.Comp {
		s.meta.Comp = "mixed"
	}
	s.meta.TrailerTS = trailer.TS
	s.meta.HashToken = trailer.HashToken
	if trailer.FileHashToken != "" {
		s.meta.FileHashToken = trailer.FileHashToken
	}
	if trailer.Metadata != nil {
		s.meta.TrailerMetadata = cloneTrailerMetadata(trailer.Metadata)
	}

	closeErr := s.logical.Close()
	s.logical = nil
	return closeErr
}

func parseFXHeader(line string) (FileFrameMeta, error) {
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

type frameTrailer struct {
	FileID         uint64
	TS             int64
	HashToken      string
	FileHashToken  string
	ChecksumPrefix string
	Next           *int64
	Metadata       *FileTrailerMetadata
}

func parseFXTrailer(line string) (frameTrailer, error) {
	prefix, hashToken, err := splitTrailerPrefixAndHash(line)
	if err != nil {
		return frameTrailer{}, err
	}
	fields := strings.Fields(prefix)
	if len(fields) < 3 || fields[0] != "FXT/1" {
		return frameTrailer{}, errors.New("invalid FXT/1 trailer")
	}
	fileID, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return frameTrailer{}, fmt.Errorf("invalid trailer file id: %w", err)
	}
	status := ""
	var fileHashToken string
	var ts int64 = -1
	var nextOffset *int64
	meta := FileTrailerMetadata{
		Size:    -1,
		MtimeNS: -1,
	}
	hasMeta := false
	for _, token := range fields[2:] {
		if val, ok := strings.CutPrefix(token, "status="); ok {
			status = val
		} else if tsRaw, ok := strings.CutPrefix(token, "ts="); ok {
			parsedTS, parseErr := strconv.ParseInt(tsRaw, 10, 64)
			if parseErr != nil || parsedTS < 0 {
				return frameTrailer{}, errors.New("invalid trailer ts")
			}
			ts = parsedTS
		} else if val, ok := strings.CutPrefix(token, "file-hash="); ok {
			fileHashToken = val
		} else if nextRaw, ok := strings.CutPrefix(token, "next="); ok {
			nextValue, parseErr := strconv.ParseInt(nextRaw, 10, 64)
			if parseErr != nil || nextValue < 0 {
				return frameTrailer{}, errors.New("invalid trailer next offset")
			}
			nextOffset = &nextValue
		}
		if strings.HasPrefix(token, "meta:") {
			parts := strings.SplitN(token, "=", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimPrefix(parts[0], "meta:")
			val := parts[1]
			switch key {
			case "size":
				sizeVal, parseErr := strconv.ParseInt(val, 10, 64)
				if parseErr != nil || sizeVal < 0 {
					return frameTrailer{}, errors.New("invalid trailer meta:size")
				}
				meta.Size = sizeVal
				hasMeta = true
			case "mtime_ns":
				mtimeVal, parseErr := strconv.ParseInt(val, 10, 64)
				if parseErr != nil || mtimeVal < 0 {
					return frameTrailer{}, errors.New("invalid trailer meta:mtime_ns")
				}
				meta.MtimeNS = mtimeVal
				hasMeta = true
			case "mode":
				meta.Mode = val
				hasMeta = true
			case "uid":
				meta.UID = val
				hasMeta = true
			case "gid":
				meta.GID = val
				hasMeta = true
			case "user":
				meta.User = val
				hasMeta = true
			case "group":
				meta.Group = val
				hasMeta = true
			}
		}
	}
	if status != "ok" {
		return frameTrailer{}, fmt.Errorf("trailer status not ok: %s", status)
	}
	if ts < 0 {
		return frameTrailer{}, errors.New("trailer missing ts")
	}
	if fileHashToken != "" && !validHashToken(fileHashToken) {
		return frameTrailer{}, errors.New("trailer invalid file hash token")
	}
	var metaPtr *FileTrailerMetadata
	if hasMeta {
		if meta.Size < 0 {
			meta.Size = 0
		}
		if meta.MtimeNS < 0 {
			meta.MtimeNS = 0
		}
		metaPtr = &meta
	}
	return frameTrailer{
		FileID:         fileID,
		TS:             ts,
		HashToken:      hashToken,
		FileHashToken:  fileHashToken,
		ChecksumPrefix: prefix,
		Next:           nextOffset,
		Metadata:       metaPtr,
	}, nil
}

func splitTrailerPrefixAndHash(line string) (string, string, error) {
	idx := strings.LastIndex(line, " hash=")
	if idx <= 0 {
		return strings.TrimSpace(line), "", nil
	}
	prefix := line[:idx]
	hashToken := strings.TrimSpace(line[idx+len(" hash="):])
	if !validHashToken(hashToken) {
		return "", "", errors.New("trailer missing or invalid hash token")
	}
	return prefix, hashToken, nil
}

func parseAgeIdentity(raw string) (age.Identity, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	identity, err := age.ParseX25519Identity(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid age identity: %w", err)
	}
	return identity, nil
}

func validHashToken(raw string) bool {
	if raw == "" {
		return false
	}
	parts := strings.SplitN(raw, ":", 2)
	return len(parts) == 2 && parts[0] != "" && parts[1] != ""
}

func decodePayloadReader(payload io.Reader, comp string, enc string, identity age.Identity) (io.ReadCloser, error) {
	switch enc {
	case "none":
		return decodePayloadReaderByComp(payload, comp)
	case "age":
		if identity == nil {
			return nil, errors.New("missing age identity for encrypted frame")
		}
		decrypted, err := age.Decrypt(payload, identity)
		if err != nil {
			return nil, err
		}
		return decodePayloadReaderByComp(decrypted, comp)
	default:
		return nil, fmt.Errorf("unsupported encryption mode: %s", enc)
	}
}

func decodePayloadReaderByComp(payload io.Reader, comp string) (io.ReadCloser, error) {
	switch comp {
	case "none":
		return io.NopCloser(payload), nil
	case EncodingZstd, EncodingLz4:
		reader, err := intencoding.WrapDecompressedReader(payload, comp)
		if err != nil {
			return nil, err
		}
		return reader, nil
	default:
		return nil, fmt.Errorf("unsupported compression mode: %s", comp)
	}
}
