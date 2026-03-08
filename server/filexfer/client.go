package filexfer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"filippo.io/age"
	intcodec "github.com/jolynch/pinch/internal/filexfer/codec"
	"github.com/zeebo/xxh3"
)

const (
	EncodingIdentity = "identity"
	EncodingZstd     = "zstd"
	EncodingLz4      = "lz4"
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

type Client struct {
	BaseURL                 string
	FileClient              *http.Client
	FileRequestWindowBytes  int64
	FrameBufferBytes        int
	MaxFrameReadBufferBytes int

	// bufferPool caches reusable frame-read buffers keyed by bucketed size.
	bufferPool sync.Map // map[int]*sync.Pool
}

type Manifest struct {
	TransferID string
	Root       string
	Entries    []ManifestEntry
	Raw        []byte
}

type ManifestEntry struct {
	ID    uint64
	Size  int64
	Mtime int64
	Mode  os.FileMode
	Path  string
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

type DownloadFileRequest struct {
	Manifest                *Manifest
	FileID                  uint64
	OutRoot                 string
	OutFile                 string
	Stdout                  io.Writer
	UseZeroCopy             bool
	AcceptEncoding          string
	AgePublicKey            string
	AgeIdentity             string
	AckEveryBytes           int64
	NoSync                  bool
	MetadataApplyBestEffort bool
	ProgressUpdates         chan<- DownloadProgressUpdate
	OnAck                   func(AckProgressEvent)
}

type DownloadProgressUpdate struct {
	FileID   uint64
	AckBytes int64
}

type AckProgressEvent struct {
	TransferID  string
	FileID      uint64
	AckBytes    int64
	TargetBytes int64
	PrevAckTime time.Time
	AckTime     time.Time
}

type DownloadFileResponse struct {
	DestinationPath string
	Meta            FileFrameMeta
	LocalFileHash   string
}

type FetchManifestRequest struct {
	Directory      string
	Verbose        bool
	MaxChunkSize   int
	AcceptEncoding string
	AgePublicKey   string
	AgeIdentity    string
}

type FetchManifestResponse struct {
	Manifest *Manifest
}

type FetchFileRequest struct {
	TransferID     string
	FileID         uint64
	FullPath       string
	UseZeroCopy    bool
	AcceptEncoding string
	AgePublicKey   string
	AgeIdentity    string
	AckBytes       int64
}

type FetchFileResponse struct {
	Reader io.ReadCloser
	Meta   *FileFrameMeta
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
	defaultClientRequestWindowBytes      int64 = 1024 * 1024 * 1024
	defaultClientFrameBufferBytes        int   = 8 * 1024 * 1024
	defaultClientMaxFrameReadBufferBytes int   = 64 * 1024 * 1024
	minClientFrameReadBufferBytes        int   = 32 * 1024
	defaultClientAckEveryBytes           int64 = 256 * 1024 * 1024
)

func maxSocketReadBufferBytes() int {
	return maxSocketBufferBytes("/proc/sys/net/core/rmem_max")
}

func maxSocketBufferBytes(sysctlPath string) int {
	// 4MiB baseline if kernel cap cannot be read.
	const baseline = 4 * 1024 * 1024
	raw, err := os.ReadFile(sysctlPath)
	if err != nil {
		return baseline
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || v <= 0 {
		return baseline
	}
	if v > baseline {
		return v
	}
	return baseline
}

func newTunedHTTPClient(readBufBytes int) *http.Client {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok || base == nil {
		return &http.Client{}
	}
	transport := base.Clone()
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetReadBuffer(readBufBytes)
			_ = tc.SetNoDelay(true)
		}
		return conn, nil
	}
	return &http.Client{Transport: transport}
}

func NewClient(baseURL string, hc *http.Client) *Client {
	if hc == nil {
		hc = newTunedHTTPClient(maxSocketReadBufferBytes())
	}
	c := &Client{
		BaseURL:                 strings.TrimRight(baseURL, "/"),
		FileClient:              hc,
		FileRequestWindowBytes:  defaultClientRequestWindowBytes,
		FrameBufferBytes:        defaultClientFrameBufferBytes,
		MaxFrameReadBufferBytes: defaultClientMaxFrameReadBufferBytes,
	}
	c.bufferPool = sync.Map{}
	return c
}

func (c *Client) FetchManifest(ctx context.Context, request FetchManifestRequest) (FetchManifestResponse, error) {
	if c == nil {
		return FetchManifestResponse{}, errors.New("nil client")
	}
	if request.Directory == "" {
		return FetchManifestResponse{}, errors.New("missing directory")
	}

	u, err := url.Parse(c.BaseURL + "/fs/transfer")
	if err != nil {
		return FetchManifestResponse{}, fmt.Errorf("build transfer url: %w", err)
	}
	q := u.Query()
	q.Set("directory", request.Directory)
	if request.Verbose {
		q.Set("verbose", "true")
	}
	if request.MaxChunkSize > 0 {
		q.Set("max-manifest-chunk-size", strconv.Itoa(request.MaxChunkSize))
	}
	if request.AgePublicKey != "" {
		q.Set("age-public-key", request.AgePublicKey)
	}
	u.RawQuery = q.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), nil)
	if err != nil {
		return FetchManifestResponse{}, fmt.Errorf("build manifest request: %w", err)
	}
	if request.AcceptEncoding != "" {
		httpReq.Header.Set("Accept-Encoding", request.AcceptEncoding)
	}

	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return FetchManifestResponse{}, fmt.Errorf("request manifest: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return FetchManifestResponse{}, fmt.Errorf("manifest request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	reader, err := intcodec.WrapDecompressedReader(resp.Body, resp.Header.Get("Content-Encoding"))
	if err != nil {
		return FetchManifestResponse{}, fmt.Errorf("decode manifest body: %w", err)
	}
	defer reader.Close()

	raw, err := io.ReadAll(reader)
	if err != nil {
		return FetchManifestResponse{}, fmt.Errorf("read manifest body: %w", err)
	}
	if strings.EqualFold(strings.TrimSpace(resp.Header.Get("X-Manifest-Enc")), "age") {
		raw, err = decryptAgeBytes(raw, request.AgeIdentity)
		if err != nil {
			return FetchManifestResponse{}, fmt.Errorf("decrypt manifest body: %w", err)
		}
	}

	manifest, err := parseManifest(raw)
	if err != nil {
		return FetchManifestResponse{}, err
	}
	return FetchManifestResponse{Manifest: manifest}, nil
}

func SaveManifest(path string, manifest *Manifest) error {
	if manifest == nil {
		return errors.New("nil manifest")
	}
	if len(manifest.Raw) == 0 {
		return errors.New("manifest has no raw payload")
	}
	if path == "" {
		return errors.New("missing path")
	}
	parent := filepath.Dir(path)
	if parent != "." && parent != "" {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("create manifest parent directory: %w", err)
		}
	}
	if err := os.WriteFile(path, manifest.Raw, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
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
	reader, meta, err := c.fetchFileWindow(
		ctx,
		request.TransferID,
		request.FileID,
		request.FullPath,
		request.UseZeroCopy,
		request.AcceptEncoding,
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
	useZeroCopy bool,
	acceptEncoding string,
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

	filePath := fmt.Sprintf("%s/fs/file/%s/%d", c.BaseURL, url.PathEscape(txferID), fileID)
	if useZeroCopy {
		filePath += "/zerocopy"
	}
	u, err := url.Parse(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("build file url: %w", err)
	}
	q := u.Query()
	q.Set("path", fullPath)
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	if size >= 0 {
		q.Set("size", strconv.FormatInt(size, 10))
	}
	if !useZeroCopy {
		q.Set("comp", normalizeRequestedComp(acceptEncoding))
		if agePublicKey != "" {
			q.Set("age-public-key", agePublicKey)
		}
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build file request: %w", err)
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("request file: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			body := strings.TrimSpace(string(msg))
			return nil, nil, fmt.Errorf("%w: %w", ErrFileMissing, &fileMissingError{
				Status: resp.StatusCode,
				Body:   body,
			})
		}
		return nil, nil, fmt.Errorf("file request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	if useZeroCopy {
		metaSize := size
		if metaSize < 0 && resp.ContentLength >= 0 {
			metaSize = resp.ContentLength
		}
		if metaSize < 0 {
			metaSize = 0
		}
		return resp.Body, &FileFrameMeta{
			FileID:   fileID,
			Comp:     "none",
			Enc:      "none",
			Offset:   offset,
			Size:     metaSize,
			WireSize: metaSize,
		}, nil
	}

	body := io.ReadCloser(resp.Body)
	if agePublicKey != "" {
		identity, identityErr := parseAgeIdentity(ageIdentity)
		if identityErr != nil {
			resp.Body.Close()
			return nil, nil, identityErr
		}
		if identity == nil {
			resp.Body.Close()
			return nil, nil, errors.New("missing age identity for encrypted response")
		}
		decryptedReader, decryptErr := age.Decrypt(resp.Body, identity)
		if decryptErr != nil {
			resp.Body.Close()
			return nil, nil, fmt.Errorf("decrypt file response: %w", decryptErr)
		}
		body = &readerWithCloser{
			Reader: decryptedReader,
			Closer: resp.Body,
		}
	}

	stream, meta, err := newFileStream(body, ageIdentity)
	if err != nil {
		resp.Body.Close()
		return nil, nil, err
	}
	if meta.FileID != fileID {
		stream.Close()
		return nil, nil, fmt.Errorf("file id mismatch: expected %d got %d", fileID, meta.FileID)
	}
	return stream, meta, nil
}

func (c *Client) DownloadFileFromManifest(ctx context.Context, req DownloadFileRequest) (DownloadFileResponse, error) {
	if req.Manifest == nil {
		return DownloadFileResponse{}, errors.New("nil manifest")
	}
	entry, serverPath, err := resolveManifestEntryPath(req.Manifest, req.FileID)
	if err != nil {
		return DownloadFileResponse{}, err
	}

	destPath := req.OutFile
	if destPath == "" {
		if req.OutRoot == "" {
			req.OutRoot = "."
		}
		destPath = filepath.Clean(filepath.Join(req.OutRoot, filepath.FromSlash(entry.Path)))
	}
	destPath = filepath.Clean(destPath)

	writer := io.Writer(nil)
	closeWriter := func() error { return nil }
	fileWriter := (*os.File)(nil)
	if destPath == "-" {
		if req.Stdout == nil {
			req.Stdout = os.Stdout
		}
		writer = req.Stdout
	} else {
		if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
			return DownloadFileResponse{}, fmt.Errorf("create output parent directory: %w", err)
		}
		fd, err := os.Create(destPath)
		if err != nil {
			return DownloadFileResponse{}, fmt.Errorf("create output file: %w", err)
		}
		writer = fd
		closeWriter = fd.Close
		fileWriter = fd
	}
	var (
		frameBuf        []byte
		releaseFrameBuf func()
	)
	defer func() {
		if releaseFrameBuf != nil {
			releaseFrameBuf()
		}
	}()

	windowSize := c.FileRequestWindowBytes
	if windowSize <= 0 {
		windowSize = defaultClientRequestWindowBytes
	}
	ackEvery := req.AckEveryBytes
	if ackEvery <= 0 {
		ackEvery = defaultClientAckEveryBytes
	}

	if req.UseZeroCopy {
		return c.downloadFileFromManifestZeroCopy(ctx, req, entry, serverPath, destPath, writer, closeWriter, fileWriter)
	}

	if fileWriter == nil {
		localHasher := xxh3.New128()
		reader, meta, err := c.fetchFileWindow(ctx, req.Manifest.TransferID, req.FileID, serverPath, false, req.AcceptEncoding, req.AgePublicKey, req.AgeIdentity, 0, -1)
		if err != nil {
			return DownloadFileResponse{}, err
		}
		frameBuf, releaseFrameBuf = c.acquireFrameReadBuffer(meta.MaxWireSizeHint)
		copyErr := copyStream(writer, reader, frameBuf, localHasher)
		closeReadErr := reader.Close()
		closeWriteErr := closeWriter()
		if copyErr != nil {
			return DownloadFileResponse{}, fmt.Errorf("stream output file: %w", copyErr)
		}
		if closeReadErr != nil {
			return DownloadFileResponse{}, closeReadErr
		}
		if closeWriteErr != nil {
			return DownloadFileResponse{}, fmt.Errorf("close output file: %w", closeWriteErr)
		}
		localHash := intcodec.FormatXXH128HashToken(localHasher.Sum128())
		if meta.FileHashToken != "" && !strings.EqualFold(meta.FileHashToken, localHash) {
			return DownloadFileResponse{}, fmt.Errorf("file hash mismatch: server=%s client=%s", meta.FileHashToken, localHash)
		}
		return DownloadFileResponse{
			DestinationPath: destPath,
			Meta:            *meta,
			LocalFileHash:   localHash,
		}, nil
	}

	totalSize := entry.Size
	if totalSize < 0 {
		closeWriteErr := closeWriter()
		if closeWriteErr != nil {
			return DownloadFileResponse{}, fmt.Errorf("close output file: %w", closeWriteErr)
		}
		return DownloadFileResponse{}, errors.New("manifest entry has negative size")
	}

	var (
		offset     int64
		firstMeta  *FileFrameMeta
		resultMeta FileFrameMeta
		mixedComp  bool
		fileHasher = xxh3.New128()
		lastTS     int64
		lastAcked  int64
		synced     int64
		intervalTS = time.Now()
	)

	ackCtx, stopAck := context.WithCancel(ctx)
	defer stopAck()
	ackQueue := make(chan ackEvent, 8)
	ackErrCh := make(chan error, 1)
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		c.runAckWorker(ackCtx, ackQueue, ackErrCh, ackRequestBase{
			TransferID:      req.Manifest.TransferID,
			FileID:          req.FileID,
			FullPath:        serverPath,
			TargetBytes:     entry.Size,
			OnAck:           req.OnAck,
			ProgressUpdates: req.ProgressUpdates,
		})
	}()
	ackClosed := false
	closeAckWorker := func() {
		if !ackClosed {
			close(ackQueue)
			ackClosed = true
		}
		<-ackDone
	}
	defer closeAckWorker()

	for offset < totalSize {
		window := totalSize - offset
		if window > windowSize {
			window = windowSize
		}
		windowReader, windowMeta, err := c.fetchFileWindow(
			ctx,
			req.Manifest.TransferID,
			req.FileID,
			serverPath,
			false,
			req.AcceptEncoding,
			req.AgePublicKey,
			req.AgeIdentity,
			offset,
			window,
		)
		if err != nil {
			var missingErr *fileMissingError
			if errors.Is(err, ErrFileMissing) && errors.As(err, &missingErr) && shouldAcknowledgeMissing404(missingErr.Body) {
				if qErr := enqueueAck(ctx, ackQueue, ackErrCh, ackEvent{AckBytes: -1, Final: true}); qErr != nil {
					_ = closeWriter()
					return DownloadFileResponse{}, fmt.Errorf("%w (failed to ack missing: %v)", err, qErr)
				}
				closeAckWorker()
			}
			_ = closeWriter()
			return DownloadFileResponse{}, err
		}
		if frameBuf == nil {
			frameBuf, releaseFrameBuf = c.acquireFrameReadBuffer(windowMeta.MaxWireSizeHint)
		}
		getTrailerTS := func() int64 {
			if tsReader, ok := windowReader.(interface{ LastTrailerTS() int64 }); ok {
				if ts := tsReader.LastTrailerTS(); ts > 0 {
					return ts
				}
			}
			return lastTS
		}
		copyErr := copyStreamWithProgress(writer, windowReader, frameBuf, fileHasher, func(written int64) error {
			currentTotal := offset + written
			deltaBytes := currentTotal - lastAcked
			if deltaBytes < ackEvery {
				return nil
			}
			recvMS := time.Since(intervalTS).Milliseconds()
			syncMS := int64(0)
			if !req.NoSync {
				syncStart := time.Now()
				if err := dataSyncFile(fileWriter); err != nil {
					return fmt.Errorf("fdatasync output file: %w", err)
				}
				syncMS = time.Since(syncStart).Milliseconds()
			}
			synced = currentTotal
			// Final progress ack must include hash token; avoid sending a non-final ack at EOF.
			if deltaBytes >= ackEvery && synced < totalSize {
				if qErr := enqueueAck(ctx, ackQueue, ackErrCh, ackEvent{
					AckBytes:   synced,
					ServerTS:   getTrailerTS(),
					DeltaBytes: deltaBytes,
					RecvMS:     recvMS,
					SyncMS:     syncMS,
				}); qErr != nil {
					return fmt.Errorf("enqueue ack failed: %w", qErr)
				}
				lastAcked = synced
				intervalTS = time.Now()
			}
			return nil
		})
		closeReadErr := windowReader.Close()
		if copyErr != nil {
			_ = closeWriter()
			return DownloadFileResponse{}, fmt.Errorf("stream output file: %w", copyErr)
		}
		if closeReadErr != nil {
			_ = closeWriter()
			return DownloadFileResponse{}, closeReadErr
		}
		if windowMeta.Size != window {
			_ = closeWriter()
			return DownloadFileResponse{}, fmt.Errorf("window size mismatch: requested=%d got=%d", window, windowMeta.Size)
		}
		offset += windowMeta.Size
		lastTS = windowMeta.TrailerTS

		if firstMeta == nil {
			tmp := *windowMeta
			tmp.CompCounts = copyCompCounts(windowMeta.CompCounts)
			tmp.TrailerMetadata = cloneTrailerMetadata(windowMeta.TrailerMetadata)
			firstMeta = &tmp
			resultMeta = tmp
		} else {
			if resultMeta.Comp != windowMeta.Comp {
				mixedComp = true
			}
			resultMeta.Size += windowMeta.Size
			resultMeta.WireSize += windowMeta.WireSize
			resultMeta.TrailerTS = windowMeta.TrailerTS
			resultMeta.HashToken = windowMeta.HashToken
			mergeCompCounts(resultMeta.CompCounts, windowMeta.CompCounts)
			if windowMeta.FileHashToken != "" {
				resultMeta.FileHashToken = windowMeta.FileHashToken
			}
			if windowMeta.TrailerMetadata != nil {
				resultMeta.TrailerMetadata = cloneTrailerMetadata(windowMeta.TrailerMetadata)
			}
		}
	}
	if firstMeta == nil {
		resultMeta = FileFrameMeta{
			FileID:    req.FileID,
			Comp:      "none",
			Enc:       "none",
			Offset:    0,
			Size:      0,
			WireSize:  0,
			HashToken: "",
		}
	}
	if mixedComp {
		resultMeta.Comp = "mixed"
	}

	finalRecvMS := time.Since(intervalTS).Milliseconds()
	finalSyncMS := int64(0)
	if !req.NoSync {
		finalSyncStart := time.Now()
		if err := dataSyncFile(fileWriter); err != nil {
			_ = closeWriter()
			return DownloadFileResponse{}, fmt.Errorf("fdatasync output file: %w", err)
		}
		finalSyncMS = time.Since(finalSyncStart).Milliseconds()
	}
	synced = offset
	fullHash := intcodec.FormatXXH128HashToken(fileHasher.Sum128())
	if resultMeta.FileHashToken != "" && !strings.EqualFold(resultMeta.FileHashToken, fullHash) {
		_ = closeWriter()
		return DownloadFileResponse{}, fmt.Errorf("file hash mismatch: server=%s client=%s", resultMeta.FileHashToken, fullHash)
	}
	if err := applyVerifiedFileMetadata(fileWriter, resultMeta.TrailerMetadata, req.MetadataApplyBestEffort); err != nil {
		_ = closeWriter()
		return DownloadFileResponse{}, err
	}
	if qErr := enqueueAck(ctx, ackQueue, ackErrCh, ackEvent{
		AckBytes:   synced,
		ServerTS:   lastTS,
		HashToken:  fullHash,
		DeltaBytes: synced - lastAcked,
		RecvMS:     finalRecvMS,
		SyncMS:     finalSyncMS,
		Final:      true,
	}); qErr != nil {
		_ = closeWriter()
		return DownloadFileResponse{}, fmt.Errorf("enqueue final ack failed: %w", qErr)
	}
	closeAckWorker()
	if err := waitAckWorker(ackErrCh); err != nil {
		_ = closeWriter()
		return DownloadFileResponse{}, fmt.Errorf("acknowledge download failed: %w", err)
	}
	closeWriteErr := closeWriter()
	if closeWriteErr != nil {
		return DownloadFileResponse{}, fmt.Errorf("close output file: %w", closeWriteErr)
	}
	return DownloadFileResponse{
		DestinationPath: destPath,
		Meta:            resultMeta,
		LocalFileHash:   fullHash,
	}, nil
}

func (c *Client) downloadFileFromManifestZeroCopy(
	ctx context.Context,
	req DownloadFileRequest,
	entry ManifestEntry,
	serverPath string,
	destPath string,
	writer io.Writer,
	closeWriter func() error,
	fileWriter *os.File,
) (DownloadFileResponse, error) {
	localHasher := xxh3.New128()
	reader, _, err := c.fetchFileWindow(
		ctx,
		req.Manifest.TransferID,
		req.FileID,
		serverPath,
		true,
		req.AcceptEncoding,
		req.AgePublicKey,
		req.AgeIdentity,
		0,
		-1,
	)
	if err != nil {
		_ = closeWriter()
		return DownloadFileResponse{}, err
	}
	frameBuf, releaseFrameBuf := c.acquireFrameReadBuffer(8 * 1024 * 1024)
	defer releaseFrameBuf()

	var copied int64
	copyErr := copyStreamWithProgress(writer, reader, frameBuf, localHasher, func(written int64) error {
		copied = written
		return nil
	})
	closeReadErr := reader.Close()
	if copyErr != nil {
		_ = closeWriter()
		return DownloadFileResponse{}, fmt.Errorf("stream output file: %w", copyErr)
	}
	if closeReadErr != nil {
		_ = closeWriter()
		return DownloadFileResponse{}, closeReadErr
	}
	if !req.NoSync {
		if err := dataSyncFile(fileWriter); err != nil {
			_ = closeWriter()
			return DownloadFileResponse{}, fmt.Errorf("fdatasync output file: %w", err)
		}
	}
	if copied != entry.Size {
		_ = closeWriter()
		return DownloadFileResponse{}, fmt.Errorf("zerocopy size mismatch: expected=%d got=%d", entry.Size, copied)
	}
	closeWriteErr := closeWriter()
	if closeWriteErr != nil {
		return DownloadFileResponse{}, fmt.Errorf("close output file: %w", closeWriteErr)
	}

	if req.ProgressUpdates != nil {
		select {
		case req.ProgressUpdates <- DownloadProgressUpdate{FileID: req.FileID, AckBytes: copied}:
		default:
		}
	}
	if req.OnAck != nil {
		now := time.Now()
		req.OnAck(AckProgressEvent{
			TransferID:  req.Manifest.TransferID,
			FileID:      req.FileID,
			AckBytes:    copied,
			TargetBytes: entry.Size,
			AckTime:     now,
		})
	}

	meta := FileFrameMeta{
		FileID:   req.FileID,
		Comp:     "none",
		Enc:      "none",
		Offset:   0,
		Size:     copied,
		WireSize: copied,
	}
	localHash := intcodec.FormatXXH128HashToken(localHasher.Sum128())
	return DownloadFileResponse{
		DestinationPath: destPath,
		Meta:            meta,
		LocalFileHash:   localHash,
	}, nil
}

func (c *Client) GetTransferStatus(ctx context.Context, request GetTransferStatusRequest) (GetTransferStatusResponse, error) {
	if c == nil {
		return GetTransferStatusResponse{}, errors.New("nil client")
	}
	if request.TransferID == "" {
		return GetTransferStatusResponse{}, errors.New("missing transfer id")
	}
	u := fmt.Sprintf("%s/fs/transfer/%s/status", c.BaseURL, url.PathEscape(request.TransferID))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return GetTransferStatusResponse{}, fmt.Errorf("build status request: %w", err)
	}
	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return GetTransferStatusResponse{}, fmt.Errorf("request transfer status: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return GetTransferStatusResponse{}, fmt.Errorf("transfer status failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	var status TransferStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return GetTransferStatusResponse{}, fmt.Errorf("decode transfer status: %w", err)
	}
	return GetTransferStatusResponse{Status: &status}, nil
}

func shouldAcknowledgeMissing404(body string) bool {
	return strings.EqualFold(strings.TrimSpace(body), "file not found")
}

func (c *Client) AcknowledgeFileProgress(ctx context.Context, request AcknowledgeFileProgressRequest) (AcknowledgeFileProgressResponse, error) {
	if request.TransferID == "" {
		return AcknowledgeFileProgressResponse{}, errors.New("missing transfer id")
	}
	if request.FullPath == "" {
		return AcknowledgeFileProgressResponse{}, errors.New("missing full path")
	}
	ackToken, err := buildAckToken(request.AckBytes, request.ServerTS, request.HashToken)
	if err != nil {
		return AcknowledgeFileProgressResponse{}, err
	}
	if request.AckBytes >= 0 {
		if request.DeltaBytes < 0 {
			return AcknowledgeFileProgressResponse{}, errors.New("ack delta bytes must be >= 0")
		}
		if request.RecvMS < 0 {
			return AcknowledgeFileProgressResponse{}, errors.New("ack recv-ms must be >= 0")
		}
		if request.SyncMS < 0 {
			return AcknowledgeFileProgressResponse{}, errors.New("ack sync-ms must be >= 0")
		}
	}

	u, err := url.Parse(fmt.Sprintf("%s/fs/file/%s/%d/ack", c.BaseURL, url.PathEscape(request.TransferID), request.FileID))
	if err != nil {
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("build progress-ack url: %w", err)
	}
	q := u.Query()
	q.Set("path", request.FullPath)
	q.Set("ack-bytes", ackToken)
	if request.AckBytes >= 0 {
		q.Set("delta-bytes", strconv.FormatInt(request.DeltaBytes, 10))
		q.Set("recv-ms", strconv.FormatInt(request.RecvMS, 10))
		q.Set("sync-ms", strconv.FormatInt(request.SyncMS, 10))
	}
	u.RawQuery = q.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), nil)
	if err != nil {
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("build progress-ack request: %w", err)
	}
	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("progress-ack request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("progress-ack failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return AcknowledgeFileProgressResponse{}, nil
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

type ackEvent struct {
	AckBytes   int64
	ServerTS   int64
	HashToken  string
	DeltaBytes int64
	RecvMS     int64
	SyncMS     int64
	Final      bool
}

type ackRequestBase struct {
	TransferID      string
	FileID          uint64
	FullPath        string
	TargetBytes     int64
	OnAck           func(AckProgressEvent)
	ProgressUpdates chan<- DownloadProgressUpdate
}

func enqueueAck(ctx context.Context, ackQueue chan<- ackEvent, ackErrCh <-chan error, evt ackEvent) error {
	select {
	case err := <-ackErrCh:
		return err
	default:
	}
	select {
	case ackQueue <- evt:
		return nil
	case err := <-ackErrCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitAckWorker(ackErrCh <-chan error) error {
	select {
	case err := <-ackErrCh:
		return err
	default:
		return nil
	}
}

func (c *Client) runAckWorker(ctx context.Context, ackQueue <-chan ackEvent, ackErrCh chan<- error, base ackRequestBase) {
	var prevAckTime time.Time
	for evt := range ackQueue {
		req := AcknowledgeFileProgressRequest{
			TransferID: base.TransferID,
			FileID:     base.FileID,
			FullPath:   base.FullPath,
			AckBytes:   evt.AckBytes,
			ServerTS:   evt.ServerTS,
			HashToken:  evt.HashToken,
			DeltaBytes: evt.DeltaBytes,
			RecvMS:     evt.RecvMS,
			SyncMS:     evt.SyncMS,
		}
		err := retryAck(ctx, func(callCtx context.Context) error {
			_, err := c.AcknowledgeFileProgress(callCtx, req)
			return err
		})
		if err != nil {
			if evt.Final {
				select {
				case ackErrCh <- err:
				default:
				}
				return
			}
			log.Printf(
				"filexfer client: non-final ack failed tid=%s fid=%d bytes=%d err=%v",
				base.TransferID,
				base.FileID,
				evt.AckBytes,
				err,
			)
			continue
		}
		if evt.AckBytes >= 0 && base.ProgressUpdates != nil {
			select {
			case base.ProgressUpdates <- DownloadProgressUpdate{FileID: base.FileID, AckBytes: evt.AckBytes}:
			default:
			}
		}
		if evt.AckBytes >= 0 && base.OnAck != nil {
			ackTime := time.Now()
			base.OnAck(AckProgressEvent{
				TransferID:  base.TransferID,
				FileID:      base.FileID,
				AckBytes:    evt.AckBytes,
				TargetBytes: base.TargetBytes,
				PrevAckTime: prevAckTime,
				AckTime:     ackTime,
			})
			prevAckTime = ackTime
		}
	}
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

func copyStream(dst io.Writer, src io.Reader, buf []byte, hash *xxh3.Hasher128) error {
	return copyStreamWithProgress(dst, src, buf, hash, nil)
}

func copyCompCounts(src map[string]uint64) map[string]uint64 {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(src))
	for k, v := range src {
		out[k] = v
	}
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
	target = frameReadBucketSize(target)
	if target < minClientFrameReadBufferBytes {
		target = minClientFrameReadBufferBytes
	}
	if target > capSize {
		target = capSize
	}
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

func dataSyncFile(fd *os.File) error {
	if fd == nil {
		return nil
	}
	return syscall.Fdatasync(int(fd.Fd()))
}

func (c *Client) httpClient() *http.Client {
	if c.FileClient != nil {
		return c.FileClient
	}
	return newTunedHTTPClient(maxSocketReadBufferBytes())
}

func normalizeRequestedComp(raw string) string {
	comp := strings.ToLower(strings.TrimSpace(raw))
	switch comp {
	case "", "adapt":
		return "adapt"
	case EncodingZstd:
		return EncodingZstd
	case EncodingLz4:
		return EncodingLz4
	case "none", EncodingIdentity:
		return "none"
	}
	// Legacy CSV Accept-Encoding style values map to adaptive mode.
	if strings.Contains(comp, ",") || strings.Contains(comp, ";") {
		return "adapt"
	}
	return "adapt"
}

func parseManifest(raw []byte) (*Manifest, error) {
	reader := bufio.NewReader(bytes.NewReader(raw))
	manifest := &Manifest{
		Raw: append([]byte(nil), raw...),
	}
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
			if strings.HasPrefix(trimmed, "FM/1 ") {
				txferID, root, parseErr := parseManifestHeader(trimmed)
				if parseErr != nil {
					return nil, parseErr
				}
				if !seenHeader {
					manifest.TransferID = txferID
					manifest.Root = root
					seenHeader = true
				} else if manifest.TransferID != txferID || manifest.Root != root {
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

func applyVerifiedFileMetadata(fileWriter *os.File, meta *FileTrailerMetadata, bestEffort bool) error {
	if fileWriter == nil || meta == nil {
		return nil
	}
	if modeRaw := strings.TrimSpace(meta.Mode); modeRaw != "" {
		mode, err := parseManifestModeToken(modeRaw)
		if err != nil {
			if bestEffort {
				log.Printf("filexfer client: unable to parse trailer mode %q: %v", modeRaw, err)
			} else {
				return fmt.Errorf("parse trailer mode %q: %w", modeRaw, err)
			}
		} else if err := fileWriter.Chmod(mode); err != nil {
			if bestEffort {
				log.Printf("filexfer client: chmod failed mode=%s err=%v", modeRaw, err)
			} else {
				return fmt.Errorf("chmod output file to %s: %w", modeRaw, err)
			}
		}
	}
	uidRaw := strings.TrimSpace(meta.UID)
	gidRaw := strings.TrimSpace(meta.GID)
	if uidRaw == "" && gidRaw == "" {
		return nil
	}
	if uidRaw == "" || gidRaw == "" {
		err := errors.New("trailer uid/gid must both be set")
		if bestEffort {
			log.Printf("filexfer client: skipping chown: %v uid=%q gid=%q", err, uidRaw, gidRaw)
			return nil
		}
		return err
	}
	uid, err := strconv.Atoi(uidRaw)
	if err != nil {
		if bestEffort {
			log.Printf("filexfer client: invalid trailer uid %q: %v", uidRaw, err)
			return nil
		}
		return fmt.Errorf("invalid trailer uid %q: %w", uidRaw, err)
	}
	gid, err := strconv.Atoi(gidRaw)
	if err != nil {
		if bestEffort {
			log.Printf("filexfer client: invalid trailer gid %q: %v", gidRaw, err)
			return nil
		}
		return fmt.Errorf("invalid trailer gid %q: %w", gidRaw, err)
	}
	if err := fileWriter.Chown(uid, gid); err != nil {
		if bestEffort {
			log.Printf("filexfer client: chown failed uid=%d gid=%d err=%v", uid, gid, err)
			return nil
		}
		return fmt.Errorf("chown output file uid=%d gid=%d: %w", uid, gid, err)
	}
	return nil
}

func parseManifestHeader(line string) (string, string, error) {
	rest := strings.TrimPrefix(line, "FM/1 ")
	sep := strings.IndexByte(rest, ' ')
	if sep <= 0 || sep == len(rest)-1 {
		return "", "", errors.New("invalid manifest header")
	}
	txferID := rest[:sep]
	rootToken := rest[sep+1:]
	root, err := parseLenPrefixed(rootToken)
	if err != nil {
		return "", "", fmt.Errorf("invalid manifest root token: %w", err)
	}
	return txferID, root, nil
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

func parseLenPrefixed(token string) (string, error) {
	sep := strings.IndexByte(token, ':')
	if sep <= 0 {
		return "", errors.New("invalid len-prefixed token")
	}
	n, err := strconv.Atoi(token[:sep])
	if err != nil || n < 0 {
		return "", errors.New("invalid len prefix")
	}
	data := token[sep+1:]
	if len(data) != n {
		return "", errors.New("len prefix mismatch")
	}
	return data, nil
}

func decodeMtimeToken(prev string, token string) (string, error) {
	sep := strings.IndexByte(token, ':')
	if sep < 0 {
		return "", errors.New("invalid mtime token")
	}
	prefixLen, err := strconv.Atoi(token[:sep])
	if err != nil || prefixLen < 0 {
		return "", errors.New("invalid mtime prefix length")
	}
	if prefixLen > len(prev) {
		return "", errors.New("mtime prefix length exceeds previous value")
	}
	suffix := token[sep+1:]
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
	br       *bufio.Reader
	identity age.Identity

	meta *FileFrameMeta

	frameMeta   FileFrameMeta
	logical     io.ReadCloser
	frameHash   *xxh3.Hasher
	logicalRead int64

	expectOffset    bool
	expectedOffset  int64
	expectNextFrame bool

	pendingErr error
	finished   bool
	closed     bool
}

type readerWithCloser struct {
	io.Reader
	io.Closer
}

func (s *fileStream) LastTrailerTS() int64 {
	if s == nil || s.meta == nil {
		return 0
	}
	return s.meta.TrailerTS
}

func newFileStream(respBody io.ReadCloser, ageIdentity string) (io.ReadCloser, *FileFrameMeta, error) {
	identity, err := parseAgeIdentity(ageIdentity)
	if err != nil {
		return nil, nil, err
	}
	stream := &fileStream{
		respBody: respBody,
		br:       bufio.NewReader(respBody),
		identity: identity,
		meta:     &FileFrameMeta{},
	}
	if err := stream.openNextFrame(); err != nil {
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
	if s.expectOffset && !s.expectNextFrame {
		return errors.New("unexpected extra frame after terminal trailer")
	}

	meta, err := parseFXHeader(strings.TrimRight(headerLine, "\r\n"))
	if err != nil {
		return err
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
	s.frameHash = xxh3.New()
	_, _ = s.frameHash.Write([]byte(headerLine))
	payloadReader = io.TeeReader(payloadReader, s.frameHash)
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
	if s.frameHash == nil {
		return errors.New("missing frame hash state")
	}
	_, _ = s.frameHash.Write([]byte(trailer.ChecksumPrefix))
	if err := validateFrameHashToken(trailer.HashToken, s.frameHash.Sum64()); err != nil {
		return err
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
	s.frameHash = nil
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
		if strings.HasPrefix(token, "status=") {
			status = strings.TrimPrefix(token, "status=")
		}
		if strings.HasPrefix(token, "ts=") {
			tsRaw := strings.TrimPrefix(token, "ts=")
			parsedTS, parseErr := strconv.ParseInt(tsRaw, 10, 64)
			if parseErr != nil || parsedTS < 0 {
				return frameTrailer{}, errors.New("invalid trailer ts")
			}
			ts = parsedTS
		}
		if strings.HasPrefix(token, "file-hash=") {
			fileHashToken = strings.TrimPrefix(token, "file-hash=")
		}
		if strings.HasPrefix(token, "next=") {
			nextRaw := strings.TrimPrefix(token, "next=")
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
	if !validHashToken(hashToken) {
		return frameTrailer{}, errors.New("trailer missing or invalid hash token")
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
		return "", "", errors.New("trailer missing hash token")
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

func decryptAgeBytes(ciphertext []byte, ageIdentity string) ([]byte, error) {
	identity, err := parseAgeIdentity(ageIdentity)
	if err != nil {
		return nil, err
	}
	if identity == nil {
		return nil, errors.New("missing age identity for encrypted response")
	}
	reader, err := age.Decrypt(bytes.NewReader(ciphertext), identity)
	if err != nil {
		return nil, err
	}
	plaintext, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}

func validHashToken(raw string) bool {
	if raw == "" {
		return false
	}
	parts := strings.SplitN(raw, ":", 2)
	return len(parts) == 2 && parts[0] != "" && parts[1] != ""
}

func validateFrameHashToken(token string, actual uint64) error {
	parts := strings.SplitN(token, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return errors.New("invalid frame hash token")
	}
	algo := strings.ToLower(parts[0])
	switch algo {
	case "xxh64":
		expected := fmt.Sprintf("%016x", actual)
		value := strings.ToLower(parts[1])
		if value != expected {
			return fmt.Errorf("xxh64 mismatch: trailer=%s actual=%s", value, expected)
		}
		return nil
	default:
		return fmt.Errorf("unsupported frame hash algorithm: %s", parts[0])
	}
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
		reader, err := intcodec.WrapDecompressedReader(payload, comp)
		if err != nil {
			return nil, err
		}
		return reader, nil
	default:
		return nil, fmt.Errorf("unsupported compression mode: %s", comp)
	}
}
