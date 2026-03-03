package filexfer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zeebo/xxh3"
)

type Client struct {
	BaseURL                 string
	HTTP                    *http.Client
	FileRequestWindowBytes  int64
	FrameBufferBytes        int
	MaxFrameReadBufferBytes int
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
	Path  string
}

type FileFrameMeta struct {
	FileID          uint64
	Comp            string
	Enc             string
	Offset          int64
	Size            int64
	WireSize        int64
	MaxWireSizeHint int64
	HeaderTS        int64
	TrailerTS       int64
	HashToken       string
	FileHashToken   string
}

type DownloadFileRequest struct {
	Manifest       *Manifest
	FileID         uint64
	OutRoot        string
	OutFile        string
	Stdout         io.Writer
	AcceptEncoding string
	AckEveryBytes  int64
	SyncEveryBytes int64
	ProgressUpdates chan<- DownloadProgressUpdate
}

type DownloadProgressUpdate struct {
	FileID   uint64
	AckBytes int64
}

type DownloadFileResponse struct {
	DestinationPath string
	Meta            FileFrameMeta
}

type FetchManifestRequest struct {
	Directory      string
	Verbose        bool
	MaxChunkSize   int
	AcceptEncoding string
}

type FetchManifestResponse struct {
	Manifest *Manifest
}

type FetchFileRequest struct {
	TransferID     string
	FileID         uint64
	FullPath       string
	AcceptEncoding string
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
	defaultClientSyncEveryBytes          int64 = 256 * 1024 * 1024
)

func NewClient(baseURL string, hc *http.Client) *Client {
	return &Client{
		BaseURL:                 strings.TrimRight(baseURL, "/"),
		HTTP:                    hc,
		FileRequestWindowBytes:  defaultClientRequestWindowBytes,
		FrameBufferBytes:        defaultClientFrameBufferBytes,
		MaxFrameReadBufferBytes: defaultClientMaxFrameReadBufferBytes,
	}
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

	reader, err := WrapDecompressedReader(resp.Body, resp.Header.Get("Content-Encoding"))
	if err != nil {
		return FetchManifestResponse{}, fmt.Errorf("decode manifest body: %w", err)
	}
	defer reader.Close()

	raw, err := io.ReadAll(reader)
	if err != nil {
		return FetchManifestResponse{}, fmt.Errorf("read manifest body: %w", err)
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
		request.AcceptEncoding,
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
	acceptEncoding string,
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

	u, err := url.Parse(fmt.Sprintf("%s/fs/file/%s/%d", c.BaseURL, url.PathEscape(txferID), fileID))
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
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build file request: %w", err)
	}
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}

	resp, err := c.fileHTTPClient().Do(req)
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

	stream, meta, err := newFileStream(resp.Body)
	if err != nil {
		resp.Body.Close()
		return nil, nil, err
	}
	if meta.FileID != fileID {
		stream.Close()
		return nil, nil, fmt.Errorf("file id mismatch: expected %d got %d", fileID, meta.FileID)
	}
	if meta.Enc != "none" {
		stream.Close()
		return nil, nil, fmt.Errorf("unsupported enc mode: %s", meta.Enc)
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
	var frameBuf []byte

	windowSize := c.FileRequestWindowBytes
	if windowSize <= 0 {
		windowSize = defaultClientRequestWindowBytes
	}
	ackEvery := req.AckEveryBytes
	if ackEvery <= 0 {
		ackEvery = defaultClientAckEveryBytes
	}
	syncEvery := req.SyncEveryBytes
	if syncEvery <= 0 {
		syncEvery = defaultClientSyncEveryBytes
	}

	if fileWriter == nil {
		reader, meta, err := c.fetchFileWindow(ctx, req.Manifest.TransferID, req.FileID, serverPath, req.AcceptEncoding, 0, -1)
		if err != nil {
			return DownloadFileResponse{}, err
		}
		frameBufSize := effectiveFrameReadBufferSize(c.FrameBufferBytes, meta.MaxWireSizeHint, c.MaxFrameReadBufferBytes)
		frameBuf = make([]byte, frameBufSize)
		copyErr := copyStream(writer, reader, frameBuf, nil)
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
		return DownloadFileResponse{DestinationPath: destPath, Meta: *meta}, nil
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
			TransferID: req.Manifest.TransferID,
			FileID:     req.FileID,
			FullPath:   serverPath,
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
			req.AcceptEncoding,
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
		targetBufSize := effectiveFrameReadBufferSize(c.FrameBufferBytes, windowMeta.MaxWireSizeHint, c.MaxFrameReadBufferBytes)
		if len(frameBuf) != targetBufSize {
			frameBuf = make([]byte, targetBufSize)
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
			if currentTotal-synced < syncEvery {
				return nil
			}
			recvMS := time.Since(intervalTS).Milliseconds()
			syncStart := time.Now()
			if err := dataSyncFile(fileWriter); err != nil {
				return fmt.Errorf("fdatasync output file: %w", err)
			}
			syncMS := time.Since(syncStart).Milliseconds()
			synced = currentTotal
			deltaBytes := synced - lastAcked
			if deltaBytes >= ackEvery {
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
	finalSyncStart := time.Now()
	if err := dataSyncFile(fileWriter); err != nil {
		_ = closeWriter()
		return DownloadFileResponse{}, fmt.Errorf("fdatasync output file: %w", err)
	}
	finalSyncMS := time.Since(finalSyncStart).Milliseconds()
	synced = offset
	fullHash := formatXXH128HashToken(fileHasher.Sum128())
	if resultMeta.FileHashToken != "" && !strings.EqualFold(resultMeta.FileHashToken, fullHash) {
		_ = closeWriter()
		return DownloadFileResponse{}, fmt.Errorf("file hash mismatch: server=%s client=%s", resultMeta.FileHashToken, fullHash)
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
	return DownloadFileResponse{DestinationPath: destPath, Meta: resultMeta}, nil
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
	resp, err := c.fileHTTPClient().Do(httpReq)
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
	AckBytes  int64
	ServerTS  int64
	HashToken string
	DeltaBytes int64
	RecvMS     int64
	SyncMS     int64
	Final     bool
}

type ackRequestBase struct {
	TransferID string
	FileID     uint64
	FullPath   string
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
	if target < minClientFrameReadBufferBytes {
		target = minClientFrameReadBufferBytes
	}
	if target > capSize {
		target = capSize
	}
	return target
}

func dataSyncFile(fd *os.File) error {
	if fd == nil {
		return nil
	}
	return syscall.Fdatasync(int(fd.Fd()))
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{}
}

func (c *Client) fileHTTPClient() *http.Client {
	base := c.httpClient()
	clone := *base

	if base.Transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.DisableCompression = true
		clone.Transport = t
		return &clone
	}
	if t, ok := base.Transport.(*http.Transport); ok {
		tc := t.Clone()
		tc.DisableCompression = true
		clone.Transport = tc
	}
	return &clone
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

	idRaw := line[:first]
	sizeRaw := line[first+1 : second]
	mtimeToken := line[second+1 : third]
	pathToken := line[third+1:]

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
		Path:  pathResolved,
	}
	return entry, pathResolved, mtimeResolved, nil
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

	meta *FileFrameMeta

	frameMeta   FileFrameMeta
	logical     io.ReadCloser
	hash        *xxh3.Hasher128
	logicalRead int64

	expectOffset    bool
	expectedOffset  int64
	expectNextFrame bool

	pendingErr error
	finished   bool
	closed     bool
}

func (s *fileStream) LastTrailerTS() int64 {
	if s == nil || s.meta == nil {
		return 0
	}
	return s.meta.TrailerTS
}

func newFileStream(respBody io.ReadCloser) (io.ReadCloser, *FileFrameMeta, error) {
	stream := &fileStream{
		respBody: respBody,
		br:       bufio.NewReader(respBody),
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
			_, _ = s.hash.Write(p[:n])
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
	logicalReader, err := decodePayloadReaderByComp(payloadReader, meta.Comp)
	if err != nil {
		return fmt.Errorf("decode payload reader: %w", err)
	}
	s.frameMeta = meta
	s.logical = logicalReader
	s.hash = xxh3.New128()
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
	actual := s.hash.Sum128()
	if err := validateTrailerHashToken(trailer.HashToken, actual); err != nil {
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
	if s.meta.Comp != s.frameMeta.Comp {
		s.meta.Comp = "mixed"
	}
	s.meta.TrailerTS = trailer.TS
	s.meta.HashToken = trailer.HashToken
	if trailer.FileHashToken != "" {
		s.meta.FileHashToken = trailer.FileHashToken
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
	FileID        uint64
	TS            int64
	HashToken     string
	FileHashToken string
	Next          *int64
}

func parseFXTrailer(line string) (frameTrailer, error) {
	fields := strings.Fields(line)
	if len(fields) < 3 || fields[0] != "FXT/1" {
		return frameTrailer{}, errors.New("invalid FXT/1 trailer")
	}
	fileID, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return frameTrailer{}, fmt.Errorf("invalid trailer file id: %w", err)
	}
	status := ""
	var hashToken string
	var fileHashToken string
	var ts int64 = -1
	var nextOffset *int64
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
		if strings.HasPrefix(token, "hash=") {
			hashToken = strings.TrimPrefix(token, "hash=")
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
	return frameTrailer{
		FileID:        fileID,
		TS:            ts,
		HashToken:     hashToken,
		FileHashToken: fileHashToken,
		Next:          nextOffset,
	}, nil
}

func validHashToken(raw string) bool {
	if raw == "" {
		return false
	}
	parts := strings.SplitN(raw, ":", 2)
	return len(parts) == 2 && parts[0] != "" && parts[1] != ""
}

func validateTrailerHashToken(token string, actual xxh3.Uint128) error {
	parts := strings.SplitN(token, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return errors.New("invalid trailer hash token")
	}
	algo := strings.ToLower(parts[0])
	value := strings.ToLower(parts[1])
	switch algo {
	case "xxh128":
		expected := hash128Hex(actual)
		if value != expected {
			return fmt.Errorf("xxh128 mismatch: trailer=%s actual=%s", value, expected)
		}
		return nil
	default:
		return fmt.Errorf("unsupported trailer hash algorithm: %s", parts[0])
	}
}

func hash128Hex(v xxh3.Uint128) string {
	b := v.Bytes()
	return hex.EncodeToString(b[:])
}

func decodePayloadReaderByComp(payload io.Reader, comp string) (io.ReadCloser, error) {
	switch comp {
	case "none":
		return io.NopCloser(payload), nil
	case EncodingZstd, EncodingLz4:
		reader, err := WrapDecompressedReader(payload, comp)
		if err != nil {
			return nil, err
		}
		return reader, nil
	default:
		return nil, fmt.Errorf("unsupported compression mode: %s", comp)
	}
}
