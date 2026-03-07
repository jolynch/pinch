package fhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	pub "github.com/jolynch/pinch/filexfer"
	intcodec "github.com/jolynch/pinch/internal/filexfer/codec"
	intframe "github.com/jolynch/pinch/internal/filexfer/frame"
	intlimit "github.com/jolynch/pinch/internal/filexfer/limit"
	intpolicy "github.com/jolynch/pinch/internal/filexfer/policy"
	intstore "github.com/jolynch/pinch/internal/filexfer/store"
	"github.com/zeebo/xxh3"
)

const (
	EncodingIdentity = intcodec.EncodingIdentity
	EncodingZstd     = intcodec.EncodingZstd
	EncodingLz4      = intcodec.EncodingLz4
)

func SelectEncoding(acceptEncoding string) string {
	return intcodec.SelectEncoding(acceptEncoding)
}

func WrapCompressedWriter(dst io.Writer, acceptEncoding string) (io.Writer, func() error, string, error) {
	return intcodec.WrapCompressedWriter(dst, acceptEncoding)
}

func maxFrameWireSizeHintBytes(comp string, logicalSize int64) (int64, error) {
	return intcodec.MaxFrameWireSizeHintBytes(comp, logicalSize)
}

func maxEncodedFrameSizeBytes(comp string, logicalSize int64) (int64, error) {
	return intcodec.MaxEncodedFrameSizeBytes(comp, logicalSize)
}

func ceilingMaxWSizeBucketBytes(size int64) int64 {
	return intcodec.CeilingMaxWSizeBucketBytes(size)
}

type CompressionMode = intpolicy.CompressionMode
type CompressionMetrics = intpolicy.CompressionMetrics
type CompressionDecision = intpolicy.CompressionDecision

const (
	CompressionModeZstdDefault = intpolicy.CompressionModeZstdDefault
	CompressionModeZstdLevel1  = intpolicy.CompressionModeZstdLevel1
	CompressionModeLz4         = intpolicy.CompressionModeLz4
	CompressionModeNone        = intpolicy.CompressionModeNone
)

func NewCompressionPolicy() *intpolicy.CompressionPolicy {
	return intpolicy.NewCompressionPolicy()
}

func frameCompTokenForMode(mode CompressionMode) string {
	return intpolicy.FrameCompTokenForMode(mode)
}

type Transfer = intstore.Transfer
type TransferFileState = intstore.TransferFileState
type TransferFileStateUpdate = intstore.TransferFileStateUpdate
type FileRef = intstore.FileRef
type FileLookupError = intstore.FileLookupError

const (
	TransferStateStarted = intstore.TransferStateStarted
	TransferStateRunning = intstore.TransferStateRunning
	TransferStateDone    = intstore.TransferStateDone
	TransferStateMissing = intstore.TransferStateMissing
)

func NewTransfer(directory string, numFiles int, totalSize int64) (Transfer, error) {
	return intstore.NewTransfer(directory, numFiles, totalSize)
}

func SetTransferState(txferID string, state uint8) bool {
	return intstore.SetTransferState(txferID, state)
}

func RegisterTransferFileState(txferID string, updatesCh <-chan TransferFileStateUpdate, state uint8) <-chan struct{} {
	return intstore.RegisterTransferFileState(txferID, updatesCh, state)
}

func RegisterTransferFileStates(txferID string, updates []TransferFileStateUpdate, state uint8) {
	intstore.RegisterTransferFileStates(txferID, updates, state)
}

func DeleteTransfer(txferID string) bool {
	return intstore.DeleteTransfer(txferID)
}

func GetTransfer(txferID string) (Transfer, bool) {
	return intstore.GetTransfer(txferID)
}

func GetFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error) {
	return intstore.GetFileRef(txferID, fileID, fullPathRaw)
}

func GetFile(txferID string, fileID uint64, fullPathRaw string) (*os.File, FileRef, error) {
	return intstore.GetFile(txferID, fileID, fullPathRaw)
}

func GetTransferFileStates(txferID string) ([]TransferFileState, bool) {
	return intstore.GetTransferFileStates(txferID)
}

func AcknowledgeTransferFile(txferID string, fileID uint64, ackBytes int64) bool {
	return intstore.AcknowledgeTransferFile(txferID, fileID, ackBytes)
}

func UpdateTransferFileHash(txferID string, fileID uint64, offset int64, chunk []byte) bool {
	return intstore.UpdateTransferFileHash(txferID, fileID, offset, chunk)
}

func VerifyTransferFileHash(txferID string, fileID uint64, expectedBytes int64, hashToken string) bool {
	return intstore.VerifyTransferFileHash(txferID, fileID, expectedBytes, hashToken)
}

func FinalizeTransferFileHash(txferID string, fileID uint64) (string, bool) {
	return intstore.FinalizeTransferFileHash(txferID, fileID)
}

func GetTransferFileCompressionMode(txferID string, fileID uint64) (CompressionMode, bool) {
	return intstore.GetTransferFileCompressionMode(txferID, fileID)
}

func SetTransferFileCompressionMode(txferID string, fileID uint64, mode CompressionMode) bool {
	return intstore.SetTransferFileCompressionMode(txferID, fileID, mode)
}

func SetTransferFileState(txferID string, fileID uint64, state uint8) bool {
	return intstore.SetTransferFileState(txferID, fileID, state)
}

func ClipTransfer(txferID string) bool {
	return intstore.ClipTransfer(txferID)
}

func resetTransferStore() {
	intstore.ResetTransferStoreForTest()
}

func transferCountForTest() int {
	return intstore.TransferCountForTest()
}

func listTransfersForTest() []Transfer {
	return intstore.ListTransfersForTest()
}

type fileStreamLimitConfig = intlimit.FileStreamLimitConfig

func configureFileStreamLimits(cfg fileStreamLimitConfig) error {
	return intlimit.ConfigureFileStreamLimits(cfg)
}

func currentFileStreamLimitState() intlimit.FileStreamLimitState {
	return intlimit.CurrentFileStreamLimitState()
}

type limitedResponseWriter struct {
	*intlimit.LimitedResponseWriter
}

func wrapLimitedResponseWriter(w http.ResponseWriter, ctx context.Context, state intlimit.FileStreamLimitState) *limitedResponseWriter {
	return &limitedResponseWriter{
		LimitedResponseWriter: intlimit.WrapLimitedResponseWriter(w, ctx, state),
	}
}

func (w *limitedResponseWriter) wroteAnyBody() bool {
	if w == nil || w.LimitedResponseWriter == nil {
		return false
	}
	return w.LimitedResponseWriter.WroteAnyBody()
}

var errFileStreamTimeLimitExceeded = intlimit.ErrFileStreamTimeLimitExceeded

type fileFrameMetadata = intframe.FileFrameMetadata
type frameWriteArgs = intframe.WriteArgs
type frameWriteStats = intframe.WriteStats
type FileFrameMeta = intframe.FileFrameMeta
type frameTrailer = intframe.FrameTrailer

func collectFileFrameMetadata(path string, info os.FileInfo) fileFrameMetadata {
	return intframe.CollectFileFrameMetadata(path, info)
}

func writeFrame(w http.ResponseWriter, args frameWriteArgs) (frameWriteStats, error) {
	return intframe.WriteFrame(w, args)
}

func parseFXHeader(line string) (FileFrameMeta, error) {
	return intframe.ParseFXHeader(line)
}

func parseFXTrailer(line string) (frameTrailer, error) {
	return intframe.ParseFXTrailer(line)
}

func validHashToken(raw string) bool {
	return intframe.ValidHashToken(raw)
}

func decodePayloadReaderByComp(payload io.Reader, comp string) (io.ReadCloser, error) {
	return intframe.DecodePayloadReaderByComp(payload, comp)
}

func writeLookupErr(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	var lookupErr *intstore.FileLookupError
	if errors.As(err, &lookupErr) {
		http.Error(w, lookupErr.Msg, lookupErr.Code)
		return
	}
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func formatXXH128HashToken(v xxh3.Uint128) string {
	return intcodec.FormatXXH128HashToken(v)
}

func formatXXH64HashToken(v uint64) string {
	return intcodec.FormatXXH64HashToken(v)
}

func humanRate(bps float64) string {
	if bps <= 0 {
		return "0 B/s"
	}
	units := []string{"B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s"}
	unit := 0
	for bps >= 1024 && unit < len(units)-1 {
		bps /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%.0f %s", bps, units[unit])
	}
	return fmt.Sprintf("%.2f %s", bps, units[unit])
}

type Client = pub.Client
type Manifest = pub.Manifest
type ManifestEntry = pub.ManifestEntry
type DownloadFileRequest = pub.DownloadFileRequest
type DownloadFileResponse = pub.DownloadFileResponse
type FetchManifestRequest = pub.FetchManifestRequest

func NewClient(baseURL string, hc *http.Client) *Client {
	return pub.NewClient(baseURL, hc)
}
