package ftcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/jolynch/pinch/utils"
	"github.com/zeebo/xxh3"
)

type txferRequest struct {
	Directory    string
	Verbose      bool
	MaxChunkSize int
	Mode         string
	LinkMbps     int64
	Concurrency  int
}

func parseTXFERRequest(req Request) (txferRequest, error) {
	if req.Verb != VerbTXFER {
		return txferRequest{}, protocolErr{code: "BAD_COMMAND", message: "not TXFER"}
	}
	if len(req.Params) != 1 {
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "invalid TXFER arguments"}
	}
	p := req.Params[0]
	for key := range p {
		switch key {
		case "directory", "verbose", "max-manifest-chunk-size", "mode", "link-mbps", "concurrency":
		default:
			return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "unknown TXFER option"}
		}
	}
	directory := p["directory"]
	if strings.TrimSpace(directory) == "" {
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "missing TXFER directory"}
	}
	verbose := false
	if raw := p["verbose"]; raw != "" {
		if raw == "1" || strings.EqualFold(raw, "true") {
			verbose = true
		}
	}
	maxChunkSize := 0
	if raw := p["max-manifest-chunk-size"]; raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "max-manifest-chunk-size must be a positive integer"}
		}
		maxChunkSize = v
	}
	mode := strings.ToLower(strings.TrimSpace(p["mode"]))
	if mode != "fast" && mode != "gentle" {
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "mode must be fast or gentle"}
	}
	linkMbps, err := strconv.ParseInt(strings.TrimSpace(p["link-mbps"]), 10, 64)
	if err != nil || linkMbps < 0 {
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "link-mbps must be >= 0"}
	}
	concurrency, err := strconv.Atoi(strings.TrimSpace(p["concurrency"]))
	if err != nil || concurrency <= 0 {
		return txferRequest{}, protocolErr{code: "BAD_REQUEST", message: "concurrency must be > 0"}
	}
	return txferRequest{
		Directory:    directory,
		Verbose:      verbose,
		MaxChunkSize: maxChunkSize,
		Mode:         mode,
		LinkMbps:     linkMbps,
		Concurrency:  concurrency,
	}, nil
}

func handleTXFER(_ context.Context, req Request, out io.Writer, deps Deps) error {
	parsed, err := parseTXFERRequest(req)
	if err != nil {
		return err
	}
	if err := validateDirectory(parsed.Directory); err != nil {
		return protocolErr{code: "UNPROCESSABLE", message: err.Error()}
	}

	root := filepath.Clean(parsed.Directory)
	transfer, err := deps.NewTransfer(root, 0, 0)
	if err != nil {
		return protocolErr{code: "INTERNAL", message: "failed to initialize transfer"}
	}
	if ok := deps.SetTransferHints(transfer.ID, parsed.Mode, parsed.LinkMbps, parsed.Concurrency); !ok {
		return protocolErr{code: "INTERNAL", message: "failed to persist transfer hints"}
	}
	manifestMode := parsed.Mode
	manifestLinkMbps := parsed.LinkMbps
	manifestConcurrency := parsed.Concurrency
	if stored, ok := deps.GetTransfer(transfer.ID); ok {
		if strings.TrimSpace(stored.Mode) != "" {
			manifestMode = strings.ToLower(strings.TrimSpace(stored.Mode))
		}
		if stored.LinkMbps >= 0 {
			manifestLinkMbps = stored.LinkMbps
		}
		if stored.Concurrency > 0 {
			manifestConcurrency = stored.Concurrency
		}
	}
	cleanupTransfer := true
	defer func() {
		if cleanupTransfer {
			deps.DeleteTransfer(transfer.ID)
		}
	}()

	if err := encodeManifest(out, transfer.ID, root, manifestMode, manifestLinkMbps, manifestConcurrency, parsed.MaxChunkSize, parsed.Verbose, deps); err != nil {
		if isBrokenPipe(err) {
			return nil
		}
		return protocolErr{code: "BAD_REQUEST", message: err.Error()}
	}
	cleanupTransfer = false
	return nil
}

func validateDirectory(directory string) error {
	if !filepath.IsAbs(directory) {
		return errors.New("directory must be an absolute path")
	}
	stat, err := os.Stat(directory)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.New("directory does not exist")
		}
		return errors.New("directory is not usable")
	}
	if !stat.IsDir() {
		return errors.New("directory is not a directory")
	}
	if _, err := os.ReadDir(directory); err != nil {
		return errors.New("directory is not readable")
	}
	return nil
}

func encodeManifest(
	w io.Writer,
	transferID string,
	root string,
	mode string,
	linkMbps int64,
	concurrency int,
	maxChunkSize int,
	verbose bool,
	deps Deps,
) error {
	rootToken := fmt.Sprintf("%d:%s", len(root), root)
	header := fmt.Sprintf(
		"FM/2 %s %s mode=%s link-mbps=%d concurrency=%d\n",
		transferID,
		rootToken,
		mode,
		linkMbps,
		concurrency,
	)
	if maxChunkSize > 0 && len(header) > maxChunkSize {
		return errors.New("max-manifest-chunk-size is too small for header")
	}

	chunkBytes := 0
	prevPath := ""
	prevMtime := ""
	updatesCh := make(chan TransferFileStateUpdate, 1000)
	done := deps.RegisterTransferFileState(transferID, updatesCh, TransferStateStarted)
	defer func() {
		close(updatesCh)
		<-done
	}()
	fileID := 0

	startChunk := func() error {
		if _, err := io.WriteString(w, header); err != nil {
			return err
		}
		chunkBytes = len(header)
		prevPath = ""
		prevMtime = ""
		return nil
	}
	if err := startChunk(); err != nil {
		return err
	}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		entryPath := filepath.ToSlash(rel)
		entryMtime := strconv.FormatInt(info.ModTime().UnixNano(), 10)
		entryMode := formatManifestMode(info.Mode())

		pathToken := frontToken(prevPath, entryPath, verbose)
		mtimeToken := mtimeFrontToken(prevMtime, entryMtime, verbose)
		line := fmt.Sprintf("%d %d %s %s %s\n", fileID, info.Size(), mtimeToken, entryMode, pathToken)

		if maxChunkSize > 0 && chunkBytes+len(line) > maxChunkSize {
			if chunkBytes == len(header) {
				return errors.New("max-manifest-chunk-size is too small for manifest entry")
			}
			if _, err := io.WriteString(w, "\n"); err != nil {
				return err
			}
			if err := startChunk(); err != nil {
				return err
			}
			pathToken = frontToken("", entryPath, verbose)
			mtimeToken = mtimeFrontToken("", entryMtime, verbose)
			line = fmt.Sprintf("%d %d %s %s %s\n", fileID, info.Size(), mtimeToken, entryMode, pathToken)
			if chunkBytes+len(line) > maxChunkSize {
				return errors.New("max-manifest-chunk-size is too small for manifest entry")
			}
		}

		fullPath := filepath.Clean(filepath.Join(root, entryPath))
		updatesCh <- TransferFileStateUpdate{
			FileID:   uint64(fileID),
			PathHash: xxh3.Hash128([]byte(fullPath)),
			FileSize: info.Size(),
		}
		if _, err := io.WriteString(w, line); err != nil {
			return err
		}
		chunkBytes += len(line)
		prevPath = entryPath
		prevMtime = entryMtime
		fileID++
		return nil
	})
	if err != nil {
		return err
	}
	deps.ClipTransfer(transferID)
	return nil
}

func isBrokenPipe(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET)
}

func frontToken(prev string, curr string, verbose bool) string {
	prefix := 0
	if !verbose {
		prefix = utils.CommonPrefixLen(prev, curr)
	}
	suffix := curr[prefix:]
	return fmt.Sprintf("%d:%d:%s", prefix, len(suffix), suffix)
}

func mtimeFrontToken(prev string, curr string, verbose bool) string {
	prefix := 0
	if !verbose {
		prefix = utils.CommonPrefixLen(prev, curr)
	}
	suffix := curr[prefix:]
	return fmt.Sprintf("%d:%s", prefix, suffix)
}

func formatManifestMode(mode os.FileMode) string {
	bits := mode.Perm() | (mode & (os.ModeSetuid | os.ModeSetgid | os.ModeSticky))
	return fmt.Sprintf("%04o", bits)
}
