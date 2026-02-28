package filexfer

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/jolynch/pinch/utils"
)

type manifestEntry struct {
	path  string
	size  int64
	mtime string
}

func TransferHandler(w http.ResponseWriter, req *http.Request) {
	directory := req.URL.Query().Get("directory")
	if directory == "" {
		http.Error(w, "missing required query parameter: directory", http.StatusBadRequest)
		return
	}
	verbose, err := parseVerbose(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	maxChunkSize, err := parseMaxChunkSize(req.URL.Query().Get("max-manifest-chunk-size"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := validateDirectory(directory); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	entries, err := scanManifestEntries(directory)
	if err != nil {
		http.Error(w, "failed to build manifest", http.StatusInternalServerError)
		return
	}

	root := filepath.Clean(directory)
	totalSize := totalManifestSize(entries)
	transfer, err := NewTransfer(root, len(entries), totalSize)
	if err != nil {
		http.Error(w, "failed to initialize transfer", http.StatusInternalServerError)
		return
	}
	SetTransferState(transfer.ID, TransferStateRunning)
	defer SetTransferState(transfer.ID, TransferStateDone)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Add("Vary", "Accept-Encoding")

	cw := &countingWriter{w: w}
	out, closeEncoded, contentEncoding, err := WrapCompressedWriter(cw, req.Header.Get("Accept-Encoding"))
	if err != nil {
		http.Error(w, "failed to initialize compressed response", http.StatusInternalServerError)
		return
	}
	if contentEncoding != "" {
		w.Header().Set("Content-Encoding", contentEncoding)
	}

	if err := encodeManifest(out, transfer.ID, root, entries, maxChunkSize, verbose); err != nil {
		if cw.n == 0 {
			w.Header().Del("Content-Encoding")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("[filexfer][transfer] stream interrupted after %d bytes: %v", cw.n, err)
		return
	}

	if err := closeEncoded(); err != nil {
		if cw.n == 0 {
			w.Header().Del("Content-Encoding")
			http.Error(w, "failed to finalize encoded response", http.StatusInternalServerError)
			return
		}
		log.Printf("[filexfer][transfer] encoding close interrupted after %d bytes: %v", cw.n, err)
	}
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

func parseMaxChunkSize(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	val, err := strconv.Atoi(raw)
	if err != nil || val <= 0 {
		return 0, errors.New("max-manifest-chunk-size must be a positive integer")
	}
	return val, nil
}

func parseVerbose(req *http.Request) (bool, error) {
	vals, ok := req.URL.Query()["verbose"]
	if !ok {
		return false, nil
	}
	if len(vals) == 0 || vals[0] == "" {
		return true, nil
	}
	verbose, err := strconv.ParseBool(vals[0])
	if err != nil {
		return false, errors.New("verbose must be a boolean value")
	}
	return verbose, nil
}

func scanManifestEntries(root string) ([]manifestEntry, error) {
	entries := make([]manifestEntry, 0, 128)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
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
		rel = filepath.ToSlash(rel)
		entries = append(entries, manifestEntry{
			path:  rel,
			size:  info.Size(),
			mtime: strconv.FormatInt(info.ModTime().UnixNano(), 10),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].path < entries[j].path
	})
	return entries, nil
}

func encodeManifest(w io.Writer, transferID string, root string, entries []manifestEntry, maxChunkSize int, verbose bool) error {
	rootToken := fmt.Sprintf("%d:%s", len(root), root)
	header := fmt.Sprintf("FM/1 %s %s\n", transferID, rootToken)

	if maxChunkSize > 0 && len(header) > maxChunkSize {
		return errors.New("max-manifest-chunk-size is too small for header")
	}

	chunkBytes := 0
	prevPath := ""
	prevMtime := ""

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

	for id, entry := range entries {
		pathToken := frontToken(prevPath, entry.path, verbose)
		mtimeToken := mtimeFrontToken(prevMtime, entry.mtime, verbose)
		line := fmt.Sprintf("%d %d %s %s\n", id, entry.size, mtimeToken, pathToken)

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

			pathToken = frontToken("", entry.path, verbose)
			mtimeToken = mtimeFrontToken("", entry.mtime, verbose)
			line = fmt.Sprintf("%d %d %s %s\n", id, entry.size, mtimeToken, pathToken)
			if chunkBytes+len(line) > maxChunkSize {
				return errors.New("max-manifest-chunk-size is too small for manifest entry")
			}
		}

		if _, err := io.WriteString(w, line); err != nil {
			return err
		}
		chunkBytes += len(line)
		prevPath = entry.path
		prevMtime = entry.mtime
	}

	return nil
}

func totalManifestSize(entries []manifestEntry) int64 {
	var total int64
	for _, e := range entries {
		total += e.size
	}
	return total
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

type countingWriter struct {
	w io.Writer
	n int
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += n
	return n, err
}
