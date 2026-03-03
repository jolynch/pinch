package filexfer

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultCLIEncodings = "zstd,lz4,identity"

func RunCLI(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) < 2 {
		printCLIUsage(stderr)
		return 2
	}
	serverURL := args[0]
	if err := validateServerURL(serverURL); err != nil {
		fmt.Fprintf(stderr, "invalid server-url: %v\n", err)
		printCLIUsage(stderr)
		return 2
	}
	cmd := args[1]
	cmdArgs := args[2:]

	switch cmd {
	case "transfer":
		return runTransferCLI(serverURL, cmdArgs, stdout, stderr)
	case "start":
		return runStartCLI(serverURL, cmdArgs, stdout, stderr)
	case "status":
		return runStatusCLI(serverURL, cmdArgs, stdout, stderr)
	case "get":
		return runGetCLI(serverURL, cmdArgs, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown cli command: %s\n", cmd)
		printCLIUsage(stderr)
		return 2
	}
}

func validateServerURL(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return fmt.Errorf("first argument must be server URL, for example http://localhost:8080")
	}
	if strings.HasPrefix(raw, "-") {
		return fmt.Errorf("first argument must be server URL, for example http://localhost:8080")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("first argument must be server URL, for example http://localhost:8080")
	}
	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("first argument must be server URL, for example http://localhost:8080")
	}
	return nil
}

func printCLIUsage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  pinch cli <server-url> transfer -s <abs> [--source-directory <abs>] [-o <manifest-path>] [--verbose] [--max-manifest-chunk-size N]")
	fmt.Fprintln(w, "  pinch cli <server-url> start --tid <id> [--manifest <path>] [--out-root <dir>] [--accept-encoding <csv>] [--concurrency N] [-A|--ack-every <bytes>] [-S|--sync-every <bytes>]")
	fmt.Fprintln(w, "  pinch cli <server-url> status --tid <id>")
	fmt.Fprintln(w, "  pinch cli <server-url> get --tid <id> --fd <uint64> [--manifest <path>] [--out-root <dir>] [-o <path|->] [--accept-encoding <csv>] [-A|--ack-every <bytes>] [-S|--sync-every <bytes>]")
}

func runTransferCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("transfer", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var sourceDir string
	var manifestOut string
	var verbose bool
	var maxChunk int
	fs.StringVar(&sourceDir, "s", "", "absolute source directory to transfer")
	fs.StringVar(&sourceDir, "source-directory", "", "absolute source directory to transfer")
	fs.StringVar(&manifestOut, "o", "", "output path for saved manifest")
	fs.BoolVar(&verbose, "verbose", false, "disable front-coding")
	fs.IntVar(&maxChunk, "max-manifest-chunk-size", 0, "max chunk bytes for manifest stream")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if sourceDir == "" {
		fmt.Fprintln(stderr, "transfer requires --source-directory (or -s)")
		return 2
	}
	if maxChunk < 0 {
		fmt.Fprintln(stderr, "--max-manifest-chunk-size must be >= 0")
		return 2
	}

	client := NewClient(serverURL, nil)
	start := time.Now()
	manifestResp, err := client.FetchManifest(context.Background(), FetchManifestRequest{
		Directory:      sourceDir,
		Verbose:        verbose,
		MaxChunkSize:   maxChunk,
		AcceptEncoding: defaultCLIEncodings,
	})
	if err != nil {
		fmt.Fprintf(stderr, "transfer failed: %v\n", err)
		return 1
	}
	manifest := manifestResp.Manifest
	if manifestOut == "" {
		manifestOut = manifest.TransferID + ".fm1"
	}
	if err := SaveManifest(manifestOut, manifest); err != nil {
		fmt.Fprintf(stderr, "save manifest failed: %v\n", err)
		return 1
	}

	var total int64
	for _, e := range manifest.Entries {
		total += e.Size
	}
	fmt.Fprintf(
		stdout,
		"transfer loaded: tid=%s files=%d total_size=%d root=%s elapsed=%s manifest=%s\n",
		manifest.TransferID,
		len(manifest.Entries),
		total,
		manifest.Root,
		time.Since(start).Round(time.Millisecond),
		manifestOut,
	)
	return 0
}

func runStatusCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var txferID string
	fs.StringVar(&txferID, "tid", "", "transfer id")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if txferID == "" {
		fmt.Fprintln(stderr, "status requires --tid")
		return 2
	}

	client := NewClient(serverURL, nil)
	statusResp, err := client.GetTransferStatus(context.Background(), GetTransferStatusRequest{
		TransferID: txferID,
	})
	if err != nil {
		fmt.Fprintf(stderr, "status failed: %v\n", err)
		return 1
	}
	status := statusResp.Status

	fmt.Fprintf(stdout, "transfer=%s files=%d done=%d done_size=%d total_size=%d\n", status.TransferID, status.NumFiles, status.Done, status.DoneSize, status.TotalSize)
	fmt.Fprintf(stdout, "complete: files=%.2f%% bytes=%.2f%%\n", status.PercentFiles, status.PercentBytes)
	fmt.Fprintf(
		stdout,
		"downloads: started=%d running=%d done=%d missing=%d\n",
		status.DownloadStatus.Started,
		status.DownloadStatus.Running,
		status.DownloadStatus.Done,
		status.DownloadStatus.Missing,
	)
	return 0
}

func runGetCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var txferID string
	var manifestPath string
	var fileIDRaw string
	var outRoot string
	var outFile string
	var acceptEncoding string
	var ackEvery int64
	var syncEvery int64
	fs.StringVar(&txferID, "tid", "", "transfer id")
	fs.StringVar(&manifestPath, "manifest", "", "path to manifest file (default: <tid>.fm1)")
	fs.StringVar(&fileIDRaw, "fd", "", "file id to download")
	fs.StringVar(&outRoot, "out-root", ".", "output root")
	fs.StringVar(&outFile, "o", "", "output file path, or '-' for stdout")
	fs.StringVar(&acceptEncoding, "accept-encoding", defaultCLIEncodings, "accept-encoding header")
	fs.Int64Var(&ackEvery, "A", defaultClientAckEveryBytes, "bytes between progress acks")
	fs.Int64Var(&ackEvery, "ack-every", defaultClientAckEveryBytes, "bytes between progress acks")
	fs.Int64Var(&syncEvery, "S", defaultClientSyncEveryBytes, "bytes between fdatasync operations")
	fs.Int64Var(&syncEvery, "sync-every", defaultClientSyncEveryBytes, "bytes between fdatasync operations")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if txferID == "" {
		fmt.Fprintln(stderr, "get requires --tid")
		return 2
	}
	if fileIDRaw == "" {
		fmt.Fprintln(stderr, "get requires --fd")
		return 2
	}
	if ackEvery <= 0 {
		fmt.Fprintln(stderr, "--ack-every must be > 0")
		return 2
	}
	if syncEvery <= 0 {
		fmt.Fprintln(stderr, "--sync-every must be > 0")
		return 2
	}
	fileID, err := parseFileID(fileIDRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --fd: %v\n", err)
		return 2
	}
	manifest, err := loadManifestForTransfer(txferID, manifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "load manifest failed: %v\n", err)
		return 1
	}

	client := NewClient(serverURL, nil)
	start := time.Now()
	downloadResp, err := client.DownloadFileFromManifest(context.Background(), DownloadFileRequest{
		Manifest:       manifest,
		FileID:         fileID,
		OutRoot:        outRoot,
		OutFile:        outFile,
		Stdout:         stdout,
		AcceptEncoding: acceptEncoding,
		AckEveryBytes:  ackEvery,
		SyncEveryBytes: syncEvery,
	})
	elapsed := time.Since(start)
	if err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	printFileMetrics(stdout, manifest.TransferID, fileID, downloadResp.DestinationPath, downloadResp.Meta, elapsed)
	return 0
}

func runStartCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var txferID string
	var manifestPath string
	var outRoot string
	var acceptEncoding string
	var concurrency int
	var ackEvery int64
	var syncEvery int64
	fs.StringVar(&txferID, "tid", "", "transfer id")
	fs.StringVar(&manifestPath, "manifest", "", "path to manifest file (default: <tid>.fm1)")
	fs.StringVar(&outRoot, "out-root", ".", "output root")
	fs.StringVar(&acceptEncoding, "accept-encoding", defaultCLIEncodings, "accept-encoding header")
	fs.IntVar(&concurrency, "concurrency", 4, "parallel download workers")
	fs.Int64Var(&ackEvery, "A", defaultClientAckEveryBytes, "bytes between progress acks")
	fs.Int64Var(&ackEvery, "ack-every", defaultClientAckEveryBytes, "bytes between progress acks")
	fs.Int64Var(&syncEvery, "S", defaultClientSyncEveryBytes, "bytes between fdatasync operations")
	fs.Int64Var(&syncEvery, "sync-every", defaultClientSyncEveryBytes, "bytes between fdatasync operations")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if txferID == "" {
		fmt.Fprintln(stderr, "start requires --tid")
		return 2
	}
	if concurrency <= 0 {
		fmt.Fprintln(stderr, "--concurrency must be > 0")
		return 2
	}
	if ackEvery <= 0 {
		fmt.Fprintln(stderr, "--ack-every must be > 0")
		return 2
	}
	if syncEvery <= 0 {
		fmt.Fprintln(stderr, "--sync-every must be > 0")
		return 2
	}
	manifest, err := loadManifestForTransfer(txferID, manifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "load manifest failed: %v\n", err)
		return 1
	}
	client := NewClient(serverURL, nil)

	startAll := time.Now()
	workCh := make(chan ManifestEntry)
	errCh := make(chan error, len(manifest.Entries))
	var wg sync.WaitGroup
	var completed atomic.Int64

	worker := func() {
		defer wg.Done()
		for entry := range workCh {
			startOne := time.Now()
			downloadResp, err := client.DownloadFileFromManifest(context.Background(), DownloadFileRequest{
				Manifest:       manifest,
				FileID:         entry.ID,
				OutRoot:        outRoot,
				OutFile:        "",
				AcceptEncoding: acceptEncoding,
				AckEveryBytes:  ackEvery,
				SyncEveryBytes: syncEvery,
			})
			if err != nil {
				errCh <- fmt.Errorf("id=%d: %w", entry.ID, err)
				continue
			}
			completed.Add(1)
			printFileMetrics(stdout, manifest.TransferID, entry.ID, downloadResp.DestinationPath, downloadResp.Meta, time.Since(startOne))
		}
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker()
	}
	for _, entry := range manifest.Entries {
		workCh <- entry
	}
	close(workCh)
	wg.Wait()
	close(errCh)

	var failures int
	for err := range errCh {
		failures++
		fmt.Fprintf(stderr, "start error: %v\n", err)
	}

	fmt.Fprintf(
		stdout,
		"start complete: tid=%s requested=%d downloaded=%d failed=%d elapsed=%s\n",
		manifest.TransferID,
		len(manifest.Entries),
		completed.Load(),
		failures,
		time.Since(startAll).Round(time.Millisecond),
	)
	if failures > 0 {
		return 1
	}
	return 0
}

func parseFileID(raw string) (uint64, error) {
	return strconv.ParseUint(raw, 10, 64)
}

func loadManifestForTransfer(txferID string, manifestPath string) (*Manifest, error) {
	if manifestPath == "" {
		manifestPath = txferID + ".fm1"
	}
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	if manifest.TransferID != txferID {
		return nil, fmt.Errorf("manifest transfer id mismatch: expected %s got %s", txferID, manifest.TransferID)
	}
	return manifest, nil
}

func printFileMetrics(stdout io.Writer, txferID string, fileID uint64, path string, meta FileFrameMeta, elapsed time.Duration) {
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 0.000001
	}
	speed := float64(meta.Size) / seconds
	var ratio float64
	if meta.WireSize > 0 {
		ratio = float64(meta.Size) / float64(meta.WireSize)
	}
	var savings float64
	if meta.Size > 0 {
		savings = (1.0 - float64(meta.WireSize)/float64(meta.Size)) * 100.0
	}
	serverFrameMS := meta.TrailerTS - meta.HeaderTS
	serverLogicalBps := 0.0
	serverWireBps := 0.0
	if serverFrameMS > 0 {
		serverSeconds := float64(serverFrameMS) / 1000.0
		serverLogicalBps = float64(meta.Size) / serverSeconds
		serverWireBps = float64(meta.WireSize) / serverSeconds
	}
	fmt.Fprintf(
		stdout,
		"file: tid=%s fd=%d\n  path: %s\n  transfer: comp=%s logical=%d wire=%d speed=%s ratio=%.3f savings=%.2f%%\n  checksum: hash=%s\n  timing: elapsed=%s ts0=%d ts1=%d server_frame_ms=%d server_logical=%s server_wire=%s\n\n",
		txferID,
		fileID,
		path,
		meta.Comp,
		meta.Size,
		meta.WireSize,
		humanRate(speed),
		ratio,
		savings,
		meta.HashToken,
		elapsed.Round(time.Millisecond),
		meta.HeaderTS,
		meta.TrailerTS,
		serverFrameMS,
		humanRate(serverLogicalBps),
		humanRate(serverWireBps),
	)
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
