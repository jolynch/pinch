package filexfercli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"filippo.io/age"
	. "github.com/jolynch/pinch/filexfer"
	"github.com/jolynch/pinch/internal/filexfer/encoding"
)

const defaultVerboseStatusInterval = 10 * time.Second
const defaultCLIAckEveryBytes int64 = 256 * 1024 * 1024
const defaultVerboseProgressInterval = 2 * time.Second
const defaultCLIProbeBytes int64 = 1 * 1024 * 1024

type synchronizedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (sw *synchronizedWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}

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
	errMsg := "first argument must be file-listener address, for example 127.0.0.1:3453"
	if strings.TrimSpace(raw) == "" {
		return errors.New(errMsg)
	}
	if strings.HasPrefix(raw, "-") {
		return errors.New(errMsg)
	}
	host, port, splitErr := net.SplitHostPort(raw)
	if splitErr != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return errors.New(errMsg)
	}
	return nil
}

func printCLIUsage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  pinch cli <file-listener> transfer -s <abs> [--source-directory <abs>] [-o <manifest-path>] [--encrypt age] [--load-strategy fast|gentle] [--probe-bytes <size>] [-v|--verbose] [--max-manifest-chunk-size N]")
	fmt.Fprintln(w, "  pinch cli <file-listener> start [--tid <id>] [--manifest <path>] [--out-root <dir>] [--encrypt age] [--concurrency N] [-a|--ack-every <size>] [--batch-size <size>] [--no-sync] [-v|--verbose]")
	fmt.Fprintln(w, "  pinch cli <file-listener> status --tid <id>")
	fmt.Fprintln(w, "  pinch cli <file-listener> get [--tid <id>] --fd <uint64> [--manifest <path>] [--out-root <dir>] [-o <path|->] [--encrypt age] [--load-strategy fast|gentle] [-a|--ack-every <size>] [--batch-size <size>] [--no-sync] [-v|--verbose]")
}

func resolveEncryptionOptions(mode string) (string, string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case "", "none":
		return "", "", nil
	case "age":
		identity, err := age.GenerateX25519Identity()
		if err != nil {
			return "", "", fmt.Errorf("generate age identity: %w", err)
		}
		return identity.Recipient().String(), identity.String(), nil
	default:
		return "", "", fmt.Errorf("unsupported --encrypt value %q (only \"age\" is supported)", mode)
	}
}

func resolveLoadStrategy(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", LoadStrategyFast:
		return LoadStrategyFast, nil
	case LoadStrategyGentle:
		return LoadStrategyGentle, nil
	default:
		return "", fmt.Errorf("unsupported --load-strategy value %q (supported: fast, gentle)", raw)
	}
}

func runTransferCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("transfer", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var sourceDir string
	var manifestOut string
	var encryptMode string
	var loadStrategyRaw string
	var probeBytesRaw string
	var verbose bool
	var maxChunk int
	fs.StringVar(&sourceDir, "s", "", "absolute source directory to transfer")
	fs.StringVar(&sourceDir, "source-directory", "", "absolute source directory to transfer")
	fs.StringVar(&manifestOut, "o", "", "output path for saved manifest")
	fs.StringVar(&encryptMode, "encrypt", "", "response encryption mode (supported: age)")
	fs.StringVar(&loadStrategyRaw, "load-strategy", LoadStrategyFast, "server load strategy (fast|gentle)")
	probeBytesRaw = encoding.HumanBytes(defaultCLIProbeBytes)
	fs.StringVar(&probeBytesRaw, "probe-bytes", probeBytesRaw, "probe payload size for transfer metadata")
	fs.BoolVar(&verbose, "v", false, "disable front-coding")
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
	probeBytes, err := encoding.ParseByteSize(probeBytesRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --probe-bytes: %v\n", err)
		return 2
	}
	if probeBytes <= 0 {
		fmt.Fprintln(stderr, "--probe-bytes must be > 0")
		return 2
	}
	loadStrategy, err := resolveLoadStrategy(loadStrategyRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --load-strategy: %v\n", err)
		return 2
	}
	agePublicKey, ageIdentity, err := resolveEncryptionOptions(encryptMode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}

	client := NewClient(serverURL, WithLoadStrategy(loadStrategy))
	start := time.Now()
	probeResult, err := client.ProbeLink(context.Background(), ProbeRequest{
		Samples:      3,
		ProbeBytes:   probeBytes,
		LoadStrategy: loadStrategy,
		AgePublicKey: agePublicKey,
		AgeIdentity:  ageIdentity,
	})
	if err != nil {
		fmt.Fprintf(stderr, "probe failed: %v\n", err)
		return 1
	}
	fmt.Fprintf(
		stdout,
		"transfer-probe: strategy=%s server_cpu=%d avg_ms=%d est_link=%dMbps concurrency=%d\n",
		loadStrategy,
		probeResult.ServerCPU,
		probeResult.AvgLatencyMS,
		probeResult.LinkMbps,
		probeResult.SuggestedConcurrency,
	)
	manifestResp, err := client.FetchManifest(context.Background(), FetchManifestRequest{
		Directory:    sourceDir,
		Verbose:      verbose,
		MaxChunkSize: maxChunk,
		Mode:         loadStrategy,
		LinkMbps:     probeResult.LinkMbps,
		Concurrency:  probeResult.SuggestedConcurrency,
		AgePublicKey: agePublicKey,
		AgeIdentity:  ageIdentity,
	})
	if err != nil {
		fmt.Fprintf(stderr, "transfer failed: %v\n", err)
		return 1
	}
	manifest := manifestResp.Manifest

	var total int64
	for _, e := range manifest.Entries {
		total += e.Size
	}
	if manifestOut == "" || manifestOut == "-" {
		manifestBytes, err := MarshalManifest(manifest)
		if err != nil {
			fmt.Fprintf(stderr, "encode manifest failed: %v\n", err)
			return 1
		}
		if _, err := stdout.Write(manifestBytes); err != nil {
			fmt.Fprintf(stderr, "write manifest failed: %v\n", err)
			return 1
		}
		fmt.Fprintf(
			stderr,
			"transfer loaded: tid=%s files=%d total_size=%d root=%s elapsed=%s manifest=stdout\n",
			manifest.TransferID,
			len(manifest.Entries),
			total,
			manifest.Root,
			time.Since(start).Round(time.Millisecond),
		)
		return 0
	}
	if err := SaveManifest(manifestOut, manifest); err != nil {
		fmt.Fprintf(stderr, "save manifest failed: %v\n", err)
		return 1
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

	client := NewClient(serverURL)
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
	var encryptMode string
	var loadStrategyRaw string
	var ackEveryRaw string
	var batchSizeRaw string
	var noSync bool
	var verbose bool
	fs.StringVar(&txferID, "tid", "", "transfer id")
	fs.StringVar(&manifestPath, "manifest", "", "path to manifest file (default: <tid>.fm2)")
	fs.StringVar(&fileIDRaw, "fd", "", "file id to download")
	fs.StringVar(&outRoot, "out-root", ".", "output root")
	fs.StringVar(&outFile, "o", "", "output file path, or '-' for stdout")
	fs.StringVar(&encryptMode, "encrypt", "", "response encryption mode (supported: age)")
	fs.StringVar(&loadStrategyRaw, "load-strategy", LoadStrategyFast, "server load strategy (fast|gentle)")
	fs.BoolVar(&verbose, "v", false, "verbose progress output")
	fs.BoolVar(&verbose, "verbose", false, "verbose progress output")
	ackEveryRaw = encoding.HumanBytes(defaultCLIAckEveryBytes)
	fs.StringVar(&ackEveryRaw, "a", ackEveryRaw, "bytes between progress acks")
	fs.StringVar(&ackEveryRaw, "ack-every", ackEveryRaw, "bytes between progress acks")
	fs.StringVar(&batchSizeRaw, "b", ackEveryRaw, "parallel batch size, unit of work per concurrent request")
	fs.StringVar(&batchSizeRaw, "batch-size", ackEveryRaw, "parallel batch size, unit of work per concurrent request")
	fs.BoolVar(&noSync, "no-sync", false, "ack without disk sync")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fileIDRaw == "" {
		fmt.Fprintln(stderr, "get requires --fd")
		return 2
	}
	ackEvery, err := encoding.ParseByteSize(ackEveryRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --ack-every: %v\n", err)
		return 2
	}
	if ackEvery <= 0 {
		fmt.Fprintln(stderr, "--ack-every must be > 0")
		return 2
	}
	batchSize, err := encoding.ParseByteSize(batchSizeRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --batch-size: %v\n", err)
		return 2
	}
	if batchSize <= 0 {
		fmt.Fprintln(stderr, "--batch-size must be > 0")
		return 2
	}
	agePublicKey, ageIdentity, err := resolveEncryptionOptions(encryptMode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
	loadStrategy, err := resolveLoadStrategy(loadStrategyRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --load-strategy: %v\n", err)
		return 2
	}
	fileID, err := parseFileID(fileIDRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --fd: %v\n", err)
		return 2
	}
	manifest, resolvedManifestPath, resolvedTxferID, err := loadManifestForGet(txferID, manifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "load manifest failed: %v\n", err)
		return 1
	}
	txferID = resolvedTxferID
	progressPath := resolvedManifestPath + ".progress"
	progressState, err := loadProgressState(progressPath)
	if err != nil {
		fmt.Fprintf(stderr, "load progress failed: %v\n", err)
		return 1
	}
	applyProgressStateToManifest(manifest, progressState)
	progressUpdates := make(chan DownloadProgressUpdate, 128)
	var onProgressUpdate func(DownloadProgressUpdate)
	if verbose {
		progressReporter := newVerboseProgressReporter(stderr)
		onProgressUpdate = progressReporter.ReportUpdate
	}
	forwardProgress := func(update DownloadProgressUpdate) {
		applyProgressUpdateToManifest(manifest, update)
		if onProgressUpdate != nil {
			onProgressUpdate(update)
		}
	}
	stopProgress, markMetadataDonePersisted := startProgressWriter(progressPath, progressState, progressUpdates, forwardProgress, stderr)
	markMetadataDone := func(fileID uint64) {
		markManifestEntryMetadataDone(manifest, fileID)
		markMetadataDonePersisted(fileID)
	}
	defer stopProgress()

	client := NewClient(serverURL, WithLoadStrategy(loadStrategy))
	start := time.Now()
	entry, ok := manifest.EntryByID(fileID)
	if !ok {
		fmt.Fprintf(stderr, "get failed: file id %d not in manifest\n", fileID)
		return 1
	}
	progress := entry.Progress
	if progress.AckBytes >= entry.Size {
		if !progress.MetadataDone {
			if err := refreshCompletedFileMetadata(context.Background(), client, manifest, fileID, outRoot, outFile, agePublicKey, ageIdentity); err != nil {
				fmt.Fprintf(stderr, "get metadata refresh failed: %v\n", err)
				return 1
			}
			markMetadataDone(fileID)
			fmt.Fprintf(stderr, "get metadata refreshed: fd=%d\n", fileID)
			return 0
		}
		fmt.Fprintf(stderr, "get skipped: already complete fd=%d ack=%d\n", fileID, progress.AckBytes)
		return 0
	}
	outputPath := resolveDownloadDestinationPath(entry, outRoot, outFile)
	downloadBatchResp, err := client.DownloadFilesFromManifestBatch(context.Background(), DownloadBatchRequest{
		Manifest:      manifest,
		FileIDs:       []uint64{fileID},
		BatchMaxBytes: batchSize,
		OutputWriter: func(entry ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
			destPath := resolveDownloadDestinationPath(entry, outRoot, outFile)
			return openDownloadOutput(entry, offset, destPath, stdout, noSync)
		},
		AgePublicKey:    agePublicKey,
		AgeIdentity:     ageIdentity,
		ProgressUpdates: progressUpdates,
	})
	elapsed := time.Since(start)
	if err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	if len(downloadBatchResp.Files) != 1 {
		fmt.Fprintf(stderr, "get failed: expected one downloaded file, got %d\n", len(downloadBatchResp.Files))
		return 1
	}
	downloadResp := downloadBatchResp.Files[0]
	if err := applyDownloadedTrailerMetadata(outputPath, downloadResp.Meta.TrailerMetadata); err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	markMetadataDone(fileID)
	printFileMetrics(stdout, manifest.TransferID, fileID, outputPath, downloadResp.Meta, downloadResp.LocalFileHash, elapsed)
	return 0
}

func runStartCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	outputMu := &sync.Mutex{}
	stdout = &synchronizedWriter{mu: outputMu, w: stdout}
	stderr = &synchronizedWriter{mu: outputMu, w: stderr}

	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var txferID string
	var manifestPath string
	var outRoot string
	var encryptMode string
	var concurrency int
	var ackEveryRaw string
	var batchSizeRaw string
	var noSync bool
	var verbose bool
	fs.StringVar(&txferID, "tid", "", "transfer id")
	fs.StringVar(&manifestPath, "manifest", "", "path to manifest file (default: <tid>.fm2)")
	fs.StringVar(&outRoot, "out-root", ".", "output root")
	fs.StringVar(&encryptMode, "encrypt", "", "response encryption mode (supported: age)")
	fs.BoolVar(&verbose, "v", false, "verbose progress output")
	fs.BoolVar(&verbose, "verbose", false, "verbose progress output")
	fs.IntVar(&concurrency, "concurrency", 0, "parallel download workers (0=manifest default)")
	ackEveryRaw = encoding.HumanBytes(defaultCLIAckEveryBytes)
	fs.StringVar(&ackEveryRaw, "a", ackEveryRaw, "bytes between progress acks")
	fs.StringVar(&ackEveryRaw, "ack-every", ackEveryRaw, "bytes between progress acks")
	fs.StringVar(&batchSizeRaw, "b", ackEveryRaw, "parallel batch size, unit of work per concurrent request")
	fs.StringVar(&batchSizeRaw, "batch-size", ackEveryRaw, "parallel batch size, unit of work per concurrent request")
	fs.BoolVar(&noSync, "no-sync", false, "ack without fdatasync")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	concurrencyExplicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "concurrency" {
			concurrencyExplicit = true
		}
	})
	if concurrencyExplicit && concurrency <= 0 {
		fmt.Fprintln(stderr, "--concurrency must be > 0")
		return 2
	}
	ackEvery, err := encoding.ParseByteSize(ackEveryRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --ack-every: %v\n", err)
		return 2
	}
	if ackEvery <= 0 {
		fmt.Fprintln(stderr, "--ack-every must be > 0")
		return 2
	}
	batchSize, err := encoding.ParseByteSize(batchSizeRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --batch-size: %v\n", err)
		return 2
	}
	if batchSize <= 0 {
		fmt.Fprintln(stderr, "--batch-size must be > 0")
		return 2
	}
	agePublicKey, ageIdentity, err := resolveEncryptionOptions(encryptMode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
	manifest, resolvedManifestPath, resolvedTxferID, err := loadManifestForStart(txferID, manifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "load manifest failed: %v\n", err)
		return 1
	}
	loadStrategy, err := resolveLoadStrategy(manifest.Mode)
	if err != nil {
		fmt.Fprintf(stderr, "load manifest failed: invalid manifest mode %q\n", manifest.Mode)
		return 1
	}
	manifestConcurrency := manifest.Concurrency
	if manifestConcurrency <= 0 {
		fmt.Fprintf(stderr, "load manifest failed: invalid manifest concurrency %d\n", manifestConcurrency)
		return 1
	}
	effectiveConcurrency := manifestConcurrency
	if concurrencyExplicit {
		effectiveConcurrency = concurrency
	}
	txferID = resolvedTxferID
	progressPath := resolvedManifestPath + ".progress"
	progressState, err := loadProgressState(progressPath)
	if err != nil {
		fmt.Fprintf(stderr, "load progress failed: %v\n", err)
		return 1
	}
	applyProgressStateToManifest(manifest, progressState)
	progressUpdates := make(chan DownloadProgressUpdate, 1024)
	var onStartProgressUpdate func(DownloadProgressUpdate)
	if verbose {
		progressReporter := newVerboseProgressReporter(stderr)
		onStartProgressUpdate = progressReporter.ReportUpdate
	}
	forwardProgress := func(update DownloadProgressUpdate) {
		applyProgressUpdateToManifest(manifest, update)
		if onStartProgressUpdate != nil {
			onStartProgressUpdate(update)
		}
	}
	stopProgress, markMetadataDonePersisted := startProgressWriter(progressPath, progressState, progressUpdates, forwardProgress, stderr)
	markMetadataDone := func(fileID uint64) {
		markManifestEntryMetadataDone(manifest, fileID)
		markMetadataDonePersisted(fileID)
	}
	defer stopProgress()
	client := NewClient(serverURL, WithLoadStrategy(loadStrategy))
	fmt.Fprintf(
		stdout,
		"start-plan: strategy=%s link=%dMbps concurrency=%d (manifest=%d)\n",
		loadStrategy,
		manifest.LinkMbps,
		effectiveConcurrency,
		manifestConcurrency,
	)

	startAll := time.Now()
	var completed int64
	var totalTransferred int64
	var failures []error
	var failuresMu sync.Mutex
	recordFailure := func(err error) {
		if err == nil {
			return
		}
		failuresMu.Lock()
		failures = append(failures, err)
		failuresMu.Unlock()
	}
	var stopStatusPolling func()
	if verbose {
		stopStatusPolling = startVerboseStatusPolling(txferID, client, stderr)
		defer stopStatusPolling()
	}
	pendingEntries := make([]ManifestEntry, 0, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		progress := entry.Progress
		if progress.AckBytes >= entry.Size {
			if progress.MetadataDone {
				completed++
				continue
			}
			if err := refreshCompletedFileMetadata(context.Background(), client, manifest, entry.ID, outRoot, "", agePublicKey, ageIdentity); err != nil {
				recordFailure(fmt.Errorf("id=%d metadata refresh failed: %w", entry.ID, err))
				continue
			}
			markMetadataDone(entry.ID)
			completed++
			continue
		}
		pendingEntries = append(pendingEntries, entry)
	}
	startResp, err := client.StartFromManifest(context.Background(), StartFromManifestRequest{
		Manifest: manifest,
		Entries:  pendingEntries,
		OutputWriter: func(entry ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
			destPath := resolveDownloadDestinationPath(entry, outRoot, "")
			return openDownloadOutput(entry, offset, destPath, nil, noSync)
		},
		AgePublicKey:    agePublicKey,
		AgeIdentity:     ageIdentity,
		Concurrency:     effectiveConcurrency,
		BatchMaxBytes:   batchSize,
		ProgressUpdates: progressUpdates,
		OnFileDone: func(evt StartFileDoneEvent) {
			entry, ok := manifest.EntryByID(evt.File.Meta.FileID)
			if !ok {
				recordFailure(fmt.Errorf("id=%d metadata apply failed: file id not in manifest", evt.File.Meta.FileID))
				return
			}
			destPath := resolveDownloadDestinationPath(entry, outRoot, "")
			if err := applyDownloadedTrailerMetadata(destPath, evt.File.Meta.TrailerMetadata); err != nil {
				recordFailure(fmt.Errorf("id=%d metadata apply failed: %w", evt.File.Meta.FileID, err))
				return
			}
			markMetadataDone(evt.File.Meta.FileID)
			printStartFileSummary(stdout, evt.File.Meta.FileID, destPath, evt.File.Meta, evt.File.LocalFileHash, evt.File.WindowChecksumPassed, evt.File.WindowChecksumTotal, evt.Elapsed)
		},
	})
	if err != nil {
		fmt.Fprintf(stderr, "start failed: %v\n", err)
		return 1
	}
	completed += int64(startResp.Downloaded)
	totalTransferred += startResp.TransferredBytes
	for _, startErr := range startResp.Errors {
		recordFailure(startErr)
	}
	failuresMu.Lock()
	finalFailures := append([]error(nil), failures...)
	failuresMu.Unlock()
	for _, err := range finalFailures {
		fmt.Fprintf(stderr, "start error: %v\n", err)
	}

	elapsedAll := time.Since(startAll)
	overallSpeed := 0.0
	if elapsedAll > 0 {
		overallSpeed = float64(totalTransferred) / elapsedAll.Seconds()
	}
	fmt.Fprintf(
		stdout,
		"start complete: tid=%s requested=%d downloaded=%d failed=%d transferred=%s speed=%s elapsed=%s\n",
		txferID,
		len(manifest.Entries),
		completed,
		len(finalFailures),
		encoding.HumanBytes(totalTransferred),
		encoding.HumanRate(overallSpeed),
		elapsedAll.Round(time.Millisecond),
	)
	if len(finalFailures) > 0 {
		return 1
	}
	return 0
}

func printStartFileSummary(stdout io.Writer, fileID uint64, path string, meta FileFrameMeta, localFileHash string, windowChecksumPassed, windowChecksumTotal int, elapsed time.Duration) {
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 0.000001
	}
	speed := float64(meta.Size) / seconds
	compSummary := formatCompSummary(meta)
	var checksum string
	switch {
	case windowChecksumTotal > 0:
		checksum = fmt.Sprintf("wxsum=[%d/%d]", windowChecksumPassed, windowChecksumTotal)
	case meta.FileHashToken != "" && localFileHash != "" && strings.EqualFold(meta.FileHashToken, localFileHash):
		checksum = "checksum=[ok]"
	case meta.FileHashToken != "" && localFileHash != "":
		checksum = "checksum=[x]"
	default:
		checksum = "checksum=[-]"
	}
	fmt.Fprintf(
		stdout,
		"start-file: fd=%d path=%s %s comp=%s rate=%s\n",
		fileID,
		path,
		checksum,
		compSummary,
		encoding.HumanRate(speed),
	)
}

func startVerboseStatusPolling(txferID string, client *Client, stderr io.Writer) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(defaultVerboseStatusInterval)
		defer ticker.Stop()
		for {
			statusResp, statusErr := client.GetTransferStatus(ctx, GetTransferStatusRequest{
				TransferID: txferID,
			})
			if statusErr != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Fprintf(stderr, "status refresh failed: %v\n", statusErr)
			} else {
				fmt.Fprintf(
					stderr,
					"transfer-progress: files=%.2f%% bytes=%.2f%%\n",
					statusResp.Status.PercentFiles,
					statusResp.Status.PercentBytes,
				)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func parseFileID(raw string) (uint64, error) {
	return strconv.ParseUint(raw, 10, 64)
}

type noOpWriteCloser struct {
	io.Writer
}

func (n noOpWriteCloser) Close() error {
	return nil
}

func isDiscardDestination(destPath string) bool {
	if destPath == "-" {
		return true
	}
	return filepath.Clean(destPath) == filepath.Clean(os.DevNull)
}

func resolveDownloadDestinationPath(entry ManifestEntry, outRoot string, outFile string) string {
	outFile = strings.TrimSpace(outFile)
	if outFile != "" {
		return outFile
	}
	if outRoot == "" {
		outRoot = "."
	}
	if filepath.Clean(outRoot) == filepath.Clean(os.DevNull) {
		return os.DevNull
	}
	return filepath.Clean(filepath.Join(outRoot, filepath.FromSlash(entry.Path)))
}

func openDownloadOutput(entry ManifestEntry, offset int64, destPath string, stdout io.Writer, noSync bool) (io.WriteCloser, func() error, error) {
	if destPath == "-" {
		if offset > 0 {
			return nil, nil, errors.New("cannot resume when output is stdout")
		}
		if stdout == nil {
			stdout = os.Stdout
		}
		return noOpWriteCloser{Writer: stdout}, func() error { return nil }, nil
	}
	if filepath.Clean(destPath) == filepath.Clean(os.DevNull) {
		return noOpWriteCloser{Writer: io.Discard}, func() error { return nil }, nil
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create output parent directory: %w", err)
	}
	resumeBase := entry.Progress.AckBytes
	if resumeBase < 0 {
		resumeBase = 0
	}
	var (
		fd  *os.File
		err error
	)
	if resumeBase > 0 {
		fd, err = os.OpenFile(destPath, os.O_RDWR, 0)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, nil, fmt.Errorf("resume requested at offset %d but output file is missing", resumeBase)
			}
			return nil, nil, fmt.Errorf("open output file for resume: %w", err)
		}
		stat, statErr := fd.Stat()
		if statErr != nil {
			_ = fd.Close()
			return nil, nil, fmt.Errorf("stat output file for resume: %w", statErr)
		}
		if stat.Size() < resumeBase {
			_ = fd.Close()
			return nil, nil, fmt.Errorf("resume requested at offset %d but output file has only %d bytes", resumeBase, stat.Size())
		}
	} else if offset > 0 {
		fd, err = os.OpenFile(destPath, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("open output file for sparse write: %w", err)
		}
	} else {
		fd, err = os.Create(destPath)
		if err != nil {
			return nil, nil, fmt.Errorf("create output file: %w", err)
		}
	}
	if offset > 0 {
		if _, err := fd.Seek(offset, io.SeekStart); err != nil {
			_ = fd.Close()
			return nil, nil, fmt.Errorf("seek output file for resume: %w", err)
		}
	}
	syncOutput := func() error {
		if noSync {
			return nil
		}
		return syscall.Fdatasync(int(fd.Fd()))
	}
	return fd, syncOutput, nil
}

func applyDownloadedTrailerMetadata(destPath string, meta *FileTrailerMetadata) error {
	if meta == nil || isDiscardDestination(destPath) {
		return nil
	}
	if err := applyTrailerMetadataToPath(destPath, meta); err != nil {
		return fmt.Errorf("apply trailer metadata to %s: %w", destPath, err)
	}
	return nil
}

func applyProgressStateToManifest(manifest *Manifest, state map[uint64]ManifestProgress) {
	if manifest == nil || len(manifest.Entries) == 0 || len(state) == 0 {
		return
	}
	for i := range manifest.Entries {
		if progress, ok := state[manifest.Entries[i].ID]; ok {
			manifest.Entries[i].Progress = progress
		}
	}
}

func applyProgressUpdateToManifest(manifest *Manifest, update DownloadProgressUpdate) {
	if manifest == nil {
		return
	}
	for i := range manifest.Entries {
		if manifest.Entries[i].ID != update.FileID {
			continue
		}
		if update.AckBytes > manifest.Entries[i].Progress.AckBytes {
			manifest.Entries[i].Progress.AckBytes = update.AckBytes
		}
		return
	}
}

func markManifestEntryMetadataDone(manifest *Manifest, fileID uint64) {
	if manifest == nil {
		return
	}
	for i := range manifest.Entries {
		if manifest.Entries[i].ID == fileID {
			manifest.Entries[i].Progress.MetadataDone = true
			return
		}
	}
}

func loadManifestForStart(txferID string, manifestPath string) (*Manifest, string, string, error) {
	return loadManifestWithOptionalTID("start", txferID, manifestPath)
}

func loadManifestForGet(txferID string, manifestPath string) (*Manifest, string, string, error) {
	return loadManifestWithOptionalTID("get", txferID, manifestPath)
}

func loadManifestWithOptionalTID(cmd string, txferID string, manifestPath string) (*Manifest, string, string, error) {
	if manifestPath == "" {
		if txferID == "" {
			return nil, "", "", fmt.Errorf("%s requires --manifest when --tid is not provided", cmd)
		}
		manifestPath = txferID + ".fm2"
	}
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		return nil, "", "", err
	}
	if txferID == "" {
		txferID = manifest.TransferID
	}
	if manifest.TransferID != txferID {
		return nil, "", "", fmt.Errorf("manifest transfer id mismatch: expected %s got %s", txferID, manifest.TransferID)
	}
	return manifest, manifestPath, txferID, nil
}

func loadProgressState(progressPath string) (map[uint64]ManifestProgress, error) {
	state := make(map[uint64]ManifestProgress)
	fd, err := os.Open(progressPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return state, nil
		}
		return nil, err
	}
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 2 && len(parts) != 3 {
			return nil, fmt.Errorf("invalid progress line: %q", line)
		}
		fileID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid progress file id %q: %w", parts[0], err)
		}
		ack, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid progress ack %q: %w", parts[1], err)
		}
		metadataDone := false
		if len(parts) == 3 {
			switch parts[2] {
			case "0":
				metadataDone = false
			case "1":
				metadataDone = true
			default:
				return nil, fmt.Errorf("invalid progress metadata flag %q", parts[2])
			}
		}
		prev, ok := state[fileID]
		if !ok || ack > prev.AckBytes || (ack == prev.AckBytes && metadataDone && !prev.MetadataDone) {
			state[fileID] = ManifestProgress{
				AckBytes:     ack,
				MetadataDone: metadataDone,
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return state, nil
}

type metadataProgressUpdate struct {
	FileID uint64
}

func startProgressWriter(progressPath string, initial map[uint64]ManifestProgress, updates <-chan DownloadProgressUpdate, onUpdate func(DownloadProgressUpdate), stderr io.Writer) (func(), func(uint64)) {
	state := initial
	if state == nil {
		state = make(map[uint64]ManifestProgress)
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	metadataDoneCh := make(chan metadataProgressUpdate, 1024)

	writeSnapshot := func() error {
		dir := filepath.Dir(progressPath)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
		}
		tmpPath := progressPath + ".tmp"
		fd, err := os.Create(tmpPath)
		if err != nil {
			return err
		}
		ids := make([]uint64, 0, len(state))
		for fileID := range state {
			ids = append(ids, fileID)
		}
		slices.Sort(ids)
		for _, fileID := range ids {
			entry := state[fileID]
			metaDone := 0
			if entry.MetadataDone {
				metaDone = 1
			}
			if _, err := fmt.Fprintf(fd, "%d %d %d\n", fileID, entry.AckBytes, metaDone); err != nil {
				_ = fd.Close()
				return err
			}
		}
		if err := fd.Close(); err != nil {
			return err
		}
		return os.Rename(tmpPath, progressPath)
	}

	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		dirty := false
		applyProgress := func(update DownloadProgressUpdate) {
			if onUpdate != nil {
				onUpdate(update)
			}
			prev := state[update.FileID]
			if update.AckBytes > prev.AckBytes {
				prev.AckBytes = update.AckBytes
				state[update.FileID] = prev
				dirty = true
			}
		}
		applyMetadataDone := func(update metadataProgressUpdate) {
			prev := state[update.FileID]
			if !prev.MetadataDone {
				prev.MetadataDone = true
				state[update.FileID] = prev
				dirty = true
			}
		}
		drainPending := func() {
			for {
				select {
				case update, ok := <-updates:
					if !ok {
						updates = nil
						continue
					}
					applyProgress(update)
				case update := <-metadataDoneCh:
					applyMetadataDone(update)
				default:
					return
				}
			}
		}
		for {
			select {
			case <-stopCh:
				drainPending()
				if dirty {
					if err := writeSnapshot(); err != nil {
						fmt.Fprintf(stderr, "progress flush failed: %v\n", err)
					}
				}
				return
			case update, ok := <-updates:
				if !ok {
					if dirty {
						if err := writeSnapshot(); err != nil {
							fmt.Fprintf(stderr, "progress flush failed: %v\n", err)
						}
					}
					return
				}
				applyProgress(update)
			case update := <-metadataDoneCh:
				applyMetadataDone(update)
			case <-ticker.C:
				if dirty {
					if err := writeSnapshot(); err != nil {
						fmt.Fprintf(stderr, "progress flush failed: %v\n", err)
					} else {
						dirty = false
					}
				}
			}
		}
	}()

	stop := func() {
		close(stopCh)
		<-doneCh
	}
	markMetadataDone := func(fileID uint64) {
		select {
		case <-doneCh:
			return
		case metadataDoneCh <- metadataProgressUpdate{FileID: fileID}:
		default:
			// Do not block download workers on progress persistence.
		}
	}
	return stop, markMetadataDone
}

func refreshCompletedFileMetadata(ctx context.Context, client *Client, manifest *Manifest, fileID uint64, outRoot string, outFile string, agePublicKey string, ageIdentity string) error {
	if manifest == nil {
		return errors.New("nil manifest")
	}
	entry, ok := manifest.EntryByID(fileID)
	if !ok {
		return fmt.Errorf("file id %d not in manifest", fileID)
	}
	destPath := outFile
	if destPath == "" {
		if outRoot == "" {
			outRoot = "."
		}
		destPath = filepath.Clean(filepath.Join(outRoot, filepath.FromSlash(entry.Path)))
	}
	if destPath == "-" {
		return nil
	}
	if isDiscardDestination(destPath) {
		return nil
	}
	serverPath := filepath.Clean(filepath.Join(manifest.Root, filepath.FromSlash(entry.Path)))
	if !filepath.IsAbs(serverPath) {
		return fmt.Errorf("resolved file path is not absolute: %s", serverPath)
	}
	meta, err := fetchTerminalTrailerMetadataFromChecksum(ctx, client, manifest.TransferID, fileID, serverPath, entry.Size, agePublicKey, ageIdentity)
	if err != nil {
		return err
	}
	if meta == nil {
		return errors.New("checksum response missing terminal trailer metadata")
	}
	return applyTrailerMetadataToPath(destPath, meta)
}

func fetchTerminalTrailerMetadataFromChecksum(ctx context.Context, client *Client, transferID string, fileID uint64, serverPath string, fileSize int64, agePublicKey string, ageIdentity string) (*FileTrailerMetadata, error) {
	resp, err := client.FetchChecksumStream(ctx, FetchChecksumStreamRequest{
		TransferID:   transferID,
		FileID:       fileID,
		FullPath:     serverPath,
		WindowSize:   fileSize,
		ChecksumsCSV: "xxh128",
		AgePublicKey: agePublicKey,
		AgeIdentity:  ageIdentity,
	})
	if err != nil {
		return nil, fmt.Errorf("checksum request failed: %w", err)
	}
	defer resp.Reader.Close()
	br := bufio.NewReader(resp.Reader)
	var terminal *FileTrailerMetadata
	for {
		headerLine, readErr := br.ReadString('\n')
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && strings.TrimSpace(headerLine) == "" {
				break
			}
			return nil, fmt.Errorf("read checksum frame header: %w", readErr)
		}
		wsize, err := parseFrameWireSize(strings.TrimRight(headerLine, "\r\n"))
		if err != nil {
			return nil, err
		}
		if wsize > 0 {
			if _, err := io.CopyN(io.Discard, br, wsize); err != nil {
				return nil, fmt.Errorf("discard checksum frame payload: %w", err)
			}
		}
		trailerLine, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read checksum frame trailer: %w", err)
		}
		meta, isTerminal, err := parseTrailerMetadata(strings.TrimRight(trailerLine, "\r\n"))
		if err != nil {
			return nil, err
		}
		if isTerminal && meta != nil {
			terminal = meta
		}
	}
	return terminal, nil
}

func parseFrameWireSize(line string) (int64, error) {
	fields := strings.Fields(line)
	if len(fields) < 3 || fields[0] != "FX/1" {
		return 0, errors.New("invalid checksum frame header")
	}
	for _, token := range fields[2:] {
		if strings.HasPrefix(token, "wsize=") {
			v, err := strconv.ParseInt(strings.TrimPrefix(token, "wsize="), 10, 64)
			if err != nil || v < 0 {
				return 0, errors.New("invalid checksum frame wsize")
			}
			return v, nil
		}
	}
	return 0, errors.New("checksum frame missing wsize")
}

func parseTrailerMetadata(line string) (*FileTrailerMetadata, bool, error) {
	fields := strings.Fields(line)
	if len(fields) < 3 || fields[0] != "FXT/1" {
		return nil, false, errors.New("invalid checksum frame trailer")
	}
	isTerminal := false
	meta := &FileTrailerMetadata{}
	hasMeta := false
	for _, token := range fields[2:] {
		if strings.HasPrefix(token, "next=") {
			nextRaw := strings.TrimPrefix(token, "next=")
			next, err := strconv.ParseInt(nextRaw, 10, 64)
			if err != nil || next < 0 {
				return nil, false, errors.New("invalid checksum frame trailer next offset")
			}
			isTerminal = next == 0
			continue
		}
		if strings.HasPrefix(token, "meta:") {
			parts := strings.SplitN(token, "=", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimPrefix(parts[0], "meta:")
			val := parts[1]
			switch key {
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
			case "group":
				meta.Group = val
			case "size":
				meta.Size, _ = strconv.ParseInt(val, 10, 64)
			case "mtime_ns":
				meta.MtimeNS, _ = strconv.ParseInt(val, 10, 64)
			}
		}
	}
	if !hasMeta {
		meta = nil
	}
	return meta, isTerminal, nil
}

func applyTrailerMetadataToPath(path string, meta *FileTrailerMetadata) error {
	if meta == nil {
		return nil
	}
	fd, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open destination for metadata apply: %w", err)
	}
	defer fd.Close()

	modeRaw := strings.TrimSpace(meta.Mode)
	if modeRaw != "" {
		modeBits, err := strconv.ParseUint(modeRaw, 8, 32)
		if err != nil || modeBits > 0o7777 {
			return fmt.Errorf("invalid trailer mode %q", modeRaw)
		}
		if err := fd.Chmod(os.FileMode(modeBits)); err != nil {
			return fmt.Errorf("chmod destination to %s: %w", modeRaw, err)
		}
	}
	uidRaw := strings.TrimSpace(meta.UID)
	gidRaw := strings.TrimSpace(meta.GID)
	if uidRaw == "" && gidRaw == "" {
		return nil
	}
	if uidRaw == "" || gidRaw == "" {
		return errors.New("trailer uid/gid must both be set")
	}
	uid, err := strconv.Atoi(uidRaw)
	if err != nil {
		return fmt.Errorf("invalid trailer uid %q: %w", uidRaw, err)
	}
	gid, err := strconv.Atoi(gidRaw)
	if err != nil {
		return fmt.Errorf("invalid trailer gid %q: %w", gidRaw, err)
	}
	if err := fd.Chown(uid, gid); err != nil {
		return fmt.Errorf("chown destination uid=%d gid=%d: %w", uid, gid, err)
	}
	return nil
}

type verboseProgressReporter struct {
	mu     sync.Mutex
	stderr io.Writer
	now    func() time.Time
	state  map[uint64]*verboseProgressState
}

type verboseProgressState struct {
	targetBytes     int64
	copiedBytes     int64
	ackedBytes      int64
	nextPct         int64
	startedAt       time.Time
	lastEmitAt      time.Time
	lastEmitBytes   int64
	completeEmitted bool
}

func newVerboseProgressReporter(stderr io.Writer) *verboseProgressReporter {
	return &verboseProgressReporter{
		stderr: stderr,
		now:    time.Now,
		state:  make(map[uint64]*verboseProgressState),
	}
}

func (r *verboseProgressReporter) ReportUpdate(update DownloadProgressUpdate) {
	if r == nil || r.stderr == nil || update.TargetBytes <= 0 {
		return
	}
	now := update.UpdateTime
	if now.IsZero() {
		now = r.now()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	st := r.ensureStateLocked(update.FileID, update.TargetBytes, now)
	if st.targetBytes <= 0 {
		return
	}
	copied := clampInt64(update.CopiedBytes, 0, st.targetBytes)
	if copied > st.copiedBytes {
		st.copiedBytes = copied
	}
	acked := clampInt64(update.AckBytes, 0, st.targetBytes)
	if acked > st.ackedBytes {
		st.ackedBytes = acked
	}

	if st.completeEmitted {
		return
	}

	shouldEmit := false
	progressPct := (st.copiedBytes * 100) / st.targetBytes
	for st.nextPct <= 100 && progressPct >= st.nextPct {
		shouldEmit = true
		st.nextPct += 20
	}

	lastActivity := st.lastEmitAt
	if lastActivity.IsZero() {
		lastActivity = st.startedAt
	}
	if !shouldEmit && now.Sub(lastActivity) >= defaultVerboseProgressInterval && st.copiedBytes > st.lastEmitBytes {
		shouldEmit = true
	}
	if st.copiedBytes >= st.targetBytes {
		shouldEmit = true
	}
	if shouldEmit {
		r.emitLocked(update.FileID, st, now)
	}
}

func (r *verboseProgressReporter) ensureStateLocked(fileID uint64, targetBytes int64, now time.Time) *verboseProgressState {
	st := r.state[fileID]
	if st == nil {
		st = &verboseProgressState{
			targetBytes: targetBytes,
			nextPct:     20,
			startedAt:   now,
		}
		r.state[fileID] = st
	}
	if st.startedAt.IsZero() {
		st.startedAt = now
	}
	if targetBytes > 0 {
		st.targetBytes = targetBytes
	}
	if st.nextPct <= 0 {
		st.nextPct = 20
	}
	return st
}

func (r *verboseProgressReporter) emitLocked(fileID uint64, st *verboseProgressState, now time.Time) {
	if st == nil || st.targetBytes <= 0 {
		return
	}

	copied := clampInt64(st.copiedBytes, 0, st.targetBytes)
	acked := clampInt64(st.ackedBytes, 0, st.targetBytes)
	pct := (copied * 100) / st.targetBytes
	if pct > 100 {
		pct = 100
	}

	rateBps := 0.0
	if !st.lastEmitAt.IsZero() && now.After(st.lastEmitAt) && copied > st.lastEmitBytes {
		rateBps = float64(copied-st.lastEmitBytes) / now.Sub(st.lastEmitAt).Seconds()
	}
	if rateBps <= 0 && !st.startedAt.IsZero() && now.After(st.startedAt) && copied > 0 {
		rateBps = float64(copied) / now.Sub(st.startedAt).Seconds()
	}

	eta := "n/a"
	if rateBps > 0 && copied < st.targetBytes {
		remaining := st.targetBytes - copied
		eta = humanETA(time.Duration(float64(remaining) / rateBps * float64(time.Second)))
	}

	fmt.Fprintf(
		r.stderr,
		"progress: fd=%d %d%% bytes=%s/%s [%s] rate=%s eta=%s\n",
		fileID,
		pct,
		encoding.HumanBytes(copied),
		encoding.HumanBytes(st.targetBytes),
		encoding.HumanBytes(acked),
		encoding.HumanRate(rateBps),
		eta,
	)

	st.lastEmitAt = now
	st.lastEmitBytes = copied
	if copied >= st.targetBytes {
		st.completeEmitted = true
	}
}

func clampInt64(value int64, min int64, max int64) int64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func humanETA(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	return d.Round(time.Second).String()
}

func printFileMetrics(stdout io.Writer, txferID string, fileID uint64, path string, meta FileFrameMeta, localFileHash string, elapsed time.Duration) {
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 0.000001
	}
	speed := float64(meta.Size) / seconds
	var ratio float64
	if meta.WireSize > 0 {
		ratio = float64(meta.Size) / float64(meta.WireSize)
	}
	serverFrameMS := meta.TrailerTS - meta.HeaderTS
	serverLogicalBps := 0.0
	serverWireBps := 0.0
	if serverFrameMS > 0 {
		serverSeconds := float64(serverFrameMS) / 1000.0
		serverLogicalBps = float64(meta.Size) / serverSeconds
		serverWireBps = float64(meta.WireSize) / serverSeconds
	}
	serverFileHash := meta.FileHashToken
	if serverFileHash == "" {
		serverFileHash = "n/a"
	}
	if localFileHash == "" {
		localFileHash = "n/a"
	}
	serverFileHashDisplay := encoding.AbbrevHashToken(serverFileHash)
	localFileHashDisplay := encoding.AbbrevHashToken(localFileHash)
	compSummary := formatCompSummary(meta)
	fmt.Fprintf(
		stdout,
		"file: tid=%s fd=%d\n  path: %s\n  transfer: comp=%s logical=%d wire=%d speed=%s ratio=%.3f\n  checksum: server=%s client=%s\n  timing: elapsed=%s ts0=%d ts1=%d server_frame_ms=%d server_logical=%s server_wire=%s\n\n",
		txferID,
		fileID,
		path,
		compSummary,
		meta.Size,
		meta.WireSize,
		encoding.HumanRate(speed),
		ratio,
		serverFileHashDisplay,
		localFileHashDisplay,
		elapsed.Round(time.Millisecond),
		meta.HeaderTS,
		meta.TrailerTS,
		serverFrameMS,
		encoding.HumanRate(serverLogicalBps),
		encoding.HumanRate(serverWireBps),
	)
}

func formatCompSummary(meta FileFrameMeta) string {
	if len(meta.CompCounts) == 0 {
		return meta.Comp
	}
	parts := make([]string, 0, len(meta.CompCounts))
	preferred := []string{"none", "lz4", "zstd"}
	used := make(map[string]bool, len(preferred))
	for _, key := range preferred {
		if count, ok := meta.CompCounts[key]; ok && count > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", key, count))
			used[key] = true
		}
	}
	other := make([]string, 0, len(meta.CompCounts))
	for key, count := range meta.CompCounts {
		if count <= 0 || used[key] {
			continue
		}
		other = append(other, fmt.Sprintf("%s=%d", key, count))
	}
	sort.Strings(other)
	parts = append(parts, other...)
	return "[" + strings.Join(parts, ", ") + "]"
}
