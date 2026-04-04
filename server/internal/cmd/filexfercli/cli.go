package filexfercli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
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

	"runtime/trace"

	"sync/atomic"

	"filippo.io/age"
	. "github.com/jolynch/pinch/filexfer"
	"github.com/jolynch/pinch/internal/filexfer"
	"github.com/jolynch/pinch/internal/filexfer/encoding"
	"github.com/jolynch/pinch/utils"
	"github.com/zeebo/xxh3"
)

func startTracing(path string, stderr io.Writer) (stop func()) {
	if path == "" {
		return func() {}
	}
	tf, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(stderr, "trace: failed to create file %s: %v\n", path, err)
		return func() {}
	}
	if err := trace.Start(tf); err != nil {
		fmt.Fprintf(stderr, "trace: failed to start: %v\n", err)
		_ = tf.Close()
		return func() {}
	}
	return func() {
		trace.Stop()
		_ = tf.Close()
	}
}

const defaultVerboseStatusInterval = 10 * time.Second
const defaultCLIAckEveryBytes int64 = 128 * 1024 * 1024
const defaultVerboseProgressInterval = 2 * time.Second
const defaultCLIProbeBytes int64 = 1 * 1024 * 1024
const defaultVerifySampleFrameSize int64 = 8 * 1024 * 1024
const verifySampleBytes int64 = 8

var syncPromptInput io.Reader = os.Stdin

var syncPromptIsTerminal = func() bool {
	stat, err := os.Stdin.Stat()
	return err == nil && stat != nil && (stat.Mode()&os.ModeCharDevice) != 0
}

type synchronizedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (sw *synchronizedWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}

type countingWriter struct {
	io.Writer
	total *atomic.Int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.Writer.Write(p)
	if n > 0 {
		cw.total.Add(int64(n))
	}
	return n, err
}

func (cw *countingWriter) Close() error {
	if c, ok := cw.Writer.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

const defaultFileListener = "127.0.0.1:3453"
const maxSyncRounds = 3

// pinchState computes all state file paths from a target output directory.
// Given targetDir="/var/lib/pinch/dst", state lives in the parent:
//
//	/var/lib/pinch/.pinch/manifest         ← client state: what's on disk (written by start/sync)
//	/var/lib/pinch/.pinch/manifest.server  ← server state: written by transfer, read by start/get
//	/var/lib/pinch/.pinch/manifest.progress
//	/var/lib/pinch/.pinch/remote/          (staging for start)
type pinchState struct {
	TargetDir          string // the user-facing output directory
	StateDir           string // parent/.pinch
	ManifestPath       string // StateDir/manifest        (client state: what's on disk)
	ServerManifestPath string // StateDir/manifest.server (server state: from transfer)
	ProgressPath       string // StateDir/manifest.progress
	StagingDir         string // StateDir/remote
}

func newPinchState(targetDir string) (*pinchState, error) {
	targetDir = filepath.Clean(targetDir)
	parent := filepath.Dir(targetDir)
	if parent == targetDir {
		return nil, fmt.Errorf("target directory %q has no distinct parent", targetDir)
	}
	stateDir := filepath.Join(parent, ".pinch")
	return &pinchState{
		TargetDir:          targetDir,
		StateDir:           stateDir,
		ManifestPath:       filepath.Join(stateDir, "manifest"),
		ServerManifestPath: filepath.Join(stateDir, "manifest.server"),
		ProgressPath:       filepath.Join(stateDir, "manifest.progress"),
		StagingDir:         filepath.Join(stateDir, "remote"),
	}, nil
}

func (ps *pinchState) ensureStateDir() error   { return os.MkdirAll(ps.StateDir, 0o755) }
func (ps *pinchState) ensureStagingDir() error { return os.MkdirAll(ps.StagingDir, 0o755) }

// scanLocalDir walks targetDir and returns a Manifest representing the files
// currently on disk, using meta for header fields (Root, Mode, etc.).
// If targetDir does not exist the returned manifest has no entries.
func scanLocalDir(targetDir string, meta *Manifest) (*Manifest, error) {
	out := &Manifest{
		TransferID:  meta.TransferID,
		Root:        meta.Root,
		Mode:        meta.Mode,
		LinkMbps:    meta.LinkMbps,
		Concurrency: meta.Concurrency,
	}
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		return out, nil
	}
	var id uint64
	err := filepath.WalkDir(targetDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(targetDir, path)
		if err != nil {
			return err
		}
		out.Entries = append(out.Entries, ManifestEntry{
			ID:    id,
			Size:  info.Size(),
			Mtime: info.ModTime().UnixNano(),
			Mode:  info.Mode(),
			Path:  filepath.ToSlash(rel),
		})
		id++
		return nil
	})
	return out, err
}

func isKnownCommand(s string) bool {
	switch s {
	case "copy", "transfer", "start", "status", "get", "sync":
		return true
	}
	return false
}

func confirmSyncProceed(stderr io.Writer, newCount int, staleCount int, rmCount int) bool {
	if !syncPromptIsTerminal() {
		return true
	}

	defaultYes := rmCount == 0 && (newCount > 0 || staleCount > 0)
	prompt := "[y/N]"
	if defaultYes {
		prompt = "[Y/n]"
	}
	fmt.Fprintf(stderr, "proceed? %s: ", prompt)

	scanner := bufio.NewScanner(syncPromptInput)
	if !scanner.Scan() {
		fmt.Fprintln(stderr, "aborted")
		return false
	}
	answer := strings.ToLower(strings.TrimSpace(scanner.Text()))
	if answer == "" {
		if defaultYes {
			return true
		}
		fmt.Fprintln(stderr, "aborted")
		return false
	}
	if strings.HasPrefix(answer, "y") {
		return true
	}
	fmt.Fprintln(stderr, "aborted")
	return false
}

func RunCLI(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) < 1 {
		printCLIUsage(stderr)
		return 2
	}

	// Handle top-level help.
	if args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		printCLIUsage(stderr)
		return 0
	}

	// If first arg is a known command, default the server address.
	serverURL := args[0]
	cmdStart := 1
	if isKnownCommand(args[0]) {
		serverURL = defaultFileListener
		cmdStart = 0
	} else {
		if err := validateServerURL(serverURL); err != nil {
			fmt.Fprintf(stderr, "invalid server-url: %v\n", err)
			printCLIUsage(stderr)
			return 2
		}
	}

	if cmdStart >= len(args) {
		printCLIUsage(stderr)
		return 2
	}

	cmd := args[cmdStart]
	cmdArgs := args[cmdStart+1:]

	switch cmd {
	case "copy":
		return runCopyCLI(serverURL, cmdArgs, stdout, stderr)
	case "status":
		return runStatusCLI(serverURL, cmdArgs, stdout, stderr)
	case "get":
		return runGetCLI(serverURL, cmdArgs, stdout, stderr)
	case "--help", "-h", "help":
		printCLIUsage(stderr)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", cmd)
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
	fmt.Fprintf(w, `usage: pinch filecli [<addr>] <command> [options]

Commands:
  copy       Copy REMOTE_SRC to LOCAL_DST
  status     Query and monitor transfer progress
  get        Download a single remote file

State is stored in <local-dst>/../.pinch/ (manifest, progress, staging).
Default server address: %s
Run 'pinch filecli <command> --help' for command-specific options.
`, defaultFileListener)
}

func resolveEncryptionOptions(mode string) (pubKey string, identity string, encMode string, err error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case "", "none":
		return "", "", "", nil
	case "auto", "aes", "chacha20":
		id, genErr := age.GenerateX25519Identity()
		if genErr != nil {
			return "", "", "", fmt.Errorf("generate age identity: %w", genErr)
		}
		return id.Recipient().String(), id.String(), mode, nil
	default:
		return "", "", "", fmt.Errorf("unsupported --encrypt value %q (supported: none, auto, aes, chacha20)", mode)
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

func resolveCompress(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return "", nil
	case "adapt", "none", "lz4", "zstd":
		return strings.ToLower(strings.TrimSpace(raw)), nil
	default:
		return "", fmt.Errorf("unsupported --compress value %q (supported: adapt, none, lz4, zstd)", raw)
	}
}

type copyCLIConfig struct {
	remoteSrc               string
	localDst                string
	encryptMode             string
	compressRaw             string
	modeRaw                 string
	concurrency             int
	ackEveryRaw             string
	probeSizeRaw            string
	deadlineRaw             string
	traceFile               string
	progressFilePath        string
	progressFileIntervalRaw string
	clean                   bool
	skipFetch               bool
	skipWrite               bool
	skipFsync               bool
	verifyMeta              bool
	verifyDataSamplePct     int
	verbose                 bool
	progress                bool
	yes                     bool
}

type transferArgs struct {
	sourceDir    string
	targetDir    string
	agePublicKey string
	ageIdentity  string
	encMode      string
	loadStrategy string
	probeBytes   int64
	verbose      bool
	maxChunk     int
	deadlineMS   int64
}

type syncArgs struct {
	sourceDir           string
	targetDir           string
	agePublicKey        string
	ageIdentity         string
	encMode             string
	concurrency         int
	concurrencyExplicit bool
	ackEvery            int64
	compress            string
	noSync              bool
	skipWrite           bool
	verbose             bool
	yes                 bool
	probeBytes          int64
	traceFile           string
	progressFilePath    string
	progressInterval    time.Duration
}

type startArgs struct {
	targetDir           string
	agePublicKey        string
	ageIdentity         string
	encMode             string
	verbosity           int
	concurrency         int
	concurrencyExplicit bool
	ackEvery            int64
	compress            string
	noSync              bool
	discard             bool
	deadlineMS          int64
	traceFile           string
	progressFilePath    string
	progressInterval    time.Duration
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func cleanupCopyState(targetDir string, stderr io.Writer) int {
	ps, err := newPinchState(targetDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
		return 1
	}
	if err := os.RemoveAll(ps.StateDir); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(stderr, "remove state directory failed: %v\n", err)
		return 1
	}
	return 0
}

func runCopyCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := newCLIFlags("copy")
	cf.SetOutput(stderr)
	cf.fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: pinch filecli [addr] copy [flags] REMOTE_SRC LOCAL_DST")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Copy REMOTE_SRC from the remote to LOCAL_DST on the local machine.")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Behavior:")
		fmt.Fprintln(stderr, "  - If LOCAL_DST does not exist: full transfer")
		fmt.Fprintln(stderr, "  - If LOCAL_DST exists: diff remote and send deltas")
		fmt.Fprintln(stderr, "  - --clean removes LOCAL_DST first and forces a clean transfer")
		fmt.Fprintln(stderr, "  - --skip-fetch fetches and writes manifest state only; no start/sync")
		fmt.Fprintln(stderr, "  - --skip-write fetches bodies to a discard sink and never mutates LOCAL_DST")
		fmt.Fprintln(stderr, "  - --verify-meta reruns read-only metadata verification after copy")
		fmt.Fprintln(stderr, "  - --verify-data-sample=N implies --verify-meta and verifies N percent of data")
		fmt.Fprintln(stderr)
		cf.PrintDefaults(stderr)
	}
	cfg := copyCLIConfig{
		ackEveryRaw:             encoding.HumanBytes(defaultCLIAckEveryBytes),
		probeSizeRaw:            encoding.HumanBytes(defaultCLIProbeBytes),
		progressFileIntervalRaw: "1s",
		progress:                true,
	}
	cf.BoolVar(&cfg.clean, "", "clean", false, "Remove LOCAL_DST first, then force a clean transfer")
	cf.BoolVar(&cfg.skipFetch, "", "skip-fetch", false, "Fetch and persist remote manifest state only; do not start or sync files")
	cf.BoolVar(&cfg.skipWrite, "", "skip-write", false, "Do not mutate LOCAL_DST; fetch file bodies to discard instead of writing them")
	cf.BoolVar(&cfg.skipFsync, "", "skip-fsync", false, "Acknowledge writes without fdatasync")
	cf.BoolVar(&cfg.verifyMeta, "", "verify-meta", false, "Run read-only metadata verification after copy; with --skip-fetch this is allowed only if LOCAL_DST already exists")
	cf.IntVar(&cfg.verifyDataSamplePct, "", "verify-data-sample", 0, "Percent of frame slots to sample per file for data verification (0-100); implies --verify-meta; not allowed with --skip-fetch or --skip-write")
	cf.StringVar(&cfg.modeRaw, "", "mode", LoadStrategyFast, "Server read strategy: fast|gentle")
	cf.StringVar(&cfg.encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.StringVar(&cfg.compressRaw, "", "compress", "", "Compression algorithm: adapt|none|lz4|zstd (default: adapt)")
	cf.IntVar(&cfg.concurrency, "", "concurrency", 0, "Parallel download / verification workers (0=adapt from server)")
	cf.BoolVar(&cfg.progress, "", "progress", true, "Show transfer progress every 2s")
	cf.BoolVar(&cfg.verbose, "v", "verbose", false, "Per-file progress output")
	cf.StringVar(&cfg.progressFilePath, "", "progress-file", "", "Write integer % to this file/pipe")
	cf.StringVar(&cfg.progressFileIntervalRaw, "", "progress-file-interval", cfg.progressFileIntervalRaw, "Progress write interval (e.g. 500ms, 10s)")
	cf.BoolVar(&cfg.yes, "y", "yes", false, "Skip confirmation prompt on sync paths")
	cf.StringVar(&cfg.ackEveryRaw, "a", "ack-every", cfg.ackEveryRaw, "Bytes between progress acks; e.g. 1B, 4KiB, 8MiB")
	cf.StringVar(&cfg.probeSizeRaw, "", "probe-size", cfg.probeSizeRaw, "Probe payload size; e.g. 1B, 4KiB, 8MiB")
	cf.StringVar(&cfg.deadlineRaw, "", "deadline", "", "Transfer deadline (e.g. 60s, 5m)")
	cf.StringVar(&cfg.traceFile, "", "trace", "", "Write runtime/trace output to this file")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if cf.NArg() != 2 {
		fmt.Fprintln(stderr, "copy requires exactly two positional arguments: REMOTE_SRC LOCAL_DST")
		return 2
	}
	cfg.remoteSrc = cf.Arg(0)
	cfg.localDst = cf.Arg(1)
	if !filepath.IsAbs(cfg.remoteSrc) {
		fmt.Fprintln(stderr, "copy requires REMOTE_SRC to be an absolute server path")
		return 2
	}
	if cfg.verifyDataSamplePct < 0 || cfg.verifyDataSamplePct > 100 {
		fmt.Fprintln(stderr, "--verify-data-sample must be between 0 and 100")
		return 2
	}
	if cfg.verifyDataSamplePct > 0 {
		cfg.verifyMeta = true
	}
	if cfg.verifyDataSamplePct > 0 && (cfg.skipFetch || cfg.skipWrite) {
		fmt.Fprintln(stderr, "--verify-data-sample cannot be used with --skip-fetch or --skip-write")
		return 2
	}
	if cfg.verifyMeta && cfg.skipWrite {
		fmt.Fprintln(stderr, "--verify-meta cannot be used with --skip-write")
		return 2
	}
	agePublicKey, ageIdentity, encMode, err := resolveEncryptionOptions(cfg.encryptMode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
	compress, err := resolveCompress(cfg.compressRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --compress: %v\n", err)
		return 2
	}
	loadStrategy, err := resolveLoadStrategy(cfg.modeRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --mode: %v\n", err)
		return 2
	}
	probeBytes, err := encoding.ParseByteSize(cfg.probeSizeRaw)
	if err != nil || probeBytes <= 0 {
		fmt.Fprintf(stderr, "invalid --probe-size: %v\n", err)
		return 2
	}
	ackEvery, err := encoding.ParseByteSize(cfg.ackEveryRaw)
	if err != nil || ackEvery <= 0 {
		fmt.Fprintf(stderr, "invalid --ack-every: %v\n", err)
		return 2
	}
	progressInterval, err := time.ParseDuration(cfg.progressFileIntervalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --progress-file-interval: %v\n", err)
		return 2
	}
	var deadlineMS int64
	if cfg.deadlineRaw != "" {
		d, err := time.ParseDuration(cfg.deadlineRaw)
		if err != nil {
			fmt.Fprintf(stderr, "invalid --deadline: %v\n", err)
			return 2
		}
		if d <= 0 {
			fmt.Fprintln(stderr, "--deadline must be > 0")
			return 2
		}
		deadlineMS = d.Milliseconds()
	}

	localExists := pathExists(cfg.localDst)
	if cfg.verifyMeta && cfg.skipFetch && !localExists {
		fmt.Fprintln(stderr, "--verify-meta with --skip-fetch requires an existing LOCAL_DST")
		return 2
	}
	if cfg.clean && !cfg.skipFetch {
		if err := os.RemoveAll(cfg.localDst); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(stderr, "remove local destination failed: %v\n", err)
			return 1
		}
		localExists = false
	}

	transferCfg := transferArgs{
		sourceDir:    cfg.remoteSrc,
		targetDir:    cfg.localDst,
		agePublicKey: agePublicKey,
		ageIdentity:  ageIdentity,
		encMode:      encMode,
		loadStrategy: loadStrategy,
		probeBytes:   probeBytes,
		verbose:      false,
		maxChunk:     0,
		deadlineMS:   deadlineMS,
	}
	if code := runTransfer(serverURL, transferCfg, stdout, stderr); code != 0 {
		return code
	}

	if cfg.skipFetch {
		if cfg.verifyMeta {
			return verifyCopy(serverURL, cfg, stdout, stderr)
		}
		return 0
	}

	if localExists {
		syncCfg := syncArgs{
			sourceDir:           cfg.remoteSrc,
			targetDir:           cfg.localDst,
			agePublicKey:        agePublicKey,
			ageIdentity:         ageIdentity,
			encMode:             encMode,
			concurrency:         cfg.concurrency,
			concurrencyExplicit: cfg.concurrency > 0,
			ackEvery:            ackEvery,
			compress:            compress,
			noSync:              cfg.skipFsync,
			skipWrite:           cfg.skipWrite,
			verbose:             cfg.verbose,
			yes:                 cfg.yes,
			probeBytes:          probeBytes,
			traceFile:           cfg.traceFile,
			progressFilePath:    cfg.progressFilePath,
			progressInterval:    progressInterval,
		}
		if code := runSync(serverURL, syncCfg, stdout, stderr); code != 0 {
			return code
		}
	} else {
		verbosity := 0
		if cfg.verbose {
			verbosity = 2
		} else if cfg.progress {
			verbosity = 1
		}
		startCfg := startArgs{
			targetDir:           cfg.localDst,
			agePublicKey:        agePublicKey,
			ageIdentity:         ageIdentity,
			encMode:             encMode,
			verbosity:           verbosity,
			concurrency:         cfg.concurrency,
			concurrencyExplicit: cfg.concurrency > 0,
			ackEvery:            ackEvery,
			compress:            compress,
			noSync:              cfg.skipFsync,
			discard:             cfg.skipWrite,
			deadlineMS:          deadlineMS,
			traceFile:           cfg.traceFile,
			progressFilePath:    cfg.progressFilePath,
			progressInterval:    progressInterval,
		}
		if code := runStart(serverURL, startCfg, stdout, stderr); code != 0 {
			return code
		}
	}

	if cfg.verifyMeta {
		if code := verifyCopy(serverURL, cfg, stdout, stderr); code != 0 {
			return code
		}
	}
	if code := cleanupCopyState(cfg.localDst, stderr); code != 0 {
		return code
	}
	return 0
}

type manifestDelta struct {
	newFiles       []ManifestEntry
	staleFiles     []ManifestEntry
	unchangedFiles []ManifestEntry
	removedPaths   []string
	newBytes       int64
	staleBytes     int64
}

func verifyCopy(serverURL string, cfg copyCLIConfig, stdout io.Writer, stderr io.Writer) int {
	ps, err := newPinchState(cfg.localDst)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
		return 2
	}
	serverManifest, err := LoadManifest(ps.ServerManifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "load server manifest failed: %v\n", err)
		return 1
	}
	localManifest, err := scanLocalDir(ps.TargetDir, serverManifest)
	if err != nil {
		fmt.Fprintf(stderr, "scan local directory failed: %v\n", err)
		return 1
	}
	delta := compareManifestEntries(localManifest, serverManifest)
	if len(delta.newFiles) > 0 || len(delta.staleFiles) > 0 || len(delta.removedPaths) > 0 {
		fmt.Fprintf(
			stderr,
			"copy-verify-meta: mismatch new=%d (%s) stale=%d (%s) rm=%d\n",
			len(delta.newFiles),
			encoding.HumanBytes(delta.newBytes),
			len(delta.staleFiles),
			encoding.HumanBytes(delta.staleBytes),
			len(delta.removedPaths),
		)
		return 1
	}
	fmt.Fprintf(stdout, "copy-verify-meta: ok files=%d\n", len(serverManifest.Entries))
	if cfg.verifyDataSamplePct <= 0 {
		return 0
	}
	sampledFiles, sampledRanges, err := verifyCopyDataSamples(serverURL, cfg, serverManifest, stdout)
	if err != nil {
		fmt.Fprintf(stderr, "copy-verify-data: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "copy-verify-data: ok files=%d samples=%d pct=%d\n", sampledFiles, sampledRanges, cfg.verifyDataSamplePct)
	return 0
}

func compareManifestEntries(localManifest *Manifest, serverManifest *Manifest) manifestDelta {
	delta := manifestDelta{}
	localByPath := make(map[string]ManifestEntry, len(localManifest.Entries))
	for _, entry := range localManifest.Entries {
		localByPath[entry.Path] = entry
	}
	serverByPath := make(map[string]ManifestEntry, len(serverManifest.Entries))
	for _, entry := range serverManifest.Entries {
		serverByPath[entry.Path] = entry
		local, ok := localByPath[entry.Path]
		if !ok {
			delta.newFiles = append(delta.newFiles, entry)
			delta.newBytes += entry.Size
			continue
		}
		if manifestEntryMatches(local, entry) {
			delta.unchangedFiles = append(delta.unchangedFiles, entry)
			continue
		}
		delta.staleFiles = append(delta.staleFiles, entry)
		delta.staleBytes += entry.Size
	}
	for _, entry := range localManifest.Entries {
		if _, ok := serverByPath[entry.Path]; !ok {
			delta.removedPaths = append(delta.removedPaths, entry.Path)
		}
	}
	sort.Strings(delta.removedPaths)
	return delta
}

func manifestEntryMatches(local ManifestEntry, remote ManifestEntry) bool {
	return local.Size == remote.Size && local.Mtime == remote.Mtime && local.Mode == remote.Mode
}

type verifySample struct {
	Offset int64
	Size   int64
}

type verifySampleTask struct {
	entry      ManifestEntry
	serverPath string
	localPath  string
	samples    []verifySample
}

func verifyCopyDataSamples(serverURL string, cfg copyCLIConfig, manifest *Manifest, stdout io.Writer) (int, int, error) {
	if manifest == nil {
		return 0, 0, errors.New("missing manifest")
	}
	agePublicKey, ageIdentity, encMode, err := resolveEncryptionOptions(cfg.encryptMode)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid --encrypt: %w", err)
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	tasks := make([]verifySampleTask, 0, len(manifest.Entries))
	totalRanges := 0
	for _, entry := range manifest.Entries {
		samples := buildVerifySamples(entry.Size, cfg.verifyDataSamplePct, rng)
		if len(samples) == 0 {
			continue
		}
		tasks = append(tasks, verifySampleTask{
			entry:      entry,
			serverPath: filepath.Clean(filepath.Join(manifest.Root, filepath.FromSlash(entry.Path))),
			localPath:  filepath.Join(cfg.localDst, filepath.FromSlash(entry.Path)),
			samples:    samples,
		})
		totalRanges += len(samples)
	}
	if len(tasks) == 0 {
		return 0, 0, nil
	}
	workers := cfg.concurrency
	if workers <= 0 {
		workers = manifest.Concurrency
	}
	if workers <= 0 {
		workers = 1
	}
	taskCh := make(chan verifySampleTask, workers)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var completed atomic.Int64
	var progressDone chan struct{}
	if stdout != nil {
		progressDone = make(chan struct{})
		go func() {
			defer close(progressDone)
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					done := completed.Load()
					fmt.Fprintf(stdout, "copy-verify-data: progress files=%d/%d samples=%d pct=%d\n", done, len(tasks), totalRanges, cfg.verifyDataSamplePct)
				}
			}
		}()
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := NewClient(serverURL, WithClientAgePublicKey(agePublicKey), WithClientAgeIdentity(ageIdentity), WithEncryptMode(encMode))
			for task := range taskCh {
				if err := verifySampleTaskData(ctx, client, manifest.TransferID, task); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
				completed.Add(1)
			}
		}()
	}
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			close(taskCh)
			wg.Wait()
			if progressDone != nil {
				<-progressDone
			}
			select {
			case err := <-errCh:
				return 0, 0, err
			default:
				return 0, 0, context.Canceled
			}
		case err := <-errCh:
			cancel()
			close(taskCh)
			wg.Wait()
			if progressDone != nil {
				<-progressDone
			}
			return 0, 0, err
		case taskCh <- task:
		}
	}
	close(taskCh)
	wg.Wait()
	cancel()
	if progressDone != nil {
		<-progressDone
	}
	select {
	case err := <-errCh:
		return 0, 0, err
	default:
	}
	return len(tasks), totalRanges, nil
}

func buildVerifySamples(fileSize int64, pct int, rng *rand.Rand) []verifySample {
	if fileSize <= 0 || pct <= 0 {
		return nil
	}
	frameSlots := int((fileSize + defaultVerifySampleFrameSize - 1) / defaultVerifySampleFrameSize)
	if frameSlots <= 0 {
		return nil
	}
	sampleCount := (frameSlots*pct + 99) / 100
	if sampleCount <= 0 {
		sampleCount = 1
	}
	slotIndexes := make([]int, 0, sampleCount)
	if sampleCount >= frameSlots {
		for i := 0; i < frameSlots; i++ {
			slotIndexes = append(slotIndexes, i)
		}
	} else {
		perm := rng.Perm(frameSlots)
		slotIndexes = append(slotIndexes, perm[:sampleCount]...)
		sort.Ints(slotIndexes)
	}
	samples := make([]verifySample, 0, len(slotIndexes))
	for _, slotIndex := range slotIndexes {
		slotStart := int64(slotIndex) * defaultVerifySampleFrameSize
		slotLen := minInt64(defaultVerifySampleFrameSize, fileSize-slotStart)
		size := minInt64(verifySampleBytes, slotLen)
		offset := slotStart
		maxJitter := slotLen - size
		if maxJitter > 0 {
			offset += rng.Int63n(maxJitter + 1)
		}
		samples = append(samples, verifySample{Offset: offset, Size: size})
	}
	return samples
}

func verifySampleTaskData(ctx context.Context, client *Client, transferID string, task verifySampleTask) error {
	fd, err := os.Open(task.localPath)
	if err != nil {
		return fmt.Errorf("open local sample path %s: %w", task.localPath, err)
	}
	defer fd.Close()

	targets := make([]ChecksumTarget, 0, len(task.samples))
	wantHashes := make([]string, 0, len(task.samples))
	buf := make([]byte, verifySampleBytes)
	for _, sample := range task.samples {
		targets = append(targets, ChecksumTarget{
			FileID:   task.entry.ID,
			FullPath: task.serverPath,
			Offset:   sample.Offset,
			Size:     sample.Size,
			Algo:     "xxh128",
		})
		want, err := computeLocalSampleHash(fd, sample.Offset, sample.Size, buf)
		if err != nil {
			return fmt.Errorf("hash local sample %s@%d: %w", task.localPath, sample.Offset, err)
		}
		wantHashes = append(wantHashes, want)
	}

	verifyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := client.GetChecksum(verifyCtx, GetChecksumRequest{
		TransferID: transferID,
		Targets:    targets,
	})
	if err != nil {
		return fmt.Errorf("checksum request failed for %s: %w", task.serverPath, err)
	}
	defer resp.Reader.Close()

	results, err := readChecksumResults(resp.Reader)
	if err != nil {
		return fmt.Errorf("read checksum response for %s: %w", task.serverPath, err)
	}
	if len(results) != len(task.samples) {
		return fmt.Errorf("checksum response count mismatch for %s: got %d want %d", task.serverPath, len(results), len(task.samples))
	}
	for i, result := range results {
		sample := task.samples[i]
		if result.FileID != task.entry.ID {
			return fmt.Errorf("checksum file id mismatch for %s: got %d want %d", task.serverPath, result.FileID, task.entry.ID)
		}
		if result.Offset != sample.Offset || result.Size != sample.Size {
			return fmt.Errorf("checksum range mismatch for %s: got offset=%d size=%d want offset=%d size=%d", task.serverPath, result.Offset, result.Size, sample.Offset, sample.Size)
		}
		if !strings.EqualFold(result.FileHashToken, wantHashes[i]) {
			return fmt.Errorf("checksum mismatch for %s at offset=%d size=%d", task.localPath, sample.Offset, sample.Size)
		}
	}
	return nil
}

func computeLocalSampleHash(fd *os.File, offset int64, size int64, scratch []byte) (string, error) {
	if size < 0 {
		return "", errors.New("negative sample size")
	}
	if size == 0 {
		return encoding.FormatXXH128HashToken(xxh3.Hash128(nil)), nil
	}
	buf := scratch
	if int64(len(buf)) < size {
		buf = make([]byte, size)
	}
	n, err := fd.ReadAt(buf[:size], offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	if int64(n) != size {
		return "", io.ErrUnexpectedEOF
	}
	return encoding.FormatXXH128HashToken(xxh3.Hash128(buf[:size])), nil
}

func minInt64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func runTransferCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := newCLIFlags("transfer")
	cf.SetOutput(stderr)
	cf.fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: pinch filecli transfer -s <dir> [flags] <target-dir>")
		cf.PrintDefaults(stderr)
	}
	var sourceDir string
	var encryptMode string
	var loadStrategyRaw string
	var probeSizeRaw string
	var verbose bool
	var maxChunk int
	var deadlineRaw string
	cf.StringVar(&sourceDir, "s", "source-directory", "", "Absolute source directory to transfer")
	cf.StringVar(&encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.StringVar(&loadStrategyRaw, "", "load-strategy", LoadStrategyFast, "Server load strategy (fast|gentle)")
	probeSizeRaw = encoding.HumanBytes(defaultCLIProbeBytes)
	cf.StringVar(&probeSizeRaw, "", "probe-size", probeSizeRaw, "Probe payload size for transfer metadata; 1B, 4KiB, 8MiB")
	cf.BoolVar(&verbose, "v", "verbose", false, "Disable front-coding")
	cf.IntVar(&maxChunk, "", "max-manifest-chunk-size", 0, "Max chunk bytes for manifest stream")
	cf.StringVar(&deadlineRaw, "", "deadline", "", "Transfer deadline (e.g. 60s, 5m)")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if sourceDir == "" {
		fmt.Fprintln(stderr, "transfer requires --source-directory (or -s)")
		return 2
	}
	if cf.NArg() != 1 {
		fmt.Fprintln(stderr, "transfer requires exactly one positional argument: <target-dir>")
		return 2
	}
	if maxChunk < 0 {
		fmt.Fprintln(stderr, "--max-manifest-chunk-size must be >= 0")
		return 2
	}
	probeBytes, err := encoding.ParseByteSize(probeSizeRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --probe-size: %v\n", err)
		return 2
	}
	if probeBytes <= 0 {
		fmt.Fprintln(stderr, "--probe-size must be > 0")
		return 2
	}
	loadStrategy, err := resolveLoadStrategy(loadStrategyRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --load-strategy: %v\n", err)
		return 2
	}
	agePublicKey, ageIdentity, resolvedEncMode, err := resolveEncryptionOptions(encryptMode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
	var deadlineMS int64
	if deadlineRaw != "" {
		d, err := time.ParseDuration(deadlineRaw)
		if err != nil {
			fmt.Fprintf(stderr, "invalid --deadline: %v\n", err)
			return 2
		}
		if d <= 0 {
			fmt.Fprintln(stderr, "--deadline must be > 0")
			return 2
		}
		deadlineMS = d.Milliseconds()
	}
	return runTransfer(serverURL, transferArgs{
		sourceDir:    sourceDir,
		targetDir:    cf.Arg(0),
		agePublicKey: agePublicKey,
		ageIdentity:  ageIdentity,
		encMode:      resolvedEncMode,
		loadStrategy: loadStrategy,
		probeBytes:   probeBytes,
		verbose:      verbose,
		maxChunk:     maxChunk,
		deadlineMS:   deadlineMS,
	}, stdout, stderr)
}

func runTransfer(serverURL string, cfg transferArgs, stdout io.Writer, stderr io.Writer) int {
	ps, err := newPinchState(cfg.targetDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
		return 2
	}
	// Wipe all previous state so every transfer starts clean.
	if err := os.RemoveAll(ps.StateDir); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(stderr, "remove state directory failed: %v\n", err)
		return 1
	}

	fmt.Fprintf(stderr, "transfer(addr=[%s], source=[%s])\n", serverURL, cfg.sourceDir)

	client := NewClient(serverURL, WithLoadStrategy(cfg.loadStrategy), WithClientAgePublicKey(cfg.agePublicKey), WithClientAgeIdentity(cfg.ageIdentity), WithEncryptMode(cfg.encMode))
	start := time.Now()
	probeResult, err := client.ProbeLink(context.Background(), ProbeRequest{
		Samples:      3,
		ProbeBytes:   cfg.probeBytes,
		LoadStrategy: cfg.loadStrategy,
	})
	if err != nil {
		fmt.Fprintf(stderr, "probe failed: %v\n", err)
		return 1
	}
	cipherDisplay := "none"
	if probeResult.Cipher != "" {
		cipherDisplay = probeResult.Cipher
	}
	fmt.Fprintf(
		stdout,
		"transfer-probe : strategy=%s cipher=%s avg_ms=%d est_link=%dMbps srv-conc=(%d cpu * %d io = %d)\n",
		cfg.loadStrategy,
		cipherDisplay,
		probeResult.AvgLatencyMS,
		probeResult.LinkMbps,
		probeResult.ServerCPU, probeResult.ServerIODepth, probeResult.SuggestedConcurrency,
	)
	manifestResp, err := client.GetManifest(context.Background(), GetManifestRequest{
		Directory:    cfg.sourceDir,
		Verbose:      cfg.verbose,
		MaxChunkSize: cfg.maxChunk,
		Mode:         cfg.loadStrategy,
		LinkMbps:     probeResult.LinkMbps,
		Concurrency:  probeResult.SuggestedConcurrency,
		DeadlineMS:   cfg.deadlineMS,
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
	if err := ps.ensureStateDir(); err != nil {
		fmt.Fprintf(stderr, "create state directory failed: %v\n", err)
		return 1
	}
	if err := SaveManifest(ps.ServerManifestPath, manifest); err != nil {
		fmt.Fprintf(stderr, "save manifest failed: %v\n", err)
		return 1
	}
	fmt.Fprintf(
		stdout,
		"transfer-loaded: tid[%s] %d files (%s) from [%s] elapsed=%s\n",
		manifest.TransferID,
		len(manifest.Entries),
		encoding.HumanBytes(total),
		manifest.Root,
		time.Since(start).Round(time.Millisecond),
	)
	fmt.Fprintf(stderr, "transfer-state : >(%s)\n", ps.ServerManifestPath)

	return 0
}

func runStatusCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := newCLIFlags("status")
	cf.SetOutput(stderr)
	cf.fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: pinch filecli [addr] status [--tid <id>] [LOCAL_DST]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Query and monitor transfer progress.")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Modes:")
		fmt.Fprintln(stderr, "  status LOCAL_DST       Discover transfer from .pinch/ state and poll until complete")
		fmt.Fprintln(stderr, "  status --tid <id>      Poll a transfer by ID (server-side progress only)")
		fmt.Fprintln(stderr, "  status                 List all active transfers on the server")
		fmt.Fprintln(stderr)
		cf.PrintDefaults(stderr)
	}
	var txferID string
	cf.StringVar(&txferID, "", "tid", "", "Transfer ID")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if cf.NArg() > 1 {
		fmt.Fprintln(stderr, "status accepts at most one positional argument: LOCAL_DST")
		return 2
	}

	client := NewClient(serverURL)

	// Mode 1: LOCAL_DST given — discover transfer from .pinch/ state.
	if cf.NArg() == 1 {
		localDst := cf.Arg(0)
		ps, err := newPinchState(localDst)
		if err != nil {
			fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
			return 2
		}
		if _, err := os.Stat(ps.ServerManifestPath); os.IsNotExist(err) {
			fmt.Fprintf(stderr, "No active transfer for %s\n", localDst)
			return 0
		}
		manifest, err := LoadManifest(ps.ServerManifestPath)
		if err != nil {
			fmt.Fprintf(stderr, "load manifest failed: %v\n", err)
			return 1
		}
		txferID = manifest.TransferID
		return pollTransferStatus(client, txferID, manifest, ps.ProgressPath, stdout, stderr)
	}

	// Mode 2: --tid given — poll by transfer ID (no local progress).
	if txferID != "" {
		return pollTransferStatus(client, txferID, nil, "", stdout, stderr)
	}

	// Mode 3: no args — list all active transfers.
	listResp, err := client.ListStatuses(context.Background(), ListStatusesRequest{})
	if err != nil {
		fmt.Fprintf(stderr, "status failed: %v\n", err)
		return 1
	}
	if len(listResp.Statuses) == 0 {
		fmt.Fprintln(stdout, "No active transfers")
		return 0
	}
	for _, s := range listResp.Statuses {
		fmt.Fprintf(
			stdout,
			"[%s] source=[%s] files=[%d/%d](%.1f%%) bytes=[%s/%s](%.1f%%)\n",
			s.TransferID,
			s.Directory,
			s.Done, s.NumFiles, s.PercentFiles,
			encoding.HumanBytes(s.DoneSize), encoding.HumanBytes(s.TotalSize), s.PercentBytes,
		)
	}
	return 0
}

func computeLocalProgress(manifest *Manifest, progressPath string) (doneFiles int, totalFiles int, doneBytes int64, totalBytes int64) {
	if manifest == nil || progressPath == "" {
		return
	}
	totalFiles = len(manifest.Entries)
	for _, e := range manifest.Entries {
		totalBytes += e.Size
	}
	progressState, err := loadProgressState(progressPath)
	if err != nil {
		return
	}
	for _, e := range manifest.Entries {
		if p, ok := progressState[e.ID]; ok {
			ack := p.AckBytes
			if ack > e.Size {
				ack = e.Size
			}
			doneBytes += ack
			if ack >= e.Size {
				doneFiles++
			}
		}
	}
	return
}

func pollTransferStatus(client *Client, txferID string, manifest *Manifest, progressPath string, stdout io.Writer, stderr io.Writer) int {
	hasLocal := manifest != nil && progressPath != ""
	var totalBytes int64
	var totalFiles int
	if manifest != nil {
		totalFiles = len(manifest.Entries)
		for _, e := range manifest.Entries {
			totalBytes += e.Size
		}
	}

	ticker := time.NewTicker(defaultVerboseProgressInterval)
	defer ticker.Stop()
	var prevDoneSize int64
	prevTime := time.Now()
	startTime := prevTime

	// Track high-water marks for local progress so it never regresses
	// (the progress file may be cleaned up when the copy finishes).
	var peakLocalDoneFiles int
	var peakLocalDoneBytes int64

	for {
		statusResp, statusErr := client.GetStatus(context.Background(), GetStatusRequest{
			TransferID: txferID,
		})
		if statusErr != nil {
			if strings.Contains(statusErr.Error(), "NOT_FOUND") {
				fmt.Fprintf(stderr, "Transfer %s expired on server\n", txferID)
				return 0
			}
			fmt.Fprintf(stderr, "status failed: %v\n", statusErr)
			return 1
		}
		s := statusResp.Status
		now := time.Now()
		dt := now.Sub(prevTime).Seconds()
		var rateBps float64
		if dt > 0 {
			rateBps = float64(s.DoneSize-prevDoneSize) / dt
		}
		prevDoneSize = s.DoneSize
		prevTime = now

		etaPart := ""
		if rateBps > 0 && s.TotalSize > s.DoneSize {
			remaining := float64(s.TotalSize - s.DoneSize)
			etaSec := remaining / rateBps
			etaPart = fmt.Sprintf(" eta=%s", (time.Duration(etaSec * float64(time.Second))).Round(time.Second))
		}
		fmt.Fprintf(
			stdout,
			"server: files=[%d/%d](%.1f%%) bytes=[%s/%s](%.1f%%) rate=%s%s\n",
			s.Done, s.NumFiles, s.PercentFiles,
			encoding.HumanBytes(s.DoneSize), encoding.HumanBytes(s.TotalSize),
			s.PercentBytes,
			encoding.HumanRate(rateBps), etaPart,
		)
		if hasLocal {
			localDoneFiles, localTotalFiles, localDoneBytes, localTotalBytes := computeLocalProgress(manifest, progressPath)
			if localDoneFiles > peakLocalDoneFiles {
				peakLocalDoneFiles = localDoneFiles
			}
			if localDoneBytes > peakLocalDoneBytes {
				peakLocalDoneBytes = localDoneBytes
			}
			// If the server is done, the client must be done too — the progress
			// file may have already been cleaned up, so clamp peaks to totals.
			if s.PercentBytes >= 100.0 {
				peakLocalDoneFiles = localTotalFiles
				peakLocalDoneBytes = localTotalBytes
			}
			var localPctFiles, localPctBytes float64
			if localTotalFiles > 0 {
				localPctFiles = float64(peakLocalDoneFiles) * 100.0 / float64(localTotalFiles)
			}
			if localTotalBytes > 0 {
				localPctBytes = float64(peakLocalDoneBytes) * 100.0 / float64(localTotalBytes)
			}
			fmt.Fprintf(
				stdout,
				"client: files=[%d/%d](%.1f%%) bytes=[%s/%s](%.1f%%)\n",
				peakLocalDoneFiles, localTotalFiles, localPctFiles,
				encoding.HumanBytes(peakLocalDoneBytes), encoding.HumanBytes(localTotalBytes),
				localPctBytes,
			)
		}

		// Check for completion.
		serverDone := s.PercentBytes >= 100.0
		localDone := true
		if hasLocal {
			localDone = peakLocalDoneFiles >= totalFiles && peakLocalDoneBytes >= totalBytes
		}
		if serverDone && localDone {
			elapsed := time.Since(startTime)
			overallSpeed := 0.0
			if elapsed.Seconds() > 0 {
				overallSpeed = float64(s.TotalSize) / elapsed.Seconds()
			}
			fmt.Fprintf(
				stdout,
				"\ntransfer complete: tid=%s files=%d size=%s elapsed=%s speed=%s\n",
				txferID,
				s.NumFiles,
				encoding.HumanBytes(s.TotalSize),
				elapsed.Round(time.Millisecond),
				encoding.HumanRate(overallSpeed),
			)
			return 0
		}

		<-ticker.C
	}
}

func runGetCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := newCLIFlags("get")
	cf.SetOutput(stderr)
	cf.fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: pinch filecli [addr] get [flags] REMOTE_PATH")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Download a single remote file. REMOTE_PATH must be an absolute path to a file")
		fmt.Fprintln(stderr, "on the server. Output defaults to the file's basename in the current directory.")
		fmt.Fprintln(stderr)
		cf.PrintDefaults(stderr)
	}
	var outFile string
	var encryptMode string
	var compressRaw string
	var ackEveryRaw string
	var skipWrite bool
	var skipFsync bool
	var concurrency int
	var verbose bool
	var progress bool
	var deadlineRaw string
	var traceFile string
	var progressFilePath string
	var progressFileIntervalRaw string
	cf.StringVar(&outFile, "o", "", "", "Output file path, or '-' for stdout")
	cf.StringVar(&encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.StringVar(&compressRaw, "", "compress", "", "Compression algorithm: adapt|none|lz4|zstd (default: adapt)")
	cf.IntVar(&concurrency, "", "concurrency", 0, "Parallel download workers (0=auto)")
	cf.BoolVar(&skipWrite, "", "skip-write", false, "Do not write the file; fetch to discard instead")
	cf.BoolVar(&skipFsync, "", "skip-fsync", false, "Acknowledge writes without fdatasync")
	cf.BoolVar(&progress, "", "progress", true, "Show transfer progress every 2s")
	cf.BoolVar(&verbose, "v", "verbose", false, "Per-file progress output")
	cf.StringVar(&progressFilePath, "", "progress-file", "", "Write integer % to this file/pipe")
	cf.StringVar(&progressFileIntervalRaw, "", "progress-file-interval", "1s", "Progress write interval (e.g. 500ms, 10s)")
	ackEveryRaw = encoding.HumanBytes(defaultCLIAckEveryBytes)
	cf.StringVar(&ackEveryRaw, "a", "ack-every", ackEveryRaw, "Bytes between progress acks; e.g. 1B, 4KiB, 8MiB")
	cf.StringVar(&deadlineRaw, "", "deadline", "", "Transfer deadline (e.g. 60s, 5m)")
	cf.StringVar(&traceFile, "", "trace", "", "Write runtime/trace output to this file")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	stopTracing := startTracing(traceFile, stderr)
	defer stopTracing()
	if cf.NArg() != 1 {
		fmt.Fprintln(stderr, "get requires exactly one positional argument: REMOTE_PATH")
		return 2
	}
	remotePath := cf.Arg(0)
	if !filepath.IsAbs(remotePath) {
		fmt.Fprintln(stderr, "get requires REMOTE_PATH to be an absolute server path")
		return 2
	}
	progressInterval, err := time.ParseDuration(progressFileIntervalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --progress-file-interval: %v\n", err)
		return 2
	}
	ackEvery, err := encoding.ParseByteSize(ackEveryRaw)
	if err != nil || ackEvery <= 0 {
		fmt.Fprintf(stderr, "invalid --ack-every: %v\n", err)
		return 2
	}
	agePublicKey, ageIdentity, resolvedEncMode, err := resolveEncryptionOptions(encryptMode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
	compress, err := resolveCompress(compressRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --compress: %v\n", err)
		return 2
	}
	var deadlineMS int64
	if deadlineRaw != "" {
		d, dErr := time.ParseDuration(deadlineRaw)
		if dErr != nil || d <= 0 {
			fmt.Fprintf(stderr, "invalid --deadline: %v\n", dErr)
			return 2
		}
		deadlineMS = d.Milliseconds()
	}

	// Resolve output path: -o overrides, default is basename in cwd.
	outputPath := strings.TrimSpace(outFile)
	if outputPath == "" {
		outputPath = filepath.Base(remotePath)
	}
	if skipWrite {
		outputPath = os.DevNull
	}

	effectiveConcurrency := DefaultClientConcurrency()
	if concurrency > 0 {
		effectiveConcurrency = concurrency
	}

	client := NewClient(serverURL, WithLoadStrategy(LoadStrategyFast), WithComp(compress), WithClientAgePublicKey(agePublicKey), WithClientAgeIdentity(ageIdentity), WithEncryptMode(resolvedEncMode))

	// Fetch manifest for the single file (skip full probe).
	fmt.Fprintf(stderr, "get(addr=[%s], path=[%s])\n", serverURL, remotePath)
	manifestResp, err := client.GetManifest(context.Background(), GetManifestRequest{
		Directory:   remotePath,
		Mode:        LoadStrategyFast,
		LinkMbps:    0,
		Concurrency: effectiveConcurrency,
		DeadlineMS:  deadlineMS,
	})
	if err != nil {
		fmt.Fprintf(stderr, "get failed: %v\n", err)
		return 1
	}
	manifest := manifestResp.Manifest
	if len(manifest.Entries) != 1 {
		fmt.Fprintf(stderr, "get failed: expected single file manifest, got %d entries\n", len(manifest.Entries))
		return 1
	}
	entry := manifest.Entries[0]
	fmt.Fprintf(stderr, "get-manifest: tid=%s file=%s size=%s\n", manifest.TransferID, entry.Path, encoding.HumanBytes(entry.Size))

	// Mini-probe to detect server send buffer and compute batch size.
	var miniProbe ProbeResponse
	if probe, probeErr := client.ProbeLink(context.Background(), ProbeRequest{Samples: 1, ProbeBytes: 1}); probeErr == nil {
		miniProbe = probe
	}
	batchSize := SuggestBatchMaxBytes(
		miniProbe.SuggestedConcurrency,
		client.WindowConcurrency,
		client.FileRequestWindowBytes,
		miniProbe.ServerSendBufBytes,
	)

	start := time.Now()
	progressUpdates := make(chan DownloadProgressUpdate, 128)
	var onProgressUpdate func(DownloadProgressUpdate)
	if verbose {
		progressReporter := newVerboseProgressReporter(stderr)
		onProgressUpdate = progressReporter.ReportUpdate
	}
	forwardProgress := func(update DownloadProgressUpdate) {
		if onProgressUpdate != nil {
			onProgressUpdate(update)
		}
	}
	// Use a no-op progress writer (no .pinch state for single-file get).
	go func() {
		for update := range progressUpdates {
			forwardProgress(update)
		}
	}()

	var stopStatusPolling func()
	if progress {
		stopStatusPolling = startVerboseStatusPolling(manifest.TransferID, client, stderr)
		defer stopStatusPolling()
	}

	var totalCopied atomic.Int64
	outputWriter := func(me ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
		w, syncFn, wErr := openDownloadOutput(me, offset, outputPath, stdout, skipFsync)
		if wErr != nil {
			return nil, nil, wErr
		}
		return &countingWriter{Writer: w, total: &totalCopied}, syncFn, nil
	}
	if progressFilePath != "" {
		totalBytes := entry.Size
		stopProgressFile := filexfer.StartProgressFileWriter(context.Background(), progressFilePath, progressInterval, func() int {
			if totalBytes <= 0 {
				return 100
			}
			pct := int(totalCopied.Load() * 100 / totalBytes)
			if pct > 100 {
				pct = 100
			}
			return pct
		})
		defer func() { stopProgressFile(err == nil) }()
	}

	downloadBatchResp, err := client.GetFiles(context.Background(), GetFilesRequest{
		Manifest:        manifest,
		FileIDs:         []uint64{0},
		BatchMaxBytes:   batchSize,
		OutputWriter:    outputWriter,
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
	printFileMetrics(stdout, manifest.TransferID, 0, outputPath, downloadResp.Meta, downloadResp.LocalFileHash, elapsed)
	return 0
}

func runSyncCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := newCLIFlags("sync")
	cf.SetOutput(stderr)
	cf.fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: pinch filecli sync [-s <dir>] [flags] <target-dir>")
		cf.PrintDefaults(stderr)
	}
	var sourceDir string
	var encryptMode string
	var concurrency int
	var ackEveryRaw string
	var compressRaw string
	var noSync bool
	var skipWrite bool
	var verbose bool
	var yes bool
	var probeSizeRaw string
	var traceFile string
	var progressFilePath string
	var progressFileIntervalRaw string
	cf.StringVar(&sourceDir, "s", "source-directory", "", "Absolute source directory on server (default: manifest root)")
	cf.StringVar(&encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.StringVar(&compressRaw, "", "compress", "", "Compression algorithm: adapt|none|lz4|zstd (default: adapt)")
	cf.IntVar(&concurrency, "", "concurrency", 0, "Parallel download workers (0=manifest default)")
	cf.BoolVar(&yes, "y", "yes", false, "Skip confirmation prompt")
	cf.BoolVar(&verbose, "v", "verbose", false, "Per-file progress output")
	cf.StringVar(&progressFilePath, "", "progress-file", "", "Write integer % to this file/pipe")
	cf.StringVar(&progressFileIntervalRaw, "", "progress-file-interval", "1s", "Progress write interval (e.g. 500ms, 10s)")
	ackEveryRaw = encoding.HumanBytes(defaultCLIAckEveryBytes)
	cf.StringVar(&ackEveryRaw, "a", "ack-every", ackEveryRaw, "Bytes between progress acks; 1B, 4KiB, 8MiB")
	cf.BoolVar(&skipWrite, "", "skip-write", false, "Do not mutate the target directory; fetch bodies to discard instead of writing them")
	cf.BoolVar(&noSync, "", "skip-fsync", false, "Ack without fdatasync")
	cf.BoolVar(&noSync, "", "no-sync", false, "Ack without fdatasync")
	probeSizeRaw = encoding.HumanBytes(defaultCLIProbeBytes)
	cf.StringVar(&probeSizeRaw, "", "probe-size", probeSizeRaw, "Probe payload size; 1B, 4KiB, 8MiB")
	cf.StringVar(&traceFile, "", "trace", "", "Write runtime/trace output to this file")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if cf.NArg() != 1 {
		fmt.Fprintln(stderr, "sync requires exactly one positional argument: <target-dir>")
		return 2
	}
	progressInterval, err := time.ParseDuration(progressFileIntervalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --progress-file-interval: %v\n", err)
		return 2
	}
	ackEvery, err := encoding.ParseByteSize(ackEveryRaw)
	if err != nil || ackEvery <= 0 {
		fmt.Fprintf(stderr, "invalid --ack-every: %v\n", err)
		return 2
	}
	probeBytes, err := encoding.ParseByteSize(probeSizeRaw)
	if err != nil || probeBytes <= 0 {
		fmt.Fprintf(stderr, "invalid --probe-size: %v\n", err)
		return 2
	}
	agePublicKey, ageIdentity, resolvedEncMode, err := resolveEncryptionOptions(encryptMode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
	compress, err := resolveCompress(compressRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --compress: %v\n", err)
		return 2
	}
	concurrencyExplicit := false
	cf.Visit(func(f *flag.Flag) {
		if f.Name == "concurrency" {
			concurrencyExplicit = true
		}
	})
	if concurrencyExplicit && concurrency <= 0 {
		fmt.Fprintln(stderr, "--concurrency must be > 0")
		return 2
	}
	return runSync(serverURL, syncArgs{
		sourceDir:           sourceDir,
		targetDir:           cf.Arg(0),
		agePublicKey:        agePublicKey,
		ageIdentity:         ageIdentity,
		encMode:             resolvedEncMode,
		concurrency:         concurrency,
		concurrencyExplicit: concurrencyExplicit,
		ackEvery:            ackEvery,
		compress:            compress,
		noSync:              noSync,
		skipWrite:           skipWrite,
		verbose:             verbose,
		yes:                 yes,
		probeBytes:          probeBytes,
		traceFile:           traceFile,
		progressFilePath:    progressFilePath,
		progressInterval:    progressInterval,
	}, stdout, stderr)
}

func runSync(serverURL string, cfg syncArgs, stdout io.Writer, stderr io.Writer) int {
	outputMu := &sync.Mutex{}
	stdout = &synchronizedWriter{mu: outputMu, w: stdout}
	stderr = &synchronizedWriter{mu: outputMu, w: stderr}
	stopTracing := startTracing(cfg.traceFile, stderr)
	defer stopTracing()
	ps, err := newPinchState(cfg.targetDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
		return 2
	}
	fmt.Fprintf(stderr, "sync-state: >(%s) <(%s)\n", ps.ManifestPath, ps.TargetDir)

	outRoot := ps.TargetDir

	// Load server manifest once for metadata (Root, Mode, Concurrency, etc.).
	serverManifest, serverManifestErr := LoadManifest(ps.ServerManifestPath)
	if serverManifestErr != nil {
		if cfg.sourceDir == "" {
			fmt.Fprintf(stderr, "load server manifest failed (run transfer first, or provide -s): %v\n", serverManifestErr)
			return 1
		}
		// No server manifest yet; use minimal defaults and rely on -s.
		serverManifest = &Manifest{Mode: LoadStrategyFast, Concurrency: 48, LinkMbps: 1000}
	}

	syncSourceDir := cfg.sourceDir
	if syncSourceDir == "" {
		syncSourceDir = serverManifest.Root
	}
	if syncSourceDir == "" {
		fmt.Fprintln(stderr, "sync requires --source-directory (or -s) when manifest.server has no root")
		return 2
	}
	loadStrategy, err := resolveLoadStrategy(serverManifest.Mode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid manifest mode: %v\n", err)
		return 1
	}

	for round := 0; round < maxSyncRounds; round++ {
		// Build local manifest by scanning the target directory (what client has on disk).
		oldManifest, err := scanLocalDir(ps.TargetDir, serverManifest)
		if err != nil {
			fmt.Fprintf(stderr, "scan local directory failed: %v\n", err)
			return 1
		}

		// Probe link bandwidth.
		client := NewClient(serverURL, WithLoadStrategy(loadStrategy), WithComp(cfg.compress), WithClientAgePublicKey(cfg.agePublicKey), WithClientAgeIdentity(cfg.ageIdentity), WithEncryptMode(cfg.encMode))
		probeResult, err := client.ProbeLink(context.Background(), ProbeRequest{
			Samples:      3,
			ProbeBytes:   cfg.probeBytes,
			LoadStrategy: loadStrategy,
		})
		if err != nil {
			fmt.Fprintf(stderr, "probe failed: %v\n", err)
			return 1
		}
		batchSize := SuggestBatchMaxBytes(
			probeResult.SuggestedConcurrency,
			client.WindowConcurrency,
			client.FileRequestWindowBytes,
			probeResult.ServerSendBufBytes,
		)

		// SYNC: send old manifest, receive new manifest + removed paths.
		syncResp, err := client.SyncManifest(context.Background(), SyncManifestRequest{
			Directory:   syncSourceDir,
			OldManifest: oldManifest,
			Mode:        loadStrategy,
			LinkMbps:    probeResult.LinkMbps,
			Concurrency: probeResult.SuggestedConcurrency,
		})
		if err != nil {
			fmt.Fprintf(stderr, "sync failed: %v\n", err)
			return 1
		}
		newManifest := syncResp.Manifest

		// Compute delta: new, stale, unchanged, removed.
		oldByPath := make(map[string]ManifestEntry, len(oldManifest.Entries))
		for _, e := range oldManifest.Entries {
			oldByPath[e.Path] = e
		}
		var newFiles, staleFiles, unchangedFiles []ManifestEntry
		var newBytes, staleBytes int64
		for _, entry := range newManifest.Entries {
			if old, ok := oldByPath[entry.Path]; ok {
				if old.Size == entry.Size && old.Mtime == entry.Mtime && old.Mode == entry.Mode {
					entry.Progress = ManifestProgress{AckBytes: entry.Size, MetadataDone: true}
					unchangedFiles = append(unchangedFiles, entry)
				} else {
					staleFiles = append(staleFiles, entry)
					staleBytes += entry.Size
				}
			} else {
				newFiles = append(newFiles, entry)
				newBytes += entry.Size
			}
		}
		rmPaths := syncResp.RemovedPaths
		newCount := encoding.HumanCount(uint64(len(newFiles)), 6)
		staleCount := encoding.HumanCount(uint64(len(staleFiles)), 6)
		unchangedCount := encoding.HumanCount(uint64(len(unchangedFiles)), 6)
		rmCount := encoding.HumanCount(uint64(len(rmPaths)), 6)

		fmt.Fprintf(stdout,
			"sync-delta[%d]: new[%s (%s)] stale[%s (%s)] same[%s] rm[%s] link=%dMbps srv-conc=(%d cpu * %d io = %d) batch=%s\n",
			round,
			newCount, encoding.HumanBytes(newBytes),
			staleCount, encoding.HumanBytes(staleBytes),
			unchangedCount,
			rmCount,
			probeResult.LinkMbps,
			probeResult.ServerCPU, probeResult.ServerIODepth, probeResult.SuggestedConcurrency,
			encoding.HumanBytes(batchSize),
		)

		if len(newFiles) == 0 && len(staleFiles) == 0 && len(rmPaths) == 0 {
			fmt.Fprintln(stdout, "sync: converged, nothing to do")
			return 0
		}

		// Prompt for confirmation on the first round only.
		if round == 0 && !cfg.yes && !cfg.skipWrite {
			if !confirmSyncProceed(stderr, len(newFiles), len(staleFiles), len(rmPaths)) {
				return 0
			}
		}

		// Build merged manifest with new entries, carry progress for unchanged.
		mergedManifest := &Manifest{
			TransferID:  newManifest.TransferID,
			Root:        newManifest.Root,
			Mode:        newManifest.Mode,
			LinkMbps:    newManifest.LinkMbps,
			Concurrency: newManifest.Concurrency,
			Entries:     newManifest.Entries,
		}
		for _, uf := range unchangedFiles {
			for i := range mergedManifest.Entries {
				if mergedManifest.Entries[i].ID == uf.ID {
					mergedManifest.Entries[i].Progress = uf.Progress
					break
				}
			}
		}

		// Update server manifest with the latest server state for next round / future commands.
		if err := SaveManifest(ps.ServerManifestPath, mergedManifest); err != nil {
			fmt.Fprintf(stderr, "save server manifest failed: %v\n", err)
			return 1
		}
		serverManifest = mergedManifest

		// Always delete removed files to converge.
		if !cfg.skipWrite {
			for _, rmPath := range rmPaths {
				localPath := filepath.Join(outRoot, filepath.FromSlash(rmPath))
				if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
					fmt.Fprintf(stderr, "sync: rm %s: %v\n", localPath, err)
				}
			}
			// Truncate local files for stale entries.
			for _, entry := range staleFiles {
				localPath := filepath.Join(outRoot, filepath.FromSlash(entry.Path))
				if err := os.Truncate(localPath, 0); err != nil && !os.IsNotExist(err) {
					fmt.Fprintf(stderr, "sync: truncate %s: %v\n", localPath, err)
				}
			}
		}

		// Build progress state for merged manifest.
		mergedProgress := make(map[uint64]ManifestProgress, len(mergedManifest.Entries))
		for _, e := range mergedManifest.Entries {
			if e.Progress.AckBytes > 0 || e.Progress.MetadataDone {
				mergedProgress[e.ID] = e.Progress
			}
		}

		// Download pending entries (new + stale).
		pendingEntries := make([]ManifestEntry, 0, len(newFiles)+len(staleFiles))
		for _, entry := range mergedManifest.Entries {
			if entry.Progress.AckBytes >= entry.Size && entry.Progress.MetadataDone {
				continue
			}
			pendingEntries = append(pendingEntries, entry)
		}

		if len(pendingEntries) == 0 {
			if cfg.skipWrite {
				fmt.Fprintln(stdout, "sync: skip-write, no downloads needed")
				return 0
			}
			fmt.Fprintln(stdout, "sync: no downloads needed")
			continue
		}

		effectiveConcurrency := mergedManifest.Concurrency
		if cfg.concurrencyExplicit {
			effectiveConcurrency = cfg.concurrency
		}

		progressUpdates := make(chan DownloadProgressUpdate, 1024)
		entryByID := manifestEntriesByID(mergedManifest)
		var onProgressUpdate func(DownloadProgressUpdate)
		if cfg.verbose {
			progressReporter := newVerboseProgressReporter(stderr)
			onProgressUpdate = progressReporter.ReportUpdate
		}
		forwardProgress := func(update DownloadProgressUpdate) {
			if onProgressUpdate != nil {
				onProgressUpdate(update)
			}
		}
		stopProgress, persistProgressAck, markMetadataDonePersisted := startProgressWriter(ps.ProgressPath, mergedProgress, progressUpdates, forwardProgress, stderr)
		persistFileDone := func(fileID uint64, ackBytes int64) {
			persistProgressAck(fileID, ackBytes)
		}
		markMetadataDone := func(fileID uint64) {
			markMetadataDonePersisted(fileID)
		}

		var totalCopied atomic.Int64
		outputWriter := func(entry ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
			destPath := resolveDownloadDestinationPath(entry, outRoot, "")
			if cfg.skipWrite {
				destPath = os.DevNull
			}
			w, syncFn, err := openDownloadOutput(entry, offset, destPath, nil, cfg.noSync)
			if err != nil {
				return nil, nil, err
			}
			return &countingWriter{Writer: w, total: &totalCopied}, syncFn, nil
		}

		startAll := time.Now()
		var completed int64
		var totalTransferred int64
		var failures []error
		var failuresMu sync.Mutex

		if cfg.progressFilePath != "" {
			var totalBytes int64
			for _, e := range pendingEntries {
				totalBytes += e.Size
			}
			stopProgressFile := filexfer.StartProgressFileWriter(context.Background(), cfg.progressFilePath, cfg.progressInterval, func() int {
				if totalBytes <= 0 {
					return 100
				}
				pct := int(totalCopied.Load() * 100 / totalBytes)
				if pct > 100 {
					pct = 100
				}
				return pct
			})
			defer func() { stopProgressFile(len(failures) == 0) }()
		}
		recordFailure := func(err error) {
			if err == nil {
				return
			}
			failuresMu.Lock()
			failures = append(failures, err)
			failuresMu.Unlock()
		}

		startResp, err := client.StartFromManifest(context.Background(), StartFromManifestRequest{
			Manifest:        mergedManifest,
			Entries:         pendingEntries,
			OutputWriter:    outputWriter,
			Concurrency:     effectiveConcurrency,
			BatchMaxBytes:   batchSize,
			ProgressUpdates: progressUpdates,
			OnFileDone: func(evt StartFileDoneEvent) {
				entry, ok := entryByID[evt.File.Meta.FileID]
				if !ok {
					recordFailure(fmt.Errorf("id=%d metadata apply failed: file id not in manifest", evt.File.Meta.FileID))
					return
				}
				destPath := resolveDownloadDestinationPath(entry, outRoot, "")
				if cfg.skipWrite {
					destPath = os.DevNull
				}
				if err := applyDownloadedTrailerMetadata(destPath, evt.File.Meta.TrailerMetadata); err != nil {
					recordFailure(fmt.Errorf("id=%d metadata apply failed: %w", evt.File.Meta.FileID, err))
					return
				}
				persistFileDone(evt.File.Meta.FileID, entry.Size)
				markMetadataDone(evt.File.Meta.FileID)
				printStartFileSummary(stdout, evt.File.Meta.FileID, destPath, evt.File.Meta, evt.File.LocalFileHash, evt.File.WindowChecksumPassed, evt.File.WindowChecksumTotal, evt.Elapsed)
			},
		})
		stopProgress()
		applyProgressStateToManifest(mergedManifest, mergedProgress)
		if err != nil {
			fmt.Fprintf(stderr, "sync download failed: %v\n", err)
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
			fmt.Fprintf(stderr, "sync error: %v\n", err)
		}

		elapsedAll := time.Since(startAll)
		overallSpeed := 0.0
		if elapsedAll > 0 {
			overallSpeed = float64(totalTransferred) / elapsedAll.Seconds()
		}
		fmt.Fprintf(stdout,
			"sync complete[%d]: tid=%s downloaded=%d failed=%d transferred=%s speed=%s elapsed=%s\n",
			round,
			mergedManifest.TransferID,
			completed,
			len(finalFailures),
			encoding.HumanBytes(totalTransferred),
			encoding.HumanRate(overallSpeed),
			elapsedAll.Round(time.Millisecond),
		)
		if len(finalFailures) > 0 {
			return 1
		}
		if cfg.skipWrite {
			return 0
		}
	}

	fmt.Fprintf(stderr, "sync: failed to converge after %d rounds\n", maxSyncRounds)
	return 1
}

func runStartCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := newCLIFlags("start")
	cf.SetOutput(stderr)
	cf.fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: pinch filecli start [flags] <target-dir>")
		cf.PrintDefaults(stderr)
	}
	var encryptMode string
	var concurrency int
	var ackEveryRaw string
	var compressRaw string
	var noSync bool
	var verbose bool
	var progress bool
	var discard bool
	var deadlineRaw string
	var progressFilePath string
	var progressFileIntervalRaw string
	cf.StringVar(&encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.BoolVar(&progress, "", "progress", true, "Show transfer progress every 2s")
	cf.BoolVar(&verbose, "v", "verbose", false, "Per-file progress output")
	cf.StringVar(&progressFilePath, "", "progress-file", "", "Write integer % to this file/pipe")
	cf.StringVar(&progressFileIntervalRaw, "", "progress-file-interval", "1s", "Progress write interval (e.g. 500ms, 10s)")
	cf.BoolVar(&discard, "", "skip-write", false, "Discard downloaded file contents instead of writing to the target directory")
	cf.BoolVar(&discard, "", "discard", false, "Discard downloaded file contents instead of writing to the target directory")
	cf.IntVar(&concurrency, "", "concurrency", 0, "Parallel download workers (0=manifest default)")
	ackEveryRaw = encoding.HumanBytes(defaultCLIAckEveryBytes)
	cf.StringVar(&ackEveryRaw, "a", "ack-every", ackEveryRaw, "Bytes between progress acks; 1B, 4KiB, 8MiB")
	cf.StringVar(&compressRaw, "", "compress", "", "Compression algorithm: adapt|none|lz4|zstd (default: adapt)")
	cf.BoolVar(&noSync, "", "skip-fsync", false, "Ack without fdatasync")
	cf.BoolVar(&noSync, "", "no-sync", false, "Ack without fdatasync")
	cf.StringVar(&deadlineRaw, "", "deadline", "", "Transfer deadline (e.g. 60s, 5m)")
	var traceFile string
	cf.StringVar(&traceFile, "", "trace", "", "Write runtime/trace output to this file")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if cf.NArg() != 1 {
		fmt.Fprintln(stderr, "start requires exactly one positional argument: <target-dir>")
		return 2
	}
	progressInterval, err := time.ParseDuration(progressFileIntervalRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --progress-file-interval: %v\n", err)
		return 2
	}
	verbosity := 0
	if verbose {
		verbosity = 2
	} else if progress {
		verbosity = 1
	}
	var deadlineMS int64
	if deadlineRaw != "" {
		d, err := time.ParseDuration(deadlineRaw)
		if err != nil {
			fmt.Fprintf(stderr, "invalid --deadline: %v\n", err)
			return 2
		}
		if d <= 0 {
			fmt.Fprintln(stderr, "--deadline must be > 0")
			return 2
		}
		deadlineMS = d.Milliseconds()
	}
	concurrencyExplicit := false
	cf.Visit(func(f *flag.Flag) {
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
	agePublicKey, ageIdentity, resolvedEncMode, err := resolveEncryptionOptions(encryptMode)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
	compress, err := resolveCompress(compressRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --compress: %v\n", err)
		return 2
	}
	return runStart(serverURL, startArgs{
		targetDir:           cf.Arg(0),
		agePublicKey:        agePublicKey,
		ageIdentity:         ageIdentity,
		encMode:             resolvedEncMode,
		verbosity:           verbosity,
		concurrency:         concurrency,
		concurrencyExplicit: concurrencyExplicit,
		ackEvery:            ackEvery,
		compress:            compress,
		noSync:              noSync,
		discard:             discard,
		deadlineMS:          deadlineMS,
		traceFile:           traceFile,
		progressFilePath:    progressFilePath,
		progressInterval:    progressInterval,
	}, stdout, stderr)
}

func runStart(serverURL string, cfg startArgs, stdout io.Writer, stderr io.Writer) int {
	outputMu := &sync.Mutex{}
	stdout = &synchronizedWriter{mu: outputMu, w: stdout}
	stderr = &synchronizedWriter{mu: outputMu, w: stderr}

	stopTracing := startTracing(cfg.traceFile, stderr)
	defer stopTracing()
	ps, err := newPinchState(cfg.targetDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
		return 2
	}
	fmt.Fprintf(stderr, "start-state: <(%s) > %s\n", ps.ServerManifestPath, ps.TargetDir)
	manifest, err := LoadManifest(ps.ServerManifestPath)
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
	if cfg.concurrencyExplicit {
		effectiveConcurrency = cfg.concurrency
	}
	txferID := manifest.TransferID
	outRoot := ps.StagingDir
	if cfg.discard {
		outRoot = os.DevNull
	} else {
		if err := ps.ensureStagingDir(); err != nil {
			fmt.Fprintf(stderr, "create staging directory failed: %v\n", err)
			return 1
		}
	}
	progressState, err := loadProgressState(ps.ProgressPath)
	if err != nil {
		fmt.Fprintf(stderr, "load progress failed: %v\n", err)
		return 1
	}
	applyProgressStateToManifest(manifest, progressState)
	if cfg.deadlineMS > 0 {
		manifest.DeadlineMS = cfg.deadlineMS
	}
	progressUpdates := make(chan DownloadProgressUpdate, 1024)
	entryByID := manifestEntriesByID(manifest)
	var onStartProgressUpdate func(DownloadProgressUpdate)
	if cfg.verbosity >= 2 {
		progressReporter := newVerboseProgressReporter(stderr)
		onStartProgressUpdate = progressReporter.ReportUpdate
	}
	forwardProgress := func(update DownloadProgressUpdate) {
		if onStartProgressUpdate != nil {
			onStartProgressUpdate(update)
		}
	}
	stopProgress, persistProgressAck, markMetadataDonePersisted := startProgressWriter(ps.ProgressPath, progressState, progressUpdates, forwardProgress, stderr)
	persistFileDone := func(fileID uint64, ackBytes int64) {
		persistProgressAck(fileID, ackBytes)
	}
	markMetadataDone := func(fileID uint64) {
		markMetadataDonePersisted(fileID)
	}
	progressStopped := false
	defer func() {
		if !progressStopped {
			stopProgress()
		}
	}()
	client := NewClient(serverURL, WithLoadStrategy(loadStrategy), WithComp(cfg.compress), WithClientAgePublicKey(cfg.agePublicKey), WithClientAgeIdentity(cfg.ageIdentity), WithEncryptMode(cfg.encMode))
	var miniProbe ProbeResponse
	if probe, probeErr := client.ProbeLink(context.Background(), ProbeRequest{Samples: 1, ProbeBytes: 1}); probeErr == nil {
		miniProbe = probe
	}
	batchSize := SuggestBatchMaxBytes(
		miniProbe.SuggestedConcurrency,
		client.WindowConcurrency,
		client.FileRequestWindowBytes,
		miniProbe.ServerSendBufBytes,
	)
	fmt.Fprintf(
		stdout,
		"start-plan: strategy=%s link=%dMbps conc=%d srv-conc=(%d cpu * %d io = %d) batch=%s cli-sendbuf=%s srv-recvbuf=%s\n",
		loadStrategy,
		manifest.LinkMbps,
		effectiveConcurrency,
		miniProbe.ServerCPU, miniProbe.ServerIODepth, miniProbe.SuggestedConcurrency,
		encoding.HumanBytes(batchSize),
		encoding.HumanBytes(miniProbe.ServerSendBufBytes),
		encoding.HumanBytes(int64(utils.MaxSocketReadBufferBytes())),
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
	if cfg.verbosity >= 1 {
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
			if err := refreshCompletedFileMetadata(context.Background(), client, manifest, entry.ID, outRoot, ""); err != nil {
				recordFailure(fmt.Errorf("id=%d metadata refresh failed: %w", entry.ID, err))
				continue
			}
			persistFileDone(entry.ID, entry.Size)
			markMetadataDone(entry.ID)
			completed++
			continue
		}
		pendingEntries = append(pendingEntries, entry)
	}
	var totalCopied atomic.Int64
	outputWriter := func(entry ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
		destPath := resolveDownloadDestinationPath(entry, outRoot, "")
		w, syncFn, err := openDownloadOutput(entry, offset, destPath, nil, cfg.noSync)
		if err != nil {
			return nil, nil, err
		}
		return &countingWriter{Writer: w, total: &totalCopied}, syncFn, nil
	}
	if cfg.progressFilePath != "" {
		var totalBytes int64
		for _, e := range pendingEntries {
			totalBytes += e.Size
		}
		stopProgressFile := filexfer.StartProgressFileWriter(context.Background(), cfg.progressFilePath, cfg.progressInterval, func() int {
			if totalBytes <= 0 {
				return 100
			}
			pct := int(totalCopied.Load() * 100 / totalBytes)
			if pct > 100 {
				pct = 100
			}
			return pct
		})
		defer func() { stopProgressFile(len(failures) == 0) }()
	}
	startResp, err := client.StartFromManifest(context.Background(), StartFromManifestRequest{
		Manifest:        manifest,
		Entries:         pendingEntries,
		OutputWriter:    outputWriter,
		Concurrency:     effectiveConcurrency,
		BatchMaxBytes:   batchSize,
		ProgressUpdates: progressUpdates,
		OnFileDone: func(evt StartFileDoneEvent) {
			entry, ok := entryByID[evt.File.Meta.FileID]
			if !ok {
				recordFailure(fmt.Errorf("id=%d metadata apply failed: file id not in manifest", evt.File.Meta.FileID))
				return
			}
			destPath := resolveDownloadDestinationPath(entry, outRoot, "")
			if err := applyDownloadedTrailerMetadata(destPath, evt.File.Meta.TrailerMetadata); err != nil {
				recordFailure(fmt.Errorf("id=%d metadata apply failed: %w", evt.File.Meta.FileID, err))
				return
			}
			persistFileDone(evt.File.Meta.FileID, entry.Size)
			markMetadataDone(evt.File.Meta.FileID)
			if cfg.verbosity >= 2 {
				printStartFileSummary(stdout, evt.File.Meta.FileID, destPath, evt.File.Meta, evt.File.LocalFileHash, evt.File.WindowChecksumPassed, evt.File.WindowChecksumTotal, evt.Elapsed)
			}
		},
	})
	if err != nil {
		stopProgress()
		progressStopped = true
		fmt.Fprintf(stderr, "start failed: %v\n", err)
		return 1
	}
	completed += int64(startResp.Downloaded)
	totalTransferred += startResp.TransferredBytes
	for _, startErr := range startResp.Errors {
		recordFailure(startErr)
	}
	stopProgress()
	progressStopped = true
	applyProgressStateToManifest(manifest, progressState)
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
		"start-complete: tid=%s requested=%d downloaded=%d failed=%d transferred=%s speed=%s elapsed=%s\n",
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
	if cfg.discard {
		if err := os.Remove(ps.ProgressPath); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(stderr, "remove progress state failed: %v\n", err)
			return 1
		}
		return 0
	}

	if err := os.RemoveAll(ps.TargetDir); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(stderr, "remove old target directory failed: %v\n", err)
		return 1
	}
	if err := os.Rename(ps.StagingDir, ps.TargetDir); err != nil {
		fmt.Fprintf(stderr, "rename staging to target failed: %v\n", err)
		return 1
	}
	if err := SaveManifest(ps.ManifestPath, manifest); err != nil {
		fmt.Fprintf(stderr, "save local manifest failed: %v\n", err)
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
	// Build the full line before writing to avoid multiple Write calls (and lock
	// acquisitions) on the synchronized stdout writer.
	var sb strings.Builder
	sb.Grow(128)
	sb.WriteString("start-file: fd=")
	sb.WriteString(strconv.FormatUint(fileID, 10))
	sb.WriteString(" path=")
	sb.WriteString(path)
	sb.WriteByte(' ')
	sb.WriteString(checksum)
	sb.WriteString(" comp=")
	sb.WriteString(compSummary)
	sb.WriteString(" rate=")
	sb.WriteString(encoding.HumanRate(speed))
	sb.WriteByte('\n')
	io.WriteString(stdout, sb.String())
}

func startVerboseStatusPolling(txferID string, client *Client, stderr io.Writer) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(defaultVerboseProgressInterval)
		defer ticker.Stop()
		var prevDoneSize int64
		prevTime := time.Now()
		for {
			statusResp, statusErr := client.GetStatus(ctx, GetStatusRequest{
				TransferID: txferID,
			})
			if statusErr != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Fprintf(stderr, "status refresh failed: %v\n", statusErr)
			} else {
				s := statusResp.Status
				now := time.Now()
				dt := now.Sub(prevTime).Seconds()
				var rateBps float64
				if dt > 0 {
					rateBps = float64(s.DoneSize-prevDoneSize) / dt
				}
				prevDoneSize = s.DoneSize
				prevTime = now

				etaPart := ""
				if rateBps > 0 && s.TotalSize > s.DoneSize {
					remaining := float64(s.TotalSize - s.DoneSize)
					etaSec := remaining / rateBps
					etaPart = fmt.Sprintf(" eta=%s", (time.Duration(etaSec * float64(time.Second))).Round(time.Second))
				}
				fmt.Fprintf(
					stderr,
					"transfer-progress:[%6s/%6s](%5.1f%%) bytes=[%8s/%8s](%5.1f%%) rate=%6s%s\n",
					encoding.HumanCount(s.Done, 6), encoding.HumanCount(uint64(s.NumFiles), 6),
					s.PercentFiles,
					encoding.HumanBytes(s.DoneSize), encoding.HumanBytes(s.TotalSize),
					s.PercentBytes,
					encoding.HumanRate(rateBps), etaPart,
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

func manifestEntriesByID(manifest *Manifest) map[uint64]ManifestEntry {
	if manifest == nil || len(manifest.Entries) == 0 {
		return nil
	}
	entries := make(map[uint64]ManifestEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		entries[entry.ID] = entry
	}
	return entries
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

type persistedProgressUpdate struct {
	FileID   uint64
	AckBytes int64
}

func startProgressWriter(progressPath string, initial map[uint64]ManifestProgress, updates <-chan DownloadProgressUpdate, onUpdate func(DownloadProgressUpdate), stderr io.Writer) (func(), func(uint64, int64), func(uint64)) {
	state := initial
	if state == nil {
		state = make(map[uint64]ManifestProgress)
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	persistedProgressCh := make(chan persistedProgressUpdate, 1024)
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
		hasPersistedState := func() bool {
			return len(state) > 0
		}
		flushSnapshot := func(force bool) {
			if !force && !dirty {
				return
			}
			if !hasPersistedState() {
				return
			}
			if err := writeSnapshot(); err != nil {
				fmt.Fprintf(stderr, "progress flush failed: %v\n", err)
				return
			}
			dirty = false
		}
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
		applyPersistedProgress := func(update persistedProgressUpdate) {
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
				case update := <-persistedProgressCh:
					applyPersistedProgress(update)
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
				flushSnapshot(true)
				return
			case update, ok := <-updates:
				if !ok {
					flushSnapshot(hasPersistedState())
					return
				}
				applyProgress(update)
			case update := <-persistedProgressCh:
				applyPersistedProgress(update)
			case update := <-metadataDoneCh:
				applyMetadataDone(update)
			case <-ticker.C:
				flushSnapshot(false)
			}
		}
	}()

	stop := func() {
		close(stopCh)
		<-doneCh
	}
	persistProgressAck := func(fileID uint64, ackBytes int64) {
		update := persistedProgressUpdate{FileID: fileID, AckBytes: ackBytes}
		select {
		case <-doneCh:
			return
		case persistedProgressCh <- update:
		}
	}
	markMetadataDone := func(fileID uint64) {
		update := metadataProgressUpdate{FileID: fileID}
		select {
		case <-doneCh:
			return
		case metadataDoneCh <- update:
		}
	}
	return stop, persistProgressAck, markMetadataDone
}

func refreshCompletedFileMetadata(ctx context.Context, client *Client, manifest *Manifest, fileID uint64, outRoot string, outFile string) error {
	if manifest == nil {
		return errors.New("nil manifest")
	}
	entry, ok := manifest.EntryByID(fileID)
	if !ok {
		return fmt.Errorf("file id %d not in manifest", fileID)
	}
	destPath := outFile
	if destPath == "" {
		destPath = resolveDownloadDestinationPath(entry, outRoot, "")
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
	meta, err := fetchTerminalTrailerMetadataFromChecksum(ctx, client, manifest.TransferID, fileID, serverPath, entry.Size)
	if err != nil {
		return err
	}
	if meta == nil {
		return errors.New("checksum response missing terminal trailer metadata")
	}
	return applyTrailerMetadataToPath(destPath, meta)
}

func fetchTerminalTrailerMetadataFromChecksum(ctx context.Context, client *Client, transferID string, fileID uint64, serverPath string, fileSize int64) (*FileTrailerMetadata, error) {
	resp, err := client.GetChecksum(ctx, GetChecksumRequest{
		TransferID: transferID,
		Targets: []ChecksumTarget{{
			FileID:   fileID,
			FullPath: serverPath,
			Offset:   0,
			Size:     fileSize,
			Algo:     "xxh128",
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("checksum request failed: %w", err)
	}
	defer resp.Reader.Close()
	results, err := readChecksumResults(resp.Reader)
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		if result.FileID == fileID && result.Metadata != nil {
			return result.Metadata, nil
		}
	}
	return nil, nil
}

type checksumFrameHeader struct {
	FileID   uint64
	Offset   int64
	Size     int64
	WireSize int64
}

type checksumFrameTrailer struct {
	FileID        uint64
	FileHashToken string
	Next          int64
	Metadata      *FileTrailerMetadata
}

type checksumResult struct {
	FileID        uint64
	Offset        int64
	Size          int64
	FileHashToken string
	Metadata      *FileTrailerMetadata
}

func readChecksumResults(reader io.Reader) ([]checksumResult, error) {
	br := bufio.NewReader(reader)
	results := make([]checksumResult, 0, 8)
	for {
		headerLine, err := br.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && headerLine == "" {
				return results, nil
			}
			return nil, fmt.Errorf("read checksum frame header: %w", err)
		}
		trimmedHeader := strings.TrimRight(headerLine, "\r\n")
		if trimmedHeader == "" {
			continue
		}
		if isChecksumOKLine(trimmedHeader) {
			return results, nil
		}
		if strings.HasPrefix(trimmedHeader, "ERR ") {
			return nil, errors.New(trimmedHeader)
		}
		header, err := parseChecksumFrameHeader(trimmedHeader)
		if err != nil {
			return nil, err
		}
		if header.WireSize > 0 {
			if _, err := io.CopyN(io.Discard, br, header.WireSize); err != nil {
				return nil, fmt.Errorf("discard checksum frame payload: %w", err)
			}
		}
		trailerLine, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read checksum frame trailer: %w", err)
		}
		trailer, err := parseChecksumFrameTrailer(strings.TrimRight(trailerLine, "\r\n"))
		if err != nil {
			return nil, err
		}
		if trailer.FileID != header.FileID {
			return nil, errors.New("checksum frame trailer file id mismatch")
		}
		results = append(results, checksumResult{
			FileID:        header.FileID,
			Offset:        header.Offset,
			Size:          header.Size,
			FileHashToken: trailer.FileHashToken,
			Metadata:      trailer.Metadata,
		})
	}
}

func isChecksumOKLine(line string) bool {
	line = strings.TrimSpace(line)
	return line == "OK" || strings.HasPrefix(line, "OK ")
}

func parseChecksumFrameHeader(line string) (checksumFrameHeader, error) {
	fields := strings.Fields(line)
	if len(fields) < 3 || fields[0] != "FX/1" {
		return checksumFrameHeader{}, errors.New("invalid checksum frame header")
	}
	fileID, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return checksumFrameHeader{}, errors.New("invalid checksum frame file id")
	}
	props := make(map[string]string, len(fields)-2)
	for _, token := range fields[2:] {
		key, val, ok := strings.Cut(token, "=")
		if ok {
			props[key] = val
		}
	}
	offset, err := strconv.ParseInt(props["offset"], 10, 64)
	if err != nil || offset < 0 {
		return checksumFrameHeader{}, errors.New("invalid checksum frame offset")
	}
	size, err := strconv.ParseInt(props["size"], 10, 64)
	if err != nil || size < 0 {
		return checksumFrameHeader{}, errors.New("invalid checksum frame size")
	}
	wsize, err := strconv.ParseInt(props["wsize"], 10, 64)
	if err != nil || wsize < 0 {
		return checksumFrameHeader{}, errors.New("invalid checksum frame wsize")
	}
	return checksumFrameHeader{
		FileID:   fileID,
		Offset:   offset,
		Size:     size,
		WireSize: wsize,
	}, nil
}

func parseChecksumFrameTrailer(line string) (checksumFrameTrailer, error) {
	fields := strings.Fields(line)
	if len(fields) < 3 || fields[0] != "FXT/1" {
		return checksumFrameTrailer{}, errors.New("invalid checksum frame trailer")
	}
	fileID, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return checksumFrameTrailer{}, errors.New("invalid checksum frame trailer file id")
	}
	next := int64(-1)
	meta := &FileTrailerMetadata{}
	hasMeta := false
	fileHashToken := ""
	for _, token := range fields[2:] {
		if strings.HasPrefix(token, "next=") {
			nextRaw := strings.TrimPrefix(token, "next=")
			next, err = strconv.ParseInt(nextRaw, 10, 64)
			if err != nil || next < 0 {
				return checksumFrameTrailer{}, errors.New("invalid checksum frame trailer next offset")
			}
			continue
		}
		if strings.HasPrefix(token, "file-hash=") {
			fileHashToken = strings.TrimPrefix(token, "file-hash=")
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
	if next != 0 {
		return checksumFrameTrailer{}, errors.New("checksum frame trailer next offset must be 0")
	}
	if !hasMeta {
		meta = nil
	}
	return checksumFrameTrailer{
		FileID:        fileID,
		FileHashToken: fileHashToken,
		Next:          next,
		Metadata:      meta,
	}, nil
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
	if meta.MtimeNS > 0 {
		mtime := time.Unix(0, meta.MtimeNS)
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			return fmt.Errorf("set destination mtime to %d: %w", meta.MtimeNS, err)
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
		"file progress[%d]: %d%% bytes=%s/%s [%s] rate=%s eta=%s\n",
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
