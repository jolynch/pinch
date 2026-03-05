package filexfer

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	benchDataRoot = "/var/tmp/pinch-filexfer-bench-data"
	benchOutRoot  = "/var/tmp/pinch-filexfer-bench-out"
)

type benchFileGroup struct {
	Prefix string
	Count  int
	Size   int64
}

type benchWorkloadSpec struct {
	Name   string
	Groups []benchFileGroup
}

var benchWorkloads = map[string]benchWorkloadSpec{
	"small": {
		Name: "small",
		Groups: []benchFileGroup{
			// 100 * 8 MiB = 0.8 GiB
			{Prefix: "small", Count: 100, Size: 8 * 1024 * 1024},
		},
	},
	"mixed": {
		Name: "mixed",
		Groups: []benchFileGroup{
			// 100 * 8 MiB = 0.8 GiB
			{Prefix: "small", Count: 100, Size: 8 * 1024 * 1024},
			// 3 * 800 MiB = 2.4 GiB
			{Prefix: "large", Count: 3, Size: 800 * 1024 * 1024},
		},
	},
	"large": {
		Name: "large",
		Groups: []benchFileGroup{
			// 3 * 2 GiB = 6.0 GiB
			{Prefix: "large", Count: 3, Size: 2 * 1024 * 1024 * 1024},
		},
	},
}

func BenchmarkFileHandlerSmall(b *testing.B) {
	runFileHandlerBenchmark(b, benchWorkloads["small"])
}

func BenchmarkFileHandlerMixed(b *testing.B) {
	runFileHandlerBenchmark(b, benchWorkloads["mixed"])
}

func BenchmarkFileHandlerLarge(b *testing.B) {
	runFileHandlerBenchmark(b, benchWorkloads["large"])
}

func runFileHandlerBenchmark(b *testing.B, spec benchWorkloadSpec) {
	b.Helper()
	b.ReportAllocs()

	concurrency := envInt("BENCH_CONCURRENCY", runtime.NumCPU()*2)
	if concurrency < 1 {
		concurrency = 1
	}
	maxN := envInt("BENCH_MAX_N", 1)
	if maxN < 1 {
		maxN = 1
	}

	b.StopTimer()
	dataDir, err := ensureBenchData(benchDataRoot, spec)
	if err != nil {
		b.Fatalf("[%s] ensure bench data: %v", spec.Name, err)
	}
	server := newBenchServer()
	defer server.Close()
	client := NewClient(server.URL, nil)
	b.StartTimer()

	iterations := b.N
	if iterations > maxN {
		b.Logf("[%s] capping iterations: b.N=%d BENCH_MAX_N=%d", spec.Name, b.N, maxN)
		iterations = maxN
	}

	var totalBytes int64
	var totalFiles int64

	for i := 0; i < iterations; i++ {
		b.StopTimer()
		iterOut := filepath.Join(benchOutRoot, spec.Name, fmt.Sprintf("iter-%d", i))
		if err := os.RemoveAll(iterOut); err != nil {
			b.Fatalf("[%s] remove output: %v", spec.Name, err)
		}
		if err := os.MkdirAll(iterOut, 0o755); err != nil {
			b.Fatalf("[%s] create output: %v", spec.Name, err)
		}
		b.StartTimer()

		manifestResp, err := client.FetchManifest(context.Background(), FetchManifestRequest{
			Directory: dataDir,
		})
		if err != nil {
			b.Fatalf("[%s] fetch manifest: %v", spec.Name, err)
		}
		manifest := manifestResp.Manifest
		sort.Slice(manifest.Entries, func(i, j int) bool {
			return manifest.Entries[i].ID < manifest.Entries[j].ID
		})

		iterBytes, iterFiles, err := downloadAllFromManifest(context.Background(), client, manifest, iterOut, concurrency)
		if err != nil {
			b.Fatalf("[%s] download all: %v", spec.Name, err)
		}
		totalBytes += iterBytes
		totalFiles += iterFiles
	}

	elapsed := b.Elapsed().Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(totalBytes)/(1024*1024)/elapsed, "MiB/s")
		b.ReportMetric(float64(totalFiles)/elapsed, "files/s")
	}
	if iterations > 0 {
		b.SetBytes(totalBytes / int64(iterations))
	}
}

func downloadAllFromManifest(ctx context.Context, client *Client, manifest *Manifest, outRoot string, concurrency int) (int64, int64, error) {
	workCh := make(chan ManifestEntry)
	errCh := make(chan error, len(manifest.Entries))
	var wg sync.WaitGroup
	var bytesDownloaded atomic.Int64
	var filesDownloaded atomic.Int64

	worker := func() {
		defer wg.Done()
		for entry := range workCh {
			resp, err := client.DownloadFileFromManifest(ctx, DownloadFileRequest{
				Manifest:       manifest,
				FileID:         entry.ID,
				OutRoot:        outRoot,
				AcceptEncoding: "adapt",
			})
			if err != nil {
				errCh <- fmt.Errorf("id=%d: %w", entry.ID, err)
				continue
			}
			bytesDownloaded.Add(resp.Meta.Size)
			filesDownloaded.Add(1)
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

	for err := range errCh {
		return bytesDownloaded.Load(), filesDownloaded.Load(), err
	}
	return bytesDownloaded.Load(), filesDownloaded.Load(), nil
}

func ensureBenchData(baseDir string, spec benchWorkloadSpec) (string, error) {
	root := filepath.Join(baseDir, spec.Name)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	for _, group := range spec.Groups {
		for i := 0; i < group.Count; i++ {
			path := filepath.Join(root, fmt.Sprintf("%s-%04d.bin", group.Prefix, i))
			if err := ensureSizedFile(path, group.Size, int64(i+len(group.Prefix))); err != nil {
				return "", err
			}
		}
	}
	return root, nil
}

func ensureSizedFile(path string, size int64, seed int64) error {
	info, err := os.Stat(path)
	if err == nil && info.Size() == size {
		return nil
	}
	if err == nil && info.IsDir() {
		return fmt.Errorf("expected file, found directory: %s", path)
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	fd, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer fd.Close()

	rng := rand.New(rand.NewSource(seed + 1))
	block := make([]byte, 1024*1024)
	if _, err := rng.Read(block); err != nil {
		return err
	}

	remaining := size
	for remaining > 0 {
		n := int64(len(block))
		if n > remaining {
			n = remaining
		}
		if _, err := fd.Write(block[:n]); err != nil {
			return err
		}
		remaining -= n
	}
	if err := fd.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func newBenchServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /fs/transfer", TransferHandler)
	mux.HandleFunc("GET /fs/file/{txferid}/{fid}", FileHandler)
	mux.HandleFunc("PUT /fs/file/{txferid}/{fid}/ack", FileAckHandler)
	mux.HandleFunc("GET /fs/transfer/{txferid}/status", TransferStatusHandler)
	return httptest.NewServer(mux)
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}
