package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"filippo.io/age"

	"github.com/jolynch/pinch/internal/cmd/filexfercli"
	"github.com/jolynch/pinch/internal/filexfer/fhttp"
	"github.com/jolynch/pinch/internal/filexfer/limit"
	"github.com/jolynch/pinch/state"
	"github.com/jolynch/pinch/utils"
)

var (
	listen       = "127.0.0.1:8080"
	inputDir     = "/var/lib/pinch/in"
	outputDir    = "/var/lib/pinch/out"
	keysDir      = "/var/lib/pinch/keys"
	tokenLength  = 8
	bufSizeBytes = 128 * 1024 // Usually pipes are 64KiB, we bump it slightly
	serverKey    *age.X25519Identity
	fsFileRate   = ""
	fsFileBurst  = "1MiB"
)

func maxSocketWriteBufferBytes() int {
	// 4MiB baseline if kernel cap cannot be read.
	const baseline = 4 * 1024 * 1024
	raw, err := os.ReadFile("/proc/sys/net/core/wmem_max")
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

func compress(
	fifos utils.FifoPair,
	timeout time.Duration,
	minLevel int,
	maxLevel int,
	encKey interface{},
) {
	defer fifos.Close()
	start := time.Now()
	input, output, name := fifos.InPath, fifos.OutPath, fifos.Handle

	var compressorCmd string
	var compressor string
	if minLevel == 0 {
		if encKey == nil {
			compressorCmd = "zstd -v --adapt=max=%d - -o %s"
			compressor = fmt.Sprintf(compressorCmd, maxLevel, output)
		} else {
			compressorCmd = "zstd -v --adapt=max=%d - | age -r %s -o %s"
			compressor = fmt.Sprintf(compressorCmd, maxLevel, encKey.(string), output)
		}
	} else {
		if encKey == nil {
			compressorCmd = "zstd -v --adapt=min=%d,max=%d - -o %s"
			compressor = fmt.Sprintf(compressorCmd, minLevel, maxLevel, output)
		} else {
			compressorCmd = "zstd -v --adapt=min=%d,max=%d - | age -r %s -o %s"
			compressor = fmt.Sprintf(compressorCmd, minLevel, maxLevel, encKey.(string), output)
		}
	}

	pipeline := fmt.Sprintf(
		"$(command -v pipetee || echo 'tee') < %s >(xxh128sum - > %s) >(b3sum --num-threads 1 - > %s) | %s",
		input,
		output+".xxh128",
		output+".blake3",
		compressor,
	)

	log.Printf("[%s][pinch]: Spawning pipeline with timeout [%s] -> [%s]", name, timeout, pipeline)
	log.Printf("[%s][pinch]: Produce data to   [%s]", name, input)
	log.Printf("[%s][pinch]: Consume data from [%s]", name, output)

	cmdTerm, cmdKill := utils.KillAfter(timeout)
	cmd := exec.Command(
		"time",
		"timeout", "-k", cmdKill, cmdTerm,
		"bash", "-o", "pipefail", "-c", pipeline,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	// To help make reaping processes on timeout easier
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	state.PreparePipeline(name)
	err := cmd.Run()

	if err != nil {
		log.Printf("[%s][pinch]: Failed! with error %s", name, err)
		// Try to remove the hash files
		state.CleanupDigests(output)
		state.FinishPipeline(
			name,
			state.PipelineResult{
				Start:    start,
				Duration: fmt.Sprintf("%s", time.Since(start)),
				Success:  false,
				Stderr:   stderr.String(),
			},
			timeout,
			output,
		)
	} else {
		// Hack to make carriage returns into newlines
		msg := strings.ReplaceAll(stderr.String(), "\r", "\r\n")
		log.Printf("[%s][pinch]: Succeeded after waiting [%s]", name, time.Since(start))
		log.Print("[" + name + "]\n" + msg)
		var xxhash string = "UNKNOWN"
		var blake3 string = "UNKNOWN"
		xfd, err := os.Open(output + ".xxh128")
		if err == nil {
			defer xfd.Close()
			fmt.Fscanf(xfd, "%s", &xxhash)
		}
		bfd, err := os.Open(output + ".blake3")
		if err == nil {
			defer bfd.Close()
			fmt.Fscanf(bfd, "%s", &blake3)
		}

		state.FinishPipeline(
			name,
			state.PipelineResult{
				Start:     start,
				Duration:  fmt.Sprintf("%s", time.Since(start)),
				Success:   true,
				Stderr:    msg,
				Checksums: state.Checksums{Xxh128: xxhash, Blake3: blake3},
			},
			timeout,
			output,
		)
	}
	log.Printf("[%s][pinch]: Done", name)
}

func decompress(
	fifos utils.FifoPair,
	timeout time.Duration,
	encKey interface{},
) {
	defer fifos.Close()
	start := time.Now()
	input, output, name := fifos.InPath, fifos.OutPath, fifos.Handle
	var decompressor string

	if encKey == nil {
		decompressor = fmt.Sprintf("zstd -d %s -c", input)
	} else {
		decompressor = fmt.Sprintf(
			"age -d -i %s/%s %s | zstd -d -c",
			keysDir, encKey.(string), input,
		)
	}

	pipeline := fmt.Sprintf(
		"%s | $(command -v pipetee || echo 'tee') >(xxh128sum - > %s) >(b3sum --num-threads 1 - > %s) > %s",
		decompressor,
		output+".xxh128",
		output+".blake3",
		output,
	)

	log.Printf("[%s][unpinch]: Spawning pipeline with timeout [%s] -> [%s]", name, timeout, pipeline)
	log.Printf("[%s][unpinch]: Produce data to   [%s]", name, input)
	log.Printf("[%s][unpinch]: Consume data from [%s]", name, output)

	cmdTerm, cmdKill := utils.KillAfter(timeout)
	cmd := exec.Command(
		"time",
		"timeout", "-k", cmdKill, cmdTerm,
		"bash", "-o", "pipefail", "-c", pipeline,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	// To help make reaping processes on timeout easier
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	state.PreparePipeline(name)
	err := cmd.Run()

	if err != nil {
		log.Printf("[%s][unpinch]: Failed! with error %s", name, err)
		// Try to remove the hash files and record a failure
		state.CleanupDigests(output)
		state.FinishPipeline(
			name,
			state.PipelineResult{
				Start:    start,
				Duration: fmt.Sprintf("%s", time.Since(start)),
				Success:  false,
				Stderr:   stderr.String(),
			},
			timeout,
			output,
		)
	} else {
		log.Printf("[%s][unpinch]: Succeeded after waiting %s", name, time.Since(start))
		log.Print("[" + name + "]\n" + stderr.String())
		var xxhash string = "UNKNOWN"
		var blake3 string = "UNKNOWN"
		xfd, err := os.Open(output + ".xxh128")
		if err == nil {
			defer xfd.Close()
			fmt.Fscanf(xfd, "%s", &xxhash)
		}
		bfd, err := os.Open(output + ".blake3")
		if err == nil {
			defer bfd.Close()
			fmt.Fscanf(bfd, "%s", &blake3)
		}

		state.FinishPipeline(
			name,
			state.PipelineResult{
				Start:     start,
				Duration:  fmt.Sprintf("%s", time.Since(start)),
				Success:   true,
				Stderr:    stderr.String(),
				Checksums: state.Checksums{Xxh128: xxhash, Blake3: blake3},
			},
			timeout,
			output,
		)
	}

	log.Printf("[%s][unpinch]: Done", name)
}

func pinch(w http.ResponseWriter, req *http.Request) {
	var (
		maxLevel int           = 10
		minLevel int           = 0
		timeout  time.Duration = time.Duration(60 * time.Second)
		numPaths int           = 1
		encKey   interface{}
		err      error
	)

	m, ok := req.URL.Query()["max-level"]
	if ok && len(m) > 0 && len(m[0]) > 0 {
		maxLevel, err = strconv.Atoi(m[0])
		// Zstd needs a lot more memory above 19
		if err != nil || maxLevel > 19 {
			http.Error(w, "Invalid max-level", http.StatusBadRequest)
			return
		}
	}

	m, ok = req.URL.Query()["min-level"]
	if ok && len(m) > 0 && len(m[0]) > 0 {
		minLevel, err = strconv.Atoi(m[0])
		// Zstd adapt doesn't accept negative levels as min-level
		if err != nil || minLevel < 0 {
			http.Error(w, "Invalid min-level", http.StatusBadRequest)
			return
		}
	}

	t, ok := req.URL.Query()["timeout"]
	if ok && len(t) > 0 && len(t[0]) > 0 {
		timeout, err = time.ParseDuration(t[0])
		if err != nil || timeout < time.Second {
			http.Error(w, "Invalid timeout, try something larger than 1s like 60s or 1m", http.StatusBadRequest)
			return
		}
	}

	k, ok := req.URL.Query()["age-public-key"]
	if ok {
		if len(k[0]) > 0 {
			encKey = k[0]
		} else {
			encKey = serverKey.Recipient().String()
		}
	}

	n, ok := req.URL.Query()["num-handles"]
	if ok && len(n) > 0 && len(n[0]) > 0 {
		numPaths, err = strconv.Atoi(n[0])
		if err != nil {
			http.Error(w, "Invalid num-handles", http.StatusBadRequest)
			return
		}
	}

	type compparams struct {
		Algorithm string `json:"algorithm,omitempty"`
		Extension string `json:"extension,omitempty"`
		MaxLevel  int    `json:"max-level,omitempty"`
		MinLevel  int    `json:"min-level,omitempty"`
	}
	type encparams struct {
		Algorithm string `json:"algorithm,omitempty"`
		Extension string `json:"extension,omitempty"`
		PublicKey string `json:"public-key,omitempty"`
	}
	type io struct {
		Http    string `json:"io-http"`
		InPipe  string `json:"in-pipe"`
		OutPipe string `json:"out-pipe"`
	}
	type resp struct {
		Handles           map[string]io `json:"handles"`
		CompressionParams compparams    `json:"compression,omitempty"`
		EncryptionParams  encparams     `json:"encryption,omitempty"`
		Ttl               time.Duration `json:"time-to-live"`
	}

	response := resp{
		Handles: make(map[string]io),
		CompressionParams: compparams{
			Algorithm: "zstd:adapt",
			Extension: "zst",
			MaxLevel:  maxLevel,
		},
		EncryptionParams: encparams{
			Algorithm: "plaintext",
		},
		Ttl: timeout,
	}

	if minLevel != 0 {
		log.Printf("Setting minlevel to %d", minLevel)
		response.CompressionParams.MinLevel = minLevel
	} else {
		log.Printf("Not setting minlevel")
	}

	if encKey != nil {
		response.EncryptionParams = encparams{
			Algorithm: "age:chacha20poly1305",
			Extension: "age",
			PublicKey: encKey.(string),
		}
	}

	var handle string
	for i := 0; i < numPaths; i++ {
		handle = token(tokenLength)
		// Make the pipes first so that when this function returns
		// the input stages and output stages are ready to start
		// consuming (even if we haven't wired them up yet)
		fifos := utils.MakeFifoPair(inputDir, outputDir, handle, bufSizeBytes)
		if fifos.In == nil || fifos.Out == nil {
			fifos.Close()
			http.Error(w, fmt.Sprintf("Could not create pipes %+v", fifos), http.StatusInternalServerError)
			return
		}

		response.Handles[handle] = io{
			Http:    "http://" + listen + "/io/" + handle,
			InPipe:  fifos.InPath,
			OutPipe: fifos.OutPath,
		}

		go compress(
			fifos,
			timeout,
			minLevel,
			maxLevel,
			encKey,
		)
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Printf("ERROR %s", err)
	}
}

func unpinch(w http.ResponseWriter, req *http.Request) {
	var (
		timeout  time.Duration = time.Duration(60 * time.Second)
		numPaths int           = 1
		encKey   interface{}
		err      error
	)

	t, ok := req.URL.Query()["timeout"]
	if ok && len(t) > 0 && len(t[0]) > 0 {
		timeout, err = time.ParseDuration(t[0])
		if err != nil || timeout < time.Second {
			http.Error(w, "Invalid timeout, try something larger than 1s like 60s or 1m", http.StatusBadRequest)
			return
		}
	}

	n, ok := req.URL.Query()["num-handles"]
	if ok && len(n) > 0 && len(n[0]) > 0 {
		numPaths, err = strconv.Atoi(n[0])
		if err != nil {
			http.Error(w, "Invalid num-handles", http.StatusBadRequest)
			return
		}
	}

	k, ok := req.URL.Query()["age-key-path"]
	if ok && len(k) > 0 && len(k[0]) > 0 {
		encKey = k[0]
	}

	type io struct {
		Http    string `json:"io-http"`
		InPipe  string `json:"in-pipe"`
		OutPipe string `json:"out-pipe"`
	}

	type resp struct {
		Handles map[string]io `json:"handles"`
		Ttl     time.Duration `json:"time-to-live"`
	}

	response := resp{
		Handles: make(map[string]io),
		Ttl:     timeout,
	}

	var handle string
	for i := 0; i < numPaths; i++ {
		handle = token(tokenLength)

		// Make the pipes first so that when this function returns
		// the input stages and output stages are ready to start
		// consuming (even if we haven't wired them up yet)
		fifos := utils.MakeFifoPair(inputDir, outputDir, handle, bufSizeBytes)
		if fifos.In == nil || fifos.Out == nil {
			fifos.Close()
			http.Error(w, "Could not create pipes", http.StatusInternalServerError)
			return
		}

		response.Handles[handle] = io{
			Http:    "http://" + listen + "/io/" + handle,
			InPipe:  fifos.InPath,
			OutPipe: fifos.OutPath,
		}

		go decompress(
			fifos,
			timeout,
			encKey,
		)

	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Printf("ERROR %s", err)
	}
}

func getStatus(w http.ResponseWriter, req *http.Request) {
	var (
		name    string        = strings.TrimPrefix(req.URL.Path, "/status/")
		waitFor time.Duration = time.Duration(1 * time.Second)
		err     error
	)

	if len(name) < 1 {
		http.Error(w, "Must supply handle as /status/{handle} suffix", http.StatusBadRequest)
		return
	}

	wf, ok := req.URL.Query()["wait-for"]
	if ok && len(wf) > 0 && len(wf[0]) > 0 {
		waitFor, err = time.ParseDuration(wf[0])
		if err != nil {
			http.Error(w, "Invalid wait-for", http.StatusBadRequest)
			return
		}
	}

	start := time.Now()
	value, ok := state.WaitForPipeline(name, waitFor)
	log.Printf("[%s][status] Waited for %s", name, time.Since(start))
	if ok {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(value); err != nil {
			log.Printf("[%s][status] failed to encode response: %v", name, err)
		}
	} else {
		http.Error(w, "Could not find handle: "+name, http.StatusNotFound)
	}
}

func handleIO(w http.ResponseWriter, req *http.Request) {
	var name string
	name = strings.TrimPrefix(req.URL.Path, "/io/")

	if len(name) < 1 {
		http.Error(w, "Must supply handle as /io/{handle} suffix", http.StatusBadRequest)
		return
	}

	if req.Method == http.MethodPut {
		writeChunk(name, w, req)
	} else {
		readChunk(name, w, req)
	}
}

func writeChunk(name string, w http.ResponseWriter, req *http.Request) {
	fd := state.AcquireWriter(inputDir, name).Fd
	if fd == nil {
		http.Error(w, "No such handle: "+name, http.StatusNotFound)
		return
	}

	_, partial := req.URL.Query()["partial"]
	_, writeOnly := req.URL.Query()["writeonly"]

	readFinished := make(chan bool)
	if !writeOnly {
		// In ReadWrite mode we send back a response with the processed
		// data, so headers become trailers after we have processed the data
		w.Header().Set("Trailer", "X-Pinch-Bytes-Written")
		go doReadChunk(w, name, readFinished)
	} else {
		readFinished <- true
	}

	log.Printf("[%s][write]: Copying to pipe [%s]", name, path.Join(inputDir, name))
	buf := make([]byte, bufSizeBytes)
	bytesWritten, err := io.CopyBuffer(fd, req.Body, buf)
	if err != nil {
		log.Printf("[%s][write]: Failed due to %s", name, err)
		http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
		return
	} else {
		if bytesWritten > 0 {
			log.Printf("[%s][write]: Copied %d bytes to pipe", name, bytesWritten)
			w.Header().Set("X-Pinch-Bytes-Written", strconv.FormatInt(bytesWritten, 10))
		}
	}

	if !partial {
		log.Printf("[%s][write]: Closing writer due to lack of partial flag", name)
		state.MaybeReleaseWriter(name)
	}

	<-readFinished
	if writeOnly {
		w.WriteHeader(http.StatusNoContent)
	}
}

func doReadChunk(w http.ResponseWriter, name string, finished chan bool) {
	fd, err := os.Open(path.Join(outputDir, name))
	if err != nil {
		log.Printf("[%s][read]: ERROR: no such handle: %s", name, err)
		http.Error(w, "No such handle: "+name, http.StatusNotFound)
		finished <- false
		return
	} else {
		defer fd.Close()
		log.Printf("[%s][read]: Opened [%s]", name, path.Join(outputDir, name))
	}

	w.Header().Set("Connection", "Keep-Alive")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// When completing a pinch/unpinch we will have checksums attached
	w.Header().Add("Trailer", "X-Pinch-Bytes-Read")
	w.Header().Add("Trailer", "X-Pinch-XXH128")
	w.Header().Add("Trailer", "X-Pinch-BLAKE3")

	log.Printf("[%s][read]: Copying from pipe [%s]", name, path.Join(outputDir, name))
	w.Header().Set("Content-Type", "application/octet-stream")

	buf := make([]byte, bufSizeBytes)
	bytesRead, err := io.CopyBuffer(w, fd, buf)
	if err != nil {
		log.Printf("[%s] [read] ERROR: %s", name, err)
		http.Error(w, fmt.Sprintf("Error reading from %s: %s", name, err), http.StatusInternalServerError)
		finished <- false
		return
	} else {
		if bytesRead > 0 {
			log.Printf("[%s][read]: Copied %d bytes from pipe", name, bytesRead)
		}
		w.Header().Set("X-Pinch-Bytes-Read", strconv.FormatInt(bytesRead, 10))

	}

	finished <- true
}

func readChunk(name string, w http.ResponseWriter, req *http.Request) {
	readFinished := make(chan bool)
	doReadChunk(w, name, readFinished)
	<-readFinished
}

func token(length int) string {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		log.Printf("Failed to generate random token bytes: %v", err)
		return ""
	}
	return hex.EncodeToString(b)
}

func die(duration time.Duration) {
	log.Printf("Will die after %s", duration)
	time.Sleep(duration)
	log.Printf("Goodbye dying now ...")
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
}

func makeDirs(path string) bool {
	err := os.MkdirAll(path, 0o777)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func cleanupDir(dirPath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}
	for _, d := range entries {
		entryPath := path.Join(dirPath, d.Name())
		log.Printf("[cleanup] Cleaning up %s", entryPath)
		if err := os.RemoveAll(entryPath); err != nil {
			return err
		}
	}
	return nil
}

func shouldRunCLI(args []string) bool {
	return len(args) > 1 && args[1] == "cli"
}

func main() {
	if shouldRunCLI(os.Args) {
		os.Exit(filexfercli.RunCLI(os.Args[2:], os.Stdout, os.Stderr))
	}

	flag.StringVar(&listen, "listen", listen, "The address to listen on")
	flag.StringVar(&inputDir, "in", inputDir, "The directory to create input pipes in")
	flag.StringVar(&outputDir, "out", outputDir, "The directory to create output pipes in")
	flag.StringVar(&keysDir, "keys", keysDir, "The directory to create output pipes in")
	flag.IntVar(&tokenLength, "tlen", tokenLength, "How long of paths to generate")
	flag.IntVar(&bufSizeBytes, "blen", bufSizeBytes, "How many bytes should buffers be")
	flag.StringVar(&fsFileRate, "fs-file-rate", fsFileRate, "Global /fs/file rate limit (examples: 100MiB, 1000mbps). Empty/0 disables limiting")
	flag.StringVar(&fsFileBurst, "fs-file-rate-burst", fsFileBurst, "Token-bucket burst for /fs/file rate limit (examples: 1MiB, 4MB)")
	fsFileTimeLimit := flag.Duration("fs-file-time-limit", 0, "Per-request wall-clock limit for GET /fs/file (0 disables)")
	dieAfter := flag.Duration("die-after", 0, "Die after this duration. Zero seconds indicates live forever")

	flag.Parse()

	if err := limit.ConfigureFileStreamLimiter(fsFileRate, fsFileBurst, *fsFileTimeLimit); err != nil {
		log.Fatalf("Invalid file stream limiter configuration: %v", err)
	}

	if !makeDirs(inputDir) {
		log.Fatalf("Could not setup input directory, dying")
	}
	if !makeDirs(outputDir) {
		log.Fatalf("Could not setup output direcotry, dying")
	}
	if !makeDirs(keysDir) {
		log.Fatalf("Could not setup key directory, dying")
	}
	serverKey, err := age.GenerateX25519Identity()
	if err != nil {
		log.Fatalf("AGE could not generate private and public keys for this node")
	} else {
		keyPath := path.Join(keysDir, "key")
		out, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)

		if err != nil {
			log.Fatalf("Error while opening key file: %v", err)
		}
		fmt.Fprintf(out, "# created: %s\n", time.Now().Format(time.RFC3339))
		fmt.Fprintf(out, "# public key: %s\n", serverKey.Recipient())
		fmt.Fprintf(out, "%s\n", serverKey)
		log.Printf("Public key %s", serverKey.Recipient().String())
		defer out.Close()
		log.Printf("AGE key file generated and saved to %s", keyPath)
	}

	log.Printf("Scanning input directory [%s] to clean up.", inputDir)
	if err := cleanupDir(inputDir); err != nil {
		log.Fatalf("failed cleaning input directory %s: %v", inputDir, err)
	}
	log.Printf("Scanning output directory [%s] to clean up", outputDir)
	if err := cleanupDir(outputDir); err != nil {
		log.Fatalf("failed cleaning output directory %s: %v", outputDir, err)
	}

	// Pinch API
	mux := http.NewServeMux()
	mux.HandleFunc("/pinch", pinch)
	mux.HandleFunc("/unpinch", unpinch)
	mux.HandleFunc("/io/", handleIO)
	mux.HandleFunc("/status/", getStatus)

	// Filesystem API
	mux.HandleFunc("PUT /fs/transfer", fhttp.TransferHandler)
	mux.HandleFunc("GET /fs/transfer/{txferid}/status", fhttp.TransferStatusHandler)
	mux.HandleFunc("GET /fs/file/{txferid}/{fid}", fhttp.FileHandler)
	mux.HandleFunc("PUT /fs/file/{txferid}/{fid}/ack", fhttp.FileAckHandler)
	mux.HandleFunc("GET /fs/file/{txferid}/{fid}/checksum", fhttp.FileChecksumHandler)

	if *dieAfter > 0 {
		go die(*dieAfter)
	}

	log.Printf("Listening at %s/pinch", listen)
	socketWriteBufBytes := maxSocketWriteBufferBytes()
	log.Printf("Detected ideal socket write buffer of size %d", socketWriteBufBytes)
	server := &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
				_ = tc.SetWriteBuffer(socketWriteBufBytes)
			}
			return ctx
		},
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Failed to bind, is another server listening at this address? error=%v", err)
	} else {
		log.Printf("Success!")
	}
}
