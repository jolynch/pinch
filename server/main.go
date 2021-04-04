package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jolynch/pinch/state"
	"github.com/jolynch/pinch/utils"
)

var (
	listen       = "127.0.0.1:8080"
	inputDir     = "/run/pinch/in"
	outputDir    = "/run/pinch/out"
	keysDir      = "/run/pinch/keys"
	tokenLength  = 8
	bufSizeBytes = 128 * 1024 // Usually pipes are 64KiB, we bump it slightly
)

func compress(
	name string,
	input string,
	output string,
	timeout time.Duration,
	minLevel int,
	maxLevel int,
	encKey interface{},
) {
	start := time.Now()

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
		"pipetee < %s >(xxh128sum - > %s) >(b3sum --num-threads 1 - > %s) | %s",
		input,
		output+".xxh128",
		output+".blake3",
		compressor,
	)

	log.Printf("[%s][pinch]: Spawning pipeline with timeout [%s] -> [%s]", name, timeout, pipeline)
	log.Printf("[%s][pinch]: Produce data to   [%s]", name, input)
	log.Printf("[%s][pinch]: Consume data from [%s]", name, output)

	cmd := exec.Command(
		"time",
		"timeout", strconv.FormatInt(int64(timeout/time.Second), 10),
		"bash", "-o", "pipefail", "-c", pipeline,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

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
				Duration: time.Since(start),
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
				Duration:  time.Since(start),
				Success:   true,
				Stderr:    msg,
				Checksums: state.Checksums{Xxh128: xxhash, Blake3: blake3},
			},
			timeout,
			output,
		)
	}

	os.Remove(input)
	os.Remove(output)
	log.Printf("[%s][pinch]: Done", name)
}

func decompress(
	name string,
	input string,
	output string,
	timeout time.Duration,
	encKey interface{},
) {
	start := time.Now()
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
		"%s | pipetee >(xxh128sum - > %s) >(b3sum --num-threads 1 - > %s) > %s",
		decompressor,
		output+".xxh128",
		output+".blake3",
		output,
	)

	log.Printf("[%s][unpinch]: Spawning pipeline with timeout [%s] -> [%s]", name, timeout, pipeline)
	log.Printf("[%s][unpinch]: Produce data to   [%s]", name, input)
	log.Printf("[%s][unpinch]: Consume data from [%s]", name, output)

	cmd := exec.Command(
		"time",
		"timeout", strconv.FormatInt(int64(timeout/time.Second), 10),
		"bash", "-o", "pipefail", "-c", pipeline,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

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
				Duration: time.Since(start),
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
				Duration:  time.Since(start),
				Success:   true,
				Stderr:    stderr.String(),
				Checksums: state.Checksums{Xxh128: xxhash, Blake3: blake3},
			},
			timeout,
			output,
		)
	}

	os.Remove(input)
	os.Remove(output)
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
	if ok && len(m[0]) >= 0 {
		maxLevel, err = strconv.Atoi(m[0])
		if err != nil {
			http.Error(w, "Invalid max-level", http.StatusBadRequest)
			return
		}
	}

	m, ok = req.URL.Query()["max-level"]
	if ok && len(m[0]) >= 0 {
		minLevel, err = strconv.Atoi(m[0])
		if err != nil {
			http.Error(w, "Invalid max-level", http.StatusBadRequest)
			return
		}
	}

	t, ok := req.URL.Query()["timeout"]
	if ok && len(t[0]) >= 0 {
		timeout, err = time.ParseDuration(t[0])
		if err != nil || timeout < time.Second {
			http.Error(w, "Invalid timeout, try something larger than 1s like 60s or 1m", http.StatusBadRequest)
			return
		}
	}

	k, ok := req.URL.Query()["age-public-key"]
	if ok && len(k[0]) >= 0 {
		encKey = k[0]
	}

	n, ok := req.URL.Query()["num-handles"]
	if ok && len(n[0]) >= 0 {
		numPaths, err = strconv.Atoi(n[0])
		if err != nil {
			http.Error(w, "Invalid num-handles", http.StatusBadRequest)
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
		utils.MakeFifo(path.Join(inputDir, handle), bufSizeBytes)
		utils.MakeFifo(path.Join(outputDir, handle), bufSizeBytes)

		response.Handles[handle] = io{
			Http:    "http://" + listen + "/io/" + handle,
			InPipe:  path.Join(inputDir, handle),
			OutPipe: path.Join(outputDir, handle),
		}

		go compress(
			handle,
			path.Join(inputDir, handle),
			path.Join(outputDir, handle),
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
	if ok && len(t[0]) >= 0 {
		timeout, err = time.ParseDuration(t[0])
		if err != nil || timeout < time.Second {
			http.Error(w, "Invalid timeout, try something larger than 1s like 60s or 1m", http.StatusBadRequest)
			return
		}
	}

	n, ok := req.URL.Query()["num-handles"]
	if ok && len(n[0]) >= 0 {
		numPaths, err = strconv.Atoi(n[0])
		if err != nil {
			http.Error(w, "Invalid num-handles", http.StatusBadRequest)
		}
	}

	k, ok := req.URL.Query()["age-key-path"]
	if ok && len(k[0]) >= 0 {
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
		utils.MakeFifo(path.Join(inputDir, handle), bufSizeBytes)
		utils.MakeFifo(path.Join(outputDir, handle), bufSizeBytes)

		response.Handles[handle] = io{
			Http:    "http://" + listen + "/write/" + handle,
			InPipe:  path.Join(inputDir, handle),
			OutPipe: path.Join(outputDir, handle),
		}

		go decompress(
			handle,
			path.Join(inputDir, handle),
			path.Join(outputDir, handle),
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
	if ok && len(wf[0]) >= 0 {
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
		json.NewEncoder(w).Encode(value)
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
	var fd *os.File = state.AcquireWriter(inputDir, name)
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

	log.Printf("[%s][write]: Copying to pipe", name)
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
		log.Printf("[%s][write]: Closing due to lack of partial flag", name)
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
	rand.Read(b)
	return hex.EncodeToString(b)
}

func die(duration time.Duration) {
	log.Printf("Will die after %s", duration)
	time.Sleep(duration)
	log.Printf("Goodbye dying now ...")
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
}

func main() {
	flag.StringVar(&listen, "listen", listen, "The address to listen on")
	flag.StringVar(&inputDir, "in", inputDir, "The directory to create input pipes in")
	flag.StringVar(&outputDir, "out", outputDir, "The directory to create output pipes in")
	flag.IntVar(&tokenLength, "tlen", tokenLength, "How long of paths to generate")
	flag.IntVar(&bufSizeBytes, "blen", bufSizeBytes, "How many bytes should buffers be")
	dieAfter := flag.Duration("die-after", time.Duration(0), "Die after this duration. Zero seconds indicates live forever")

	flag.Parse()

	log.Printf("Scanning input directory [%s] to clean up.", inputDir)
	dir, _ := ioutil.ReadDir(inputDir)
	for _, d := range dir {
		log.Printf("[cleanup] Cleaning up %s", path.Join(inputDir, d.Name()))
		os.RemoveAll(path.Join(inputDir, d.Name()))
	}
	log.Printf("Scanning output directory [%s] to clean up", outputDir)
	dir, _ = ioutil.ReadDir(outputDir)
	for _, d := range dir {
		log.Printf("[cleanup] Cleaning up %s", path.Join(outputDir, d.Name()))
		os.RemoveAll(path.Join(outputDir, d.Name()))
	}

	// Pinch API
	http.HandleFunc("/pinch", pinch)
	http.HandleFunc("/unpinch", unpinch)
	http.HandleFunc("/io/", handleIO)
	http.HandleFunc("/status/", getStatus)

	if *dieAfter > 0 {
		go die(*dieAfter)
	}

	log.Printf("Listening at %s/pinch", listen)
	http.ListenAndServe(listen, nil)
}
