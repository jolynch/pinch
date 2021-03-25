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
	"sync"
	"syscall"
	"time"
)

const (
	// Pipes are 64k usually, we might bump this in the future ...
	BUF_SIZE = 64 * 1024
)

var (
	listen      = "127.0.0.1:8080"
	inputDir    = "/run/pinch/in"
	outputDir   = "/run/pinch/out"
	keysDir     = "/run/pinch/keys"
	tokenLength = 8
	checksums   sync.Map
	writers     sync.Map
)

type write struct {
	fd *os.File
}

type checksum struct {
	loaded time.Time
	xxh128 string
	blake3 string
}

func cleanupChecksum(name string, start time.Time, expire time.Duration, output string) {
	log.Printf("[%s]: Keeping digests available for %s", name, expire)
	time.Sleep(expire)

	value, ok := checksums.Load(name)
	if ok {
		if value.(checksum).loaded == start {
			log.Printf("[%s]: Cleaned up for [%s]", name, start.Format(time.RFC3339))
			checksums.Delete(name)
			os.Remove(output + ".xxh128")
			os.Remove(output + ".blake3")
		} else {
			log.Printf("[%s]: Detected re-use, skipping clean-up", name)
		}
	} else {
		os.Remove(output + ".xxh128")
		os.Remove(output + ".blake3")
	}

	writer, hasWriter := writers.Load(name)
	if hasWriter && !ok {
		log.Printf("[%s]: Cleaning up open writer due to timeout", name)
		writers.Delete(name)
		writer.(write).fd.Close()
	}

}

func compress(
	name string,
	input string,
	output string,
	timeout int,
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
		"tee < %s >(xxh128sum - > %s) >(b3sum --num-threads 1 - > %s) | %s",
		input,
		output+".xxh128",
		output+".blake3",
		compressor,
	)

	log.Printf("[%s]: Spawning pipeline with timeout [%ds] -> [%s]", name, timeout, pipeline)
	log.Printf("[%s]: Produce data to   [%s]", name, input)
	log.Printf("[%s]: Consume data from [%s]", name, output)

	cmd := exec.Command("timeout", strconv.Itoa(timeout), "bash", "-c", pipeline)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("[%s]: FAILED to pinch: %s", name, err)
		// Try to remove the hash files
		cleanupChecksum(name, start, time.Duration(0), output)
	} else {
		log.Printf("[%s]: SUCCESS pinched after waiting %s", name, time.Since(start))
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

		ttl := time.Duration(timeout * 1e9)
		checksums.Store(name, checksum{
			loaded: start,
			xxh128: xxhash,
			blake3: blake3,
		})

		go cleanupChecksum(name, start, ttl, output)
	}

	os.Remove(input)
	os.Remove(output)
	log.Printf("[%s]: finished pinch", name)
}

func decompress(
	name string,
	input string,
	output string,
	timeout int,
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
		"%s | tee >(xxh128sum - > %s) >(b3sum --num-threads 1 - > %s) > %s",
		decompressor,
		output+".xxh128",
		output+".blake3",
		output,
	)

	log.Printf("[%s]: Spawning pipeline with timeout [%ds] -> [%s]", name, timeout, pipeline)
	log.Printf("[%s]: Produce data to   [%s]", name, input)
	log.Printf("[%s]: Consume data from [%s]", name, output)

	cmd := exec.Command("timeout", strconv.Itoa(timeout), "bash", "-c", pipeline)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("[%s]: FAILED to unpinch: %s", name, err)
		// Try to remove the hash files
		cleanupChecksum(name, start, time.Duration(0), output)
	} else {
		log.Printf("[%s]: SUCCESS unpinched after waiting %s", name, time.Since(start))
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

		ttl := time.Duration(timeout * 1e9)
		checksums.Store(name, checksum{
			loaded: start,
			xxh128: xxhash,
			blake3: blake3,
		})

		go cleanupChecksum(name, start, ttl, output)
	}

	os.Remove(input)
	os.Remove(output)
	log.Printf("[%s]: finished unpinch", name)
}

func pinch(w http.ResponseWriter, req *http.Request) {
	var (
		maxLevel int = 10
		minLevel int = 0
		timeout  int = 60
		numPaths int = 1
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
		timeout, err = strconv.Atoi(t[0])
		if err != nil {
			http.Error(w, "Invalid timeout", http.StatusBadRequest)
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
		Ttl               int           `json:"ttl"`
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
		log.Printf("NOT setting minlevel to %d", minLevel)
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
		syscall.Mkfifo(path.Join(inputDir, handle), 0666)
		syscall.Mkfifo(path.Join(outputDir, handle), 0666)

		response.Handles[handle] = io{
			Http:    "http://" + listen + "/write/" + handle,
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
		timeout  int = 60
		numPaths int = 1
		encKey   interface{}
		err      error
	)

	t, ok := req.URL.Query()["timeout"]
	if ok && len(t[0]) >= 0 {
		timeout, err = strconv.Atoi(t[0])
		if err != nil {
			http.Error(w, "Invalid timeout", http.StatusBadRequest)
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
		Ttl     int           `json:"ttl"`
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
		syscall.Mkfifo(path.Join(inputDir, handle), 0666)
		syscall.Mkfifo(path.Join(outputDir, handle), 0666)

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

func readChecksum(w http.ResponseWriter, req *http.Request) {
	var name string
	name = strings.TrimPrefix(req.URL.Path, "/checksums/")

	if len(name) < 1 {
		http.Error(w, "Must supply handle as /checksums/{handle} suffix", http.StatusBadRequest)
		return
	}

	response := make(map[string]map[string]string)

	value, ok := checksums.Load(name)
	if ok {
		response[name] = map[string]string{
			"xxh128": value.(checksum).xxh128,
			"blake3": value.(checksum).blake3,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func writeChunk(w http.ResponseWriter, req *http.Request) {
	var name string
	name = strings.TrimPrefix(req.URL.Path, "/write/")

	if len(name) < 1 {
		http.Error(w, "Must supply handle as /write/{handle} suffix", http.StatusBadRequest)
		return
	}

	var fd *os.File
	value, ok := writers.Load(name)
	if !ok {
		fd, err := os.OpenFile(path.Join(inputDir, name), os.O_RDWR, 0666)
		if err != nil {
			http.Error(w, "No such handle: "+name, http.StatusNotFound)
			return
		}
		value, ok = writers.LoadOrStore(name, write{
			fd: fd,
		})
		// Race, someone else made a FD
		if ok {
			fd.Close()
		}
	}
	fd = value.(write).fd

	_, readWrite := req.URL.Query()["rw"]
	readFinished := make(chan bool)
	if readWrite {
		go doReadChunk(w, name, readFinished)
	} else {
		readFinished <- true
	}

	log.Printf("[%s]: write copying to pipe", name)
	buf := make([]byte, BUF_SIZE)
	written, err := io.CopyBuffer(fd, req.Body, buf)
	if err != nil {
		log.Printf("[%s]: write failed due to %s", name, err)
		http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
		return
	} else {
		if written > 0 {
			log.Printf("[%s]: write copied %d bytes to pipe", name, written)
			w.Header().Set("X-Pinch-Written", strconv.FormatInt(written, 10))
		}
	}

	_, ok = req.URL.Query()["c"]
	if ok {
		log.Printf("[%s]: write asked to close ... closing", name)
		writers.Delete(name)
		fd.Close()
	}

	<-readFinished
	if !readWrite {
		w.WriteHeader(http.StatusNoContent)
	}
}

func doReadChunk(w http.ResponseWriter, name string, finished chan bool) {
	w.Header().Set("Connection", "Keep-Alive")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	fd, err := os.Open(path.Join(outputDir, name))
	if err != nil {
		log.Printf("[%s]: read error, no such handle: %s", name, err)
		http.Error(w, "No such handle: "+name, http.StatusNotFound)
		finished <- false
		return
	} else {
		log.Printf("[%s]: read opened [%s]", name, path.Join(outputDir, name))
	}

	log.Printf("[%s]: read copying from pipe [%s]", name, path.Join(outputDir, name))
	w.Header().Set("Content-Type", "application/octet-stream")

	buf := make([]byte, BUF_SIZE)
	bytesRead, err := io.CopyBuffer(w, fd, buf)
	if err != nil {
		log.Printf("ERROR %s", err)
		http.Error(w, fmt.Sprintf("Error reading from %s: %s", name, err), http.StatusInternalServerError)
		finished <- false
		return
	} else {
		if bytesRead > 0 {
			log.Printf("[%s]: read copied %d bytes from pipe", name, bytesRead)
		}
	}

	finished <- true
}

func readChunk(w http.ResponseWriter, req *http.Request) {
	name := strings.TrimPrefix(req.URL.Path, "/read/")

	if len(name) < 1 {
		http.Error(w, "Must supply handle as /read/{handle} suffix", http.StatusBadRequest)
		return
	}

	readFinished := make(chan bool)
	doReadChunk(w, name, readFinished)
	<-readFinished
}

func token(length int) string {
	b := make([]byte, length)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func main() {
	flag.StringVar(&listen, "listen", listen, "The address to listen on")
	flag.StringVar(&inputDir, "in", inputDir, "The directory to create input pipes in")
	flag.StringVar(&outputDir, "out", outputDir, "The directory to create output pipes in")
	flag.IntVar(&tokenLength, "tlen", tokenLength, "How long of paths to generate")

	flag.Parse()

	log.Printf("Scanning input directory [%s] to clean up.", inputDir)
	dir, _ := ioutil.ReadDir(inputDir)
	for _, d := range dir {
		log.Printf("Cleaning up %s", path.Join(inputDir, d.Name()))
		os.RemoveAll(path.Join(inputDir, d.Name()))
	}
	log.Printf("Scanning output directory [%s] to clean up", outputDir)
	dir, _ = ioutil.ReadDir(outputDir)
	for _, d := range dir {
		log.Printf("Cleaning up %s", path.Join(outputDir, d.Name()))
		os.RemoveAll(path.Join(outputDir, d.Name()))
	}

	// Pinch API
	http.HandleFunc("/pinch", pinch)
	http.HandleFunc("/unpinch", unpinch)
	http.HandleFunc("/write/", writeChunk)
	http.HandleFunc("/read/", readChunk)
	http.HandleFunc("/checksums/", readChecksum)

	log.Printf("Listening at %s/pinch", listen)
	http.ListenAndServe(listen, nil)
}
