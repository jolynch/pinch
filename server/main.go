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
	// Pipes are 64k usually, we might bump them in the future ...
	BUF_SIZE = 4096 * 16
)

var (
	inputDir    = "/run/pinch/in"
	outputDir   = "/run/pinch/out"
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
) {
	syscall.Mkfifo(output, 0666)
	start := time.Now()

	var compressorCmd string
	var compressor string
	if minLevel == 0 {
		compressorCmd = "zstd -v --adapt=max=%d - -o %s"
		compressor = fmt.Sprintf(compressorCmd, maxLevel, output)
	} else {
		compressorCmd = "zstd -v --adapt=min=%d,max=%d - -o %s"
		compressor = fmt.Sprintf(compressorCmd, minLevel, maxLevel, output)
	}

	pipeline := fmt.Sprintf(
		"pv %s | tee >(xxh128sum - > %s) | tee >(b3sum - > %s) | %s",
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

func pinch(w http.ResponseWriter, req *http.Request) {
	var (
		maxLevel int = 10
		minLevel int = 0
		timeout  int = 60
		err      error
	)

	m, ok := req.URL.Query()["max_level"]
	if ok && len(m[0]) >= 0 {
		maxLevel, err = strconv.Atoi(m[0])
		if err != nil {
			http.Error(w, "Invalid max_level", http.StatusBadRequest)
			return
		}
	}

	m, ok = req.URL.Query()["min_level"]
	if ok && len(m[0]) >= 0 {
		minLevel, err = strconv.Atoi(m[0])
		if err != nil {
			http.Error(w, "Invalid min_level", http.StatusBadRequest)
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

	n, ok := req.URL.Query()["num_handles"]
	numPaths := 1
	if ok && len(n[0]) >= 0 {
		numPaths, err = strconv.Atoi(n[0])
		if err != nil {
			http.Error(w, "Invalid num_handles", http.StatusBadRequest)
		}
	}

	type compparams struct {
		Algorithm string `json:"algorithm"`
		MaxLevel  int    `json:"max_level"`
		MinLevel  int    `json:"min_level"`
	}
	type resp struct {
		Handles []string   `json:"handles"`
		Params  compparams `json:"params"`
		Ttl     int        `json:"ttl"`
	}

	response := resp{
		Handles: make([]string, numPaths),
		Params: compparams{
			Algorithm: "zstd_adapt",
			MaxLevel:  maxLevel,
		},
		Ttl: timeout,
	}

	if minLevel != 0 {
		response.Params.MinLevel = minLevel
	}

	var handle string
	for i := 0; i < numPaths; i++ {
		handle = token(tokenLength)
		// Make the input pipe first so that when this function returns
		// the input stages are ready
		syscall.Mkfifo(path.Join(inputDir, handle), 0666)

		go compress(
			handle,
			path.Join(inputDir, handle),
			path.Join(outputDir, handle+".zst"),
			timeout,
			minLevel,
			maxLevel,
		)

		response.Handles[i] = handle
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
			w.Header().Set("X-Pinch-Copied", strconv.FormatInt(written, 10))
		}
	}

	_, ok = req.URL.Query()["c"]
	if ok {
		log.Printf("[%s]: write asked to close ... closing", name)
		writers.Delete(name)
		fd.Close()
	}
	w.WriteHeader(http.StatusNoContent)
}

func readChunk(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Connection", "Keep-Alive")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	var name string
	name = strings.TrimPrefix(req.URL.Path, "/read/")

	if len(name) < 1 {
		http.Error(w, "Must supply handle as /read/{handle} suffix", http.StatusBadRequest)
		return
	}

	fd, err := os.Open(path.Join(outputDir, name+".zst"))
	if err != nil {
		log.Printf("[%s]: read error, no such handle: %s", name, err)
		http.Error(w, "No such handle: "+name+".zst", http.StatusNotFound)
		return
	} else {
		log.Printf("[%s]: read opened [%s]", name, path.Join(outputDir, name+".zst"))
	}

	log.Printf("[%s]: read copying from pipe [%s]", name, path.Join(outputDir, name+".zst"))
	w.Header().Set("Content-Type", "application/octet-stream")

	buf := make([]byte, BUF_SIZE)
	bytesRead, err := io.CopyBuffer(w, fd, buf)
	if err != nil {
		log.Printf("ERROR %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	} else {
		if bytesRead > 0 {
			log.Printf("[%s]: read copied %d bytes from pipe", name, bytesRead)
			w.Header().Set("X-Pinch-Copied", strconv.FormatInt(bytesRead, 10))
		}
	}
}

func token(length int) string {
	b := make([]byte, length)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func main() {
	listen := flag.String("listen", ":8080", "The address to listen on")
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

	http.HandleFunc("/pinch", pinch)
	http.HandleFunc("/checksums/", readChecksum)
	http.HandleFunc("/write/", writeChunk)
	http.HandleFunc("/read/", readChunk)
	log.Printf("Listening at %s/pinch", *listen)
	http.ListenAndServe(*listen, nil)
}
