package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// Pipes are 64k usually, we might bump them in the future ...
const BUF_SIZE = 4096 * 16

var inputDir = "/var/run/pinch/in"
var outputDir = "/var/run/pinch/out"
var checksums sync.Map
var writers sync.Map

type write struct {
	fd *os.File
}

type checksum struct {
	loaded time.Time
	xxh128 string
	blake3 string
}

func cleanupChecksum(name string, start time.Time, expire time.Duration, output string) {
	log.Printf("[%s] Keeping digests available for %s", name, expire)
	time.Sleep(expire)

	value, ok := checksums.Load(name)
	if ok {
		if value.(checksum).loaded == start {
			log.Printf("[%s] Cleaned up for [%s]", name, start.Format(time.RFC3339))
			checksums.Delete(name)
			os.Remove(output + ".xxh128")
			os.Remove(output + ".blake3")
		} else {
			log.Printf("[%s] Detected re-use, skipping clean-up", name)
		}
	} else {
		os.Remove(output + ".xxh128")
		os.Remove(output + ".blake3")
	}

	writer, hasWriter := writers.Load(name)
	if hasWriter && !ok {
		log.Printf("[%s] Cleaning up open writer due to timeout", name)
		writers.Delete(name)
		writer.(write).fd.Close()
	}

}

func compress(
	name string,
	input string,
	output string,
	timeout int,
	maxLevel int,
) {
	syscall.Mkfifo(output, 0666)
	start := time.Now()

	compressorCmd := "zstd -v --adapt=max=%d - -o %s"
	compressor := fmt.Sprintf(compressorCmd, maxLevel, output)
	pipeline := fmt.Sprintf(
		"pv %s | tee >(xxh128sum - > %s) | tee >(b3sum - > %s) | %s",
		input,
		output+".xxh128",
		output+".blake3",
		compressor,
	)

	log.Printf("[%s]: Prepping pinch pipeling with timeout [%ds] -> [%s]", name, timeout, pipeline)
	log.Printf("[%s]: Produce data to   [%s]", name, input)
	log.Printf("[%s]: Consume data from [%s]", name, output)

	cmd := exec.Command("timeout", strconv.Itoa(timeout), "bash", "-c", pipeline)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("[%s] FAILED to pinch: %s", name, err)
		// Try to remove the hash files
		cleanupChecksum(name, start, time.Duration(0), output)
	} else {
		log.Printf("[%s] SUCCESS pinched after waiting %s", name, time.Since(start))
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
	log.Printf("[%s] DONE", name)
}

func pinch(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	m, ok := req.URL.Query()["m"]

	var (
		maxLevel int = 10
		err      error
	)
	if ok && len(m[0]) >= 0 {
		maxLevel, err = strconv.Atoi(m[0])
		if err != nil {
			http.Error(w, "Invalid max level", http.StatusBadRequest)
			return
		}
	}

	t, ok := req.URL.Query()["t"]
	timeout := 60
	if ok && len(t[0]) >= 0 {
		timeout, err = strconv.Atoi(t[0])
		if err != nil {
			http.Error(w, "Invalid timeout", http.StatusBadRequest)
			return
		}
	}

	paths, ok := req.URL.Query()["f"]
	if !ok || len(paths[0]) < 1 {
		http.Error(w, "Must supply at least one file", http.StatusPreconditionFailed)
	}
	for _, s := range paths {
		// Make the input pipe first so that when this function returns
		// the input stages are ready
		syscall.Mkfifo(path.Join(inputDir, s), 0666)
		go compress(
			s,
			path.Join(inputDir, s),
			path.Join(outputDir, s+".zst"),
			timeout,
			maxLevel,
		)
		fmt.Fprintf(w, s)
	}
}

func readChecksum(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	paths, ok := req.URL.Query()["f"]
	if !ok || len(paths) < 1 {
		http.Error(w, "Must supply f parameter", http.StatusBadRequest)
		return
	}

	response := make(map[string]map[string]string)

	for _, s := range paths {
		value, ok := checksums.Load(s)
		if ok {
			response[s] = map[string]string{
				"xxh128": value.(checksum).xxh128,
				"blake3": value.(checksum).blake3,
			}
		}
	}
	json.NewEncoder(w).Encode(response)
}

func writeChunk(w http.ResponseWriter, req *http.Request) {
	var name string
	paths, ok := req.URL.Query()["f"]
	if !ok || len(paths[0]) < 1 {
		http.Error(w, "Must supply f parameter", http.StatusBadRequest)
		return
	} else {
		name = paths[0]
	}

	var fd *os.File
	value, ok := writers.Load(name)
	if !ok {
		fd, err := os.OpenFile(path.Join(inputDir, name), os.O_RDWR, 0666)
		if err != nil {
			http.Error(w, "No such name: "+name, http.StatusNotFound)
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
		http.Error(w, "Could not copy", http.StatusInternalServerError)
		return
	} else {
		if written > 0 {
			log.Printf("[%s] Copied %d bytes to pipe", name, written)
			w.Header().Set("X-Pinch-Copied", strconv.FormatInt(written, 10))
		}
	}

	_, ok = req.URL.Query()["c"]
	if ok {
		log.Printf("[%s] Closing pipe", name)
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
	paths, ok := req.URL.Query()["f"]
	if !ok || len(paths[0]) < 1 {
		http.Error(w, "Must supply f parameter", http.StatusBadRequest)
		return
	} else {
		name = paths[0]
	}
	log.Printf("[%s]: read start", name)

	fd, err := os.Open(path.Join(outputDir, name+".zst"))
	if err != nil {
		log.Printf("[%s]: read error: %s", name, err)
		http.Error(w, "No such name: "+name+".zst", http.StatusNotFound)
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
			log.Printf("[%s] Copied %d bytes from pipe", name, bytesRead)
			w.Header().Set("X-Pinch-Copied", strconv.FormatInt(bytesRead, 10))
		}
	}
}

func main() {
	dir, _ := ioutil.ReadDir(inputDir)
	for _, d := range dir {
		os.RemoveAll(path.Join(inputDir, d.Name()))
	}
	dir, _ = ioutil.ReadDir(outputDir)
	for _, d := range dir {
		os.RemoveAll(path.Join(inputDir, d.Name()))
	}

	http.HandleFunc("/pinch", pinch)
	http.HandleFunc("/check", readChecksum)
	http.HandleFunc("/write", writeChunk)
	http.HandleFunc("/read", readChunk)
	log.Printf("Listening at 0.0.0.0:8080/pinch")
	http.ListenAndServe(":8080", nil)
}
