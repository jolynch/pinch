package state

import (
	"log"
	"os"
	"path"
	"sync"
	"time"
)

type Checksums struct {
	Xxh128 string `json:"xxh128,omitempty"`
	Blake3 string `json:"blake3,omitempty"`
}

type PipelineResult struct {
	Start     time.Time `json:"started_at"`
	Duration  string    `json:"duration"`
	Success   bool      `json:"success"`
	Stderr    string    `json:"stderr,omitempty"`
	Checksums Checksums `json:"checksums",omitempty`
}

type processResult struct {
	result   PipelineResult
	finished chan bool
	done     bool
}

// Input processes close writers on error or success so we want to hold
// a write so users can signal via a HTTP call that they are done and succeeded
type Writer struct {
	Fd *os.File
}

var (
	processes sync.Map
	writers   sync.Map
)

func PreparePipeline(name string) {
	finished := make(chan bool)
	processes.LoadOrStore(name, processResult{finished: finished, done: false})
}

func FinishPipeline(name string, pipeline PipelineResult, stateTTL time.Duration, output string) {
	PreparePipeline(name)

	val, ok := processes.Load(name)
	if ok {
		pr := val.(processResult)
		pr.result = pipeline
		close(pr.finished)
		pr.done = true
		// Why do I need this store ...
		processes.Store(name, pr)
		go cleanupPipeline(name, pipeline.Start, stateTTL, output)
	}
}

func WaitForPipeline(name string, waitFor time.Duration) (value PipelineResult, ok bool) {
	val, ok := processes.Load(name)
	if !ok {
		return PipelineResult{}, false
	}

	pr := val.(processResult)
	// Check if we have a result for this pipeline, if so just return it
	if pr.done {
		return pr.result, true
	}
	if waitFor == 0 {
		return PipelineResult{}, false
	}

	// Otherwise wait for it to exist
	select {
	case <-pr.finished:
		val, ok := processes.Load(name)
		if ok {
			return val.(processResult).result, true
		} else {
			return PipelineResult{}, false
		}
	case <-time.After(waitFor):
		return PipelineResult{}, false
	}
}

func cleanupPipeline(name string, start time.Time, expire time.Duration, output string) {
	log.Printf("[%s][cleanup]: Waiting %s before cleaning up state", name, expire)
	time.Sleep(expire)

	val, ok := processes.Load(name)
	if ok {
		pr := val.(processResult)
		if pr.result.Start == start {
			log.Printf("[%s][cleanup]: Cleaned up for [%s]", name, start.Format(time.RFC3339))
			processes.Delete(name)
			CleanupDigests(output)
		} else {
			log.Printf("[%s][cleanup]: Detected handle re-use")
		}
	} else {
		log.Printf("[%s][cleanup]: No process state found ...", name)
		CleanupDigests(output)
	}
	MaybeReleaseWriter(name)
}

func CleanupDigests(output string) {
	os.Remove(output + ".xxh128")
	os.Remove(output + ".blake3")
}

func AcquireWriter(writeDir string, name string) Writer {
	value, ok := writers.Load(name)
	if !ok {
		fd, err := os.OpenFile(path.Join(writeDir, name), os.O_WRONLY, 0666)
		if err != nil {
			log.Printf("[%s][state]: could not open writer: %s", name, err)
			return Writer{Fd: nil}
		}

		value, ok = writers.LoadOrStore(name, Writer{Fd: fd})
		// Race, someone else made a FD
		if ok {
			fd.Close()
		}
	}
	return value.(Writer)
}

func MaybeReleaseWriter(name string) {
	value, hasWriter := writers.Load(name)
	if hasWriter {
		log.Printf("[%s][cleanup]: Cleaning up open writer", name)
		writers.Delete(name)
		value.(Writer).Fd.Close()
	}
}
