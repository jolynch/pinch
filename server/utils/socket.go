package utils

import (
	"os"
	"strconv"
	"strings"
)

func maxSocketBufferBytes(procPath string) int {
	// 4MiB baseline if kernel cap cannot be read.
	const baseline = 4 * 1024 * 1024
	raw, err := os.ReadFile(procPath)
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

func MaxSocketWriteBufferBytes() int {
	return maxSocketBufferBytes("/proc/sys/net/core/wmem_max")
}

func MaxSocketReadBufferBytes() int {
	return maxSocketBufferBytes("/proc/sys/net/core/rmem_max")
}
