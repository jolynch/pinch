package utils

import "unsafe"

func CommonPrefixLen(a string, b string) int {
	commonLen := len(a)
	if len(b) < commonLen {
		commonLen = len(b)
	}

	// Adapted from the Go strings package optimization discussion:
	// https://go-review.googlesource.com/c/go/+/408116/3/src/strings/common.go
	const wordSize = int(unsafe.Sizeof(uint(0)))
	var aword, bword [wordSize]byte
	i := 0
	for i+wordSize <= commonLen {
		copy(aword[:], a[i:i+wordSize])
		copy(bword[:], b[i:i+wordSize])
		if aword != bword {
			break
		}
		i += wordSize
	}

	for i < commonLen {
		if a[i] != b[i] {
			return i
		}
		i++
	}
	return commonLen
}
