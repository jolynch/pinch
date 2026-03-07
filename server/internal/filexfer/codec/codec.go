package codec

import (
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/zeebo/xxh3"
)

const (
	EncodingIdentity = "identity"
	EncodingZstd     = "zstd"
	EncodingLz4      = "lz4"
)

type zstdMaxEncodedSizeCache struct {
	once sync.Once
	mu   sync.Mutex
	enc  *zstd.Encoder
	err  error
	size map[int]int
}

var zstdMaxSizer zstdMaxEncodedSizeCache

func (c *zstdMaxEncodedSizeCache) maxEncodedSize(n int) (int, error) {
	c.once.Do(func() {
		c.enc, c.err = zstd.NewWriter(io.Discard)
		if c.err == nil {
			c.size = make(map[int]int)
		}
	})
	if c.err != nil {
		return 0, c.err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if cached, ok := c.size[n]; ok {
		return cached, nil
	}
	maxSize := c.enc.MaxEncodedSize(n)
	if maxSize <= 0 {
		return 0, errors.New("invalid zstd max encoded size")
	}
	c.size[n] = maxSize
	return maxSize, nil
}

const (
	maxWSizeBucket1MiB  int64 = 1 * 1024 * 1024
	maxWSizeBucket2MiB  int64 = 2 * 1024 * 1024
	maxWSizeBucket4MiB  int64 = 4 * 1024 * 1024
	maxWSizeBucket8MiB  int64 = 8 * 1024 * 1024
	maxWSizeBucket16MiB int64 = 16 * 1024 * 1024
	maxWSizeBucket32MiB int64 = 32 * 1024 * 1024
	maxWSizeBucket64MiB int64 = 64 * 1024 * 1024
)

func SelectEncoding(acceptEncoding string) string {
	best := EncodingIdentity
	bestQ := 0.0

	for _, token := range strings.Split(acceptEncoding, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}

		parts := strings.Split(token, ";")
		encoding := strings.ToLower(strings.TrimSpace(parts[0]))
		q := 1.0
		for _, p := range parts[1:] {
			p = strings.TrimSpace(p)
			if !strings.HasPrefix(strings.ToLower(p), "q=") {
				continue
			}
			parsed, err := strconv.ParseFloat(strings.TrimSpace(p[2:]), 64)
			if err != nil {
				q = 0
				break
			}
			q = parsed
		}
		if q <= 0 {
			continue
		}

		switch encoding {
		case EncodingZstd, EncodingLz4, EncodingIdentity:
		default:
			continue
		}

		if q > bestQ {
			bestQ = q
			best = encoding
		}
	}

	return best
}

func WrapCompressedWriter(dst io.Writer, acceptEncoding string) (io.Writer, func() error, string, error) {
	if dst == nil {
		return nil, nil, "", errors.New("nil destination writer")
	}

	switch SelectEncoding(acceptEncoding) {
	case EncodingZstd:
		zw, err := zstd.NewWriter(dst)
		if err != nil {
			return nil, nil, "", err
		}
		return zw, zw.Close, EncodingZstd, nil
	case EncodingLz4:
		lw := lz4.NewWriter(dst)
		return lw, lw.Close, EncodingLz4, nil
	default:
		return dst, func() error { return nil }, "", nil
	}
}

func WrapDecompressedReader(src io.Reader, contentEncoding string) (io.ReadCloser, error) {
	if src == nil {
		return nil, errors.New("nil source reader")
	}

	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "", EncodingIdentity:
		return io.NopCloser(src), nil
	case EncodingZstd:
		zr, err := zstd.NewReader(src)
		if err != nil {
			return nil, err
		}
		return zr.IOReadCloser(), nil
	case EncodingLz4:
		return io.NopCloser(lz4.NewReader(src)), nil
	default:
		return nil, errors.New("unsupported content encoding")
	}
}

func MaxFrameWireSizeHintBytes(comp string, logicalSize int64) (int64, error) {
	maxWire, err := maxEncodedFrameSizeBytes(comp, logicalSize)
	if err != nil {
		return 0, err
	}
	return ceilingMaxWSizeBucketBytes(maxWire), nil
}

func MaxEncodedFrameSizeBytes(comp string, logicalSize int64) (int64, error) {
	if logicalSize <= 0 {
		return 0, errors.New("logical size must be positive")
	}
	if logicalSize > int64(^uint(0)>>1) {
		return 0, errors.New("logical size overflows int")
	}
	n := int(logicalSize)

	switch comp {
	case "none", EncodingIdentity:
		return logicalSize, nil
	case EncodingLz4:
		return int64(lz4.CompressBlockBound(n)), nil
	case EncodingZstd:
		maxSize, err := zstdMaxSizer.maxEncodedSize(n)
		if err != nil {
			return 0, err
		}
		return int64(maxSize), nil
	default:
		return 0, errors.New("unsupported compression mode")
	}
}

func ceilingMaxWSizeBucketBytes(size int64) int64 {
	if size <= maxWSizeBucket1MiB {
		return maxWSizeBucket1MiB
	}
	if size <= maxWSizeBucket2MiB {
		return maxWSizeBucket2MiB
	}
	if size <= maxWSizeBucket4MiB {
		return maxWSizeBucket4MiB
	}
	if size <= maxWSizeBucket8MiB {
		return maxWSizeBucket8MiB
	}
	if size <= maxWSizeBucket16MiB {
		return maxWSizeBucket16MiB
	}
	if size <= maxWSizeBucket32MiB {
		return maxWSizeBucket32MiB
	}
	return maxWSizeBucket64MiB
}

func maxFrameWireSizeHintBytes(comp string, logicalSize int64) (int64, error) {
	return MaxFrameWireSizeHintBytes(comp, logicalSize)
}

func maxEncodedFrameSizeBytes(comp string, logicalSize int64) (int64, error) {
	return MaxEncodedFrameSizeBytes(comp, logicalSize)
}

func CeilingMaxWSizeBucketBytes(size int64) int64 {
	return ceilingMaxWSizeBucketBytes(size)
}

func FormatXXH128HashToken(v xxh3.Uint128) string {
	b := v.Bytes()
	return "xxh128:" + hex.EncodeToString(b[:])
}

func FormatXXH64HashToken(v uint64) string {
	return "xxh64:" + hex.EncodeToString([]byte{
		byte(v >> 56),
		byte(v >> 48),
		byte(v >> 40),
		byte(v >> 32),
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	})
}
