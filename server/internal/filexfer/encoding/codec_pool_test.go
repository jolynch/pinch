package encoding

import (
	"bytes"
	"io"
	"testing"
)

func TestWrapDecompressedReaderSequentialDecode(t *testing.T) {
	payload := bytes.Repeat([]byte("pinch-filexfer-pool-test-"), 4096)
	for _, comp := range []string{EncodingZstd, EncodingLz4} {
		encoded := encodeForCodecPoolTest(t, comp, payload)
		for i := 0; i < 32; i++ {
			reader, err := WrapDecompressedReader(bytes.NewReader(encoded), comp)
			if err != nil {
				t.Fatalf("comp=%s decode setup %d: %v", comp, i, err)
			}
			got, err := io.ReadAll(reader)
			if err != nil {
				_ = reader.Close()
				t.Fatalf("comp=%s decode read %d: %v", comp, i, err)
			}
			if err := reader.Close(); err != nil {
				t.Fatalf("comp=%s decode close %d: %v", comp, i, err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("comp=%s decode mismatch at iteration %d", comp, i)
			}
		}
	}
}

func TestWrapDecompressedReaderCloseIdempotent(t *testing.T) {
	payload := bytes.Repeat([]byte("pinch-filexfer-close-test-"), 1024)
	for _, comp := range []string{EncodingZstd, EncodingLz4} {
		encoded := encodeForCodecPoolTest(t, comp, payload)
		reader, err := WrapDecompressedReader(bytes.NewReader(encoded), comp)
		if err != nil {
			t.Fatalf("comp=%s decode setup: %v", comp, err)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			_ = reader.Close()
			t.Fatalf("comp=%s decode read: %v", comp, err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("comp=%s first close: %v", comp, err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("comp=%s second close: %v", comp, err)
		}
	}
}

func BenchmarkWrapDecompressedReader(b *testing.B) {
	payload := bytes.Repeat([]byte("pinch-filexfer-bench-"), 8192)
	for _, comp := range []string{EncodingZstd, EncodingLz4} {
		encoded := encodeForCodecPoolBench(b, comp, payload)
		b.Run(comp, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for i := 0; i < b.N; i++ {
				reader, err := WrapDecompressedReader(bytes.NewReader(encoded), comp)
				if err != nil {
					b.Fatalf("decode setup: %v", err)
				}
				if _, err := io.Copy(io.Discard, reader); err != nil {
					_ = reader.Close()
					b.Fatalf("decode read: %v", err)
				}
				if err := reader.Close(); err != nil {
					b.Fatalf("decode close: %v", err)
				}
			}
		})
	}
}

func encodeForCodecPoolTest(t *testing.T, comp string, payload []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	writer, closeFn, selected, err := WrapCompressedWriter(&out, comp)
	if err != nil {
		t.Fatalf("compress setup (%s): %v", comp, err)
	}
	if selected != comp {
		t.Fatalf("compress mode mismatch: got=%s want=%s", selected, comp)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("compress write (%s): %v", comp, err)
	}
	if err := closeFn(); err != nil {
		t.Fatalf("compress close (%s): %v", comp, err)
	}
	return out.Bytes()
}

func encodeForCodecPoolBench(b *testing.B, comp string, payload []byte) []byte {
	b.Helper()
	var out bytes.Buffer
	writer, closeFn, selected, err := WrapCompressedWriter(&out, comp)
	if err != nil {
		b.Fatalf("compress setup (%s): %v", comp, err)
	}
	if selected != comp {
		b.Fatalf("compress mode mismatch: got=%s want=%s", selected, comp)
	}
	if _, err := writer.Write(payload); err != nil {
		b.Fatalf("compress write (%s): %v", comp, err)
	}
	if err := closeFn(); err != nil {
		b.Fatalf("compress close (%s): %v", comp, err)
	}
	return out.Bytes()
}
