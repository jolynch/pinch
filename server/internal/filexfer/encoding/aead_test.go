package encoding

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"testing"

	"filippo.io/age"
)

func generateTestIdentity(t testing.TB) (*age.X25519Identity, *age.X25519Recipient) {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return id, id.Recipient()
}

func roundTrip(t testing.TB, plaintext []byte, chunkSize int) {
	t.Helper()
	id, recipient := generateTestIdentity(t)

	var ciphertext bytes.Buffer
	w, err := AESGCMEncrypt(&ciphertext, recipient, chunkSize)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := AESGCMDecrypt(&ciphertext, id)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("roundtrip mismatch: wrote %d bytes, got %d bytes", len(plaintext), len(got))
	}
}

func FuzzAESGCMRoundTrip(f *testing.F) {
	// Seed corpus with interesting sizes.
	for _, size := range []int{0, 1, 16, 1023, 1024, 65535, 65536, 65537, 128*1024 - 1, 128 * 1024, 128*1024 + 1} {
		data := make([]byte, size)
		if size > 0 {
			rand.Read(data)
		}
		f.Add(data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		roundTrip(t, data, 0)
	})
}

func TestAESGCMRoundTripLarge(t *testing.T) {
	// Explicit large sizes beyond typical fuzz corpus.
	for _, size := range []int{4 * 1024 * 1024, 4*1024*1024 + 1} {
		data := make([]byte, size)
		rand.Read(data)
		roundTrip(t, data, 0)
	}
}

func TestAESGCMSmallChunkSize(t *testing.T) {
	data := make([]byte, 100*1024)
	rand.Read(data)
	roundTrip(t, data, 512)
}

func TestAESGCMTampered(t *testing.T) {
	id, recipient := generateTestIdentity(t)

	plaintext := []byte("hello world, this is a test of tamper detection")
	var ciphertext bytes.Buffer
	w, err := AESGCMEncrypt(&ciphertext, recipient, 0)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	w.Write(plaintext)
	w.Close()

	// Flip a bit in the ciphertext payload (after the stanza header).
	raw := ciphertext.Bytes()
	if len(raw) < 100 {
		t.Fatal("ciphertext too short")
	}
	raw[len(raw)-10] ^= 0x01

	r, err := AESGCMDecrypt(bytes.NewReader(raw), id)
	if err != nil {
		t.Fatalf("decrypt init: %v", err)
	}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("expected authentication error for tampered ciphertext")
	}
}

func TestAESGCMWrongKey(t *testing.T) {
	_, recipient := generateTestIdentity(t)
	wrongID, _ := generateTestIdentity(t)

	plaintext := []byte("secret message")
	var ciphertext bytes.Buffer
	w, err := AESGCMEncrypt(&ciphertext, recipient, 0)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	w.Write(plaintext)
	w.Close()

	_, err = AESGCMDecrypt(&ciphertext, wrongID)
	if err == nil {
		t.Fatal("expected error decrypting with wrong identity")
	}
}

func TestAESGCMTruncated(t *testing.T) {
	id, recipient := generateTestIdentity(t)

	plaintext := make([]byte, 200*1024)
	rand.Read(plaintext)

	var ciphertext bytes.Buffer
	w, err := AESGCMEncrypt(&ciphertext, recipient, 0)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	w.Write(plaintext)
	w.Close()

	// Truncate to half the ciphertext.
	truncated := ciphertext.Bytes()[:ciphertext.Len()/2]
	r, err := AESGCMDecrypt(bytes.NewReader(truncated), id)
	if err != nil {
		t.Fatalf("decrypt init: %v", err)
	}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("expected error reading truncated ciphertext")
	}
}

func TestAESGCMDefaultChunkSize(t *testing.T) {
	data := make([]byte, 100*1024)
	rand.Read(data)
	roundTrip(t, data, 0)
}

func TestAESGCMChunkSizeFromHeader(t *testing.T) {
	data := make([]byte, 100*1024)
	rand.Read(data)
	roundTrip(t, data, 4*1024)
}

func TestAESGCMHeaderChunkSizeTamper(t *testing.T) {
	id, recipient := generateTestIdentity(t)

	var ciphertext bytes.Buffer
	w, err := AESGCMEncrypt(&ciphertext, recipient, 4*1024)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := w.Write([]byte("tamper me")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw := ciphertext.Bytes()
	offset := stanzaChunkSizeOffset(t, raw)
	binary.BigEndian.PutUint32(raw[offset:offset+4], 8*1024)

	r, err := AESGCMDecrypt(bytes.NewReader(raw), id)
	if err != nil {
		t.Fatalf("decrypt init: %v", err)
	}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("expected authentication error for tampered chunk size")
	}
}

var aeadBenchSizes = []struct {
	name string
	n    int
}{
	{"64KiB", 64 << 10},
	{"1MiB", 1 << 20},
	{"4MiB", 4 << 20},
	{"10MiB", 10 << 20},
}

func BenchmarkEncryptThroughput(b *testing.B) {
	_, recipient := generateTestIdentity(b)
	for _, sz := range aeadBenchSizes {
		data := make([]byte, sz.n)
		rand.Read(data)
		b.Run(sz.name+"/aesgcm", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w, err := AESGCMEncrypt(io.Discard, recipient, 0)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := w.Write(data); err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
		})
		b.Run(sz.name+"/age", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w, err := age.Encrypt(io.Discard, recipient)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := w.Write(data); err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
		})
	}
}

func BenchmarkDecryptThroughput(b *testing.B) {
	id, recipient := generateTestIdentity(b)
	for _, sz := range aeadBenchSizes {
		data := make([]byte, sz.n)
		rand.Read(data)
		aesCT := encryptAESForBench(b, data, recipient)
		ageCT := encryptAgeForBench(b, data, recipient)
		b.Run(sz.name+"/aesgcm", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := AESGCMDecrypt(bytes.NewReader(aesCT), id)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, r); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
		})
		b.Run(sz.name+"/age", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := age.Decrypt(bytes.NewReader(ageCT), id)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, r); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N)*float64(sz.n)/b.Elapsed().Seconds()/1048576, "MiB/s")
		})
	}
}

func encryptAESForBench(b *testing.B, plaintext []byte, recipient *age.X25519Recipient) []byte {
	b.Helper()
	var buf bytes.Buffer
	w, err := AESGCMEncrypt(&buf, recipient, 0)
	if err != nil {
		b.Fatalf("aes encrypt setup: %v", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		b.Fatalf("aes encrypt write: %v", err)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("aes encrypt close: %v", err)
	}
	return buf.Bytes()
}

func encryptAgeForBench(b *testing.B, plaintext []byte, recipient *age.X25519Recipient) []byte {
	b.Helper()
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, recipient)
	if err != nil {
		b.Fatalf("age encrypt setup: %v", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		b.Fatalf("age encrypt write: %v", err)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("age encrypt close: %v", err)
	}
	return buf.Bytes()
}

func BenchmarkAESGCMEncrypt(b *testing.B) {
	_, recipient := generateTestIdentity(b)
	data := make([]byte, 64*1024*1024)
	rand.Read(data)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w, err := AESGCMEncrypt(io.Discard, recipient, aeadDefaultChunk)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAESGCMDecrypt(b *testing.B) {
	id, recipient := generateTestIdentity(b)
	data := make([]byte, 64*1024*1024)
	rand.Read(data)

	var ciphertext bytes.Buffer
	w, err := AESGCMEncrypt(&ciphertext, recipient, 0)
	if err != nil {
		b.Fatal(err)
	}
	w.Write(data)
	w.Close()
	cipherBytes := ciphertext.Bytes()

	discard := make([]byte, 64*1024)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r, err := AESGCMDecrypt(bytes.NewReader(cipherBytes), id)
		if err != nil {
			b.Fatal(err)
		}
		for {
			_, err := r.Read(discard)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func stanzaChunkSizeOffset(t testing.TB, ciphertext []byte) int {
	t.Helper()
	offset := 0
	if len(ciphertext) < 1 {
		t.Fatal("ciphertext too short")
	}
	offset++

	readU16 := func() int {
		if offset+2 > len(ciphertext) {
			t.Fatal("ciphertext missing u16 field")
		}
		n := int(binary.BigEndian.Uint16(ciphertext[offset : offset+2]))
		offset += 2
		return n
	}

	typeLen := readU16()
	offset += typeLen
	if offset > len(ciphertext) {
		t.Fatal("ciphertext missing stanza type")
	}

	numArgs := readU16()
	for range numArgs {
		argLen := readU16()
		offset += argLen
		if offset > len(ciphertext) {
			t.Fatal("ciphertext missing stanza arg")
		}
	}

	bodyLen := readU16()
	offset += bodyLen
	if offset+4 > len(ciphertext) {
		t.Fatal("ciphertext missing chunk size")
	}
	return offset
}
