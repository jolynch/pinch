package encoding

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"filippo.io/age"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/sys/cpu"
)

const (
	aeadTagSize      = 16
	aeadNonceSize    = 12
	aeadKeySize      = 32
	aeadFileKeySize  = 16
	aeadDefaultChunk = 64 * 1024
	aeadMinChunkSize = 1024
	aeadVersion      = 0x01
)

type Algorithm string

const (
	AlgorithmAES      Algorithm = "aes"
	AlgorithmChaCha20 Algorithm = "chacha20"
)

type Options struct {
	ChunkSize int
	Algorithm Algorithm
}

func (o Options) ResolveAlgorithm() (Algorithm, error) {
	if o.Algorithm == "" {
		return RecommendedCipher(), nil
	}
	return validateAlgorithm(o.Algorithm)
}

func (o Options) ResolveChunkSize() int {
	if o.ChunkSize <= 0 {
		return aeadDefaultChunk
	}
	if o.ChunkSize < aeadMinChunkSize {
		return aeadMinChunkSize
	}
	return o.ChunkSize
}

func (o Options) HKDFInfo() (string, error) {
	algorithm, err := o.ResolveAlgorithm()
	if err != nil {
		return "", err
	}

	algorithmName, err := hkdfAlgorithmName(algorithm)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pinch-%s-v%d", algorithmName, aeadVersion), nil
}

const aeadMaxAADChunkCount = ^uint64(0) >> 1

var aeadBufferPools sync.Map // map[int]*sync.Pool

// Encrypt creates a streaming AEAD encrypted writer. It performs X25519 key
// exchange via the recipient's Wrap method (compatible with age.X25519Recipient),
// writes a binary key-exchange header to dst, then returns a WriteCloser that
// encrypts data in fixed-size chunks.
//
// A zero ChunkSize uses the default. Values smaller than 1 KiB are clamped
// upward. A zero Algorithm uses RecommendedCipher.
//
// The caller must call Close to flush the final chunk. Not goroutine-safe.
func Encrypt(dst io.Writer, recipient age.Recipient, opts Options) (io.WriteCloser, error) {
	if dst == nil {
		return nil, errors.New("nil destination writer")
	}
	if recipient == nil {
		return nil, errors.New("nil recipient")
	}

	algorithm, err := opts.ResolveAlgorithm()
	if err != nil {
		return nil, err
	}

	fileKey := make([]byte, aeadFileKeySize)
	if _, err := rand.Read(fileKey); err != nil {
		return nil, fmt.Errorf("generate file key: %w", err)
	}

	stanzas, err := recipient.Wrap(fileKey)
	if err != nil {
		return nil, fmt.Errorf("wrap file key: %w", err)
	}
	if len(stanzas) != 1 {
		return nil, fmt.Errorf("expected 1 stanza from Wrap, got %d", len(stanzas))
	}

	chunkSize := opts.ResolveChunkSize()
	if err := writeStanzaHeader(dst, algorithm, stanzas[0], chunkSize); err != nil {
		return nil, fmt.Errorf("write stanza header: %w", err)
	}

	aeadCipher, err := newAEAD(fileKey, algorithm)
	if err != nil {
		return nil, err
	}

	buf, chunkSize, release := acquireAEADBuffer(chunkSize)
	half := len(buf) / 2

	return &aeadWriter{
		aead:       aeadCipher,
		dst:        dst,
		plainBuf:   buf[:half],
		sealBuf:    buf[half:],
		chunkSize:  chunkSize,
		backingBuf: buf,
		releaseBuf: release,
	}, nil
}

// Decrypt creates a streaming AEAD decrypting reader. It reads the key-exchange
// header from src, recovers the file key via the identity's Unwrap method
// (compatible with age.X25519Identity), then returns a Reader that decrypts
// chunks on the fly.
//
// Not goroutine-safe.
func Decrypt(src io.Reader, identity age.Identity) (io.Reader, error) {
	if src == nil {
		return nil, errors.New("nil source reader")
	}
	if identity == nil {
		return nil, errors.New("nil identity")
	}

	algorithm, stanza, chunkSize, err := readStanzaHeader(src)
	if err != nil {
		return nil, fmt.Errorf("read stanza header: %w", err)
	}

	fileKey, err := identity.Unwrap([]*age.Stanza{stanza})
	if err != nil {
		return nil, fmt.Errorf("unwrap file key: %w", err)
	}

	aeadCipher, err := newAEAD(fileKey, algorithm)
	if err != nil {
		return nil, err
	}

	buf, chunkSize, release := acquireAEADBuffer(chunkSize)
	half := len(buf) / 2

	return &aeadReader{
		aead:       aeadCipher,
		src:        src,
		cipherBuf:  buf[:half],
		plainBuf:   buf[half:],
		chunkSize:  chunkSize,
		backingBuf: buf,
		releaseBuf: release,
	}, nil
}

func RecommendedCipher() Algorithm {
	switch runtime.GOARCH {
	case "386", "amd64":
		if cpu.X86.HasAES {
			return AlgorithmAES
		}
	case "arm":
		if cpu.ARM.HasAES {
			return AlgorithmAES
		}
	case "arm64":
		if cpu.ARM64.HasAES {
			return AlgorithmAES
		}
	case "s390x":
		if cpu.S390X.HasAES {
			return AlgorithmAES
		}
	}
	return AlgorithmChaCha20
}

func validateAlgorithm(algorithm Algorithm) (Algorithm, error) {
	switch algorithm {
	case AlgorithmAES, AlgorithmChaCha20:
		return algorithm, nil
	default:
		return "", fmt.Errorf("unsupported AEAD algorithm: %q", algorithm)
	}
}

func newAEAD(fileKey []byte, algorithm Algorithm) (cipher.AEAD, error) {
	key, err := deriveAEADKey(fileKey, algorithm)
	if err != nil {
		return nil, err
	}

	switch algorithm {
	case AlgorithmAES:
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	case AlgorithmChaCha20:
		return chacha20poly1305.New(key)
	default:
		return nil, fmt.Errorf("unsupported AEAD algorithm: %q", algorithm)
	}
}

func deriveAEADKey(fileKey []byte, algorithm Algorithm) ([]byte, error) {
	info, err := Options{Algorithm: algorithm}.HKDFInfo()
	if err != nil {
		return nil, err
	}
	deriveName, err := cipherName(algorithm)
	if err != nil {
		return nil, err
	}
	hk := hkdf.New(sha256.New, fileKey, nil, []byte(info))
	key := make([]byte, aeadKeySize)
	if _, err := io.ReadFull(hk, key); err != nil {
		return nil, fmt.Errorf("derive %s key: %w", deriveName, err)
	}
	return key, nil
}

func hkdfAlgorithmName(algorithm Algorithm) (string, error) {
	switch algorithm {
	case AlgorithmAES:
		return "aes-gcm", nil
	case AlgorithmChaCha20:
		return "chacha20-poly1305", nil
	default:
		return "", fmt.Errorf("unsupported AEAD algorithm: %q", algorithm)
	}
}

func cipherName(algorithm Algorithm) (string, error) {
	switch algorithm {
	case AlgorithmAES:
		return "AES", nil
	case AlgorithmChaCha20:
		return "ChaCha20-Poly1305", nil
	default:
		return "", fmt.Errorf("unsupported AEAD algorithm: %q", algorithm)
	}
}

func algorithmID(algorithm Algorithm) (byte, error) {
	switch algorithm {
	case AlgorithmAES:
		return 0x01, nil
	case AlgorithmChaCha20:
		return 0x02, nil
	default:
		return 0, fmt.Errorf("unsupported AEAD algorithm: %q", algorithm)
	}
}

func parseAlgorithmID(id byte) (Algorithm, error) {
	switch id {
	case 0x01:
		return AlgorithmAES, nil
	case 0x02:
		return AlgorithmChaCha20, nil
	default:
		return "", fmt.Errorf("unsupported AEAD algorithm id: %d", id)
	}
}

func acquireAEADBuffer(chunkSize int) ([]byte, int, func()) {
	chunkSize = Options{ChunkSize: chunkSize}.ResolveChunkSize()
	bufSize := 2 * (chunkSize + aeadTagSize)
	pool := aeadBufferPool(bufSize)
	raw := pool.Get()
	buf := raw.([]byte)
	if cap(buf) < bufSize {
		buf = make([]byte, bufSize)
	}
	buf = buf[:bufSize]
	return buf, chunkSize, func() {
		clear(buf)
		pool.Put(buf[:bufSize])
	}
}

func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func aeadBufferPool(size int) *sync.Pool {
	if existing, ok := aeadBufferPools.Load(size); ok {
		return existing.(*sync.Pool)
	}
	sz := size
	created := &sync.Pool{
		New: func() any { return make([]byte, sz) },
	}
	actual, _ := aeadBufferPools.LoadOrStore(size, created)
	return actual.(*sync.Pool)
}

// ---------------------------------------------------------------------------
// Binary stanza header serialization
// ---------------------------------------------------------------------------
//
// Format:
//   [1 byte: version=0x01]
//   [1 byte: algorithm]
//   [2 bytes BE: type_len][type_len bytes: Stanza.Type]
//   [2 bytes BE: num_args]
//     per arg: [2 bytes BE: arg_len][arg_len bytes: arg]
//   [2 bytes BE: body_len][body_len bytes: Stanza.Body]
//   [4 bytes BE: chunk_size]

func writeStanzaHeader(w io.Writer, algorithm Algorithm, s *age.Stanza, chunkSize int) error {
	id, err := algorithmID(algorithm)
	if err != nil {
		return err
	}
	if err := writeFull(w, []byte{aeadVersion, id}); err != nil {
		return err
	}
	if err := writeU16Prefixed(w, []byte(s.Type)); err != nil {
		return err
	}
	var numArgs [2]byte
	binary.BigEndian.PutUint16(numArgs[:], uint16(len(s.Args)))
	if err := writeFull(w, numArgs[:]); err != nil {
		return err
	}
	for _, arg := range s.Args {
		if err := writeU16Prefixed(w, []byte(arg)); err != nil {
			return err
		}
	}
	if err := writeU16Prefixed(w, s.Body); err != nil {
		return err
	}
	var chunkSizeBuf [4]byte
	binary.BigEndian.PutUint32(chunkSizeBuf[:], uint32(chunkSize))
	return writeFull(w, chunkSizeBuf[:])
}

func readStanzaHeader(r io.Reader) (Algorithm, *age.Stanza, int, error) {
	var header [2]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return "", nil, 0, fmt.Errorf("read header: %w", err)
	}
	if header[0] != aeadVersion {
		return "", nil, 0, fmt.Errorf("unsupported AEAD version: %d", header[0])
	}
	algorithm, err := parseAlgorithmID(header[1])
	if err != nil {
		return "", nil, 0, err
	}

	typ, err := readU16Prefixed(r)
	if err != nil {
		return "", nil, 0, fmt.Errorf("read stanza type: %w", err)
	}
	if string(typ) != "X25519" {
		return "", nil, 0, fmt.Errorf("unsupported stanza type: %q", string(typ))
	}

	var numArgsBuf [2]byte
	if _, err := io.ReadFull(r, numArgsBuf[:]); err != nil {
		return "", nil, 0, fmt.Errorf("read num_args: %w", err)
	}
	numArgs := int(binary.BigEndian.Uint16(numArgsBuf[:]))
	args := make([]string, numArgs)
	for i := range args {
		a, err := readU16Prefixed(r)
		if err != nil {
			return "", nil, 0, fmt.Errorf("read arg %d: %w", i, err)
		}
		args[i] = string(a)
	}

	body, err := readU16Prefixed(r)
	if err != nil {
		return "", nil, 0, fmt.Errorf("read body: %w", err)
	}

	var chunkSizeBuf [4]byte
	if _, err := io.ReadFull(r, chunkSizeBuf[:]); err != nil {
		return "", nil, 0, fmt.Errorf("read chunk size: %w", err)
	}
	chunkSize := int(binary.BigEndian.Uint32(chunkSizeBuf[:]))
	if chunkSize < aeadMinChunkSize {
		return "", nil, 0, fmt.Errorf("invalid AEAD chunk size: %d", chunkSize)
	}

	return algorithm, &age.Stanza{Type: string(typ), Args: args, Body: body}, chunkSize, nil
}

func writeU16Prefixed(w io.Writer, data []byte) error {
	if len(data) > 65535 {
		return errors.New("data too large for u16 length prefix")
	}
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(len(data)))
	if err := writeFull(w, buf[:]); err != nil {
		return err
	}
	if len(data) > 0 {
		return writeFull(w, data)
	}
	return nil
}

func readU16Prefixed(r io.Reader) ([]byte, error) {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(buf[:]))
	if n == 0 {
		return nil, nil
	}
	data := make([]byte, n)
	_, err := io.ReadFull(r, data)
	return data, err
}

// ---------------------------------------------------------------------------
// aeadWriter — streaming AEAD encrypt
// ---------------------------------------------------------------------------
//
// Nonce layout (12 bytes):
//   bytes 0-2:  zero (high bits of chunk count, never reached in practice)
//   bytes 3-10: big-endian uint64 chunk count
//   byte 11:    0x00 = non-final chunk, 0x01 = final chunk

type aeadWriter struct {
	aead       cipher.AEAD
	dst        io.Writer
	plainBuf   []byte // first half of caller buffer — plaintext accumulation
	sealBuf    []byte // second half of caller buffer — Seal output
	backingBuf []byte
	releaseBuf func()
	plainN     int // bytes currently in plainBuf
	chunkSize  int
	nonce      [aeadNonceSize]byte
	chunkCount uint64
	err        error
	closed     bool
}

func (w *aeadWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("write to closed AEAD writer")
	}
	if w.err != nil {
		return 0, w.err
	}
	total := 0
	for len(p) > 0 {
		n := copy(w.plainBuf[w.plainN:w.chunkSize], p)
		w.plainN += n
		p = p[n:]
		total += n

		// Flush when full AND there is still data to write. This ensures we
		// never emit the final chunk here — that happens in Close().
		if w.plainN == w.chunkSize && len(p) > 0 {
			if err := w.flushChunk(false); err != nil {
				w.err = err
				return total, err
			}
		}
	}
	return total, nil
}

func (w *aeadWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	defer w.release()
	if w.err != nil {
		return w.err
	}
	return w.flushChunk(true)
}

func (w *aeadWriter) flushChunk(last bool) error {
	w.buildNonce(last)
	aad, err := buildChunkAAD(w.chunkSize, w.chunkCount, last)
	if err != nil {
		return err
	}
	sealed := w.aead.Seal(w.sealBuf[:0], w.nonce[:], w.plainBuf[:w.plainN], aad[:])
	if err := writeFull(w.dst, sealed); err != nil {
		return err
	}
	w.plainN = 0
	w.chunkCount++
	return nil
}

func (w *aeadWriter) buildNonce(last bool) {
	binary.BigEndian.PutUint64(w.nonce[3:11], w.chunkCount)
	if last {
		w.nonce[11] = 0x01
	} else {
		w.nonce[11] = 0x00
	}
}

func (w *aeadWriter) release() {
	if w.releaseBuf == nil {
		return
	}
	w.releaseBuf()
	w.releaseBuf = nil
	w.backingBuf = nil
	w.plainBuf = nil
	w.sealBuf = nil
}

// ---------------------------------------------------------------------------
// aeadReader — streaming AEAD decrypt
// ---------------------------------------------------------------------------

type aeadReader struct {
	aead       cipher.AEAD
	src        io.Reader
	cipherBuf  []byte // first half of caller buffer — ReadFull target
	plainBuf   []byte // second half of caller buffer — Open output
	backingBuf []byte
	releaseBuf func()
	unread     []byte // unconsumed plaintext, subslice of plainBuf
	chunkSize  int
	nonce      [aeadNonceSize]byte
	chunkCount uint64
	err        error
	done       bool
}

func (r *aeadReader) Read(p []byte) (int, error) {
	if len(r.unread) > 0 {
		n := copy(p, r.unread)
		r.unread = r.unread[n:]
		return n, nil
	}
	if r.done {
		r.release()
		return 0, io.EOF
	}
	if r.err != nil {
		r.release()
		return 0, r.err
	}
	if err := r.readChunk(); err != nil {
		r.err = err
		r.release()
		return 0, err
	}
	if r.done && len(r.unread) == 0 {
		r.release()
		return 0, io.EOF
	}
	n := copy(p, r.unread)
	r.unread = r.unread[n:]
	if r.done && len(r.unread) == 0 {
		r.release()
	}
	return n, nil
}

func (r *aeadReader) readChunk() error {
	sealedSize := r.chunkSize + aeadTagSize
	n, err := io.ReadFull(r.src, r.cipherBuf[:sealedSize])

	switch {
	case err == nil:
		// Full-size read. Try non-final nonce first.
		r.buildNonce(false)
		aad, aadErr := buildChunkAAD(r.chunkSize, r.chunkCount, false)
		if aadErr != nil {
			return aadErr
		}
		plaintext, openErr := r.aead.Open(r.plainBuf[:0], r.nonce[:], r.cipherBuf[:n], aad[:])
		if openErr == nil {
			r.unread = plaintext
			r.chunkCount++
			return nil
		}
		// Non-final failed — this may be a final chunk that happens to be
		// exactly chunkSize bytes of plaintext.
		r.buildNonce(true)
		aad, aadErr = buildChunkAAD(r.chunkSize, r.chunkCount, true)
		if aadErr != nil {
			return aadErr
		}
		plaintext, openErr = r.aead.Open(r.plainBuf[:0], r.nonce[:], r.cipherBuf[:n], aad[:])
		if openErr != nil {
			return fmt.Errorf("AEAD authentication failed: %w", openErr)
		}
		r.unread = plaintext
		r.done = true
		r.chunkCount++
		return nil

	case errors.Is(err, io.ErrUnexpectedEOF) && n > 0:
		// Short read — must be the final (possibly short) chunk.
		r.buildNonce(true)
		aad, aadErr := buildChunkAAD(r.chunkSize, r.chunkCount, true)
		if aadErr != nil {
			return aadErr
		}
		plaintext, openErr := r.aead.Open(r.plainBuf[:0], r.nonce[:], r.cipherBuf[:n], aad[:])
		if openErr != nil {
			return fmt.Errorf("AEAD authentication failed on final chunk: %w", openErr)
		}
		r.unread = plaintext
		r.done = true
		r.chunkCount++
		return nil

	case errors.Is(err, io.EOF) && n == 0:
		// Stream ended without a final chunk — truncated.
		return io.ErrUnexpectedEOF

	default:
		return err
	}
}

func (r *aeadReader) buildNonce(last bool) {
	binary.BigEndian.PutUint64(r.nonce[3:11], r.chunkCount)
	if last {
		r.nonce[11] = 0x01
	} else {
		r.nonce[11] = 0x00
	}
}

func (r *aeadReader) release() {
	if r.releaseBuf == nil {
		return
	}
	r.unread = nil
	r.releaseBuf()
	r.releaseBuf = nil
	r.backingBuf = nil
	r.cipherBuf = nil
	r.plainBuf = nil
}

func buildChunkAAD(chunkSize int, chunkCount uint64, last bool) ([13]byte, error) {
	var aad [13]byte
	if chunkCount > aeadMaxAADChunkCount {
		return aad, errors.New("AEAD chunk count exceeded int64 AAD range")
	}
	binary.BigEndian.PutUint32(aad[1:5], uint32(chunkSize))
	signedChunkCount := int64(chunkCount)
	if last {
		signedChunkCount = ^signedChunkCount
	}
	aad[0] = aeadVersion
	binary.BigEndian.PutUint64(aad[5:13], uint64(signedChunkCount))
	return aad, nil
}
