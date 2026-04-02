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
	"sync"

	"filippo.io/age"
	"golang.org/x/crypto/hkdf"
)

const (
	aeadTagSize      = 16
	aeadNonceSize    = 12
	aeadKeySize      = 32
	aeadFileKeySize  = 16
	aeadDefaultChunk = 64 * 1024
	aeadMinChunkSize = 1024
	aeadHKDFInfo     = "pinch-aes-gcm-v1"
	aeadVersion      = 0x01
)

const aeadMaxAADChunkCount = ^uint64(0) >> 1

var aeadBufferPools sync.Map // map[int]*sync.Pool

// AESGCMEncrypt creates a streaming AES-GCM encrypted writer. It performs
// X25519 key exchange via the recipient's Wrap method (compatible with
// age.X25519Recipient), writes a binary key-exchange header to dst, then
// returns a WriteCloser that encrypts data in fixed-size chunks.
//
// chunkSize is the plaintext chunk size in bytes. A non-positive value uses the
// default size. Values smaller than 1 KiB are clamped upward.
//
// The caller must call Close to flush the final chunk. Not goroutine-safe.
func AESGCMEncrypt(dst io.Writer, recipient age.Recipient, chunkSize int) (io.WriteCloser, error) {
	if dst == nil {
		return nil, errors.New("nil destination writer")
	}
	if recipient == nil {
		return nil, errors.New("nil recipient")
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

	chunkSize = normalizeAEADChunkSize(chunkSize)

	if err := writeStanzaHeader(dst, stanzas[0], chunkSize); err != nil {
		return nil, fmt.Errorf("write stanza header: %w", err)
	}

	gcm, err := newGCM(fileKey)
	if err != nil {
		return nil, err
	}

	buf, chunkSize, release := acquireAEADBuffer(chunkSize)
	half := len(buf) / 2

	return &aesgcmWriter{
		gcm:        gcm,
		dst:        dst,
		plainBuf:   buf[:half],
		sealBuf:    buf[half:],
		chunkSize:  chunkSize,
		backingBuf: buf,
		releaseBuf: release,
	}, nil
}

// AESGCMDecrypt creates a streaming AES-GCM decrypting reader. It reads
// the key-exchange header from src, recovers the file key via the identity's
// Unwrap method (compatible with age.X25519Identity), then returns a Reader
// that decrypts chunks on the fly.
//
// Not goroutine-safe.
func AESGCMDecrypt(src io.Reader, identity age.Identity) (io.Reader, error) {
	if src == nil {
		return nil, errors.New("nil source reader")
	}
	if identity == nil {
		return nil, errors.New("nil identity")
	}

	stanza, chunkSize, err := readStanzaHeader(src)
	if err != nil {
		return nil, fmt.Errorf("read stanza header: %w", err)
	}

	fileKey, err := identity.Unwrap([]*age.Stanza{stanza})
	if err != nil {
		return nil, fmt.Errorf("unwrap file key: %w", err)
	}

	gcm, err := newGCM(fileKey)
	if err != nil {
		return nil, err
	}

	buf, chunkSize, release := acquireAEADBuffer(chunkSize)
	half := len(buf) / 2

	return &aesgcmReader{
		gcm:        gcm,
		src:        src,
		cipherBuf:  buf[:half],
		plainBuf:   buf[half:],
		chunkSize:  chunkSize,
		backingBuf: buf,
		releaseBuf: release,
	}, nil
}

// newGCM derives an AES-256 key from the age file key via HKDF-SHA256 and
// returns a GCM cipher instance.
func newGCM(fileKey []byte) (cipher.AEAD, error) {
	hk := hkdf.New(sha256.New, fileKey, nil, []byte(aeadHKDFInfo))
	aesKey := make([]byte, aeadKeySize)
	if _, err := io.ReadFull(hk, aesKey); err != nil {
		return nil, fmt.Errorf("derive AES key: %w", err)
	}
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func acquireAEADBuffer(chunkSize int) ([]byte, int, func()) {
	chunkSize = normalizeAEADChunkSize(chunkSize)
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

func normalizeAEADChunkSize(chunkSize int) int {
	if chunkSize <= 0 {
		return aeadDefaultChunk
	}
	if chunkSize < aeadMinChunkSize {
		return aeadMinChunkSize
	}
	return chunkSize
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
//   [2 bytes BE: type_len][type_len bytes: Stanza.Type]
//   [2 bytes BE: num_args]
//     per arg: [2 bytes BE: arg_len][arg_len bytes: arg]
//   [2 bytes BE: body_len][body_len bytes: Stanza.Body]
//   [4 bytes BE: chunk_size]

func writeStanzaHeader(w io.Writer, s *age.Stanza, chunkSize int) error {
	if err := writeFull(w, []byte{aeadVersion}); err != nil {
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

func readStanzaHeader(r io.Reader) (*age.Stanza, int, error) {
	var ver [1]byte
	if _, err := io.ReadFull(r, ver[:]); err != nil {
		return nil, 0, fmt.Errorf("read version: %w", err)
	}
	if ver[0] != aeadVersion {
		return nil, 0, fmt.Errorf("unsupported AEAD version: %d", ver[0])
	}

	typ, err := readU16Prefixed(r)
	if err != nil {
		return nil, 0, fmt.Errorf("read stanza type: %w", err)
	}
	if string(typ) != "X25519" {
		return nil, 0, fmt.Errorf("unsupported stanza type: %q", string(typ))
	}

	var numArgsBuf [2]byte
	if _, err := io.ReadFull(r, numArgsBuf[:]); err != nil {
		return nil, 0, fmt.Errorf("read num_args: %w", err)
	}
	numArgs := int(binary.BigEndian.Uint16(numArgsBuf[:]))
	args := make([]string, numArgs)
	for i := range args {
		a, err := readU16Prefixed(r)
		if err != nil {
			return nil, 0, fmt.Errorf("read arg %d: %w", i, err)
		}
		args[i] = string(a)
	}

	body, err := readU16Prefixed(r)
	if err != nil {
		return nil, 0, fmt.Errorf("read body: %w", err)
	}

	var chunkSizeBuf [4]byte
	if _, err := io.ReadFull(r, chunkSizeBuf[:]); err != nil {
		return nil, 0, fmt.Errorf("read chunk size: %w", err)
	}
	chunkSize := int(binary.BigEndian.Uint32(chunkSizeBuf[:]))
	if chunkSize < aeadMinChunkSize {
		return nil, 0, fmt.Errorf("invalid AEAD chunk size: %d", chunkSize)
	}

	return &age.Stanza{Type: string(typ), Args: args, Body: body}, chunkSize, nil
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
// aesgcmWriter — streaming AEAD encrypt
// ---------------------------------------------------------------------------
//
// Nonce layout (12 bytes):
//   bytes 0–2:  zero (high bits of chunk count, never reached in practice)
//   bytes 3–10: big-endian uint64 chunk count
//   byte 11:    0x00 = non-final chunk, 0x01 = final chunk

type aesgcmWriter struct {
	gcm        cipher.AEAD
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

func (w *aesgcmWriter) Write(p []byte) (int, error) {
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

func (w *aesgcmWriter) Close() error {
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

func (w *aesgcmWriter) flushChunk(last bool) error {
	w.buildNonce(last)
	aad, err := buildChunkAAD(w.chunkSize, w.chunkCount, last)
	if err != nil {
		return err
	}
	sealed := w.gcm.Seal(w.sealBuf[:0], w.nonce[:], w.plainBuf[:w.plainN], aad[:])
	if err := writeFull(w.dst, sealed); err != nil {
		return err
	}
	w.plainN = 0
	w.chunkCount++
	return nil
}

func (w *aesgcmWriter) buildNonce(last bool) {
	binary.BigEndian.PutUint64(w.nonce[3:11], w.chunkCount)
	if last {
		w.nonce[11] = 0x01
	} else {
		w.nonce[11] = 0x00
	}
}

func (w *aesgcmWriter) release() {
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
// aesgcmReader — streaming AEAD decrypt
// ---------------------------------------------------------------------------

type aesgcmReader struct {
	gcm        cipher.AEAD
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

func (r *aesgcmReader) Read(p []byte) (int, error) {
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

func (r *aesgcmReader) readChunk() error {
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
		plaintext, openErr := r.gcm.Open(r.plainBuf[:0], r.nonce[:], r.cipherBuf[:n], aad[:])
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
		plaintext, openErr = r.gcm.Open(r.plainBuf[:0], r.nonce[:], r.cipherBuf[:n], aad[:])
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
		plaintext, openErr := r.gcm.Open(r.plainBuf[:0], r.nonce[:], r.cipherBuf[:n], aad[:])
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

func (r *aesgcmReader) buildNonce(last bool) {
	binary.BigEndian.PutUint64(r.nonce[3:11], r.chunkCount)
	if last {
		r.nonce[11] = 0x01
	} else {
		r.nonce[11] = 0x00
	}
}

func (r *aesgcmReader) release() {
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
