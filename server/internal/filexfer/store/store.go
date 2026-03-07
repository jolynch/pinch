package store

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jolynch/pinch/internal/filexfer/codec"
	"github.com/jolynch/pinch/internal/filexfer/policy"
	"github.com/zeebo/xxh3"
)

const (
	defaultTransferTTL = 10 * time.Minute
	// State enum for transfer lifecycle.
	TransferStateStarted uint8 = iota
	TransferStateRunning
	TransferStateDone
	TransferStateMissing
)

type CompressionMode = policy.CompressionMode

const (
	CompressionModeZstdDefault = policy.CompressionModeZstdDefault
	CompressionModeZstdLevel1  = policy.CompressionModeZstdLevel1
	CompressionModeLz4         = policy.CompressionModeLz4
	CompressionModeNone        = policy.CompressionModeNone
)

type Transfer struct {
	ID        string
	Directory string
	NumFiles  int
	TotalSize int64
	Done      uint64
	DoneSize  int64
	State     []uint8
	PathHash  []xxh3.Uint128
	FileSize  []int64
	AckedSize []int64
	CreatedAt time.Time
	ExpiresAt time.Time
}

type TransferFileState struct {
	State    uint8
	PathHash xxh3.Uint128
}

type TransferFileStateUpdate struct {
	FileID   uint64
	PathHash xxh3.Uint128
	FileSize int64
}

type FileRef struct {
	TransferID string
	FileID     uint64
	Path       string
	Directory  string
	FileSize   int64
}

type FileLookupError struct {
	Code int
	Msg  string
}

func (e *FileLookupError) Error() string {
	if e == nil {
		return ""
	}
	return e.Msg
}

type transferStore struct {
	mu         sync.RWMutex
	transfers  map[string]Transfer
	fileHashes map[fileHashKey]fileHashState
}

type fileHashKey struct {
	txferID string
	fileID  uint64
}

type fileHashState struct {
	hasher        *xxh3.Hasher128
	hashedSize    int64
	hashToken     string
	valid         bool
	finalized     bool
	latestComp    uint8
	hasLatestComp bool
	expiresAt     time.Time
}

var (
	ttl     = defaultTransferTTL
	manager = newTransferStore()
)

func init() {
	go manager.reapExpiredLoop()
}

func newTransferStore() *transferStore {
	return &transferStore{
		transfers:  make(map[string]Transfer),
		fileHashes: make(map[fileHashKey]fileHashState),
	}
}

func shouldAdvanceState(current uint8, next uint8) bool {
	return next >= current
}

func (s *transferStore) create(transfer Transfer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.transfers[transfer.ID]; exists {
		return false
	}
	s.transfers[transfer.ID] = transfer
	return true
}

func (s *transferStore) setState(txferID string, state uint8) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	transfer, ok := s.transfers[txferID]
	if !ok {
		return false
	}
	for i := range transfer.State {
		if shouldAdvanceState(transfer.State[i], state) {
			transfer.State[i] = state
		}
	}
	s.transfers[txferID] = transfer
	return true
}

func (s *transferStore) appendFileStates(txferID string, updates []TransferFileStateUpdate, state uint8) {
	if len(updates) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	transfer, ok := s.transfers[txferID]
	if !ok {
		return
	}

	ensureLen := func(n int) {
		if n <= len(transfer.State) {
			return
		}
		oldLen := len(transfer.State)
		growBy := n - oldLen
		transfer.State = append(transfer.State, make([]uint8, growBy)...)
		transfer.PathHash = append(transfer.PathHash, make([]xxh3.Uint128, growBy)...)
		transfer.FileSize = append(transfer.FileSize, make([]int64, growBy)...)
		transfer.AckedSize = append(transfer.AckedSize, make([]int64, growBy)...)
		for i := oldLen; i < n; i++ {
			transfer.State[i] = TransferStateStarted
		}
	}

	for _, update := range updates {
		idx := int(update.FileID)
		ensureLen(idx + 1)

		if idx+1 > transfer.NumFiles {
			transfer.NumFiles = idx + 1
		}

		transfer.TotalSize += update.FileSize - transfer.FileSize[idx]
		transfer.FileSize[idx] = update.FileSize
		transfer.PathHash[idx] = update.PathHash
		if shouldAdvanceState(transfer.State[idx], state) {
			transfer.State[idx] = state
		}
	}
	s.transfers[txferID] = transfer
}

func (s *transferStore) appendFileHashChunk(txferID string, fileID uint64, offset int64, chunk []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	transfer, ok := s.transfers[txferID]
	if !ok {
		return false
	}
	if fileID >= uint64(len(transfer.State)) || offset < 0 {
		return false
	}
	key := fileHashKey{txferID: txferID, fileID: fileID}
	state, exists := s.fileHashes[key]
	if !exists || offset == 0 {
		state = fileHashState{
			hasher:    xxh3.New128(),
			valid:     true,
			finalized: false,
		}
	}
	if !state.valid || state.finalized {
		state.expiresAt = time.Now().Add(ttl)
		s.fileHashes[key] = state
		return true
	}
	if offset != state.hashedSize {
		state.valid = false
		state.expiresAt = time.Now().Add(ttl)
		s.fileHashes[key] = state
		return true
	}

	if len(chunk) > 0 {
		if _, err := state.hasher.Write(chunk); err != nil {
			state.valid = false
			state.expiresAt = time.Now().Add(ttl)
			s.fileHashes[key] = state
			return true
		}
	}
	state.hashedSize += int64(len(chunk))
	state.hashToken = ""
	state.expiresAt = time.Now().Add(ttl)
	s.fileHashes[key] = state
	return true
}

func (s *transferStore) finalizeFileHash(txferID string, fileID uint64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fileHashKey{txferID: txferID, fileID: fileID}
	state, ok := s.fileHashes[key]
	if !ok || !state.valid || state.hasher == nil {
		return "", false
	}
	state.hashToken = formatXXH128HashToken(state.hasher.Sum128())
	state.finalized = true
	state.hasher = nil
	state.expiresAt = time.Now().Add(ttl)
	s.fileHashes[key] = state
	return state.hashToken, true
}

func (s *transferStore) getFileCompressionMode(txferID string, fileID uint64) (CompressionMode, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := fileHashKey{txferID: txferID, fileID: fileID}
	state, ok := s.fileHashes[key]
	if !ok || !state.hasLatestComp {
		return CompressionModeLz4, false
	}
	return policy.CompressionModeFromStored(state.latestComp), true
}

func (s *transferStore) setFileCompressionMode(txferID string, fileID uint64, mode CompressionMode) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fileHashKey{txferID: txferID, fileID: fileID}
	state, ok := s.fileHashes[key]
	if !ok {
		state = fileHashState{
			hasher:    xxh3.New128(),
			valid:     true,
			finalized: false,
		}
	}
	state.latestComp = uint8(mode)
	state.hasLatestComp = true
	state.expiresAt = time.Now().Add(ttl)
	s.fileHashes[key] = state
	return true
}

func (s *transferStore) verifyFileHashToken(txferID string, fileID uint64, expectedBytes int64, token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fileHashKey{txferID: txferID, fileID: fileID}
	state, ok := s.fileHashes[key]
	if !ok || !state.valid || !state.finalized {
		return false
	}
	if expectedBytes >= 0 && state.hashedSize != expectedBytes {
		return false
	}
	expectedToken := strings.ToLower(strings.TrimSpace(state.hashToken))
	actualToken := strings.ToLower(strings.TrimSpace(token))
	if expectedToken == "" || expectedToken != actualToken {
		return false
	}
	delete(s.fileHashes, key)
	return true
}

func (s *transferStore) delete(txferID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.transfers[txferID]; !ok {
		return false
	}
	delete(s.transfers, txferID)
	for key := range s.fileHashes {
		if key.txferID == txferID {
			delete(s.fileHashes, key)
		}
	}
	return true
}

func (s *transferStore) get(txferID string) (Transfer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	transfer, ok := s.transfers[txferID]
	if !ok {
		return Transfer{}, false
	}
	out := transfer
	out.State = append([]uint8(nil), transfer.State...)
	out.PathHash = append([]xxh3.Uint128(nil), transfer.PathHash...)
	out.FileSize = append([]int64(nil), transfer.FileSize...)
	out.AckedSize = append([]int64(nil), transfer.AckedSize...)
	return out, true
}

func (s *transferStore) getFileStates(txferID string) ([]TransferFileState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	transfer, ok := s.transfers[txferID]
	if !ok {
		return nil, false
	}
	states := make([]TransferFileState, len(transfer.State))
	for i := range transfer.State {
		states[i] = TransferFileState{
			State:    transfer.State[i],
			PathHash: transfer.PathHash[i],
		}
	}
	return states, true
}

func (s *transferStore) resolveFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error) {
	fullPath := filepath.Clean(fullPathRaw)
	if !filepath.IsAbs(fullPath) {
		return FileRef{}, &FileLookupError{Code: http.StatusBadRequest, Msg: "path must be absolute"}
	}

	s.mu.RLock()
	transfer, ok := s.transfers[txferID]
	if !ok {
		s.mu.RUnlock()
		return FileRef{}, &FileLookupError{Code: http.StatusNotFound, Msg: "transfer not found"}
	}
	if fileID >= uint64(len(transfer.State)) {
		s.mu.RUnlock()
		return FileRef{}, &FileLookupError{Code: http.StatusNotFound, Msg: "file id out of range"}
	}
	directory := transfer.Directory
	expectedDigest := transfer.PathHash[fileID]
	fileSize := transfer.FileSize[fileID]
	s.mu.RUnlock()

	if !pathWithinRoot(directory, fullPath) {
		return FileRef{}, &FileLookupError{Code: http.StatusForbidden, Msg: "path must be within transfer root"}
	}

	computedDigest := xxh3.Hash128([]byte(fullPath))
	if expectedDigest != computedDigest {
		return FileRef{}, &FileLookupError{Code: http.StatusForbidden, Msg: "file path digest mismatch"}
	}

	return FileRef{
		TransferID: txferID,
		FileID:     fileID,
		Path:       fullPath,
		Directory:  directory,
		FileSize:   fileSize,
	}, nil
}

func (s *transferStore) resetForTest() {
	s.mu.Lock()
	s.transfers = make(map[string]Transfer)
	s.fileHashes = make(map[fileHashKey]fileHashState)
	s.mu.Unlock()
}

func (s *transferStore) countForTest() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.transfers)
}

func (s *transferStore) listForTest() []Transfer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Transfer, 0, len(s.transfers))
	for _, transfer := range s.transfers {
		copyTransfer := transfer
		copyTransfer.State = append([]uint8(nil), transfer.State...)
		copyTransfer.PathHash = append([]xxh3.Uint128(nil), transfer.PathHash...)
		copyTransfer.FileSize = append([]int64(nil), transfer.FileSize...)
		copyTransfer.AckedSize = append([]int64(nil), transfer.AckedSize...)
		out = append(out, copyTransfer)
	}
	return out
}

func (s *transferStore) setFileState(txferID string, fileID uint64, state uint8) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	transfer, ok := s.transfers[txferID]
	if !ok {
		return false
	}
	if fileID >= uint64(len(transfer.State)) {
		return false
	}
	idx := int(fileID)
	if shouldAdvanceState(transfer.State[idx], state) {
		transfer.State[idx] = state
	}
	s.transfers[txferID] = transfer
	return true
}

func (s *transferStore) acknowledgeFile(txferID string, fileID uint64, ackBytes int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	transfer, ok := s.transfers[txferID]
	if !ok {
		return false
	}
	if fileID >= uint64(len(transfer.State)) {
		return false
	}
	idx := int(fileID)
	currentState := transfer.State[idx]

	if currentState == TransferStateMissing {
		s.transfers[txferID] = transfer
		return true
	}

	if ackBytes == -1 {
		if shouldAdvanceState(currentState, TransferStateMissing) {
			wasTerminal := currentState == TransferStateDone || currentState == TransferStateMissing
			transfer.State[idx] = TransferStateMissing
			if !wasTerminal {
				transfer.Done++
			}
		}
		s.transfers[txferID] = transfer
		return true
	}

	target := ackBytes
	if target < 0 {
		target = 0
	}
	maxAck := transfer.FileSize[idx]
	if maxAck < 0 {
		maxAck = 0
	}
	if target > maxAck {
		target = maxAck
	}

	prev := transfer.AckedSize[idx]
	if target <= prev {
		s.transfers[txferID] = transfer
		return true
	}

	delta := target - prev
	transfer.AckedSize[idx] = target
	transfer.DoneSize += delta

	if prev < maxAck && target == maxAck {
		transfer.Done++
		if shouldAdvanceState(transfer.State[idx], TransferStateDone) {
			transfer.State[idx] = TransferStateDone
		}
	}

	s.transfers[txferID] = transfer
	return true
}

func (s *transferStore) clipTransfer(txferID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	transfer, ok := s.transfers[txferID]
	if !ok {
		return false
	}

	transfer.State = slices.Clip(transfer.State)
	transfer.PathHash = slices.Clip(transfer.PathHash)
	transfer.FileSize = slices.Clip(transfer.FileSize)
	transfer.AckedSize = slices.Clip(transfer.AckedSize)
	s.transfers[txferID] = transfer
	return true
}

func (s *transferStore) reapExpiredLoop() {
	interval := ttl / 2
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for now := range ticker.C {
		s.mu.Lock()
		for txferID, transfer := range s.transfers {
			if !transfer.ExpiresAt.After(now) {
				delete(s.transfers, txferID)
			}
		}
		for key, state := range s.fileHashes {
			if !state.expiresAt.After(now) {
				delete(s.fileHashes, key)
				continue
			}
			if _, ok := s.transfers[key.txferID]; !ok {
				delete(s.fileHashes, key)
			}
		}
		s.mu.Unlock()
	}
}

func NewTransfer(directory string, numFiles int, totalSize int64) (Transfer, error) {
	for attempts := 0; attempts < 5; attempts++ {
		txferID, err := transferID()
		if err != nil {
			return Transfer{}, err
		}
		now := time.Now()
		transfer := Transfer{
			ID:        txferID,
			Directory: directory,
			NumFiles:  numFiles,
			TotalSize: totalSize,
			Done:      0,
			DoneSize:  0,
			State:     make([]uint8, numFiles),
			PathHash:  make([]xxh3.Uint128, numFiles),
			FileSize:  make([]int64, numFiles),
			AckedSize: make([]int64, numFiles),
			CreatedAt: now,
			ExpiresAt: now.Add(ttl),
		}
		for i := range transfer.State {
			transfer.State[i] = TransferStateStarted
		}
		if manager.create(transfer) {
			return transfer, nil
		}
	}
	return Transfer{}, fmt.Errorf("failed to allocate unique transfer id")
}

func SetTransferState(txferID string, state uint8) bool {
	return manager.setState(txferID, state)
}

func RegisterTransferFileState(txferID string, updatesCh <-chan TransferFileStateUpdate, state uint8) <-chan struct{} {
	done := make(chan struct{})
	if updatesCh == nil {
		close(done)
		return done
	}

	go func() {
		defer close(done)
		batch := make([]TransferFileStateUpdate, 0, 1000)

		flush := func() {
			if len(batch) == 0 {
				return
			}
			manager.appendFileStates(txferID, batch, state)
			batch = batch[:0]
		}

		for update := range updatesCh {
			batch = append(batch, update)
			if len(batch) == 1000 {
				flush()
			}
		}
		flush()
	}()
	return done
}

func RegisterTransferFileStates(txferID string, updates []TransferFileStateUpdate, state uint8) {
	manager.appendFileStates(txferID, updates, state)
}

func DeleteTransfer(txferID string) bool {
	return manager.delete(txferID)
}

func GetTransfer(txferID string) (Transfer, bool) {
	return manager.get(txferID)
}

func GetFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error) {
	return manager.resolveFileRef(txferID, fileID, fullPathRaw)
}

func GetFile(txferID string, fileID uint64, fullPathRaw string) (*os.File, FileRef, error) {
	ref, err := manager.resolveFileRef(txferID, fileID, fullPathRaw)
	if err != nil {
		return nil, FileRef{}, err
	}
	fd, err := os.Open(ref.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, FileRef{}, &FileLookupError{Code: http.StatusNotFound, Msg: "file not found"}
		}
		return nil, FileRef{}, &FileLookupError{Code: http.StatusInternalServerError, Msg: "failed to open file"}
	}
	return fd, ref, nil
}

func writeLookupErr(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	var lookupErr *FileLookupError
	if errors.As(err, &lookupErr) {
		http.Error(w, lookupErr.Msg, lookupErr.Code)
		return
	}
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func GetTransferFileStates(txferID string) ([]TransferFileState, bool) {
	return manager.getFileStates(txferID)
}

func AcknowledgeTransferFile(txferID string, fileID uint64, ackBytes int64) bool {
	return manager.acknowledgeFile(txferID, fileID, ackBytes)
}

func UpdateTransferFileHash(txferID string, fileID uint64, offset int64, chunk []byte) bool {
	return manager.appendFileHashChunk(txferID, fileID, offset, chunk)
}

func VerifyTransferFileHash(txferID string, fileID uint64, expectedBytes int64, hashToken string) bool {
	return manager.verifyFileHashToken(txferID, fileID, expectedBytes, hashToken)
}

func FinalizeTransferFileHash(txferID string, fileID uint64) (string, bool) {
	return manager.finalizeFileHash(txferID, fileID)
}

func GetTransferFileCompressionMode(txferID string, fileID uint64) (CompressionMode, bool) {
	return manager.getFileCompressionMode(txferID, fileID)
}

func SetTransferFileCompressionMode(txferID string, fileID uint64, mode CompressionMode) bool {
	return manager.setFileCompressionMode(txferID, fileID, mode)
}

func SetTransferFileState(txferID string, fileID uint64, state uint8) bool {
	return manager.setFileState(txferID, fileID, state)
}

func ClipTransfer(txferID string) bool {
	return manager.clipTransfer(txferID)
}

func resetTransferStoreForTest() {
	manager.resetForTest()
}

func transferCountForTest() int {
	return manager.countForTest()
}

func listTransfersForTest() []Transfer {
	return manager.listForTest()
}

func ResetTransferStoreForTest() {
	resetTransferStoreForTest()
}

func TransferCountForTest() int {
	return transferCountForTest()
}

func ListTransfersForTest() []Transfer {
	return listTransfersForTest()
}

func transferID() (string, error) {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate transfer id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func formatXXH128HashToken(v xxh3.Uint128) string {
	return codec.FormatXXH128HashToken(v)
}

func pathWithinRoot(root string, p string) bool {
	root = filepath.Clean(root)
	p = filepath.Clean(p)
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return false
	}
	return !filepath.IsAbs(rel)
}
