package store

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	intencoding "github.com/jolynch/pinch/internal/filexfer/encoding"
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
	ID          string
	Directory   string
	Mode        string
	LinkMbps    int64
	Concurrency int
	NumFiles    int
	TotalSize   int64
	Done        uint64
	DoneSize    int64
	State       []uint8
	PathHash    []xxh3.Uint128
	FileSize    []int64
	AckedSize   []int64
	CreatedAt   time.Time
	ExpiresAt   time.Time
	DeadlineMS  int64     // 0 = no deadline
	FirstSendAt time.Time // set on first gentle SEND
	TooSlow     bool      // sticky flag set when deadline is exceeded
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

type managedTransfer struct {
	mu           sync.RWMutex
	deleted      bool
	transfer     Transfer
	fileHashes   map[uint64]fileHashState
	windowHashes map[windowHashKey]*windowHashState
}

type transferStore struct {
	mu        sync.RWMutex
	transfers map[string]*managedTransfer
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

type windowHashKey struct {
	fileID   uint64
	endBytes int64
}

type windowHashState struct {
	hashToken string
	expiresAt time.Time
}

// AckEntry is a single (transfer, file, ackBytes) tuple for batch acknowledgement.
type AckEntry struct {
	TxferID  string
	FileID   uint64
	AckBytes int64
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
		transfers: make(map[string]*managedTransfer),
	}
}

func newManagedTransfer(transfer Transfer) *managedTransfer {
	return &managedTransfer{
		transfer:     transfer,
		fileHashes:   make(map[uint64]fileHashState),
		windowHashes: make(map[windowHashKey]*windowHashState),
	}
}

func shouldAdvanceState(current uint8, next uint8) bool {
	return next >= current
}

func cloneTransfer(transfer Transfer) Transfer {
	out := transfer
	out.State = append([]uint8(nil), transfer.State...)
	out.PathHash = append([]xxh3.Uint128(nil), transfer.PathHash...)
	out.FileSize = append([]int64(nil), transfer.FileSize...)
	out.AckedSize = append([]int64(nil), transfer.AckedSize...)
	return out
}

func (s *transferStore) getManagedTransfer(txferID string) (*managedTransfer, bool) {
	s.mu.RLock()
	managed, ok := s.transfers[txferID]
	s.mu.RUnlock()
	return managed, ok
}

func (s *transferStore) create(transfer Transfer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.transfers[transfer.ID]; exists {
		return false
	}
	s.transfers[transfer.ID] = newManagedTransfer(transfer)
	return true
}

func (s *transferStore) setState(txferID string, state uint8) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	for i := range managed.transfer.State {
		if shouldAdvanceState(managed.transfer.State[i], state) {
			managed.transfer.State[i] = state
		}
	}
	return true
}

func (s *transferStore) setTransferHints(txferID string, mode string, linkMbps int64, concurrency int) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	managed.transfer.Mode = strings.ToLower(strings.TrimSpace(mode))
	managed.transfer.LinkMbps = linkMbps
	managed.transfer.Concurrency = concurrency
	return true
}

func (s *transferStore) setTransferDeadline(txferID string, deadlineMS int64) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	managed.transfer.DeadlineMS = deadlineMS
	return true
}

func (s *transferStore) recordFirstSend(txferID string) (time.Time, bool) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return time.Time{}, false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return time.Time{}, false
	}
	if managed.transfer.FirstSendAt.IsZero() {
		managed.transfer.FirstSendAt = time.Now()
	}
	return managed.transfer.FirstSendAt, true
}

func (s *transferStore) markTooSlow(txferID string) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	managed.transfer.TooSlow = true
	return true
}

func (s *transferStore) appendFileStates(txferID string, updates []TransferFileStateUpdate, state uint8) {
	if len(updates) == 0 {
		return
	}
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return
	}

	ensureLen := func(n int) {
		if n <= len(managed.transfer.State) {
			return
		}
		oldLen := len(managed.transfer.State)
		growBy := n - oldLen
		managed.transfer.State = append(managed.transfer.State, make([]uint8, growBy)...)
		managed.transfer.PathHash = append(managed.transfer.PathHash, make([]xxh3.Uint128, growBy)...)
		managed.transfer.FileSize = append(managed.transfer.FileSize, make([]int64, growBy)...)
		managed.transfer.AckedSize = append(managed.transfer.AckedSize, make([]int64, growBy)...)
		for i := oldLen; i < n; i++ {
			managed.transfer.State[i] = TransferStateStarted
		}
	}

	for _, update := range updates {
		idx := int(update.FileID)
		ensureLen(idx + 1)

		if idx+1 > managed.transfer.NumFiles {
			managed.transfer.NumFiles = idx + 1
		}

		managed.transfer.TotalSize += update.FileSize - managed.transfer.FileSize[idx]
		managed.transfer.FileSize[idx] = update.FileSize
		managed.transfer.PathHash[idx] = update.PathHash
		if shouldAdvanceState(managed.transfer.State[idx], state) {
			managed.transfer.State[idx] = state
		}
	}
}

func (s *transferStore) appendFileHashChunk(txferID string, fileID uint64, offset int64, chunk []byte) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	if fileID >= uint64(len(managed.transfer.State)) || offset < 0 {
		return false
	}

	state, exists := managed.fileHashes[fileID]
	if !exists || offset == 0 {
		state = fileHashState{
			hasher:    xxh3.New128(),
			valid:     true,
			finalized: false,
		}
	}
	if !state.valid || state.finalized {
		state.expiresAt = time.Now().Add(ttl)
		managed.fileHashes[fileID] = state
		return true
	}
	if offset != state.hashedSize {
		state.valid = false
		state.expiresAt = time.Now().Add(ttl)
		managed.fileHashes[fileID] = state
		return true
	}

	if len(chunk) > 0 {
		if _, err := state.hasher.Write(chunk); err != nil {
			state.valid = false
			state.expiresAt = time.Now().Add(ttl)
			managed.fileHashes[fileID] = state
			return true
		}
	}
	state.hashedSize += int64(len(chunk))
	state.hashToken = ""
	state.expiresAt = time.Now().Add(ttl)
	managed.fileHashes[fileID] = state
	return true
}

func (s *transferStore) finalizeFileHash(txferID string, fileID uint64) (string, bool) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return "", false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return "", false
	}
	state, ok := managed.fileHashes[fileID]
	if !ok || !state.valid || state.hasher == nil {
		return "", false
	}
	state.hashToken = formatXXH128HashToken(state.hasher.Sum128())
	state.finalized = true
	state.hasher = nil
	state.expiresAt = time.Now().Add(ttl)
	managed.fileHashes[fileID] = state
	return state.hashToken, true
}

func (s *transferStore) getFileCompressionMode(txferID string, fileID uint64) (CompressionMode, bool) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return CompressionModeLz4, false
	}

	managed.mu.RLock()
	defer managed.mu.RUnlock()
	if managed.deleted {
		return CompressionModeLz4, false
	}
	state, ok := managed.fileHashes[fileID]
	if !ok || !state.hasLatestComp {
		return CompressionModeLz4, false
	}
	return policy.CompressionModeFromStored(state.latestComp), true
}

func (s *transferStore) setFileCompressionMode(txferID string, fileID uint64, mode CompressionMode) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}

	state, ok := managed.fileHashes[fileID]
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
	managed.fileHashes[fileID] = state
	return true
}

func (s *transferStore) verifyFileHashToken(txferID string, fileID uint64, expectedBytes int64, token string) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}

	state, ok := managed.fileHashes[fileID]
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
	delete(managed.fileHashes, fileID)
	return true
}

func (s *transferStore) delete(txferID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	managed, ok := s.transfers[txferID]
	if !ok {
		return false
	}
	managed.mu.Lock()
	managed.deleted = true
	managed.mu.Unlock()
	delete(s.transfers, txferID)
	return true
}

func (s *transferStore) get(txferID string) (Transfer, bool) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return Transfer{}, false
	}

	managed.mu.RLock()
	defer managed.mu.RUnlock()
	if managed.deleted {
		return Transfer{}, false
	}
	return cloneTransfer(managed.transfer), true
}

func (s *transferStore) getFileStates(txferID string) ([]TransferFileState, bool) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return nil, false
	}

	managed.mu.RLock()
	defer managed.mu.RUnlock()
	if managed.deleted {
		return nil, false
	}
	states := make([]TransferFileState, len(managed.transfer.State))
	for i := range managed.transfer.State {
		states[i] = TransferFileState{
			State:    managed.transfer.State[i],
			PathHash: managed.transfer.PathHash[i],
		}
	}
	return states, true
}

func (s *transferStore) resolveFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error) {
	fullPath := filepath.Clean(fullPathRaw)
	if !filepath.IsAbs(fullPath) {
		return FileRef{}, &FileLookupError{Code: http.StatusBadRequest, Msg: "path must be absolute"}
	}

	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return FileRef{}, &FileLookupError{Code: http.StatusNotFound, Msg: "transfer not found"}
	}

	managed.mu.RLock()
	if managed.deleted {
		managed.mu.RUnlock()
		return FileRef{}, &FileLookupError{Code: http.StatusNotFound, Msg: "transfer not found"}
	}
	if fileID >= uint64(len(managed.transfer.State)) {
		managed.mu.RUnlock()
		return FileRef{}, &FileLookupError{Code: http.StatusNotFound, Msg: "file id out of range"}
	}
	directory := managed.transfer.Directory
	expectedDigest := managed.transfer.PathHash[fileID]
	fileSize := managed.transfer.FileSize[fileID]
	managed.mu.RUnlock()

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
	s.transfers = make(map[string]*managedTransfer)
	s.mu.Unlock()
}

func (s *transferStore) countForTest() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.transfers)
}

func (s *transferStore) listForTest() []Transfer {
	s.mu.RLock()
	transfers := make([]*managedTransfer, 0, len(s.transfers))
	for _, managed := range s.transfers {
		transfers = append(transfers, managed)
	}
	s.mu.RUnlock()

	out := make([]Transfer, 0, len(transfers))
	for _, managed := range transfers {
		managed.mu.RLock()
		if !managed.deleted {
			out = append(out, cloneTransfer(managed.transfer))
		}
		managed.mu.RUnlock()
	}
	return out
}

func (s *transferStore) setFileState(txferID string, fileID uint64, state uint8) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	if fileID >= uint64(len(managed.transfer.State)) {
		return false
	}
	idx := int(fileID)
	if !shouldAdvanceState(managed.transfer.State[idx], state) || managed.transfer.State[idx] == state {
		return true
	}
	managed.transfer.State[idx] = state
	return true
}

func (s *transferStore) acknowledgeFiles(entries []AckEntry) bool {
	grouped := make(map[string][]AckEntry)
	order := make([]string, 0)
	for _, entry := range entries {
		if _, ok := grouped[entry.TxferID]; !ok {
			order = append(order, entry.TxferID)
		}
		grouped[entry.TxferID] = append(grouped[entry.TxferID], entry)
	}

	ok := true
	for _, txferID := range order {
		managed, found := s.getManagedTransfer(txferID)
		if !found {
			ok = false
			continue
		}

		managed.mu.Lock()
		if managed.deleted {
			managed.mu.Unlock()
			ok = false
			continue
		}
		for _, entry := range grouped[txferID] {
			if !s.acknowledgeFileLocked(managed, entry.FileID, entry.AckBytes) {
				ok = false
			}
		}
		managed.mu.Unlock()
	}
	return ok
}

func (s *transferStore) acknowledgeFileLocked(managed *managedTransfer, fileID uint64, ackBytes int64) bool {
	if managed == nil || managed.deleted {
		return false
	}
	if fileID >= uint64(len(managed.transfer.State)) {
		return false
	}
	idx := int(fileID)
	currentState := managed.transfer.State[idx]

	if currentState == TransferStateMissing {
		return true
	}

	if ackBytes == -1 {
		if shouldAdvanceState(currentState, TransferStateMissing) {
			wasTerminal := currentState == TransferStateDone || currentState == TransferStateMissing
			managed.transfer.State[idx] = TransferStateMissing
			if !wasTerminal {
				managed.transfer.Done++
			}
		}
		return true
	}

	target := ackBytes
	if target < 0 {
		target = 0
	}
	maxAck := managed.transfer.FileSize[idx]
	if maxAck < 0 {
		maxAck = 0
	}
	if target > maxAck {
		target = maxAck
	}

	prev := managed.transfer.AckedSize[idx]
	if target <= prev {
		return true
	}

	delta := target - prev
	managed.transfer.AckedSize[idx] = target
	managed.transfer.DoneSize += delta

	if prev < maxAck && target == maxAck {
		managed.transfer.Done++
		if shouldAdvanceState(managed.transfer.State[idx], TransferStateDone) {
			managed.transfer.State[idx] = TransferStateDone
		}
	}

	delete(managed.windowHashes, windowHashKey{fileID: fileID, endBytes: target})
	return true
}

func (s *transferStore) clipTransfer(txferID string) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	managed.transfer.State = slices.Clip(managed.transfer.State)
	managed.transfer.PathHash = slices.Clip(managed.transfer.PathHash)
	managed.transfer.FileSize = slices.Clip(managed.transfer.FileSize)
	managed.transfer.AckedSize = slices.Clip(managed.transfer.AckedSize)
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
		survivors := make([]*managedTransfer, 0, len(s.transfers))
		for txferID, managed := range s.transfers {
			managed.mu.RLock()
			expired := !managed.deleted && !managed.transfer.ExpiresAt.After(now)
			managed.mu.RUnlock()
			if expired {
				managed.mu.Lock()
				managed.deleted = true
				managed.mu.Unlock()
				delete(s.transfers, txferID)
				continue
			}
			survivors = append(survivors, managed)
		}
		s.mu.Unlock()

		for _, managed := range survivors {
			managed.mu.Lock()
			if managed.deleted {
				managed.mu.Unlock()
				continue
			}
			for fileID, state := range managed.fileHashes {
				if !state.expiresAt.After(now) {
					delete(managed.fileHashes, fileID)
				}
			}
			for key, state := range managed.windowHashes {
				if !state.expiresAt.After(now) {
					delete(managed.windowHashes, key)
				}
			}
			managed.mu.Unlock()
		}
	}
}

func validHashToken(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	parts := strings.SplitN(raw, ":", 2)
	return len(parts) == 2 && strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != ""
}

func normalizeHashToken(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func (s *transferStore) setWindowHashToken(txferID string, fileID uint64, endBytes int64, token string) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.deleted {
		return false
	}
	if fileID >= uint64(len(managed.transfer.State)) || endBytes < 0 {
		return false
	}
	if !validHashToken(token) {
		return false
	}
	managed.windowHashes[windowHashKey{fileID: fileID, endBytes: endBytes}] = &windowHashState{
		hashToken: normalizeHashToken(token),
		expiresAt: time.Now().Add(ttl),
	}
	return true
}

func (s *transferStore) verifyWindowHashToken(txferID string, fileID uint64, endBytes int64, token string) bool {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return false
	}

	managed.mu.RLock()
	defer managed.mu.RUnlock()
	if managed.deleted || endBytes < 0 {
		return false
	}
	state, ok := managed.windowHashes[windowHashKey{fileID: fileID, endBytes: endBytes}]
	if !ok {
		return false
	}
	return state.hashToken == normalizeHashToken(token)
}

func NewTransfer(directory string, numFiles int, totalSize int64) (Transfer, error) {
	for attempts := 0; attempts < 5; attempts++ {
		txferID, err := transferID()
		if err != nil {
			return Transfer{}, err
		}
		now := time.Now()
		transfer := Transfer{
			ID:          txferID,
			Directory:   directory,
			Mode:        "",
			LinkMbps:    0,
			Concurrency: 0,
			NumFiles:    numFiles,
			TotalSize:   totalSize,
			Done:        0,
			DoneSize:    0,
			State:       make([]uint8, numFiles),
			PathHash:    make([]xxh3.Uint128, numFiles),
			FileSize:    make([]int64, numFiles),
			AckedSize:   make([]int64, numFiles),
			CreatedAt:   now,
			ExpiresAt:   now.Add(ttl),
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

func SetTransferHints(txferID string, mode string, linkMbps int64, concurrency int) bool {
	return manager.setTransferHints(txferID, mode, linkMbps, concurrency)
}

func SetTransferDeadline(txferID string, deadlineMS int64) bool {
	return manager.setTransferDeadline(txferID, deadlineMS)
}

func RecordTransferFirstSend(txferID string) (time.Time, bool) {
	return manager.recordFirstSend(txferID)
}

func MarkTransferTooSlow(txferID string) bool {
	return manager.markTooSlow(txferID)
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

func GetTransferFileStates(txferID string) ([]TransferFileState, bool) {
	return manager.getFileStates(txferID)
}

func AcknowledgeTransferFile(txferID string, fileID uint64, ackBytes int64) bool {
	return manager.acknowledgeFiles([]AckEntry{{TxferID: txferID, FileID: fileID, AckBytes: ackBytes}})
}

func AcknowledgeTransferFiles(entries []AckEntry) bool {
	return manager.acknowledgeFiles(entries)
}

func UpdateTransferFileHash(txferID string, fileID uint64, offset int64, chunk []byte) bool {
	return manager.appendFileHashChunk(txferID, fileID, offset, chunk)
}

func VerifyTransferFileHash(txferID string, fileID uint64, expectedBytes int64, hashToken string) bool {
	return manager.verifyFileHashToken(txferID, fileID, expectedBytes, hashToken)
}

func SetTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, hashToken string) bool {
	return manager.setWindowHashToken(txferID, fileID, endBytes, hashToken)
}

func VerifyTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, hashToken string) bool {
	return manager.verifyWindowHashToken(txferID, fileID, endBytes, hashToken)
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
	return intencoding.FormatXXH128HashToken(v)
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
