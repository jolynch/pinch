package filexfer

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
)

const (
	defaultTransferTTL = 10 * time.Minute
	// State enum for transfer lifecycle.
	TransferStateStarted uint8 = iota
	TransferStateRunning
	TransferStateDone
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

type transferStore struct {
	mu        sync.RWMutex
	transfers map[string]Transfer
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
		transfers: make(map[string]Transfer),
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

func (s *transferStore) delete(txferID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.transfers[txferID]; !ok {
		return false
	}
	delete(s.transfers, txferID)
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

func (s *transferStore) resetForTest() {
	s.mu.Lock()
	s.transfers = make(map[string]Transfer)
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

func GetTransferFileStates(txferID string) ([]TransferFileState, bool) {
	return manager.getFileStates(txferID)
}

func AcknowledgeTransferFile(txferID string, fileID uint64, ackBytes int64) bool {
	return manager.acknowledgeFile(txferID, fileID, ackBytes)
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

func transferID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate transfer id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
