package filexfer

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
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
	State     uint8
	CreatedAt time.Time
	ExpiresAt time.Time
}

var (
	storeMu sync.RWMutex
	ttl     = defaultTransferTTL
	ids     = make(map[string]Transfer)
)

func init() {
	go reapExpired()
}

func NewTransfer(directory string, numFiles int, totalSize int64) (Transfer, error) {
	id, err := transferID()
	if err != nil {
		return Transfer{}, err
	}
	now := time.Now()
	t := Transfer{
		ID:        id,
		Directory: directory,
		NumFiles:  numFiles,
		TotalSize: totalSize,
		State:     TransferStateStarted,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
	}
	storeMu.Lock()
	ids[id] = t
	storeMu.Unlock()
	return t, nil
}

func SetTransferState(id string, state uint8) bool {
	storeMu.Lock()
	defer storeMu.Unlock()
	val, ok := ids[id]
	if !ok {
		return false
	}
	val.State = state
	ids[id] = val
	return true
}

func reapExpired() {
	interval := ttl / 2
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for now := range ticker.C {
		storeMu.Lock()
		for id, t := range ids {
			if !t.ExpiresAt.After(now) {
				delete(ids, id)
			}
		}
		storeMu.Unlock()
	}
}

func transferID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate transfer id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
