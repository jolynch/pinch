package store

import (
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
)

func resetTransferStore() {
	resetTransferStoreForTest()
}

func waitForTransferState(t *testing.T, txferID string, check func(Transfer) bool) Transfer {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		stored, ok := GetTransfer(txferID)
		if ok && check(stored) {
			return stored
		}
		time.Sleep(5 * time.Millisecond)
	}

	_, ok := GetTransfer(txferID)
	if !ok {
		t.Fatalf("transfer %q not found in store", txferID)
	}
	t.Fatalf("timed out waiting for expected transfer state")
	return Transfer{}
}

func TestNewTransferInitializesStateByFileID(t *testing.T) {
	resetTransferStore()

	transfer, err := NewTransfer("/tmp/x", 3, 42)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}

	stored, ok := GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("transfer %q not found in store", transfer.ID)
	}
	if len(stored.State) != 3 {
		t.Fatalf("expected state len 3, got %d", len(stored.State))
	}
	if len(stored.PathHash) != 3 {
		t.Fatalf("expected hash len 3, got %d", len(stored.PathHash))
	}
	if len(stored.FileSize) != 3 {
		t.Fatalf("expected file-size len 3, got %d", len(stored.FileSize))
	}
	if len(stored.AckedSize) != 3 {
		t.Fatalf("expected acked-size len 3, got %d", len(stored.AckedSize))
	}
	if stored.Done != 0 {
		t.Fatalf("expected done 0, got %d", stored.Done)
	}
	if stored.DoneSize != 0 {
		t.Fatalf("expected done size 0, got %d", stored.DoneSize)
	}
	for i, state := range stored.State {
		if state != TransferStateStarted {
			t.Fatalf("expected file state %d to be started, got %d", i, state)
		}
	}
}

func TestRegisterTransferFileState(t *testing.T) {
	resetTransferStore()

	transfer, err := NewTransfer("/tmp/x", 1, 42)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	hash := xxh3.Hash128([]byte("/tmp/x/1"))
	updatesCh := make(chan TransferFileStateUpdate, 1)
	updatesCh <- TransferFileStateUpdate{FileID: 0, PathHash: hash, FileSize: 42}
	close(updatesCh)
	RegisterTransferFileState(transfer.ID, updatesCh, TransferStateDone)

	stored := waitForTransferState(t, transfer.ID, func(stored Transfer) bool {
		return len(stored.State) == 1 && stored.State[0] == TransferStateDone
	})
	if stored.State[0] != TransferStateDone {
		t.Fatalf("expected state[0] to be done, got %d", stored.State[0])
	}
	if stored.PathHash[0] != hash {
		t.Fatalf("expected hash[0] to be updated")
	}
	if stored.FileSize[0] != 42 {
		t.Fatalf("expected file-size[0] to be updated, got %d", stored.FileSize[0])
	}
}

func TestRegisterTransferFileStateMultipleIDs(t *testing.T) {
	resetTransferStore()

	transfer, err := NewTransfer("/tmp/x", 1, 42)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	hash0 := xxh3.Hash128([]byte("/tmp/x/0"))
	hash2 := xxh3.Hash128([]byte("/tmp/x/2"))
	updatesCh := make(chan TransferFileStateUpdate, 2)
	updatesCh <- TransferFileStateUpdate{FileID: 0, PathHash: hash0, FileSize: 100}
	updatesCh <- TransferFileStateUpdate{FileID: 1, PathHash: hash2, FileSize: 300}
	close(updatesCh)
	RegisterTransferFileState(transfer.ID, updatesCh, TransferStateRunning)

	stored := waitForTransferState(t, transfer.ID, func(stored Transfer) bool {
		return len(stored.State) == 2 && stored.State[0] == TransferStateRunning && stored.State[1] == TransferStateRunning
	})
	if stored.State[0] != TransferStateRunning {
		t.Fatalf("expected state[0] to be running, got %d", stored.State[0])
	}
	if stored.State[1] != TransferStateRunning {
		t.Fatalf("expected state[1] to be running, got %d", stored.State[1])
	}
	if stored.PathHash[0] != hash0 {
		t.Fatalf("expected hash[0] to be updated")
	}
	if stored.PathHash[1] != hash2 {
		t.Fatalf("expected hash[1] to be updated")
	}
	if stored.FileSize[0] != 100 || stored.FileSize[1] != 300 {
		t.Fatalf("expected file sizes at indices 0 and 1 to be updated")
	}
}

func TestSetTransferStateDoesNotRegressDoneToStarted(t *testing.T) {
	resetTransferStore()

	transfer, err := NewTransfer("/tmp/x", 1, 42)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	if ok := SetTransferState(transfer.ID, TransferStateDone); !ok {
		t.Fatalf("SetTransferState to done returned false")
	}
	if ok := SetTransferState(transfer.ID, TransferStateStarted); !ok {
		t.Fatalf("SetTransferState to started returned false")
	}

	stored, ok := GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("transfer %q not found in store", transfer.ID)
	}
	if stored.State[0] != TransferStateDone {
		t.Fatalf("expected state[0] to remain done, got %d", stored.State[0])
	}
}

func TestRegisterTransferFileStateDoesNotRegressDoneToStarted(t *testing.T) {
	resetTransferStore()

	transfer, err := NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	doneHash := xxh3.Hash128([]byte("/tmp/x/done"))
	doneUpdatesCh := make(chan TransferFileStateUpdate, 1)
	doneUpdatesCh <- TransferFileStateUpdate{FileID: 0, PathHash: doneHash, FileSize: 100}
	close(doneUpdatesCh)
	RegisterTransferFileState(transfer.ID, doneUpdatesCh, TransferStateDone)
	_ = waitForTransferState(t, transfer.ID, func(stored Transfer) bool {
		return len(stored.State) >= 1 && stored.State[0] == TransferStateDone
	})
	startedHash := xxh3.Hash128([]byte("/tmp/x/started"))
	startedUpdatesCh := make(chan TransferFileStateUpdate, 1)
	startedUpdatesCh <- TransferFileStateUpdate{FileID: 1, PathHash: startedHash, FileSize: 100}
	close(startedUpdatesCh)
	RegisterTransferFileState(transfer.ID, startedUpdatesCh, TransferStateStarted)

	stored := waitForTransferState(t, transfer.ID, func(stored Transfer) bool {
		return len(stored.State) == 2 && stored.State[0] == TransferStateDone && stored.State[1] == TransferStateStarted
	})
	if stored.State[0] != TransferStateDone {
		t.Fatalf("expected state[0] to remain done, got %d", stored.State[0])
	}
	if stored.PathHash[1] != startedHash {
		t.Fatalf("expected hash[1] to be set from second append update")
	}
}

func TestGetTransferFileStates(t *testing.T) {
	resetTransferStore()

	transfer, err := NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	hash0 := xxh3.Hash128([]byte("/tmp/x/0"))
	hash1 := xxh3.Hash128([]byte("/tmp/x/1"))
	updatesCh := make(chan TransferFileStateUpdate, 2)
	updatesCh <- TransferFileStateUpdate{FileID: 0, PathHash: hash0, FileSize: 10}
	updatesCh <- TransferFileStateUpdate{FileID: 1, PathHash: hash1, FileSize: 20}
	close(updatesCh)
	RegisterTransferFileState(transfer.ID, updatesCh, TransferStateRunning)
	_ = waitForTransferState(t, transfer.ID, func(stored Transfer) bool {
		return len(stored.State) >= 2 &&
			stored.State[0] == TransferStateRunning &&
			stored.State[1] == TransferStateRunning
	})

	states, ok := GetTransferFileStates(transfer.ID)
	if !ok {
		t.Fatalf("GetTransferFileStates returned not found")
	}
	if len(states) != 2 {
		t.Fatalf("expected 2 wrapped states, got %d", len(states))
	}
	if states[0].State != TransferStateRunning || states[0].PathHash != hash0 {
		t.Fatalf("unexpected wrapped state at index 0")
	}
	if states[1].State != TransferStateRunning || states[1].PathHash != hash1 {
		t.Fatalf("unexpected wrapped state at index 1")
	}
}

func TestTransferFileCompressionModeSetGet(t *testing.T) {
	resetTransferStore()
	transfer, err := NewTransfer("/tmp/x", 1, 1)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	if _, ok := GetTransferFileCompressionMode(transfer.ID, 0); ok {
		t.Fatalf("expected no compression mode before set")
	}
	if ok := SetTransferFileCompressionMode(transfer.ID, 0, CompressionModeLz4); !ok {
		t.Fatalf("SetTransferFileCompressionMode returned false")
	}
	mode, ok := GetTransferFileCompressionMode(transfer.ID, 0)
	if !ok {
		t.Fatalf("expected compression mode to be present")
	}
	if mode != CompressionModeLz4 {
		t.Fatalf("expected lz4 mode, got %v", mode)
	}
}

func TestTransferFileCompressionModeClearedAfterVerify(t *testing.T) {
	resetTransferStore()
	transfer, err := NewTransfer("/tmp/x", 1, 5)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	data := []byte("hello")
	if ok := UpdateTransferFileHash(transfer.ID, 0, 0, data); !ok {
		t.Fatalf("UpdateTransferFileHash returned false")
	}
	if _, ok := FinalizeTransferFileHash(transfer.ID, 0); !ok {
		t.Fatalf("FinalizeTransferFileHash returned false")
	}
	token := formatXXH128HashToken(xxh3.Hash128(data))
	if ok := SetTransferFileCompressionMode(transfer.ID, 0, CompressionModeZstdLevel1); !ok {
		t.Fatalf("SetTransferFileCompressionMode returned false")
	}
	if _, ok := GetTransferFileCompressionMode(transfer.ID, 0); !ok {
		t.Fatalf("expected compression mode before verify")
	}
	if ok := VerifyTransferFileHash(transfer.ID, 0, int64(len(data)), token); !ok {
		t.Fatalf("VerifyTransferFileHash returned false")
	}
	if _, ok := GetTransferFileCompressionMode(transfer.ID, 0); ok {
		t.Fatalf("expected compression mode to be cleared after verify")
	}
}

func TestRegisterTransferFileStateBatchOver1000(t *testing.T) {
	resetTransferStore()

	numFiles := 1005
	transfer, err := NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	updatesCh := make(chan TransferFileStateUpdate, numFiles)
	for i := 0; i < numFiles; i++ {
		updatesCh <- TransferFileStateUpdate{
			FileID:   uint64(i),
			PathHash: xxh3.Hash128([]byte("file-" + strconv.Itoa(i))),
			FileSize: int64(i + 1),
		}
	}
	close(updatesCh)
	RegisterTransferFileState(transfer.ID, updatesCh, TransferStateRunning)

	stored := waitForTransferState(t, transfer.ID, func(stored Transfer) bool {
		return len(stored.State) == numFiles && stored.State[numFiles-1] == TransferStateRunning
	})
	for i := 0; i < numFiles; i++ {
		if stored.State[i] != TransferStateRunning {
			t.Fatalf("expected state[%d] to be running, got %d", i, stored.State[i])
		}
	}
}

func TestAcknowledgeTransferFile(t *testing.T) {
	resetTransferStore()

	transfer, err := NewTransfer("/tmp/x", 0, 0)
	if err != nil {
		t.Fatalf("NewTransfer returned error: %v", err)
	}
	updatesCh := make(chan TransferFileStateUpdate, 1)
	updatesCh <- TransferFileStateUpdate{
		FileID:   0,
		PathHash: xxh3.Hash128([]byte("/tmp/x/0")),
		FileSize: 10,
	}
	close(updatesCh)
	RegisterTransferFileState(transfer.ID, updatesCh, TransferStateRunning)
	_ = waitForTransferState(t, transfer.ID, func(stored Transfer) bool {
		return len(stored.FileSize) >= 1 && stored.FileSize[0] == 10
	})

	if ok := AcknowledgeTransferFile(transfer.ID, 0, 4); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false")
	}
	stored, ok := GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("transfer %q not found", transfer.ID)
	}
	if stored.DoneSize != 4 || stored.Done != 0 {
		t.Fatalf("unexpected counters after partial ack: done=%d doneSize=%d", stored.Done, stored.DoneSize)
	}

	if ok := AcknowledgeTransferFile(transfer.ID, 0, 4); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false for repeated ack")
	}
	stored, _ = GetTransfer(transfer.ID)
	if stored.DoneSize != 4 || stored.Done != 0 {
		t.Fatalf("unexpected counters after repeated ack: done=%d doneSize=%d", stored.Done, stored.DoneSize)
	}

	if ok := AcknowledgeTransferFile(transfer.ID, 0, 12); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false for oversized ack")
	}
	stored, _ = GetTransfer(transfer.ID)
	if stored.DoneSize != 10 || stored.Done != 1 {
		t.Fatalf("unexpected counters after completion ack: done=%d doneSize=%d", stored.Done, stored.DoneSize)
	}
	if stored.State[0] != TransferStateDone {
		t.Fatalf("expected state done after full ack, got %d", stored.State[0])
	}
}

func TestAcknowledgeTransferFileMissing(t *testing.T) {
	resetTransferStore()
	transfer, err := NewTransfer("/tmp/x", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer failed: %v", err)
	}
	updates := []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/x/0")), FileSize: 10},
	}
	RegisterTransferFileStates(transfer.ID, updates, TransferStateStarted)

	if ok := AcknowledgeTransferFile(transfer.ID, 0, -1); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false")
	}
	stored, ok := GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("transfer not found")
	}
	if stored.State[0] != TransferStateMissing {
		t.Fatalf("expected missing state, got %d", stored.State[0])
	}
	if stored.Done != 1 {
		t.Fatalf("expected done=1 for missing file, got %d", stored.Done)
	}
}

func TestWindowHashesTrackedPerEndOffset(t *testing.T) {
	resetTransferStore()

	transfer, err := NewTransfer("/tmp/x", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer failed: %v", err)
	}
	updates := []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/x/0")), FileSize: 10},
	}
	RegisterTransferFileStates(transfer.ID, updates, TransferStateRunning)

	token4 := "xxh128:00000000000000000000000000000004"
	token8 := "xxh128:00000000000000000000000000000008"
	if ok := SetTransferFileWindowHash(transfer.ID, 0, 4, token4); !ok {
		t.Fatalf("SetTransferFileWindowHash returned false for end=4")
	}
	if ok := SetTransferFileWindowHash(transfer.ID, 0, 8, token8); !ok {
		t.Fatalf("SetTransferFileWindowHash returned false for end=8")
	}
	if !VerifyTransferFileWindowHash(transfer.ID, 0, 4, token4) {
		t.Fatalf("expected window hash verification for end=4")
	}
	if !VerifyTransferFileWindowHash(transfer.ID, 0, 8, token8) {
		t.Fatalf("expected window hash verification for end=8")
	}

	if ok := AcknowledgeTransferFile(transfer.ID, 0, 4); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false for end=4")
	}
	if VerifyTransferFileWindowHash(transfer.ID, 0, 4, token4) {
		t.Fatalf("expected end=4 window hash to be cleared after ack")
	}
	if !VerifyTransferFileWindowHash(transfer.ID, 0, 8, token8) {
		t.Fatalf("expected end=8 window hash to remain after end=4 ack")
	}

	if ok := AcknowledgeTransferFile(transfer.ID, 0, 8); !ok {
		t.Fatalf("AcknowledgeTransferFile returned false for end=8")
	}
	if VerifyTransferFileWindowHash(transfer.ID, 0, 8, token8) {
		t.Fatalf("expected end=8 window hash to be cleared after ack")
	}
}

func TestDeleteTransferInvalidatesManagedTransfer(t *testing.T) {
	resetTransferStore()

	transfer, err := NewTransfer("/tmp/x", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer failed: %v", err)
	}
	RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/x/0")), FileSize: 10},
	}, TransferStateRunning)

	managed, ok := manager.getManagedTransfer(transfer.ID)
	if !ok {
		t.Fatalf("expected managed transfer")
	}
	if ok := DeleteTransfer(transfer.ID); !ok {
		t.Fatalf("DeleteTransfer returned false")
	}
	if _, ok := GetTransfer(transfer.ID); ok {
		t.Fatalf("deleted transfer should not be visible")
	}

	managed.mu.RLock()
	if !managed.deleted {
		managed.mu.RUnlock()
		t.Fatalf("expected managed transfer to be marked deleted")
	}
	managed.mu.RUnlock()

	managed.mu.Lock()
	defer managed.mu.Unlock()
	if ok := manager.acknowledgeFileLocked(managed, 0, 4); ok {
		t.Fatalf("expected stale managed transfer mutation to fail after delete")
	}
}

func TestAcknowledgeTransferFilesMixedTransfers(t *testing.T) {
	resetTransferStore()

	first, err := NewTransfer("/tmp/a", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer first failed: %v", err)
	}
	second, err := NewTransfer("/tmp/b", 1, 12)
	if err != nil {
		t.Fatalf("NewTransfer second failed: %v", err)
	}
	RegisterTransferFileStates(first.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/a/0")), FileSize: 10},
	}, TransferStateRunning)
	RegisterTransferFileStates(second.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/b/0")), FileSize: 12},
	}, TransferStateRunning)

	firstToken := "xxh128:0000000000000000000000000000000a"
	secondToken := "xxh128:0000000000000000000000000000000b"
	if ok := SetTransferFileWindowHash(first.ID, 0, 6, firstToken); !ok {
		t.Fatalf("SetTransferFileWindowHash first returned false")
	}
	if ok := SetTransferFileWindowHash(second.ID, 0, 8, secondToken); !ok {
		t.Fatalf("SetTransferFileWindowHash second returned false")
	}

	if ok := AcknowledgeTransferFiles([]AckEntry{
		{TxferID: first.ID, FileID: 0, AckBytes: 6},
		{TxferID: second.ID, FileID: 0, AckBytes: 8},
	}); !ok {
		t.Fatalf("AcknowledgeTransferFiles returned false")
	}

	firstStored, ok := GetTransfer(first.ID)
	if !ok {
		t.Fatalf("first transfer not found")
	}
	if firstStored.DoneSize != 6 || firstStored.Done != 0 {
		t.Fatalf("unexpected first counters: done=%d doneSize=%d", firstStored.Done, firstStored.DoneSize)
	}
	secondStored, ok := GetTransfer(second.ID)
	if !ok {
		t.Fatalf("second transfer not found")
	}
	if secondStored.DoneSize != 8 || secondStored.Done != 0 {
		t.Fatalf("unexpected second counters: done=%d doneSize=%d", secondStored.Done, secondStored.DoneSize)
	}
	if VerifyTransferFileWindowHash(first.ID, 0, 6, firstToken) {
		t.Fatalf("expected first window hash cleared after ack")
	}
	if VerifyTransferFileWindowHash(second.ID, 0, 8, secondToken) {
		t.Fatalf("expected second window hash cleared after ack")
	}
}

func TestWindowHashesAreTransferLocal(t *testing.T) {
	resetTransferStore()

	first, err := NewTransfer("/tmp/a", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer first failed: %v", err)
	}
	second, err := NewTransfer("/tmp/b", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer second failed: %v", err)
	}
	RegisterTransferFileStates(first.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/a/0")), FileSize: 10},
	}, TransferStateRunning)
	RegisterTransferFileStates(second.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/b/0")), FileSize: 10},
	}, TransferStateRunning)

	firstToken := "xxh128:0000000000000000000000000000000c"
	secondToken := "xxh128:0000000000000000000000000000000d"
	if ok := SetTransferFileWindowHash(first.ID, 0, 4, firstToken); !ok {
		t.Fatalf("SetTransferFileWindowHash first returned false")
	}
	if ok := SetTransferFileWindowHash(second.ID, 0, 4, secondToken); !ok {
		t.Fatalf("SetTransferFileWindowHash second returned false")
	}
	if !VerifyTransferFileWindowHash(first.ID, 0, 4, firstToken) {
		t.Fatalf("expected first transfer window hash")
	}
	if !VerifyTransferFileWindowHash(second.ID, 0, 4, secondToken) {
		t.Fatalf("expected second transfer window hash")
	}

	if ok := AcknowledgeTransferFile(first.ID, 0, 4); !ok {
		t.Fatalf("AcknowledgeTransferFile first returned false")
	}
	if VerifyTransferFileWindowHash(first.ID, 0, 4, firstToken) {
		t.Fatalf("expected first transfer window hash cleared")
	}
	if !VerifyTransferFileWindowHash(second.ID, 0, 4, secondToken) {
		t.Fatalf("expected second transfer window hash to remain")
	}
}

func TestGetTransferSnapshotsAreCopies(t *testing.T) {
	resetTransferStore()

	transfer, err := NewTransfer("/tmp/x", 1, 10)
	if err != nil {
		t.Fatalf("NewTransfer failed: %v", err)
	}
	RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{FileID: 0, PathHash: xxh3.Hash128([]byte("/tmp/x/0")), FileSize: 10},
	}, TransferStateRunning)

	stored, ok := GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("GetTransfer returned not found")
	}
	stored.State[0] = TransferStateMissing
	stored.PathHash[0] = xxh3.Hash128([]byte("mutated"))
	stored.FileSize[0] = 999
	stored.AckedSize[0] = 999

	storedAgain, ok := GetTransfer(transfer.ID)
	if !ok {
		t.Fatalf("GetTransfer returned not found on second read")
	}
	if storedAgain.State[0] != TransferStateRunning {
		t.Fatalf("expected original transfer state to remain running, got %d", storedAgain.State[0])
	}
	if storedAgain.FileSize[0] != 10 || storedAgain.AckedSize[0] != 0 {
		t.Fatalf("expected original transfer sizes to remain unchanged")
	}

	states, ok := GetTransferFileStates(transfer.ID)
	if !ok {
		t.Fatalf("GetTransferFileStates returned not found")
	}
	states[0].State = TransferStateMissing
	states[0].PathHash = xxh3.Hash128([]byte("mutated-again"))

	statesAgain, ok := GetTransferFileStates(transfer.ID)
	if !ok {
		t.Fatalf("GetTransferFileStates returned not found on second read")
	}
	if statesAgain[0].State != TransferStateRunning {
		t.Fatalf("expected wrapped state snapshot to be isolated, got %d", statesAgain[0].State)
	}
}

func setupLookupFixture(t *testing.T, fileName string, content []byte) (string, string) {
	t.Helper()
	root := t.TempDir()
	fullPath := filepath.Join(root, fileName)
	if content != nil {
		if err := os.WriteFile(fullPath, content, 0o644); err != nil {
			t.Fatalf("write fixture file: %v", err)
		}
	}
	transfer, err := NewTransfer(root, 1, int64(len(content)))
	if err != nil {
		t.Fatalf("NewTransfer failed: %v", err)
	}
	RegisterTransferFileStates(transfer.ID, []TransferFileStateUpdate{
		{
			FileID:   0,
			PathHash: xxh3.Hash128([]byte(filepath.Clean(fullPath))),
			FileSize: int64(len(content)),
		},
	}, TransferStateStarted)
	return transfer.ID, fullPath
}

func mustLookupErr(t *testing.T, err error) *FileLookupError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected lookup error, got nil")
	}
	lookupErr, ok := err.(*FileLookupError)
	if !ok {
		t.Fatalf("expected *FileLookupError, got %T (%v)", err, err)
	}
	return lookupErr
}

func TestGetFileRefTransferNotFound(t *testing.T) {
	resetTransferStore()
	_, err := GetFileRef("missing", 0, "/tmp/x")
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusNotFound || lookupErr.Msg != "transfer not found" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileRefFileIDOutOfRange(t *testing.T) {
	resetTransferStore()
	txferID, fullPath := setupLookupFixture(t, "a.txt", []byte("hello"))
	_, err := GetFileRef(txferID, 1, fullPath)
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusNotFound || lookupErr.Msg != "file id out of range" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileRefRejectsNonAbsolutePath(t *testing.T) {
	resetTransferStore()
	txferID, _ := setupLookupFixture(t, "a.txt", []byte("hello"))
	_, err := GetFileRef(txferID, 0, "a.txt")
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusBadRequest || lookupErr.Msg != "path must be absolute" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileRefRejectsOutsideRoot(t *testing.T) {
	resetTransferStore()
	txferID, _ := setupLookupFixture(t, "a.txt", []byte("hello"))
	_, err := GetFileRef(txferID, 0, "/tmp/not-in-root.txt")
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusForbidden || lookupErr.Msg != "path must be within transfer root" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileRefRejectsDigestMismatch(t *testing.T) {
	resetTransferStore()
	txferID, fullPath := setupLookupFixture(t, "a.txt", []byte("hello"))
	altPath := filepath.Join(filepath.Dir(fullPath), "b.txt")
	_, err := GetFileRef(txferID, 0, altPath)
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusForbidden || lookupErr.Msg != "file path digest mismatch" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileReturnsNotFoundWhenMissing(t *testing.T) {
	resetTransferStore()
	txferID, fullPath := setupLookupFixture(t, "missing.txt", nil)
	fd, _, err := GetFile(txferID, 0, fullPath)
	if fd != nil {
		_ = fd.Close()
		t.Fatalf("expected nil fd for missing file")
	}
	lookupErr := mustLookupErr(t, err)
	if lookupErr.Code != http.StatusNotFound || lookupErr.Msg != "file not found" {
		t.Fatalf("unexpected lookup error: %+v", lookupErr)
	}
}

func TestGetFileSuccess(t *testing.T) {
	resetTransferStore()
	txferID, fullPath := setupLookupFixture(t, "a.txt", []byte("hello"))
	fd, ref, err := GetFile(txferID, 0, fullPath)
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	defer fd.Close()
	if ref.TransferID != txferID || ref.FileID != 0 {
		t.Fatalf("unexpected ref IDs: %+v", ref)
	}
	if ref.Path != filepath.Clean(fullPath) {
		t.Fatalf("unexpected ref path: %q", ref.Path)
	}
	if ref.FileSize != 5 {
		t.Fatalf("unexpected ref size: %d", ref.FileSize)
	}
}

func BenchmarkTransferStoreConcurrentHotPaths(b *testing.B) {
	resetTransferStore()

	const numTransfers = 32
	const filesPerTransfer = 8
	transferIDs := make([]string, 0, numTransfers)
	for i := 0; i < numTransfers; i++ {
		transfer, err := NewTransfer("/tmp/bench-"+strconv.Itoa(i), filesPerTransfer, int64(filesPerTransfer*1024))
		if err != nil {
			b.Fatalf("NewTransfer failed: %v", err)
		}
		updates := make([]TransferFileStateUpdate, 0, filesPerTransfer)
		for fileID := 0; fileID < filesPerTransfer; fileID++ {
			updates = append(updates, TransferFileStateUpdate{
				FileID:   uint64(fileID),
				PathHash: xxh3.Hash128([]byte(transfer.ID + "-" + strconv.Itoa(fileID))),
				FileSize: 1024,
			})
		}
		RegisterTransferFileStates(transfer.ID, updates, TransferStateRunning)
		transferIDs = append(transferIDs, transfer.ID)
	}

	var counter atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := counter.Add(1)
			txferID := transferIDs[int(n)%len(transferIDs)]
			fileID := uint64((n / uint64(len(transferIDs))) % filesPerTransfer)
			endBytes := int64((n%8)+1) * 64
			token := "xxh128:" + strconv.FormatUint(n, 16)

			if !SetTransferFileState(txferID, fileID, TransferStateRunning) {
				b.Fatalf("SetTransferFileState returned false")
			}
			if !SetTransferFileWindowHash(txferID, fileID, endBytes, token) {
				b.Fatalf("SetTransferFileWindowHash returned false")
			}
			if !AcknowledgeTransferFiles([]AckEntry{{TxferID: txferID, FileID: fileID, AckBytes: endBytes}}) {
				b.Fatalf("AcknowledgeTransferFiles returned false")
			}
		}
	})
}
