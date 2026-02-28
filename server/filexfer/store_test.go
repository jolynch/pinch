package filexfer

import (
	"strconv"
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
