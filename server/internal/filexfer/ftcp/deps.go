package ftcp

import (
	"errors"
	"net/http"
	"os"

	intstore "github.com/jolynch/pinch/internal/filexfer/store"
)

type Transfer = intstore.Transfer
type TransferFileState = intstore.TransferFileState
type TransferFileStateUpdate = intstore.TransferFileStateUpdate
type FileRef = intstore.FileRef
type FileLookupError = intstore.FileLookupError

const (
	TransferStateStarted = intstore.TransferStateStarted
	TransferStateRunning = intstore.TransferStateRunning
	TransferStateDone    = intstore.TransferStateDone
	TransferStateMissing = intstore.TransferStateMissing
)

type Deps interface {
	NewTransfer(directory string, numFiles int, totalSize int64) (Transfer, error)
	DeleteTransfer(txferID string) bool
	RegisterTransferFileState(txferID string, updatesCh <-chan TransferFileStateUpdate, state uint8) <-chan struct{}
	ClipTransfer(txferID string) bool

	GetTransfer(txferID string) (Transfer, bool)
	GetFile(txferID string, fileID uint64, fullPathRaw string) (*os.File, FileRef, error)
	GetFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error)

	SetTransferFileState(txferID string, fileID uint64, state uint8) bool
	SetTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, hashToken string) bool
	VerifyTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, hashToken string) bool
	AcknowledgeTransferFile(txferID string, fileID uint64, ackBytes int64) bool
}

type runtimeDeps struct{}

func NewRuntimeDeps() Deps {
	return runtimeDeps{}
}

func (runtimeDeps) NewTransfer(directory string, numFiles int, totalSize int64) (Transfer, error) {
	return intstore.NewTransfer(directory, numFiles, totalSize)
}

func (runtimeDeps) DeleteTransfer(txferID string) bool {
	return intstore.DeleteTransfer(txferID)
}

func (runtimeDeps) RegisterTransferFileState(txferID string, updatesCh <-chan TransferFileStateUpdate, state uint8) <-chan struct{} {
	return intstore.RegisterTransferFileState(txferID, updatesCh, state)
}

func (runtimeDeps) ClipTransfer(txferID string) bool {
	return intstore.ClipTransfer(txferID)
}

func (runtimeDeps) GetTransfer(txferID string) (Transfer, bool) {
	return intstore.GetTransfer(txferID)
}

func (runtimeDeps) GetFile(txferID string, fileID uint64, fullPathRaw string) (*os.File, FileRef, error) {
	return intstore.GetFile(txferID, fileID, fullPathRaw)
}

func (runtimeDeps) GetFileRef(txferID string, fileID uint64, fullPathRaw string) (FileRef, error) {
	return intstore.GetFileRef(txferID, fileID, fullPathRaw)
}

func (runtimeDeps) SetTransferFileState(txferID string, fileID uint64, state uint8) bool {
	return intstore.SetTransferFileState(txferID, fileID, state)
}

func (runtimeDeps) SetTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, hashToken string) bool {
	return intstore.SetTransferFileWindowHash(txferID, fileID, endBytes, hashToken)
}

func (runtimeDeps) VerifyTransferFileWindowHash(txferID string, fileID uint64, endBytes int64, hashToken string) bool {
	return intstore.VerifyTransferFileWindowHash(txferID, fileID, endBytes, hashToken)
}

func (runtimeDeps) AcknowledgeTransferFile(txferID string, fileID uint64, ackBytes int64) bool {
	return intstore.AcknowledgeTransferFile(txferID, fileID, ackBytes)
}

func mapLookupError(err error) error {
	if err == nil {
		return nil
	}
	var lookupErr *intstore.FileLookupError
	if errors.As(err, &lookupErr) {
		return protocolErr{code: mapHTTPErrorCode(lookupErr.Code), message: lookupErr.Msg}
	}
	return protocolErr{code: mapHTTPErrorCode(http.StatusInternalServerError), message: "internal server error"}
}
