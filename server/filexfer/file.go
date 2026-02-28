package filexfer

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/zeebo/xxh3"
)

func FileHandler(w http.ResponseWriter, req *http.Request) {
	txferID := req.PathValue("txferid")
	if txferID == "" {
		http.Error(w, "missing required path parameter: txferid", http.StatusBadRequest)
		return
	}
	fileIDRaw := req.PathValue("fid")
	if fileIDRaw == "" {
		http.Error(w, "missing required path parameter: fid", http.StatusBadRequest)
		return
	}
	fileID, err := strconv.ParseUint(fileIDRaw, 10, 64)
	if err != nil {
		http.Error(w, "invalid path parameter: fid", http.StatusBadRequest)
		return
	}
	fullPathRaw := req.URL.Query().Get("path")
	if fullPathRaw == "" {
		http.Error(w, "missing required query parameter: path", http.StatusBadRequest)
		return
	}

	ackBytes := int64(0)
	ackProvided := false
	if ackRaw := req.URL.Query().Get("ack-bytes"); ackRaw != "" {
		ackBytes, err = strconv.ParseInt(ackRaw, 10, 64)
		if err != nil {
			http.Error(w, "invalid query parameter: ack-bytes", http.StatusBadRequest)
			return
		}
		ackProvided = true
	}

	transfer, ok := GetTransfer(txferID)
	if !ok {
		http.Error(w, "transfer not found", http.StatusNotFound)
		return
	}
	if fileID >= uint64(len(transfer.State)) {
		http.Error(w, "file id out of range", http.StatusNotFound)
		return
	}

	fullPath := filepath.Clean(fullPathRaw)
	if !filepath.IsAbs(fullPath) {
		http.Error(w, "path must be absolute", http.StatusBadRequest)
		return
	}
	if !pathWithinRoot(transfer.Directory, fullPath) {
		http.Error(w, "path must be within transfer root", http.StatusForbidden)
		return
	}

	computedDigest := xxh3.Hash128([]byte(fullPath))
	if transfer.PathHash[fileID] != computedDigest {
		http.Error(w, "file path digest mismatch", http.StatusForbidden)
		return
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		http.Error(w, "failed to read file", http.StatusInternalServerError)
		return
	}

	comp := SelectEncoding(req.Header.Get("Accept-Encoding"))
	if comp == EncodingIdentity {
		comp = "none"
	}

	payload, err := encodeSingleFramePayload(data, comp)
	if err != nil {
		http.Error(w, "failed to encode response payload", http.StatusInternalServerError)
		return
	}

	header := fmt.Sprintf(
		"FX/1 %d comp=%s enc=none offset=0 size=%d wsize=%d\n",
		fileID,
		comp,
		len(data),
		len(payload),
	)
	trailer := fmt.Sprintf("FXT/1 %d status=ok xsum:xxh3=%016x\n", fileID, xxh3.Hash(data))

	if ackProvided {
		if ok := AcknowledgeTransferFile(txferID, fileID, ackBytes); !ok {
			http.Error(w, "failed to acknowledge file progress", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Add("Vary", "Accept-Encoding")
	if comp == EncodingZstd || comp == EncodingLz4 {
		w.Header().Set("Content-Encoding", comp)
	}

	var frame bytes.Buffer
	frame.WriteString(header)
	frame.Write(payload)
	frame.WriteString(trailer)
	if _, err := w.Write(frame.Bytes()); err != nil {
		return
	}
}

func encodeSingleFramePayload(data []byte, comp string) ([]byte, error) {
	switch comp {
	case "none":
		return data, nil
	case EncodingZstd, EncodingLz4:
		var buf bytes.Buffer
		out, closeEncoded, _, err := WrapCompressedWriter(&buf, comp)
		if err != nil {
			return nil, err
		}
		if _, err := out.Write(data); err != nil {
			return nil, err
		}
		if err := closeEncoded(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, errors.New("unsupported compression mode")
	}
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
