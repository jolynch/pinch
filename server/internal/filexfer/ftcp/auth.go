package ftcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"strings"

	"filippo.io/age"
	"github.com/jolynch/pinch/internal/filexfer/encoding"
)

var errNotAuthorized = errors.New("not authorized")

type authResult struct {
	recipient         age.Recipient
	encryptedRequests bool
	encryptMode       string // "age" or "aes"
	keyExchange       bool   // true for AUTH key — server should return its public key
}

func processAUTHRequest(req Request, serverID *age.X25519Identity) (authResult, error) {
	if req.Verb != VerbAUTH {
		return authResult{}, protocolErr{code: "BAD_COMMAND", message: "invalid auth command"}
	}
	if len(req.Params) == 0 {
		return authResult{}, protocolErr{code: "BAD_AUTH", message: "missing auth protocol"}
	}
	protocol := req.Params[0]["protocol"]

	switch protocol {
	case "key":
		if serverID == nil {
			return authResult{}, protocolErr{code: "NOT_AUTHORIZED", message: "server has no identity"}
		}
		return authResult{keyExchange: true}, nil

	case "age":
		if serverID == nil {
			return authResult{}, errNotAuthorized
		}
		blob := req.Params[0]["blob"]
		if strings.TrimSpace(blob) == "" {
			return authResult{}, errNotAuthorized
		}
		blobBytes, b64Err := decodeAuthBlob(blob)
		if b64Err != nil {
			return authResult{}, errNotAuthorized
		}
		dec, err := age.Decrypt(bytes.NewReader(blobBytes), serverID)
		if err != nil {
			return authResult{}, errNotAuthorized
		}
		plain, err := io.ReadAll(dec)
		if err != nil {
			return authResult{}, errNotAuthorized
		}
		recRaw := strings.TrimSpace(string(plain))
		if recRaw == "" {
			return authResult{}, errNotAuthorized
		}
		recipient, err := age.ParseX25519Recipient(recRaw)
		if err != nil {
			return authResult{}, errNotAuthorized
		}
		return authResult{recipient: recipient, encryptedRequests: true, encryptMode: "age"}, nil

	case "aes":
		if serverID == nil {
			return authResult{}, errNotAuthorized
		}
		blob := req.Params[0]["blob"]
		if strings.TrimSpace(blob) == "" {
			return authResult{}, errNotAuthorized
		}
		blobBytes, b64Err := decodeAuthBlob(blob)
		if b64Err != nil {
			return authResult{}, errNotAuthorized
		}
		dec, err := encoding.AESGCMDecrypt(bytes.NewReader(blobBytes), serverID)
		if err != nil {
			return authResult{}, errNotAuthorized
		}
		plain, err := io.ReadAll(dec)
		if err != nil {
			return authResult{}, errNotAuthorized
		}
		recRaw := strings.TrimSpace(string(plain))
		if recRaw == "" {
			return authResult{}, errNotAuthorized
		}
		recipient, err := age.ParseX25519Recipient(recRaw)
		if err != nil {
			return authResult{}, errNotAuthorized
		}
		return authResult{recipient: recipient, encryptedRequests: true, encryptMode: "aes"}, nil

	default:
		return authResult{}, protocolErr{code: "BAD_AUTH", message: "unsupported auth protocol: " + protocol}
	}
}

func handleAUTHCommand(context.Context, Request, io.Writer, Deps) error {
	return protocolErr{code: "BAD_COMMAND", message: "AUTH must be first"}
}

// decodeAuthBlob decodes the AUTH blob, which is base64-encoded to avoid
// newlines in the line-oriented protocol.
func decodeAuthBlob(raw string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
}
