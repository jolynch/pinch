package ftcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"strings"

	"filippo.io/age"
	"github.com/jolynch/pinch/internal/aead"
)

var errNotAuthorized = errors.New("not authorized")

type authResult struct {
	recipient         age.Recipient
	encryptedRequests bool
	keyExchange       bool // true for AUTH key — server should return its public key
	responseCipher    aead.Algorithm
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

	case "aes", "chacha20":
		if serverID == nil {
			return authResult{}, errNotAuthorized
		}
		cipherAlgorithm, err := resolveAuthCipher(protocol)
		if err != nil {
			return authResult{}, protocolErr{code: "BAD_AUTH", message: err.Error()}
		}
		blob := req.Params[0]["blob"]
		if strings.TrimSpace(blob) == "" {
			return authResult{}, errNotAuthorized
		}
		blobBytes, b64Err := decodeAuthBlob(blob)
		if b64Err != nil {
			return authResult{}, errNotAuthorized
		}
		dec, err := aead.DecryptWithOptions(bytes.NewReader(blobBytes), serverID, aead.Options{Algorithm: cipherAlgorithm})
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
		return authResult{
			recipient:         recipient,
			encryptedRequests: true,
			responseCipher:    cipherAlgorithm,
		}, nil

	default:
		return authResult{}, protocolErr{code: "BAD_AUTH", message: "unsupported auth protocol: " + protocol}
	}
}

func resolveAuthCipher(raw string) (aead.Algorithm, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return aead.RecommendedCipher(), nil
	case "aes":
		return aead.AlgorithmAES, nil
	case "chacha20":
		return aead.AlgorithmChaCha20, nil
	default:
		return "", errors.New("unsupported auth cipher")
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
