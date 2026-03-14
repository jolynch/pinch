package ftcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"strings"

	"filippo.io/age"
)

var errNotAuthorized = errors.New("not authorized")

type authResult struct {
	recipient         age.Recipient
	encryptedRequests bool
}

func processAUTHRequest(req Request, requireAuth bool, serverID *age.X25519Identity) (authResult, error) {
	if req.Verb != VerbAUTH {
		return authResult{}, protocolErr{code: "BAD_COMMAND", message: "invalid auth command"}
	}
	blob := ""
	if len(req.Params) > 0 {
		blob = req.Params[0]["blob"]
	}
	if strings.TrimSpace(blob) == "" {
		if requireAuth {
			return authResult{}, errNotAuthorized
		}
		return authResult{}, nil
	}
	if requireAuth {
		if serverID == nil {
			return authResult{}, errNotAuthorized
		}
		cipherBlob, err := decodeAUTHBlob(blob)
		if err != nil {
			return authResult{}, errNotAuthorized
		}
		dec, err := age.Decrypt(bytes.NewReader(cipherBlob), serverID)
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
		return authResult{recipient: recipient, encryptedRequests: true}, nil
	}
	recipient, err := age.ParseX25519Recipient(strings.TrimSpace(blob))
	if err != nil {
		return authResult{}, protocolErr{code: "BAD_AUTH", message: "invalid auth recipient"}
	}
	return authResult{recipient: recipient}, nil
}

func handleAUTHCommand(context.Context, Request, io.Writer, Deps) error {
	return protocolErr{code: "BAD_COMMAND", message: "AUTH must be first"}
}

func decodeAUTHBlob(raw string) ([]byte, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "b64:") {
		return base64.StdEncoding.DecodeString(strings.TrimPrefix(raw, "b64:"))
	}
	return []byte(raw), nil
}
