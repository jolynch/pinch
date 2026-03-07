package fhttp

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"filippo.io/age"
)

type ageEncryptedResponseWriter struct {
	http.ResponseWriter
	writer io.WriteCloser
}

func (w *ageEncryptedResponseWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *ageEncryptedResponseWriter) Flush() {
	if fl, ok := w.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

func parseAgePublicKey(raw string) (age.Recipient, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	recipient, err := age.ParseX25519Recipient(raw)
	if err != nil {
		return nil, errors.New("invalid query parameter: age-public-key")
	}
	return recipient, nil
}

func wrapAgeEncryptedResponseWriter(w http.ResponseWriter, recipient age.Recipient) (http.ResponseWriter, func() error, error) {
	if recipient == nil {
		return w, func() error { return nil }, nil
	}
	encryptedWriter, err := age.Encrypt(w, recipient)
	if err != nil {
		return nil, nil, err
	}
	return &ageEncryptedResponseWriter{
			ResponseWriter: w,
			writer:         encryptedWriter,
		},
		encryptedWriter.Close,
		nil
}
