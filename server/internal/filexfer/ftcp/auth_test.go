package ftcp

import (
	"bytes"
	"encoding/base64"
	"io"
	"testing"

	"filippo.io/age"
	"github.com/jolynch/pinch/internal/aead"
)

func TestResolveAuthCipher(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    aead.Algorithm
		wantErr bool
	}{
		{name: "aes", raw: "aes", want: aead.AlgorithmAES},
		{name: "chacha20", raw: "chacha20", want: aead.AlgorithmChaCha20},
		{name: "invalid", raw: "bogus", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveAuthCipher(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveAuthCipher err: %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolveAuthCipher(%q)=%q want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestProcessAUTHRequestUsesExplicitCipher(t *testing.T) {
	serverID, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate server identity: %v", err)
	}
	clientID, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate client identity: %v", err)
	}

	var blob bytes.Buffer
	ew, err := aead.Encrypt(&blob, serverID.Recipient(), aead.Options{Algorithm: aead.AlgorithmChaCha20})
	if err != nil {
		t.Fatalf("encrypt auth blob: %v", err)
	}
	if _, err := io.WriteString(ew, clientID.Recipient().String()); err != nil {
		t.Fatalf("write auth blob: %v", err)
	}
	if err := ew.Close(); err != nil {
		t.Fatalf("close auth blob: %v", err)
	}

	req, err := ParseRequest([]byte("AUTH chacha20 " + base64.StdEncoding.EncodeToString(blob.Bytes())))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}

	result, err := processAUTHRequest(req, serverID)
	if err != nil {
		t.Fatalf("processAUTHRequest err: %v", err)
	}
	if result.responseCipher != aead.AlgorithmChaCha20 {
		t.Fatalf("responseCipher=%q want %q", result.responseCipher, aead.AlgorithmChaCha20)
	}
	if !result.encryptedRequests {
		t.Fatal("expected encryptedRequests")
	}
}
