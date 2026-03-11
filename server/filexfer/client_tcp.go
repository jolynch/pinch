package filexfer

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"filippo.io/age"
)

const maxTCPLineBytes = 4 * 1024 * 1024

type tcpAuthState struct {
	publicKey       string
	identity        string
	hasAuth         bool
	encryptCommands bool
}

func (c *Client) dialTCP(ctx context.Context) (net.Conn, error) {
	if c == nil {
		return nil, errors.New("nil client")
	}
	addr := strings.TrimSpace(c.FileAddr)
	if addr == "" {
		return nil, errors.New("missing file listener address")
	}
	dialer := c.contextDialer
	if dialer == nil {
		dialer := net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
			if c.SocketReadBufferBytes > 0 {
				_ = tc.SetReadBuffer(c.SocketReadBufferBytes)
			}
		}
		return conn, nil
	}
	conn, err := dialer(ctx, addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		if c.SocketReadBufferBytes > 0 {
			_ = tc.SetReadBuffer(c.SocketReadBufferBytes)
		}
	}
	return conn, nil
}

func makeLenToken(raw string) string {
	return strconv.Itoa(len(raw)) + ":" + raw
}

func parseErrControlFrame(line string) error {
	code, msg, ok := parseErrControlPayload(line)
	if !ok {
		return nil
	}
	return controlFrameError{Code: code, Message: msg}
}

type controlFrameError struct {
	Code    string
	Message string
}

func (e controlFrameError) Error() string {
	if strings.TrimSpace(e.Message) == "" {
		return e.Code
	}
	return strings.TrimSpace(e.Code + " " + e.Message)
}

func parseErrControlPayload(line string) (code string, message string, ok bool) {
	msg := strings.TrimSpace(line)
	if msg == "" {
		return "", "", false
	}
	if strings.HasPrefix(msg, "ERR ") {
		rest := strings.TrimSpace(strings.TrimPrefix(msg, "ERR "))
		if rest == "" {
			return "ERR", "", true
		}
		code, message, hasMessage := strings.Cut(rest, " ")
		if !hasMessage {
			return strings.TrimSpace(code), "", true
		}
		return strings.TrimSpace(code), strings.TrimSpace(message), true
	}
	return "", "", false
}

func parseOKStatusLine(line string) (message string, ok bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "OK" {
		return "", true
	}
	if strings.HasPrefix(trimmed, "OK ") {
		return strings.TrimSpace(strings.TrimPrefix(trimmed, "OK ")), true
	}
	return "", false
}

func isStatusLine(line string) bool {
	if _, ok := parseOKStatusLine(line); ok {
		return true
	}
	_, _, ok := parseErrControlPayload(line)
	return ok
}

func readTCPLine(br *bufio.Reader, maxBytes int) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	if maxBytes > 0 && len(line) > maxBytes {
		return "", errors.New("line too large")
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}

func writeTCPLine(w io.Writer, line string) error {
	_, err := io.WriteString(w, line+"\r\n")
	return err
}

func escapeQuoted(raw string) string {
	raw = strings.ReplaceAll(raw, "\\", "\\\\")
	raw = strings.ReplaceAll(raw, "\"", "\\\"")
	return raw
}

func quoteToken(raw string) string {
	return "\"" + escapeQuoted(raw) + "\""
}

func encodeAUTHBlobToken(blob []byte, encrypted bool) string {
	if encrypted {
		encoded := base64.StdEncoding.EncodeToString(blob)
		return quoteToken("b64:" + encoded)
	}
	return quoteToken(string(blob))
}

func (c *Client) resolveTCPAuthState(requestPub string, requestIdentity string) (tcpAuthState, error) {
	state := tcpAuthState{}
	requestPub = strings.TrimSpace(requestPub)
	requestIdentity = strings.TrimSpace(requestIdentity)
	serverPub := strings.TrimSpace(c.ServerAgePublicKey)

	if serverPub != "" {
		state.hasAuth = true
		state.encryptCommands = true
		if requestPub == "" || requestIdentity == "" {
			identity, err := age.GenerateX25519Identity()
			if err != nil {
				return tcpAuthState{}, fmt.Errorf("generate age identity: %w", err)
			}
			requestPub = identity.Recipient().String()
			requestIdentity = identity.String()
		}
	}
	if requestPub != "" {
		state.hasAuth = true
		state.publicKey = requestPub
	}
	if requestIdentity != "" {
		state.identity = requestIdentity
	}
	if state.hasAuth && state.publicKey == "" {
		state.publicKey = requestPub
	}
	if state.hasAuth && state.identity == "" {
		return tcpAuthState{}, errors.New("missing age identity for authenticated response")
	}
	if state.hasAuth {
		if _, err := parseAgeIdentity(state.identity); err != nil {
			return tcpAuthState{}, err
		}
	}
	if state.encryptCommands {
		if _, err := age.ParseX25519Recipient(serverPub); err != nil {
			return tcpAuthState{}, fmt.Errorf("invalid PINCH_FILE_SERVER_AGE_PUBLIC_KEY: %w", err)
		}
	}
	return state, nil
}

func (c *Client) sendTCPAuth(conn net.Conn, state tcpAuthState) error {
	if !state.hasAuth {
		return nil
	}
	blob := []byte(state.publicKey)
	if state.encryptCommands {
		recipient, err := age.ParseX25519Recipient(strings.TrimSpace(c.ServerAgePublicKey))
		if err != nil {
			return err
		}
		encrypted := c.acquireScratchBuffer()
		defer c.releaseScratchBuffer(encrypted)
		ew, err := age.Encrypt(encrypted, recipient)
		if err != nil {
			return err
		}
		if _, err := ew.Write(blob); err != nil {
			return err
		}
		if err := ew.Close(); err != nil {
			return err
		}
		blob = encrypted.Bytes()
	}
	return writeTCPLine(conn, "AUTH "+encodeAUTHBlobToken(blob, state.encryptCommands))
}

func (c *Client) sendTCPCommand(conn net.Conn, state tcpAuthState, payload string) error {
	if !state.encryptCommands {
		return writeTCPLine(conn, payload)
	}
	recipient, err := age.ParseX25519Recipient(strings.TrimSpace(c.ServerAgePublicKey))
	if err != nil {
		return err
	}
	ew, err := age.Encrypt(conn, recipient)
	if err != nil {
		return err
	}
	if err := writeTCPLine(ew, payload); err != nil {
		return err
	}
	return ew.Close()
}

func (c *Client) responseReaderForTCP(conn net.Conn, state tcpAuthState) (io.Reader, error) {
	if !state.hasAuth {
		return conn, nil
	}
	identity, err := parseAgeIdentity(state.identity)
	if err != nil {
		return nil, err
	}
	if identity == nil {
		return nil, errors.New("missing age identity for encrypted response")
	}
	decReader, err := age.Decrypt(conn, identity)
	if err != nil {
		return nil, err
	}
	return decReader, nil
}

func (c *Client) fetchManifestTCP(ctx context.Context, request FetchManifestRequest) (FetchManifestResponse, error) {
	state, err := c.resolveTCPAuthState(request.AgePublicKey, request.AgeIdentity)
	if err != nil {
		return FetchManifestResponse{}, err
	}
	conn, err := c.dialTCP(ctx)
	if err != nil {
		return FetchManifestResponse{}, fmt.Errorf("dial file listener: %w", err)
	}
	defer conn.Close()

	if err := c.sendTCPAuth(conn, state); err != nil {
		return FetchManifestResponse{}, fmt.Errorf("send AUTH: %w", err)
	}
	cmd := "TXFER " + makeLenToken(request.Directory)
	if request.Verbose {
		cmd += " verbose=1"
	}
	if request.MaxChunkSize > 0 {
		cmd += " max-manifest-chunk-size=" + strconv.Itoa(request.MaxChunkSize)
	}
	if err := c.sendTCPCommand(conn, state, cmd); err != nil {
		return FetchManifestResponse{}, fmt.Errorf("send TXFER: %w", err)
	}

	responseReader, err := c.responseReaderForTCP(conn, state)
	if err != nil {
		return FetchManifestResponse{}, fmt.Errorf("initialize TXFER response stream: %w", err)
	}
	br := bufio.NewReader(responseReader)

	raw := c.acquireScratchBuffer()
	defer c.releaseScratchBuffer(raw)
	for {
		line, err := readTCPLine(br, maxTCPLineBytes)
		if err != nil {
			return FetchManifestResponse{}, fmt.Errorf("read TXFER response: %w", err)
		}
		if message, ok := parseOKStatusLine(line); ok {
			_ = message
			manifest, err := parseManifest(raw.Bytes())
			if err != nil {
				return FetchManifestResponse{}, err
			}
			return FetchManifestResponse{Manifest: manifest}, nil
		}
		if err := parseErrControlFrame(line); err != nil {
			return FetchManifestResponse{}, err
		}
		raw.WriteString(line)
		raw.WriteByte('\n')
	}
}

func (c *Client) fetchFileWindowTCP(
	ctx context.Context,
	txferID string,
	fileID uint64,
	fullPath string,
	agePublicKey string,
	ageIdentity string,
	offset int64,
	size int64,
) (io.ReadCloser, *FileFrameMeta, error) {
	if txferID == "" {
		return nil, nil, errors.New("missing transfer id")
	}
	if fullPath == "" {
		return nil, nil, errors.New("missing full path")
	}
	state, err := c.resolveTCPAuthState(agePublicKey, ageIdentity)
	if err != nil {
		return nil, nil, err
	}
	conn, err := c.dialTCP(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("dial file listener: %w", err)
	}
	if err := c.sendTCPAuth(conn, state); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("send AUTH: %w", err)
	}

	effectiveSize := size
	if effectiveSize < 0 {
		effectiveSize = 0
	}
	cmd := fmt.Sprintf("SEND %s %d %d %d %s", txferID, fileID, offset, effectiveSize, makeLenToken(fullPath))
	if err := c.sendTCPCommand(conn, state, cmd); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("send SEND: %w", err)
	}

	responseReader, err := c.responseReaderForTCP(conn, state)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("initialize SEND response stream: %w", err)
	}
	br := bufio.NewReader(responseReader)
	firstLine, err := br.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("read SEND response: %w", err)
	}
	trimmed := strings.TrimRight(firstLine, "\r\n")
	if err := parseErrControlFrame(trimmed); err != nil {
		conn.Close()
		var controlErr controlFrameError
		if errors.As(err, &controlErr) && strings.EqualFold(controlErr.Code, "NOT_FOUND") {
			return nil, nil, fmt.Errorf("%w: %w", ErrFileMissing, &fileMissingError{Status: 404, Body: strings.TrimSpace(controlErr.Message)})
		}
		return nil, nil, err
	}
	if _, ok := parseOKStatusLine(trimmed); ok {
		conn.Close()
		return nil, nil, errors.New("unexpected OK response for SEND")
	}

	prefixed := io.MultiReader(strings.NewReader(firstLine), br)
	stream, meta, streamErr := newFileStream(&readerWithCloser{Reader: prefixed, Closer: conn}, "")
	if streamErr != nil {
		conn.Close()
		return nil, nil, streamErr
	}
	if meta.FileID != fileID {
		stream.Close()
		return nil, nil, fmt.Errorf("file id mismatch: expected %d got %d", fileID, meta.FileID)
	}
	return stream, meta, nil
}

func (c *Client) fetchFileBatchTCP(
	ctx context.Context,
	txferID string,
	targets []FetchFileTarget,
	agePublicKey string,
	ageIdentity string,
) (io.ReadCloser, error) {
	if txferID == "" {
		return nil, errors.New("missing transfer id")
	}
	if len(targets) == 0 {
		return nil, errors.New("missing file targets")
	}
	state, err := c.resolveTCPAuthState(agePublicKey, ageIdentity)
	if err != nil {
		return nil, err
	}
	conn, err := c.dialTCP(ctx)
	if err != nil {
		return nil, fmt.Errorf("dial file listener: %w", err)
	}
	if err := c.sendTCPAuth(conn, state); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send AUTH: %w", err)
	}

	var b strings.Builder
	b.WriteString("SEND ")
	b.WriteString(txferID)
	for _, t := range targets {
		if strings.TrimSpace(t.FullPath) == "" {
			conn.Close()
			return nil, errors.New("missing full path")
		}
		b.WriteString(" ")
		b.WriteString(strconv.FormatUint(t.FileID, 10))
		b.WriteString(" 0 0 ")
		b.WriteString(makeLenToken(t.FullPath))
	}
	if err := c.sendTCPCommand(conn, state, b.String()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send SEND batch: %w", err)
	}

	responseReader, err := c.responseReaderForTCP(conn, state)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("initialize SEND response stream: %w", err)
	}
	br := bufio.NewReader(responseReader)
	firstLine, err := br.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read SEND response: %w", err)
	}
	trimmed := strings.TrimRight(firstLine, "\r\n")
	if err := parseErrControlFrame(trimmed); err != nil {
		conn.Close()
		return nil, err
	}
	if _, ok := parseOKStatusLine(trimmed); ok {
		conn.Close()
		return nil, errors.New("unexpected OK response for SEND")
	}
	return &readerWithCloser{Reader: io.MultiReader(strings.NewReader(firstLine), br), Closer: conn}, nil
}

func readTCPStatus(br *bufio.Reader) (string, error) {
	line, err := readTCPLine(br, maxTCPLineBytes)
	if err != nil {
		return "", err
	}
	if err := parseErrControlFrame(line); err != nil {
		return "", err
	}
	message, ok := parseOKStatusLine(line)
	if !ok {
		return "", fmt.Errorf("unexpected response: %s", strings.TrimSpace(line))
	}
	return message, nil
}

func (c *Client) acknowledgeFileProgressTCP(ctx context.Context, request AcknowledgeFileProgressRequest, ackToken string) (AcknowledgeFileProgressResponse, error) {
	state, err := c.resolveTCPAuthState("", "")
	if err != nil {
		return AcknowledgeFileProgressResponse{}, err
	}
	conn, err := c.dialTCP(ctx)
	if err != nil {
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("dial file listener: %w", err)
	}
	defer conn.Close()
	if err := c.sendTCPAuth(conn, state); err != nil {
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("send AUTH: %w", err)
	}

	cmd := fmt.Sprintf(
		"ACK %s %d %s %d %d %d %s",
		request.TransferID,
		request.FileID,
		ackToken,
		request.DeltaBytes,
		request.RecvMS,
		request.SyncMS,
		makeLenToken(request.FullPath),
	)
	if err := c.sendTCPCommand(conn, state, cmd); err != nil {
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("send ACK: %w", err)
	}
	responseReader, err := c.responseReaderForTCP(conn, state)
	if err != nil {
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("initialize ACK response stream: %w", err)
	}
	if _, err := readTCPStatus(bufio.NewReader(responseReader)); err != nil {
		return AcknowledgeFileProgressResponse{}, fmt.Errorf("read ACK response: %w", err)
	}
	return AcknowledgeFileProgressResponse{}, nil
}

func (c *Client) getTransferStatusTCP(ctx context.Context, request GetTransferStatusRequest) (GetTransferStatusResponse, error) {
	state, err := c.resolveTCPAuthState("", "")
	if err != nil {
		return GetTransferStatusResponse{}, err
	}
	conn, err := c.dialTCP(ctx)
	if err != nil {
		return GetTransferStatusResponse{}, fmt.Errorf("dial file listener: %w", err)
	}
	defer conn.Close()
	if err := c.sendTCPAuth(conn, state); err != nil {
		return GetTransferStatusResponse{}, fmt.Errorf("send AUTH: %w", err)
	}

	cmd := "STATUS " + request.TransferID
	if err := c.sendTCPCommand(conn, state, cmd); err != nil {
		return GetTransferStatusResponse{}, fmt.Errorf("send STATUS: %w", err)
	}
	responseReader, err := c.responseReaderForTCP(conn, state)
	if err != nil {
		return GetTransferStatusResponse{}, fmt.Errorf("initialize STATUS response stream: %w", err)
	}
	message, err := readTCPStatus(bufio.NewReader(responseReader))
	if err != nil {
		return GetTransferStatusResponse{}, fmt.Errorf("read STATUS response: %w", err)
	}
	if strings.TrimSpace(message) == "" {
		return GetTransferStatusResponse{}, errors.New("missing STATUS JSON payload")
	}
	var status TransferStatus
	if err := json.NewDecoder(strings.NewReader(message)).Decode(&status); err != nil {
		return GetTransferStatusResponse{}, fmt.Errorf("decode transfer status: %w", err)
	}
	return GetTransferStatusResponse{Status: &status}, nil
}

func (c *Client) fetchChecksumStreamTCP(ctx context.Context, request FetchChecksumStreamRequest) (io.ReadCloser, error) {
	state, err := c.resolveTCPAuthState(request.AgePublicKey, request.AgeIdentity)
	if err != nil {
		return nil, err
	}
	conn, err := c.dialTCP(ctx)
	if err != nil {
		return nil, fmt.Errorf("dial file listener: %w", err)
	}
	if err := c.sendTCPAuth(conn, state); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send AUTH: %w", err)
	}
	windowSize := request.WindowSize
	if windowSize <= 0 {
		windowSize = 64 * 1024 * 1024
	}
	checksums := strings.TrimSpace(request.ChecksumsCSV)
	if checksums == "" {
		checksums = "xxh128"
	}
	cmd := fmt.Sprintf(
		"CXSUM %s %d %d %s %s",
		request.TransferID,
		request.FileID,
		windowSize,
		checksums,
		makeLenToken(request.FullPath),
	)
	if err := c.sendTCPCommand(conn, state, cmd); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send CXSUM: %w", err)
	}

	responseReader, err := c.responseReaderForTCP(conn, state)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("initialize CXSUM response stream: %w", err)
	}
	br := bufio.NewReader(responseReader)
	firstLine, err := br.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read CXSUM response: %w", err)
	}
	trimmed := strings.TrimRight(firstLine, "\r\n")
	if err := parseErrControlFrame(trimmed); err != nil {
		conn.Close()
		return nil, err
	}
	if _, ok := parseOKStatusLine(trimmed); ok {
		conn.Close()
		return nil, errors.New("unexpected OK response for CXSUM")
	}
	return &readerWithCloser{Reader: io.MultiReader(strings.NewReader(firstLine), br), Closer: conn}, nil
}
