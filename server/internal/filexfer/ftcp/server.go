package ftcp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"runtime/trace"
	"sync/atomic"
	"time"

	"filippo.io/age"
	"github.com/jolynch/pinch/internal/aead"
	"github.com/jolynch/pinch/internal/filexfer"
	"github.com/jolynch/pinch/internal/filexfer/limit"
)

const (
	maxCommandLineBytes = 4 * 1024 * 1024
)

func gentleLimiterBurstBytes(limiter *limit.Limiter) int64 {
	if limiter == nil {
		return 1 * 1024 * 1024
	}
	cfg := limiter.Config()
	if cfg.BurstBytes > 0 {
		return cfg.BurstBytes
	}
	return 1 * 1024 * 1024
}

type ServerOptions struct {
	RequireAuth            bool
	ServerIdentity         *age.X25519Identity
	Deps                   Deps
	Limiter                *limit.Limiter
	GentleCPUPct           int
	GentleBWPct            int
	SocketWriteBufferBytes int
	SyncTimeout            time.Duration // 0 = no timeout; bounds SYNC response write time
	RootDir                string        // "/" or "" means unrestricted
	ProgressPath           string        // append transfer status + % records to this file/pipe
	ProgressInterval       time.Duration // tick interval for progress writes (default 1s)
	DisableZeroCopy        bool          // force buffered send path even when zero-copy is available
	TargetIODepth          int           // target IO depth per CPU advertised in PROBE (default 4)
}

type HandlerFunc func(context.Context, Request, io.Writer, Deps) error

var handlers = map[Verb]HandlerFunc{
	VerbAUTH:   handleAUTHCommand,
	VerbTXFER:  handleTXFER,
	VerbSEND:   handleSEND,
	VerbACK:    handleACK,
	VerbCXSUM:  handleCXSUM,
	VerbSTATUS: handleSTATUS,
	VerbPROBE:  handlePROBECommand,
	VerbSYNC:   handleSYNCCommand,
}

func Serve(listener net.Listener, opts ServerOptions) error {
	if listener == nil {
		return errors.New("nil listener")
	}
	deps := opts.Deps
	if deps == nil {
		deps = NewRuntimeDepsWithRoot(opts.RootDir)
	}

	var onTransferCreated func(string)
	if opts.ProgressPath != "" {
		interval := opts.ProgressInterval
		if interval <= 0 {
			interval = time.Second
		}
		var activeTransferID atomic.Value
		onTransferCreated = func(id string) { activeTransferID.Store(id) }
		stopProgress := filexfer.StartProgressFileWriter(
			context.Background(), opts.ProgressPath, interval, func() (string, int) {
				id, _ := activeTransferID.Load().(string)
				if id == "" {
					return filexfer.FormatProgressStatusLine("server", "", 0, 0, 0, 0), 0
				}
				t, ok := deps.GetTransfer(id)
				if !ok {
					return filexfer.FormatProgressStatusLine("server", id, 0, 0, 0, 0), 0
				}
				status := filexfer.FormatProgressStatusLine("server", id, t.Done, uint64(t.NumFiles), t.DoneSize, t.TotalSize)
				if t.TotalSize <= 0 {
					return status, 0
				}
				pct := int(t.DoneSize * 100 / t.TotalSize)
				if pct > 100 {
					pct = 100
				}
				return status, pct
			})
		defer stopProgress(true)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			if _, ok := err.(net.Error); ok {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		go handleConn(conn, opts, deps, onTransferCreated)
	}
}

type connSession struct {
	conn                   net.Conn
	requireAuth            bool
	serverID               *age.X25519Identity
	deps                   Deps
	limiter                *limit.Limiter
	gentleCPUPct           int
	gentleBWPct            int
	socketWriteBufferBytes int
	syncTimeout            time.Duration
	disableZeroCopy        bool
	targetIODepth          int
	respOut                io.Writer
	closeResp              func() error
	wroteBytes             bool
	onTransferCreated      func(string)
}

func handleConn(conn net.Conn, opts ServerOptions, deps Deps, onTransferCreated func(string)) {
	defer conn.Close()
	s := &connSession{
		conn:                   conn,
		requireAuth:            opts.RequireAuth,
		serverID:               opts.ServerIdentity,
		deps:                   deps,
		limiter:                opts.Limiter,
		gentleCPUPct:           opts.GentleCPUPct,
		gentleBWPct:            opts.GentleBWPct,
		socketWriteBufferBytes: opts.SocketWriteBufferBytes,
		syncTimeout:            opts.SyncTimeout,
		disableZeroCopy:        opts.DisableZeroCopy,
		targetIODepth:          opts.TargetIODepth,
		respOut:                conn,
		closeResp:              func() error { return nil },
		onTransferCreated:      onTransferCreated,
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		if s.socketWriteBufferBytes > 0 {
			_ = tc.SetWriteBuffer(s.socketWriteBufferBytes)
		}
	}
	if err := s.run(); err != nil {
		_ = writeErrFrame(s.respOut, err)
		_ = s.closeResp()
	}
}

func (s *connSession) run() error {
	br := bufio.NewReader(s.conn)
	firstPayload, err := readCommandLine(br, maxCommandLineBytes)
	if err != nil {
		return err
	}
	firstReq, err := ParseRequest(firstPayload)
	if err != nil {
		return err
	}

	cmdReq := firstReq
	cmdReader := br
	if firstReq.Verb == VerbAUTH {
		authRes, authErr := processAUTHRequest(firstReq, s.serverID)
		if authErr != nil {
			if errors.Is(authErr, errNotAuthorized) {
				return protocolErr{code: "NOT_AUTHORIZED", message: "authorization failed"}
			}
			return authErr
		}
		if authRes.keyExchange {
			// AUTH key — return the server's recommended cipher and public key.
			return writeOKLine(s.respOut, string(aead.RecommendedCipher())+" "+s.serverID.Recipient().String())
		}
		if authRes.recipient != nil {
			encOut, encErr := aead.Encrypt(s.conn, authRes.recipient, aead.Options{Algorithm: authRes.responseCipher})
			if encErr != nil {
				return encErr
			}
			s.respOut = encOut
			s.closeResp = encOut.Close
		}
		if authRes.encryptedRequests {
			if s.serverID == nil {
				return protocolErr{code: "NOT_AUTHORIZED", message: "server auth key unavailable"}
			}
			decIn, decErr := aead.DecryptWithOptions(br, s.serverID, aead.Options{Algorithm: authRes.responseCipher})
			if decErr != nil {
				return protocolErr{code: "NOT_AUTHORIZED", message: "request decryption failed"}
			}
			cmdReader = bufio.NewReader(decIn)
		}

		cmdPayload, cmdErr := readCommandLine(cmdReader, maxCommandLineBytes)
		if cmdErr != nil {
			return cmdErr
		}
		cmdReq, err = ParseRequest(cmdPayload)
		if err != nil {
			return err
		}
	} else if s.requireAuth {
		return protocolErr{code: "NOT_AUTHORIZED", message: "missing AUTH"}
	}

	cmdCtx, connTask := trace.NewTask(context.Background(), "tcp-connection")
	defer connTask.End()
	countingOut := &countingWriter{w: s.respOut}
	if err := s.handleCommand(cmdCtx, cmdReq, cmdReader, countingOut); err != nil {
		s.wroteBytes = countingOut.n > 0
		return err
	}
	if cmdReq.Verb == VerbTXFER || cmdReq.Verb == VerbSEND || cmdReq.Verb == VerbCXSUM || cmdReq.Verb == VerbPROBE || cmdReq.Verb == VerbSYNC {
		if err := writeOKLine(countingOut, ""); err != nil {
			s.wroteBytes = countingOut.n > 0
			return err
		}
	}
	s.wroteBytes = countingOut.n > 0
	return s.closeResp()
}

func (s *connSession) handleCommand(ctx context.Context, req Request, in io.Reader, out io.Writer) error {
	if req.Verb == VerbSEND {
		return handleSENDWithOptions(ctx, req, out, s.deps, s.limiter, s.disableZeroCopy, s.gentleBWPct)
	}
	if req.Verb == VerbPROBE {
		return handlePROBEWithInput(ctx, req, in, out, s.deps, s.targetIODepth, s.gentleCPUPct, s.gentleBWPct, gentleLimiterBurstBytes(s.limiter))
	}
	if req.Verb == VerbSYNC {
		if s.syncTimeout > 0 {
			_ = s.conn.SetWriteDeadline(time.Now().Add(s.syncTimeout))
		}
		return handleSYNCWithInput(ctx, req, in, out, s.deps, s.onTransferCreated)
	}
	if req.Verb == VerbTXFER {
		return handleTXFERWithCallback(ctx, req, out, s.deps, s.onTransferCreated)
	}
	handler, ok := handlers[req.Verb]
	if !ok || req.Verb == VerbUnknown {
		return protocolErr{code: "BAD_COMMAND", message: "unknown command"}
	}
	return handler(ctx, req, out, s.deps)
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func readCommandLine(r *bufio.Reader, maxBytes int) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if maxBytes > 0 && len(line) > maxBytes {
		return nil, errors.New("command line too large")
	}
	if len(line) == 0 {
		return nil, errors.New("empty command")
	}
	if line[len(line)-1] != '\n' {
		return nil, errors.New("invalid line terminator")
	}
	line = line[:len(line)-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	if len(line) == 0 {
		return nil, errors.New("empty command")
	}
	return line, nil
}
