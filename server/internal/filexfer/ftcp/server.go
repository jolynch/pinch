package ftcp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"time"

	"filippo.io/age"
	"github.com/jolynch/pinch/internal/filexfer/limit"
)

const (
	maxCommandLineBytes = 4 * 1024 * 1024
)

type ServerOptions struct {
	RequireAuth            bool
	ServerIdentity         *age.X25519Identity
	Deps                   Deps
	Limiter                *limit.Limiter
	SocketWriteBufferBytes int
}

type HandlerFunc func(context.Context, Request, io.Writer, Deps) error

var handlers = map[Verb]HandlerFunc{
	VerbAUTH:   handleAUTHCommand,
	VerbTXFER:  handleTXFER,
	VerbSEND:   handleSEND,
	VerbACK:    handleACK,
	VerbCXSUM:  handleCXSUM,
	VerbSTATUS: handleSTATUS,
}

func Serve(listener net.Listener, opts ServerOptions) error {
	if listener == nil {
		return errors.New("nil listener")
	}
	deps := opts.Deps
	if deps == nil {
		deps = NewRuntimeDeps()
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		go handleConn(conn, opts, deps)
	}
}

type connSession struct {
	conn                   net.Conn
	requireAuth            bool
	serverID               *age.X25519Identity
	deps                   Deps
	limiter                *limit.Limiter
	socketWriteBufferBytes int
	respOut                io.Writer
	closeResp              func() error
	wroteBytes             bool
}

func handleConn(conn net.Conn, opts ServerOptions, deps Deps) {
	defer conn.Close()
	s := &connSession{
		conn:                   conn,
		requireAuth:            opts.RequireAuth,
		serverID:               opts.ServerIdentity,
		deps:                   deps,
		limiter:                opts.Limiter,
		socketWriteBufferBytes: opts.SocketWriteBufferBytes,
		respOut:                conn,
		closeResp:              func() error { return nil },
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
	requestReader := io.Reader(br)
	if firstReq.Verb == VerbAUTH {
		authRes, authErr := processAUTHRequest(firstReq, s.requireAuth, s.serverID)
		if authErr != nil {
			if errors.Is(authErr, errNotAuthorized) {
				return protocolErr{code: "NOT_AUTHORIZED", message: "authorization failed"}
			}
			return authErr
		}
		if authRes.recipient != nil {
			encOut, encErr := age.Encrypt(s.conn, authRes.recipient)
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
			decIn, decErr := age.Decrypt(br, s.serverID)
			if decErr != nil {
				return protocolErr{code: "NOT_AUTHORIZED", message: "request decryption failed"}
			}
			requestReader = decIn
		}

		cmdPayload, cmdErr := readCommandLine(bufio.NewReader(requestReader), maxCommandLineBytes)
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

	cmdCtx := context.Background()
	cmdOut := s.respOut
	if s.limiter != nil {
		cmdOut = s.limiter.WrapRateLimitedWriter(cmdOut, cmdCtx)
	}
	countingOut := &countingWriter{w: cmdOut}
	if err := s.handleCommand(cmdCtx, cmdReq, countingOut); err != nil {
		s.wroteBytes = countingOut.n > 0
		return err
	}
	if cmdReq.Verb == VerbTXFER || cmdReq.Verb == VerbSEND || cmdReq.Verb == VerbCXSUM {
		if err := writeOKLine(countingOut, ""); err != nil {
			s.wroteBytes = countingOut.n > 0
			return err
		}
	}
	s.wroteBytes = countingOut.n > 0
	return s.closeResp()
}

func (s *connSession) handleCommand(ctx context.Context, req Request, out io.Writer) error {
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
