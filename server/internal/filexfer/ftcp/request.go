package ftcp

import (
	"errors"
	"io"
	"strconv"
	"strings"
)

type Request struct {
	Verb   Verb
	Params []map[string]string
}

func ParseRequest(payload []byte) (Request, error) {
	c := newCursor(payload)
	cmd, err := c.readToken()
	if err != nil {
		return Request{}, protocolErr{code: "BAD_COMMAND", message: "empty command"}
	}
	verb, err := ParseVerb(cmd)
	if err != nil {
		return Request{Verb: VerbUnknown}, protocolErr{code: "BAD_COMMAND", message: "unknown command"}
	}
	if c.eof() && verb != VerbAUTH {
		return Request{Verb: verb}, protocolErr{code: "BAD_REQUEST", message: "missing command arguments"}
	}
	req := Request{Verb: verb}
	switch verb {
	case VerbAUTH:
		if c.eof() {
			return req, nil
		}
		blob, readErr := c.readBlobValue()
		if readErr != nil {
			return Request{}, protocolErr{code: "BAD_AUTH", message: "invalid auth payload"}
		}
		if !c.eof() {
			return Request{}, protocolErr{code: "BAD_AUTH", message: "unexpected auth arguments"}
		}
		req.Params = append(req.Params, map[string]string{"blob": string(blob)})
		return req, nil
	case VerbTXFER:
		directory, readErr := c.readPathValue()
		if readErr != nil {
			return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid TXFER directory"}
		}
		param := map[string]string{"directory": string(directory)}
		for !c.eof() {
			tok, tokErr := c.readToken()
			if tokErr != nil {
				return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid TXFER option"}
			}
			key, val, ok := strings.Cut(tok, "=")
			if !ok {
				return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid TXFER option"}
			}
			param[key] = val
		}
		req.Params = append(req.Params, param)
		return req, nil
	case VerbSEND:
		txferID, readErr := c.readToken()
		if readErr != nil || txferID == "" {
			return Request{}, protocolErr{code: "BAD_REQUEST", message: "missing transfer id"}
		}
		header := map[string]string{"txferid": txferID}
		req.Params = append(req.Params, header)
		for !c.eof() {
			fdToken, fdErr := c.readToken()
			if fdErr != nil {
				return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND item"}
			}
			if !strings.HasPrefix(fdToken, "fd=") {
				return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND file id"}
			}
			fid := strings.TrimSpace(strings.TrimPrefix(fdToken, "fd="))
			if fid == "" {
				return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND file id"}
			}
			path, pathErr := c.readPathValue()
			if pathErr != nil {
				return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND path"}
			}
			item := map[string]string{
				"fid":  fid,
				"path": string(path),
			}
			for !c.eof() {
				if c.hasPrefix("fd=") {
					break
				}
				tok, tokErr := c.readToken()
				if tokErr != nil {
					return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND item option"}
				}
				key, val, ok := strings.Cut(tok, "=")
				if !ok {
					return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND item option"}
				}
				switch key {
				case "offset", "size", "comp":
					item[key] = val
				default:
					// Unknown keys are ignored for forward compatibility.
				}
			}
			req.Params = append(req.Params, item)
		}
		if len(req.Params) == 1 {
			return Request{}, protocolErr{code: "BAD_REQUEST", message: "SEND requires at least one item"}
		}
		return req, nil
	case VerbACK:
		txferID, txErr := c.readToken()
		if txErr != nil || txferID == "" {
			return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK arguments"}
		}
		for !c.eof() {
			fdToken, fdErr := c.readToken()
			if fdErr != nil {
				return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK arguments"}
			}
			if !strings.HasPrefix(fdToken, "fd=") {
				return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK file id"}
			}
			fid := strings.TrimSpace(strings.TrimPrefix(fdToken, "fd="))
			if fid == "" {
				return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK file id"}
			}
			path, pathErr := c.readPathValue()
			if pathErr != nil {
				return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK path"}
			}
			item := map[string]string{
				"txferid": txferID,
				"fid":     fid,
				"path":    string(path),
			}
			for !c.eof() {
				if c.hasPrefix("fd=") {
					break
				}
				tok, tokErr := c.readToken()
				if tokErr != nil {
					return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK arguments"}
				}
				key, val, ok := strings.Cut(tok, "=")
				if !ok {
					return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK arguments"}
				}
				switch key {
				case "ack-token", "delta-bytes", "recv-ms", "sync-ms":
					item[key] = val
				default:
					// Unknown keys are ignored for forward compatibility.
				}
			}
			req.Params = append(req.Params, item)
		}
		if len(req.Params) == 0 {
			return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK arguments"}
		}
		return req, nil
	case VerbCXSUM:
		txferID, txErr := c.readToken()
		fid, fidErr := c.readToken()
		windowSize, wErr := c.readToken()
		checksums, checksErr := c.readToken()
		if txErr != nil || fidErr != nil || wErr != nil || checksErr != nil {
			return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid CXSUM arguments"}
		}
		path, pathErr := c.readPathValue()
		if pathErr != nil {
			return Request{}, protocolErr{code: "BAD_REQUEST", message: "invalid CXSUM path"}
		}
		if !c.eof() {
			return Request{}, protocolErr{code: "BAD_REQUEST", message: "unexpected CXSUM arguments"}
		}
		req.Params = append(req.Params, map[string]string{
			"txferid":       txferID,
			"fid":           fid,
			"window-size":   windowSize,
			"checksums-csv": checksums,
			"path":          string(path),
		})
		return req, nil
	case VerbSTATUS:
		txferID, txErr := c.readToken()
		if txErr != nil || txferID == "" {
			return Request{}, protocolErr{code: "BAD_REQUEST", message: "missing transfer id"}
		}
		if !c.eof() {
			return Request{}, protocolErr{code: "BAD_REQUEST", message: "unexpected STATUS arguments"}
		}
		req.Params = append(req.Params, map[string]string{"txferid": txferID})
		return req, nil
	default:
		return Request{Verb: VerbUnknown}, protocolErr{code: "BAD_COMMAND", message: "unknown command"}
	}
}

type cursor struct {
	b []byte
	i int
}

func newCursor(b []byte) *cursor {
	return &cursor{b: b}
}

func (c *cursor) skipSpaces() {
	for c.i < len(c.b) && c.b[c.i] == ' ' {
		c.i++
	}
}

func (c *cursor) eof() bool {
	c.skipSpaces()
	return c.i >= len(c.b)
}

func (c *cursor) hasPrefix(prefix string) bool {
	c.skipSpaces()
	return strings.HasPrefix(string(c.b[c.i:]), prefix)
}

func (c *cursor) readToken() (string, error) {
	c.skipSpaces()
	if c.i >= len(c.b) {
		return "", io.EOF
	}
	start := c.i
	for c.i < len(c.b) && c.b[c.i] != ' ' {
		c.i++
	}
	return string(c.b[start:c.i]), nil
}

func (c *cursor) readLenValue() ([]byte, error) {
	c.skipSpaces()
	if c.i >= len(c.b) {
		return nil, io.EOF
	}
	j := c.i
	for j < len(c.b) && c.b[j] != ':' {
		if c.b[j] < '0' || c.b[j] > '9' {
			return nil, errors.New("invalid length prefix")
		}
		j++
	}
	if j >= len(c.b) || c.b[j] != ':' {
		return nil, errors.New("missing length separator")
	}
	length, err := strconv.Atoi(string(c.b[c.i:j]))
	if err != nil || length < 0 {
		return nil, errors.New("invalid length")
	}
	start := j + 1
	end := start + length
	if end > len(c.b) {
		return nil, io.ErrUnexpectedEOF
	}
	value := c.b[start:end]
	c.i = end
	if c.i < len(c.b) && c.b[c.i] != ' ' {
		return nil, errors.New("invalid token delimiter")
	}
	return value, nil
}

func (c *cursor) readQuotedValue() ([]byte, error) {
	c.skipSpaces()
	if c.i >= len(c.b) || c.b[c.i] != '"' {
		return nil, errors.New("missing quote")
	}
	c.i++
	out := make([]byte, 0, 64)
	for c.i < len(c.b) {
		ch := c.b[c.i]
		c.i++
		if ch == '"' {
			if c.i < len(c.b) && c.b[c.i] != ' ' {
				return nil, errors.New("invalid token delimiter")
			}
			return out, nil
		}
		if ch != '\\' {
			out = append(out, ch)
			continue
		}
		if c.i >= len(c.b) {
			return nil, io.ErrUnexpectedEOF
		}
		escaped := c.b[c.i]
		c.i++
		switch escaped {
		case '\\', '"':
			out = append(out, escaped)
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		default:
			out = append(out, escaped)
		}
	}
	return nil, io.ErrUnexpectedEOF
}

func (c *cursor) readPathValue() ([]byte, error) {
	c.skipSpaces()
	if c.i >= len(c.b) {
		return nil, io.EOF
	}
	ch := c.b[c.i]
	if ch == '"' {
		return c.readQuotedValue()
	}
	if ch >= '0' && ch <= '9' {
		return c.readLenValue()
	}
	return nil, errors.New("path must be quoted or length-prefixed")
}

func (c *cursor) readBlobValue() ([]byte, error) {
	c.skipSpaces()
	if c.i >= len(c.b) {
		return nil, io.EOF
	}
	ch := c.b[c.i]
	if ch == '"' {
		return c.readQuotedValue()
	}
	if ch >= '0' && ch <= '9' {
		return c.readLenValue()
	}
	token, err := c.readToken()
	if err != nil {
		return nil, err
	}
	return []byte(token), nil
}
