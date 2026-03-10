package ftcp

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type protocolErr struct {
	code    string
	message string
}

func (e protocolErr) Error() string {
	if e.message == "" {
		return e.code
	}
	return e.code + ": " + e.message
}

func mapHTTPErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "BAD_REQUEST"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "CONFLICT"
	case http.StatusUnprocessableEntity:
		return "UNPROCESSABLE"
	case http.StatusRequestedRangeNotSatisfiable:
		return "RANGE"
	case http.StatusGatewayTimeout:
		return "TIMEOUT"
	case http.StatusForbidden:
		return "NOT_AUTHORIZED"
	case http.StatusInternalServerError:
		return "INTERNAL"
	default:
		return "HTTP_" + strconv.Itoa(status)
	}
}

func writeErrFrame(w io.Writer, err error) error {
	if w == nil || err == nil {
		return nil
	}
	code := "ERR"
	msg := strings.TrimSpace(err.Error())
	var pe protocolErr
	if errors.As(err, &pe) {
		code = pe.code
		msg = pe.message
	}
	line := "ERR " + code
	if msg != "" {
		line += " " + sanitizeStatusMessage(msg)
	}
	return writeStatusLine(w, line)
}

func writeOKLine(w io.Writer, msg string) error {
	if strings.TrimSpace(msg) == "" {
		return writeStatusLine(w, "OK")
	}
	return writeStatusLine(w, "OK "+sanitizeStatusMessage(msg))
}

func writeStatusLine(w io.Writer, line string) error {
	if w == nil {
		return nil
	}
	if strings.Contains(line, "\n") || strings.Contains(line, "\r") {
		line = sanitizeStatusMessage(line)
	}
	_, err := io.WriteString(w, line+"\r\n")
	return err
}

func sanitizeStatusMessage(msg string) string {
	msg = strings.ReplaceAll(msg, "\r", " ")
	msg = strings.ReplaceAll(msg, "\n", " ")
	return strings.TrimSpace(msg)
}
