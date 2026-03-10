package ftcp

import (
	"context"
	"testing"
)

func TestParseRequestSEND(t *testing.T) {
	payload := []byte("SEND tx1 42 10 20 13:/tmp/file.txt")
	req, err := ParseRequest(payload)
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if req.Verb != VerbSEND {
		t.Fatalf("verb=%v", req.Verb)
	}
	if len(req.Params) != 2 {
		t.Fatalf("params len=%d", len(req.Params))
	}
	if req.Params[0]["txferid"] != "tx1" {
		t.Fatalf("unexpected txferid: %q", req.Params[0]["txferid"])
	}
	if req.Params[1]["fid"] != "42" || req.Params[1]["offset"] != "10" || req.Params[1]["size"] != "20" {
		t.Fatalf("unexpected item params: %#v", req.Params[1])
	}
	if req.Params[1]["path"] != "/tmp/file.txt" {
		t.Fatalf("unexpected path: %q", req.Params[1]["path"])
	}
}

func TestParseRequestMalformedLenValue(t *testing.T) {
	_, err := ParseRequest([]byte("SEND tx1 1 0 10 10:/tmp"))
	if err == nil {
		t.Fatalf("expected error")
	}
	pe, ok := err.(protocolErr)
	if !ok {
		t.Fatalf("expected protocolErr got %T", err)
	}
	if pe.code != "BAD_REQUEST" {
		t.Fatalf("code=%s", pe.code)
	}
}

func TestParseRequestUnknownVerb(t *testing.T) {
	_, err := ParseRequest([]byte("NOPE x"))
	if err == nil {
		t.Fatalf("expected error")
	}
	pe, ok := err.(protocolErr)
	if !ok {
		t.Fatalf("expected protocolErr got %T", err)
	}
	if pe.code != "BAD_COMMAND" {
		t.Fatalf("code=%s", pe.code)
	}
}

func TestParseRequestQuotedPath(t *testing.T) {
	payload := []byte(`SEND tx1 42 10 20 "/tmp/file with spaces.txt"`)
	req, err := ParseRequest(payload)
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if got := req.Params[1]["path"]; got != "/tmp/file with spaces.txt" {
		t.Fatalf("path=%q", got)
	}
}

func TestParseRequestInvalidUnquotedPath(t *testing.T) {
	_, err := ParseRequest([]byte(`SEND tx1 42 10 20 /tmp/plain`))
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestHandleCommandBadVerb(t *testing.T) {
	s := &connSession{}
	err := s.handleCommand(context.Background(), Request{Verb: VerbUnknown}, nil)
	if err == nil {
		t.Fatalf("expected error")
	}
	pe, ok := err.(protocolErr)
	if !ok {
		t.Fatalf("expected protocolErr got %T", err)
	}
	if pe.code != "BAD_COMMAND" {
		t.Fatalf("code=%s", pe.code)
	}
}
