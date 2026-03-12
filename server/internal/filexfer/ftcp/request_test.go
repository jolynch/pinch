package ftcp

import (
	"context"
	"testing"
)

func TestParseRequestSENDMinimal(t *testing.T) {
	payload := []byte(`SEND tx1 fd=42 "/tmp/file.txt"`)
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
	if got := req.Params[0]["txferid"]; got != "tx1" {
		t.Fatalf("unexpected txferid: %q", got)
	}
	if got := req.Params[1]["fid"]; got != "42" {
		t.Fatalf("unexpected fid: %q", got)
	}
	if got := req.Params[1]["path"]; got != "/tmp/file.txt" {
		t.Fatalf("unexpected path: %q", got)
	}
	if got := req.Params[1]["offset"]; got != "" {
		t.Fatalf("unexpected offset token: %q", got)
	}
}

func TestParseRequestSENDMultipleBlocksWithOptions(t *testing.T) {
	payload := []byte(`SEND tx1 fd=42 "/tmp/a.txt" offset=10 size=20 comp=none foo=bar fd=77 10:/tmp/b.txt size=99`)
	req, err := ParseRequest(payload)
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if len(req.Params) != 3 {
		t.Fatalf("params len=%d", len(req.Params))
	}
	first := req.Params[1]
	if first["fid"] != "42" || first["offset"] != "10" || first["size"] != "20" || first["comp"] != "none" {
		t.Fatalf("unexpected first block: %#v", first)
	}
	second := req.Params[2]
	if second["fid"] != "77" || second["path"] != "/tmp/b.txt" || second["size"] != "99" {
		t.Fatalf("unexpected second block: %#v", second)
	}
}

func TestParseRequestSENDCompModes(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "adapt", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=adapt`, want: "adapt"},
		{name: "lz4", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=lz4`, want: "lz4"},
		{name: "zstd", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=zstd`, want: "zstd"},
		{name: "none", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=none`, want: "none"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := ParseRequest([]byte(tc.raw))
			if err != nil {
				t.Fatalf("ParseRequest err: %v", err)
			}
			if len(req.Params) != 2 {
				t.Fatalf("params len=%d", len(req.Params))
			}
			if got := req.Params[1]["comp"]; got != tc.want {
				t.Fatalf("expected comp=%q got=%q", tc.want, got)
			}
		})
	}
}

func TestParseRequestACKMinimal(t *testing.T) {
	payload := []byte(`ACK tx1 fd=42 "/tmp/file.txt" ack-token=5@1001@xxh128:abc`)
	req, err := ParseRequest(payload)
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if req.Verb != VerbACK {
		t.Fatalf("verb=%v", req.Verb)
	}
	if len(req.Params) != 1 {
		t.Fatalf("params len=%d", len(req.Params))
	}
	item := req.Params[0]
	if item["txferid"] != "tx1" || item["fid"] != "42" || item["path"] != "/tmp/file.txt" {
		t.Fatalf("unexpected ACK item: %#v", item)
	}
	if item["ack-token"] != "5@1001@xxh128:abc" {
		t.Fatalf("unexpected ack-token: %q", item["ack-token"])
	}
}

func TestParseRequestACKMultipleBlocksWithTelemetry(t *testing.T) {
	payload := []byte(`ACK tx1 fd=1 "/tmp/a.txt" ack-token=5@1001@xxh128:aaa delta-bytes=5 recv-ms=1 sync-ms=2 foo=bar fd=2 10:/tmp/b.txt ack-token=-1`)
	req, err := ParseRequest(payload)
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if len(req.Params) != 2 {
		t.Fatalf("params len=%d", len(req.Params))
	}
	first := req.Params[0]
	if first["fid"] != "1" || first["delta-bytes"] != "5" || first["recv-ms"] != "1" || first["sync-ms"] != "2" {
		t.Fatalf("unexpected first ACK block: %#v", first)
	}
	second := req.Params[1]
	if second["fid"] != "2" || second["ack-token"] != "-1" || second["path"] != "/tmp/b.txt" {
		t.Fatalf("unexpected second ACK block: %#v", second)
	}
}

func TestParseRequestMalformedLenValue(t *testing.T) {
	_, err := ParseRequest([]byte(`SEND tx1 fd=1 10:/tmp`))
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

func TestParseRequestMissingBlockStart(t *testing.T) {
	_, err := ParseRequest([]byte(`SEND tx1 "/tmp/file.txt"`))
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestParseRequestInvalidUnquotedPath(t *testing.T) {
	_, err := ParseRequest([]byte(`SEND tx1 fd=1 /tmp/plain`))
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestParseRequestACKMissingAckToken(t *testing.T) {
	req, err := ParseRequest([]byte(`ACK tx1 fd=1 "/tmp/a.txt"`))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if len(req.Params) != 1 {
		t.Fatalf("params len=%d", len(req.Params))
	}
	if got := req.Params[0]["ack-token"]; got != "" {
		t.Fatalf("unexpected ack-token value: %q", got)
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
