package ftcp

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestHandlePROBERoundTrip(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=8 probe-bytes=1024 cts0=100`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	payload := bytes.Repeat([]byte{0x5a}, 1024)
	in := bytes.NewReader(payload)
	var out bytes.Buffer
	if err := handlePROBEWithInput(context.Background(), req, in, &out, nil); err != nil {
		t.Fatalf("handlePROBEWithInput failed: %v", err)
	}

	br := bufio.NewReader(bytes.NewReader(out.Bytes()))
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read response line: %v", err)
	}
	respReq, err := ParseRequest([]byte(strings.TrimRight(line, "\r\n")))
	if err != nil {
		t.Fatalf("parse response line: %v", err)
	}
	if respReq.Verb != VerbPROBE {
		t.Fatalf("expected PROBE response, got %v", respReq.Verb)
	}
	if got := respReq.Params[0]["probe-bytes"]; got != "1024" {
		t.Fatalf("expected probe-bytes=1024, got %q", got)
	}
	echo := make([]byte, 1024)
	if _, err := io.ReadFull(br, echo); err != nil {
		t.Fatalf("read probe echo bytes: %v", err)
	}
	if len(echo) != 1024 {
		t.Fatalf("unexpected echo length: %d", len(echo))
	}
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("expected eof after payload, got %v", err)
	}
}

func TestHandlePROBERejectsShortPayload(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=8 probe-bytes=10 cts0=100`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	in := bytes.NewReader([]byte{1, 2, 3})
	var out bytes.Buffer
	err = handlePROBEWithInput(context.Background(), req, in, &out, nil)
	if err == nil {
		t.Fatalf("expected payload validation error")
	}
}

func TestParsePROBERequestRejectsOversizedProbe(t *testing.T) {
	req := Request{
		Verb: VerbPROBE,
		Params: []map[string]string{{
			"cpu":         "4",
			"probe-bytes": strconv.FormatInt(maxProbeBytes+1, 10),
			"cts0":        "100",
		}},
	}
	_, err := parsePROBERequest(req)
	if err == nil {
		t.Fatalf("expected oversized probe error")
	}
}
