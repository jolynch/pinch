package ftcp

import "testing"

func TestParseVerb(t *testing.T) {
	cases := []struct {
		token string
		want  Verb
	}{
		{token: "AUTH", want: VerbAUTH},
		{token: "TXFER", want: VerbTXFER},
		{token: "SEND", want: VerbSEND},
		{token: "ACK", want: VerbACK},
		{token: "CXSUM", want: VerbCXSUM},
		{token: "STATUS", want: VerbSTATUS},
		{token: "status", want: VerbSTATUS},
	}
	for _, tc := range cases {
		got, err := ParseVerb(tc.token)
		if err != nil {
			t.Fatalf("ParseVerb(%q) unexpected err: %v", tc.token, err)
		}
		if got != tc.want {
			t.Fatalf("ParseVerb(%q)=%v want=%v", tc.token, got, tc.want)
		}
	}
}

func TestParseVerbUnknown(t *testing.T) {
	got, err := ParseVerb("BOGUS")
	if err == nil {
		t.Fatalf("expected error")
	}
	if got != VerbUnknown {
		t.Fatalf("got=%v want=%v", got, VerbUnknown)
	}
}

func TestDispatchMapContainsVerbs(t *testing.T) {
	verbs := []Verb{VerbAUTH, VerbTXFER, VerbSEND, VerbACK, VerbCXSUM, VerbSTATUS}
	for _, v := range verbs {
		if _, ok := handlers[v]; !ok {
			t.Fatalf("handlers missing verb %v", v)
		}
	}
}
