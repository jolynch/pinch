package main

import "testing"

func TestParsePercentFlag(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{name: "default", raw: "25%", want: 25},
		{name: "min", raw: "1%", want: 1},
		{name: "max", raw: "100%", want: 100},
		{name: "missing percent", raw: "25", wantErr: true},
		{name: "decimal", raw: "25.0%", wantErr: true},
		{name: "zero", raw: "0%", wantErr: true},
		{name: "too large", raw: "101%", wantErr: true},
		{name: "negative", raw: "-1%", wantErr: true},
		{name: "empty", raw: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePercentFlag(tc.raw, "--gentle-cpu")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("unexpected value: got=%d want=%d", got, tc.want)
			}
		})
	}
}
