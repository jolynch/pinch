package encoding

import "testing"

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "1MiB", want: 1 * 1024 * 1024},
		{in: "4MB", want: 4 * 1000 * 1000},
		{in: "512", want: 512},
		{in: "", wantErr: true},
		{in: "-1MiB", wantErr: true},
	}
	for _, tc := range tests {
		got, err := ParseByteSize(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("expected error for %q", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("unexpected value for %q: got=%d want=%d", tc.in, got, tc.want)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{in: 0, want: "0 B"},
		{in: 512, want: "512 B"},
		{in: 1024, want: "1.00 KiB"},
		{in: 1536, want: "1.50 KiB"},
		{in: 5 * 1024 * 1024, want: "5.00 MiB"},
	}
	for _, tc := range tests {
		if got := HumanBytes(tc.in); got != tc.want {
			t.Fatalf("unexpected value for %d: got=%q want=%q", tc.in, got, tc.want)
		}
	}
}

func TestHumanRate(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{in: 0, want: "0 B/s"},
		{in: 512, want: "512 B/s"},
		{in: 1024, want: "1.00 KiB/s"},
		{in: 1536, want: "1.50 KiB/s"},
		{in: 5 * 1024 * 1024, want: "5.00 MiB/s"},
	}
	for _, tc := range tests {
		if got := HumanRate(tc.in); got != tc.want {
			t.Fatalf("unexpected value for %f: got=%q want=%q", tc.in, got, tc.want)
		}
	}
}
