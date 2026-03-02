package main

import "testing"

func TestShouldRunCLI(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "empty", args: nil, want: false},
		{name: "binary only", args: []string{"pinch"}, want: false},
		{name: "cli mode", args: []string{"pinch", "cli"}, want: true},
		{name: "server mode", args: []string{"pinch", "-listen", ":9090"}, want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRunCLI(tc.args); got != tc.want {
				t.Fatalf("shouldRunCLI(%v)=%v want %v", tc.args, got, tc.want)
			}
		})
	}
}
