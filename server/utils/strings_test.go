package utils

import (
	"strings"
	"testing"
)

func naiveCommonPrefixLen(a string, b string) int {
	commonLen := len(a)
	if len(b) < commonLen {
		commonLen = len(b)
	}
	for i := 0; i < commonLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return commonLen
}

func FuzzCommonPrefixLen(f *testing.F) {
	f.Add("", "")
	f.Add("a", "")
	f.Add("", "b")
	f.Add("abc", "abc")
	f.Add("abc", "abd")
	f.Add("prefix-123", "prefix-xyz")
	f.Add("hello world", "hello worlD")
	f.Add("same", "same")
	f.Add("short", "shorter")
	f.Add("longer", "long")

	f.Fuzz(func(t *testing.T, a string, b string) {
		got := CommonPrefixLen(a, b)
		want := naiveCommonPrefixLen(a, b)
		if got != want {
			t.Fatalf("CommonPrefixLen(%q, %q) = %d, want %d", a, b, got, want)
		}
	})
}

func BenchmarkCommonPrefixLen(b *testing.B) {
	cases := []struct {
		name string
		a    string
		b    string
	}{
		{
			name: "equal-short",
			a:    "data/chunk-0001",
			b:    "data/chunk-0001",
		},
		{
			name: "equal-long",
			a:    strings.Repeat("prefix/", 32) + "file.txt",
			b:    strings.Repeat("prefix/", 32) + "file.txt",
		},
		{
			name: "early-mismatch",
			a:    "x" + strings.Repeat("a", 255),
			b:    "y" + strings.Repeat("a", 255),
		},
		{
			name: "late-mismatch",
			a:    strings.Repeat("a", 255) + "x",
			b:    strings.Repeat("a", 255) + "y",
		},
		{
			name: "different-length",
			a:    strings.Repeat("a", 256),
			b:    strings.Repeat("a", 192),
		},
	}

	for _, tc := range cases {
		tc := tc
		b.Run("fast/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = CommonPrefixLen(tc.a, tc.b)
			}
		})
		b.Run("naive/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = naiveCommonPrefixLen(tc.a, tc.b)
			}
		})
	}
}
