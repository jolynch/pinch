package filexfercli

import (
	"flag"
	"fmt"
	"io"
	"strings"
)

// flagEntry records one option for combined help rendering.
type flagEntry struct {
	short string // "s", "v" — empty for long-only flags
	long  string // "source-directory", "verbose" — empty for short-only flags
	typ   string // "string", "int" — empty for bool flags
	usage string
}

// cliFlags wraps flag.FlagSet and tracks short/long pairs so they can be
// printed combined ("-s, --source-directory string   description") rather than
// as two separate lines.
type cliFlags struct {
	fs    *flag.FlagSet
	pairs []flagEntry
}

func newCLIFlags(name string) *cliFlags {
	return &cliFlags{fs: flag.NewFlagSet(name, flag.ContinueOnError)}
}

func (c *cliFlags) SetOutput(w io.Writer)     { c.fs.SetOutput(w) }
func (c *cliFlags) Parse(args []string) error { return c.fs.Parse(args) }
func (c *cliFlags) Arg(i int) string          { return c.fs.Arg(i) }
func (c *cliFlags) Args() []string            { return c.fs.Args() }
func (c *cliFlags) NArg() int                 { return c.fs.NArg() }
func (c *cliFlags) Visit(fn func(*flag.Flag)) { c.fs.Visit(fn) }

// StringVar registers a string flag. Pass short="" for long-only, long="" for short-only.
func (c *cliFlags) StringVar(p *string, short, long, defVal, usage string) {
	if short != "" {
		c.fs.StringVar(p, short, defVal, usage)
	}
	if long != "" {
		c.fs.StringVar(p, long, defVal, usage)
	}
	c.pairs = append(c.pairs, flagEntry{short: short, long: long, typ: "string", usage: usage})
}

// BoolVar registers a bool flag. Pass short="" for long-only, long="" for short-only.
func (c *cliFlags) BoolVar(p *bool, short, long string, defVal bool, usage string) {
	if short != "" {
		c.fs.BoolVar(p, short, defVal, usage)
	}
	if long != "" {
		c.fs.BoolVar(p, long, defVal, usage)
	}
	c.pairs = append(c.pairs, flagEntry{short: short, long: long, typ: "", usage: usage})
}

// IntVar registers an int flag. Pass short="" for long-only, long="" for short-only.
func (c *cliFlags) IntVar(p *int, short, long string, defVal int, usage string) {
	if short != "" {
		c.fs.IntVar(p, short, defVal, usage)
	}
	if long != "" {
		c.fs.IntVar(p, long, defVal, usage)
	}
	c.pairs = append(c.pairs, flagEntry{short: short, long: long, typ: "int", usage: usage})
}

// leftCol returns the formatted left column for a flag entry:
//
//	"-s, --source-directory string"   (short + long)
//	"    --manifest string"           (long-only)
//	"-o string"                       (short-only)
//	"-v, --verbose"                   (bool, short + long)
func (e flagEntry) leftCol() string {
	var b strings.Builder
	switch {
	case e.short != "" && e.long != "":
		b.WriteString("-")
		b.WriteString(e.short)
		b.WriteString(", --")
		b.WriteString(e.long)
	case e.long != "":
		b.WriteString("    --")
		b.WriteString(e.long)
	default:
		b.WriteString("-")
		b.WriteString(e.short)
	}
	if e.typ != "" {
		b.WriteString(" ")
		b.WriteString(e.typ)
	}
	return b.String()
}

// PrintDefaults writes age-style combined option help to w.
func (c *cliFlags) PrintDefaults(w io.Writer) {
	if len(c.pairs) == 0 {
		return
	}
	// Compute max left-column width for alignment.
	maxW := 0
	for _, e := range c.pairs {
		if n := len(e.leftCol()); n > maxW {
			maxW = n
		}
	}
	fmt.Fprintln(w, "Options:")
	for _, e := range c.pairs {
		fmt.Fprintf(w, "  %-*s  %s\n", maxW, e.leftCol(), e.usage)
	}
}
