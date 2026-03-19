package filexfercli

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// flagEntry records one option for combined help rendering.
type flagEntry struct {
	short string // "s", "v" — empty for long-only flags
	long  string // "source-directory", "verbose" — empty for short-only flags
	typ   string // "string", "int" — empty for bool flags
	usage string
	def   string
}

// cliFlags wraps flag.FlagSet and tracks short/long pairs so they can be
// printed combined ("-s, --source-directory string   description") rather than
// as two separate lines.
type cliFlags struct {
	fs    *flag.FlagSet
	pairs []flagEntry
}

const helpWrapWidth = 88

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
	c.pairs = append(c.pairs, flagEntry{short: short, long: long, typ: "string", usage: usage, def: strconv.Quote(defVal)})
}

// BoolVar registers a bool flag. Pass short="" for long-only, long="" for short-only.
func (c *cliFlags) BoolVar(p *bool, short, long string, defVal bool, usage string) {
	if short != "" {
		c.fs.BoolVar(p, short, defVal, usage)
	}
	if long != "" {
		c.fs.BoolVar(p, long, defVal, usage)
	}
	c.pairs = append(c.pairs, flagEntry{short: short, long: long, typ: "", usage: usage, def: strconv.FormatBool(defVal)})
}

// IntVar registers an int flag. Pass short="" for long-only, long="" for short-only.
func (c *cliFlags) IntVar(p *int, short, long string, defVal int, usage string) {
	if short != "" {
		c.fs.IntVar(p, short, defVal, usage)
	}
	if long != "" {
		c.fs.IntVar(p, long, defVal, usage)
	}
	c.pairs = append(c.pairs, flagEntry{short: short, long: long, typ: "int", usage: usage, def: strconv.Itoa(defVal)})
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
		usage := e.usage
		if !strings.Contains(strings.ToLower(usage), "default") {
			usage = fmt.Sprintf("%s (default %s)", usage, e.def)
		}
		prefix := fmt.Sprintf("  %-*s  ", maxW, e.leftCol())
		lines := wrapHelpText(usage, helpWrapWidth-len(prefix))
		if len(lines) == 0 {
			fmt.Fprintln(w, prefix)
			continue
		}
		fmt.Fprintf(w, "%s%s\n", prefix, lines[0])
		indent := strings.Repeat(" ", len(prefix))
		for _, line := range lines[1:] {
			fmt.Fprintf(w, "%s%s\n", indent, line)
		}
	}
}

func wrapHelpText(text string, width int) []string {
	if text == "" {
		return nil
	}
	if width <= 0 {
		return []string{text}
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}

	lines := make([]string, 0, 4)
	current := words[0]
	for _, word := range words[1:] {
		if len(current)+1+len(word) <= width {
			current += " " + word
			continue
		}
		lines = append(lines, current)
		current = word
	}
	lines = append(lines, current)
	return lines
}
