/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package render

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"
)

// tabwriter geometry, matching kubectl's own printers so wfctl output sits
// beside `kubectl get` without a visual seam.
const (
	tabMinWidth = 6
	tabWidth    = 4
	tabPadding  = 3
)

// sink is a writer that remembers its first error, so a renderer can emit a
// whole table without an `if err != nil` after every line and still report
// the truth about a closed pipe.
type sink struct {
	w   io.Writer
	err error
}

// newSink writes to w, remembering the first failure.
func newSink(w io.Writer) *sink {
	return &sink{w: w}
}

func (s *sink) printf(format string, args ...any) {
	if s.err != nil {
		return
	}
	_, s.err = fmt.Fprintf(s.w, format, args...)
}

// line writes one line of text.
func (s *sink) line(text string) {
	s.printf("%s\n", text)
}

// blank writes a separating empty line.
func (s *sink) blank() {
	s.printf("\n")
}

// Table is a tabwriter-backed column layout with a header.
//
// Colour is applied *after* layout, never before. text/tabwriter measures a
// cell by counting its runes (Writer.updateWidth), and bracketing an escape
// sequence in tabwriter.Escape does not exempt it: endEscape calls
// updateWidth for exactly that case, so Escape protects a tab or newline
// from being read as a separator, not a colour from being counted as width.
// Feeding it coloured cells therefore pads every coloured row by the byte
// length of its ANSI sequences and skews the whole table.
//
// So rows are held until Flush, laid out from their ANSI-stripped text, and
// the styled text is substituted back into the finished lines. An
// uncoloured table takes a fast path through that substitution and its bytes
// are exactly what the tabwriter produced.
type Table struct {
	w    io.Writer
	rows [][]string
}

// NewTable starts a table with the given header row.
func NewTable(w io.Writer, header ...string) *Table {
	t := newPlainTable(w)
	t.Row(header...)
	return t
}

// newPlainTable starts a headerless aligned block — the key/value form the
// detail views use.
func newPlainTable(w io.Writer) *Table {
	return &Table{w: w}
}

// Row appends one row. A cell may carry ANSI colour; it must not contain a
// tab or a newline (see sanitize).
func (t *Table) Row(cells ...string) {
	t.rows = append(t.rows, slices.Clone(cells))
}

// Flush lays the table out and writes it.
func (t *Table) Flush() error {
	var plain bytes.Buffer
	tw := tabwriter.NewWriter(&plain, tabMinWidth, tabWidth, tabPadding, ' ', 0)
	for _, row := range t.rows {
		stripped := make([]string, len(row))
		for i, cell := range row {
			stripped[i] = stripANSI(cell)
		}
		if _, err := fmt.Fprintln(tw, strings.Join(stripped, "\t")); err != nil {
			return fmt.Errorf("laying out table: %w", err)
		}
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("laying out table: %w", err)
	}

	out := newSink(t.w)
	lines := strings.Split(plain.String(), "\n")
	for i, row := range t.rows {
		if i >= len(lines) {
			break
		}
		out.line(restyle(lines[i], row))
	}
	return out.err
}

// restyle puts the colour back into one laid-out line.
//
// The cells were written verbatim and are separated by padding spaces alone,
// so scanning left to right for each cell's stripped text finds that cell and
// nothing else. Every non-empty cell advances the cursor, coloured or not, so
// a repeated value later in the row cannot be mistaken for an earlier one.
func restyle(line string, cells []string) string {
	if !strings.ContainsRune(line, '\x1b') && !anyColoured(cells) {
		return line
	}

	var b strings.Builder
	b.Grow(len(line))
	cursor := 0
	for _, cell := range cells {
		plain := stripANSI(cell)
		if plain == "" {
			continue
		}
		offset := strings.Index(line[cursor:], plain)
		if offset < 0 {
			// Unreachable: tabwriter writes cell text unchanged. Leaving the
			// rest of the line alone keeps the layout right if it ever is.
			continue
		}
		start := cursor + offset
		b.WriteString(line[cursor:start])
		b.WriteString(cell)
		cursor = start + len(plain)
	}
	b.WriteString(line[cursor:])
	return b.String()
}

// anyColoured reports whether any cell carries an escape sequence.
func anyColoured(cells []string) bool {
	for _, cell := range cells {
		if strings.ContainsRune(cell, '\x1b') {
			return true
		}
	}
	return false
}

// stripANSI removes CSI escape sequences, leaving the text as it appears on
// screen. This is the width a table has to be laid out on.
func stripANSI(text string) string {
	if !strings.ContainsRune(text, '\x1b') {
		return text
	}

	var b strings.Builder
	b.Grow(len(text))
	for i := 0; i < len(text); {
		if text[i] != '\x1b' || i+1 >= len(text) || text[i+1] != '[' {
			b.WriteByte(text[i])
			i++
			continue
		}
		// CSI: ESC '[', parameter and intermediate bytes, then one final
		// byte in 0x40-0x7E.
		j := i + 2
		for j < len(text) && (text[j] < 0x40 || text[j] > 0x7e) {
			j++
		}
		if j < len(text) {
			j++
		}
		i = j
	}
	return b.String()
}
