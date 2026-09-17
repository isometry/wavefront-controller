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
	"strings"

	"github.com/isometry/wavefront-controller/internal/engine"
)

// ANSI SGR sequences. Deliberately the eight-colour set: it is the only
// palette every terminal and every corporate colour scheme renders sanely.
const (
	ansiReset  = "\x1b[0m"
	ansiRed    = "\x1b[31m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiBlue   = "\x1b[34m"
	ansiDim    = "\x1b[2m"
)

// Palette colours terminal output.
//
// Whether colour is wanted is the CLI's decision (NO_COLOR, --no-color,
// term.IsTerminal): render never inspects the environment, so its output is
// a pure function of its inputs and can be golden-tested. The zero Palette
// is colourless, which is what a test, a pipe, and a file all want.
type Palette struct {
	Enabled bool
}

// NewPalette returns a Palette that colours only when enabled.
func NewPalette(enabled bool) Palette {
	return Palette{Enabled: enabled}
}

// paint wraps text in an SGR sequence.
//
// Nothing here compensates for the width the sequence occupies, because
// nothing can: a caller that aligns coloured text must lay out on the
// stripped text instead (see Table, which does).
func (p Palette) paint(colour, text string) string {
	if !p.Enabled || colour == "" || text == "" {
		return text
	}
	var b strings.Builder
	b.Grow(len(colour) + len(text) + len(ansiReset))
	b.WriteString(colour)
	b.WriteString(text)
	b.WriteString(ansiReset)
	return b.String()
}

// stateColour maps a node state to its colour (plan B3): Settled green,
// Pending and Admissible yellow, Converging blue, Unhealthy red.
func stateColour(state string) string {
	switch engine.State(state) {
	case engine.StateSettled:
		return ansiGreen
	case engine.StatePending, engine.StateAdmissible:
		return ansiYellow
	case engine.StateConverging:
		return ansiBlue
	case engine.StateUnhealthy:
		return ansiRed
	default:
		return ""
	}
}

// State colours a node state cell.
func (p Palette) State(state string) string {
	return p.paint(stateColour(state), state)
}

// Held dims a cell belonging to a held source: a hold is an operator's
// deliberate act, so it reads as struck out rather than alarming.
func (p Palette) Held(text string) string {
	return p.paint(ansiDim, text)
}

// Dim renders secondary text — footnotes, absent markers, provenance.
func (p Palette) Dim(text string) string {
	return p.paint(ansiDim, text)
}

// Warn colours a value that disagrees with another, or a diagnostic.
func (p Palette) Warn(text string) string {
	return p.paint(ansiRed, text)
}

// stateCell colours a state and marks a held node with the dim treatment its
// row deserves.
func (p Palette) stateCell(state string, held bool) string {
	if held {
		return p.Held(state)
	}
	return p.State(state)
}
