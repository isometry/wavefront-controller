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

package actions

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// ErrNotATerminal is the refusal that keeps a write command safe in a pipeline.
//
// A prompt written to a non-terminal is read by nobody and answered by
// whatever happens to be on stdin, so the only safe reading of "no --yes and
// no terminal" is that consent was never given.
var ErrNotATerminal = errors.New("refusing to prompt: stdin is not a terminal (use --yes)")

// Confirmer prints a Plan and applies it if consent is given.
//
// It is a struct rather than four arguments because the terminal, the clock
// and the answer stream are exactly the things a test has to replace: with In,
// Out and IsTTY injected, every branch of the consent rule is exercisable
// without a tty, a cluster, or a human.
type Confirmer struct {
	// In is where the answer to the prompt is read from — the same stream
	// IsTTY reports on.
	In io.Reader
	// Out is where the plan and the prompt are written.
	Out io.Writer
	// Yes applies without asking (--yes).
	Yes bool
	// DryRun prints the plan and stops (--dry-run).
	DryRun bool
	// IsTTY reports whether In is an interactive terminal; nil means "not one",
	// which is the safe reading.
	IsTTY func() bool
}

// Run prints plan, obtains consent, and applies.
//
// It reports whether anything reached the cluster, which is not the same as
// whether the command succeeded: a partly applied write returns both true and
// an error, and the caller records the audit event either way.
func (c Confirmer) Run(ctx context.Context, plan *Plan) (bool, error) {
	c.print(plan)

	if c.DryRun {
		c.line("Dry run: nothing was written.")
		return false, nil
	}

	if !c.Yes {
		ok, err := c.ask()
		if err != nil {
			return false, err
		}
		if !ok {
			c.line("Aborted: nothing was written.")
			return false, nil
		}
	}

	return plan.Apply(ctx)
}

// ask puts the question. Only "y" (or "yes") consents: an operator who hits
// return at a prompt they did not expect has declined.
func (c Confirmer) ask() (bool, error) {
	if c.IsTTY == nil || !c.IsTTY() {
		return false, ErrNotATerminal
	}

	_, _ = fmt.Fprint(c.Out, "Proceed? [y/N] ")

	answer, err := bufio.NewReader(c.In).ReadString('\n')
	if err != nil && answer == "" {
		// A closed stream is not consent. io.EOF included: it means the
		// question was never answered.
		return false, nil
	}

	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

// print renders the plan: what will change, from what to what, and why the
// operator might not want it to.
func (c Confirmer) print(plan *Plan) {
	c.line(plan.Summary)

	if fields := plan.Fields(); len(fields) > 0 {
		c.line("")
		table := tabwriter.NewWriter(c.Out, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(table, "FIELD\tBEFORE\tAFTER")
		for _, field := range fields {
			_, _ = fmt.Fprintf(table, "%s\t%s\t%s\n", field, cell(plan.Before, field), cell(plan.After, field))
		}
		_ = table.Flush()
	}

	if len(plan.Warnings) > 0 {
		c.line("")
		for _, warning := range plan.Warnings {
			c.line("Warning: " + warning)
		}
	}
	c.line("")
}

// cell renders one side of the table. A field named by only one side is
// absent on the other, which is information, not a blank.
func cell(values map[string]string, field string) string {
	value, ok := values[field]
	if !ok || value == "" {
		return unset
	}
	return value
}

// line writes one line to Out. Nothing useful remains to be done if the
// operator's own terminal will not take it.
func (c Confirmer) line(text string) {
	_, _ = fmt.Fprintln(c.Out, text)
}
