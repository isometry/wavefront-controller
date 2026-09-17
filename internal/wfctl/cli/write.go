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

package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/wfctl/actions"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// Command names, shared with the tests that drive them.
const (
	cmdSuspend    = "suspend"
	cmdResume     = "resume"
	cmdMode       = "mode"
	cmdPin        = "pin"
	cmdRelease    = "release"
	cmdPinStrip   = "pin-strip"
	cmdForceAdmit = "force-admit"
)

// The two arguments `mode` takes, and the flag help that lists them.
var modes = []string{string(wavefrontv1alpha1.ModeShadow), string(wavefrontv1alpha1.ModeEnforce)}

// build turns a command's arguments into the action it performs. The client
// and the Wavefront are resolved once, by runWrite, because every write needs
// both: the write itself, and the audit event that records it.
type build func(c client.Client, wf *wavefrontv1alpha1.Wavefront) (actions.Action, error)

// addWriteCommands registers the commands that change the fleet.
func addWriteCommands(root *cobra.Command, o *Options) {
	root.AddCommand(
		newSuspendCommand(o),
		newResumeCommand(o),
		newModeCommand(o),
		newPinCommand(o),
		newReleaseCommand(o),
		newPinStripCommand(o),
		newForceAdmitCommand(o),
	)
}

// newSuspendCommand freezes every pin write fleet-wide.
func newSuspendCommand(o *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdSuspend,
		Short: "Freeze all pin writes fleet-wide (spec.suspend: true)",
		Long: `Freeze all pin writes fleet-wide by setting spec.suspend: true.

Detection, status and metrics keep running; nothing is advanced. This is the
gentle brake — the one to reach for before the break-glass pin-strip.

The change is a merge patch, so a GitOps applier keeps owning the Wavefront
spec and will revert this on its next reconcile; wfctl warns when it sees one.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.runWrite(cmd, actions.ReasonSuspended, false, suspendTo(true))
		},
	}
	o.addDryRun(cmd)
	return cmd
}

// newResumeCommand lifts the freeze.
func newResumeCommand(o *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdResume,
		Short: "Resume pin writes fleet-wide (spec.suspend: false)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.runWrite(cmd, actions.ReasonResumed, false, suspendTo(false))
		},
	}
	o.addDryRun(cmd)
	return cmd
}

// suspendTo builds the suspend/resume action.
func suspendTo(suspend bool) build {
	return func(c client.Client, wf *wavefrontv1alpha1.Wavefront) (actions.Action, error) {
		return &actions.WavefrontChange{Client: c, Wavefront: wf, Suspend: &suspend}, nil
	}
}

// newModeCommand flips Shadow ⇄ Enforce.
func newModeCommand(o *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   cmdMode + " " + strings.Join(modes, "|"),
		Short: "Set the Wavefront's mode: Shadow (report only) or Enforce (write pins)",
		Long: `Set the Wavefront's mode.

Shadow performs full detection, derivation and evaluation and writes nothing;
Enforce advances pins. See docs/runbook.md, "Shadow → Enforce flip procedure".`,
		Args:      cobra.ExactArgs(1),
		ValidArgs: modes,
		RunE: func(cmd *cobra.Command, args []string) error {
			mode, err := parseMode(args[0])
			if err != nil {
				return err
			}
			return o.runWrite(cmd, actions.ReasonModeChanged, false,
				func(c client.Client, wf *wavefrontv1alpha1.Wavefront) (actions.Action, error) {
					return &actions.WavefrontChange{Client: c, Wavefront: wf, Mode: &mode}, nil
				})
		},
	}
	o.addDryRun(cmd)
	return cmd
}

// parseMode accepts either mode in any case and returns it canonically: an
// operator who types "enforce" at 3am has said something unambiguous.
func parseMode(arg string) (wavefrontv1alpha1.Mode, error) {
	for _, mode := range modes {
		if strings.EqualFold(arg, mode) {
			return wavefrontv1alpha1.Mode(mode), nil
		}
	}
	return "", fmt.Errorf("invalid mode %q: want one of %s", arg, strings.Join(modes, ", "))
}

// newPinCommand hand-pins one source.
func newPinCommand(o *Options) *cobra.Command {
	var (
		sha        string
		force      bool
		unverified bool
	)

	cmd := &cobra.Command{
		Use:   cmdPin + " NAMESPACE/NAME --sha SHA",
		Short: "Hand-pin one source, holding it against the controller",
		Long: `Hand-pin one source: set spec.ref.commit under the wfctl field manager.

The controller reads that field manager as an external hold (DESIGN §3.5.3):
it stops advancing the source, reports HoldDetected with manager "wfctl", and
reports every node behind it as blocked by an unsettled ancestor. Hand back
control with "wfctl release".

The SHA has to be checked or explicitly not: --poll lists the remote's
advertised refs and verifies it, --unverified pins it unchecked.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.runWrite(cmd, actions.ReasonHandPinned, true,
				func(c client.Client, _ *wavefrontv1alpha1.Wavefront) (actions.Action, error) {
					source, err := snapshot.ParseSource(args[0])
					if err != nil {
						return nil, err
					}
					return &actions.Pin{
						Client:        c,
						Source:        source,
						SHA:           sha,
						Force:         force,
						Unverified:    unverified,
						Verify:        o.Poll,
						Advertisement: o.advertisement(),
					}, nil
				})
		},
	}

	cmd.Flags().StringVar(&sha, "sha", "", "Commit to pin (required)")
	cmd.Flags().BoolVar(&force, "force", false,
		"Displace a third-party owner of spec.ref.commit")
	cmd.Flags().BoolVar(&unverified, "unverified", false,
		"Pin a SHA without checking it against the remote's advertised refs")
	_ = cmd.MarkFlagRequired("sha")
	o.addDryRun(cmd)
	return cmd
}

// newReleaseCommand ends a hold.
func newReleaseCommand(o *Options) *cobra.Command {
	var float bool

	cmd := &cobra.Command{
		Use:   cmdRelease + " NAMESPACE/NAME",
		Short: "End a hold on one source, handing its pin to the controller",
		Long: `End a hold on one source.

By default the pinned value is kept and transferred: the controller re-applies
the same SHA under its own field manager with provenance restored from the
displaced-pin annotation, then the holder's claim is relinquished. The fleet
carries on running exactly the commit the hold pinned.

--float removes spec.ref.commit instead, so the source floats on its tracking
ref until the controller initial-pins it from its artifact (DESIGN §3.5.4).`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.runWrite(cmd, actions.ReasonPinReleased, false,
				func(c client.Client, _ *wavefrontv1alpha1.Wavefront) (actions.Action, error) {
					source, err := snapshot.ParseSource(args[0])
					if err != nil {
						return nil, err
					}
					return &actions.Release{Client: c, Source: source, Float: float, Now: o.now}, nil
				})
		},
	}

	cmd.Flags().BoolVar(&float, "float", false,
		"Remove the pin instead of transferring it to the controller")
	o.addDryRun(cmd)
	return cmd
}

// newPinStripCommand is the break-glass procedure.
func newPinStripCommand(o *Options) *cobra.Command {
	var (
		includeHeld bool
		suspend     bool
	)

	cmd := &cobra.Command{
		Use:   cmdPinStrip,
		Short: "Break-glass: remove spec.ref.commit from every managed source",
		Long: `Break-glass: remove spec.ref.commit from every managed source.

Every managed GitRepository reverts to plain floating-ref Flux behaviour. The
controller re-pins them all on its next sweep unless the fleet is suspended,
which is what --suspend does first, in the same command.

Hand-pinned sources are skipped unless --include-held: somebody is holding
them deliberately. See docs/runbook.md, "Break-glass: pin-strip".`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.runWrite(cmd, actions.ReasonPinStripped, false,
				func(c client.Client, wf *wavefrontv1alpha1.Wavefront) (actions.Action, error) {
					return &actions.Strip{
						Client:      c,
						Wavefront:   wf,
						IncludeHeld: includeHeld,
						Suspend:     suspend,
					}, nil
				})
		},
	}

	cmd.Flags().BoolVar(&includeHeld, "include-held", false,
		"Strip hand-pinned sources too")
	cmd.Flags().BoolVar(&suspend, "suspend", false,
		"Suspend the Wavefront first, so the controller does not re-pin")
	o.addDryRun(cmd)
	return cmd
}

// newForceAdmitCommand admits one source past its gate.
func newForceAdmitCommand(o *Options) *cobra.Command {
	var (
		sha        string
		unverified bool
	)

	cmd := &cobra.Command{
		Use:   cmdForceAdmit + " NAMESPACE/NAME",
		Short: "Admit one source past its gate, once, as the controller would",
		Long: `Admit one source past its gate, once.

wfctl lists that source's advertised refs, takes the SHA its tracking ref
points at, and writes it as the controller would: same field manager, same
provenance annotations. The controller then finds pin == observed and settles
the node without an admission of its own — no PinAdvanced event is emitted, so
the ForceAdmitted audit event is the record.

One source, one node's gate. Refused when the source is held or suspended.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.runWrite(cmd, actions.ReasonForceAdmitted, false,
				func(c client.Client, wf *wavefrontv1alpha1.Wavefront) (actions.Action, error) {
					source, err := snapshot.ParseSource(args[0])
					if err != nil {
						return nil, err
					}
					return &actions.ForceAdmit{
						Client:        c,
						Source:        source,
						Wavefront:     wf,
						SHA:           sha,
						Unverified:    unverified,
						Now:           o.now,
						Advertisement: o.advertisement(),
					}, nil
				})
		},
	}

	cmd.Flags().StringVar(&sha, "sha", "",
		"Admit this commit instead of the one the tracking ref advertises")
	cmd.Flags().BoolVar(&unverified, "unverified", false,
		"Accept --sha without checking it against the remote's advertised refs")
	o.addDryRun(cmd)
	return cmd
}

// addDryRun declares the flag every write command carries.
func (o *Options) addDryRun(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&o.DryRun, "dry-run", false,
		"Print the plan and exit without writing anything")
}

// runWrite is the whole of a write command: resolve, plan, print, confirm,
// apply, audit.
//
// Every step is shared because every write has to behave identically at each
// of them — an operator who has learned that a plan is printed before anything
// happens must be right about that for all seven.
func (o *Options) runWrite(cmd *cobra.Command, reason string, pollable bool, newAction build) error {
	if err := o.validateWrite(pollable); err != nil {
		return err
	}

	ctx := cmd.Context()
	c, err := o.writer()
	if err != nil {
		return err
	}
	wf, err := snapshot.SelectWavefront(ctx, c, o.Wavefront)
	if err != nil {
		return err
	}

	action, err := newAction(c, wf)
	if err != nil {
		return err
	}
	plan, err := action.Plan(ctx)
	if err != nil {
		return err
	}

	// written, not "succeeded": a strip that patched thirty sources and was
	// denied the thirty-first has changed the fleet and failed, and the audit
	// trail is worth most in exactly that case. The note says it was partial
	// and names the error, so the trail never implies a clean write.
	written, err := o.confirmer(cmd).Run(ctx, plan)
	if !written {
		return err
	}

	note := actions.Note(o.identity(), invocation())
	if err != nil {
		note = actions.Partial(note, err)
	}

	// An audit event that cannot be created — RBAC, a full apiserver — is a
	// gap in the trail, not a failed command, and reporting it as one would
	// tell the operator to re-run a write already performed.
	if auditErr := actions.Audit(ctx, c, wf, reason, note, o.now()); auditErr != nil {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "Warning:", auditErr)
	}
	return err
}

// validateWrite rejects the read-only flags. A write command handed --from
// would otherwise look like it had operated on the replayed snapshot.
func (o *Options) validateWrite(pollable bool) error {
	switch {
	case o.From != "":
		return errors.New("--from replays a captured snapshot; the write commands operate on a cluster")
	case o.Derive:
		return errors.New("--derive only changes how the read commands see the fleet")
	case o.Poll && !pollable:
		return errors.New("--poll verifies a hand-pinned SHA against the remote and applies only " +
			"to `pin`; `force-admit` always lists its source's refs")
	}
	return nil
}

// confirmer builds the consent gate around cobra's own streams, so a test
// drives the prompt the same way a terminal does.
func (o *Options) confirmer(cmd *cobra.Command) actions.Confirmer {
	return actions.Confirmer{
		In:     cmd.InOrStdin(),
		Out:    cmd.OutOrStdout(),
		Yes:    o.Yes,
		DryRun: o.DryRun,
		IsTTY:  o.stdinTTY,
	}
}

// stdinTTY reports whether there is a human to answer a prompt.
func (o *Options) stdinTTY() bool {
	if o.StdinTTY != nil {
		return o.StdinTTY()
	}
	return term.IsTerminal(int(os.Stdin.Fd()))
}

// advertisement carries the ref-listing seam and its bounds into an action.
func (o *Options) advertisement() actions.Advertisement {
	return actions.Advertisement{Lister: o.Lister, Timeout: o.PollTimeout}
}

// writer builds the read-write client the write commands operate through.
//
// It is separate from reader because the two answer different questions of the
// kubeconfig: a viewer's token builds a perfectly good Reader and is refused by
// the apiserver the moment a write command tries to patch anything.
func (o *Options) writer() (client.Client, error) {
	if o.NewClient != nil {
		return o.NewClient()
	}
	config, err := o.ConfigFlags.ToRESTConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	c, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("connecting to the cluster: %w", err)
	}
	return c, nil
}

// identity names who ran the command, for the audit note: the kubeconfig user
// and the context they were pointed at. No credential is read — only the names
// the kubeconfig gives them.
func (o *Options) identity() string {
	if o.Identity != nil {
		return o.Identity()
	}

	raw, err := o.ConfigFlags.ToRawKubeConfigLoader().RawConfig()
	if err != nil {
		return unknownIdentity
	}

	contextName := raw.CurrentContext
	if o.ConfigFlags.Context != nil && *o.ConfigFlags.Context != "" {
		contextName = *o.ConfigFlags.Context
	}
	user := ""
	if kubeContext, ok := raw.Contexts[contextName]; ok {
		user = kubeContext.AuthInfo
	}

	switch {
	case user != "" && contextName != "":
		return user + "@" + contextName
	case contextName != "":
		return contextName
	case user != "":
		return user
	default:
		return unknownIdentity
	}
}

// unknownIdentity is what an audit note says when the kubeconfig names nobody:
// the event is still worth recording, and a missing name is itself evidence.
const unknownIdentity = "unknown"

// invocation renders the command line for the audit note. argv[0] is reduced
// to its base name: the operator's home directory is not audit information.
func invocation() string {
	if len(os.Args) == 0 {
		return binaryName
	}
	argv := slices.Clone(os.Args)
	argv[0] = filepath.Base(argv[0])
	return strings.Join(argv, " ")
}
