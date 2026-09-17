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

// Package cli wires wfctl's command tree. It owns everything the pure
// packages below it deliberately refuse to know: the kubeconfig, the clock,
// the terminal, and which snapshot provider a given invocation should use.
//
// Nothing here decides what a picture means (that is internal/wfctl/snapshot)
// or how it reads (that is internal/wfctl/render). Keeping the split sharp is
// what lets the renderers be golden-tested and the providers be swapped for a
// file with a single flag.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	// Every client-go auth plugin, so a wfctl built here works against the
	// same clusters kubectl does.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/wfctl/render"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// Version is stamped into every snapshot's ClusterIdent so a captured picture
// says which wfctl produced it. Release builds set it with
// `-ldflags "-X .../internal/wfctl/cli.Version=v1.2.3"`.
var Version = "dev"

// The names the tree describes itself by. Installed as pluginName, kubectl
// discovers the binary on PATH and invokes it as pluginUse, so every usage
// line has to say pluginUse rather than binaryName.
const (
	binaryName = "wfctl"
	pluginName = "kubectl-wavefront"
	pluginUse  = "kubectl wavefront"
)

// Output formats. Every read command accepts table, wide, json and yaml;
// `graph` additionally accepts dot and mermaid.
const (
	outputTable   = "table"
	outputWide    = "wide"
	outputJSON    = "json"
	outputYAML    = "yaml"
	outputDOT     = "dot"
	outputMermaid = "mermaid"
)

// Exit codes (plan B3). 0 and 1 are the usual pair; 2 is reserved for the one
// question a script actually wants answered without parsing text — is the
// fleet blocked?
const (
	exitError   = 1
	exitBlocked = 2
)

// scheme carries every type the providers read. It mirrors cmd/main.go's
// registration exactly: a snapshot derived by wfctl and one derived by the
// controller must be built from the same view of the API.
var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(wavefrontv1alpha1.AddToScheme(scheme))
	utilruntime.Must(sourcev1.AddToScheme(scheme))
	utilruntime.Must(kustomizev1.AddToScheme(scheme))
}

// ExitError carries a non-default exit status out of a command.
//
// A nil Err exits silently: `status` exits 2 on a blocked fleet *after*
// printing its report, and a second "Error:" line under a table that already
// says BLOCKED would be noise, not information.
type ExitError struct {
	Code int
	Err  error
}

func (e *ExitError) Error() string {
	if e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *ExitError) Unwrap() error { return e.Err }

// Fail reports a command error and returns the process exit status.
//
// It is the whole of main's error handling, which is why it lives here where
// it can be tested rather than in the binary where it cannot.
func Fail(w io.Writer, err error) int {
	if err == nil {
		return 0
	}
	if msg := err.Error(); msg != "" {
		// Nothing useful remains to be done if stderr itself will not take it.
		_, _ = fmt.Fprintln(w, "Error:", msg)
	}
	var exit *ExitError
	if errors.As(err, &exit) && exit.Code != 0 {
		return exit.Code
	}
	return exitError
}

// Options is the state every command shares: the persistent flags, plus the
// two seams (clock and client construction) that let the command tree be
// exercised without a cluster.
type Options struct {
	ConfigFlags *genericclioptions.ConfigFlags

	// Wavefront names the Wavefront to operate on; empty auto-selects the
	// only one.
	Wavefront string
	// Output is one of the output* constants.
	Output string
	// NoColor suppresses colour even on a terminal.
	NoColor bool
	// Yes skips the confirmation prompt of the write commands. It is declared
	// here because it is a persistent flag of the tree, not of any one command.
	Yes bool
	// DryRun prints a write command's plan and stops. Unlike Yes it is a local
	// flag of each write command, because a read command has no plan to print.
	DryRun bool
	// Derive re-derives the picture live instead of reading published status.
	Derive bool
	// Poll adds a ref-advertisement sweep to a derivation.
	Poll bool
	// PollTimeout bounds one ref listing.
	PollTimeout time.Duration
	// PerHostConcurrency bounds concurrent listings per git host; 0 takes the
	// Wavefront's own spec.poll.perHostConcurrency.
	PerHostConcurrency int
	// From replays a captured snapshot instead of reading a cluster.
	From string

	// Now is the clock every rendered age is anchored to; nil means the wall
	// clock. render.Options.Now is always set from it, so no renderer ever
	// falls back to a clock of its own.
	Now func() time.Time
	// NewReader builds the cluster reader; nil means the real one, built from
	// ConfigFlags. Tests substitute a fake so the command tree can be driven
	// without an apiserver.
	NewReader func() (client.Reader, error)
	// NewClient builds the read-write client the write commands operate
	// through; nil means the real one, built from ConfigFlags. It is separate
	// from NewReader so that the read commands stay unable to write even by
	// accident.
	NewClient func() (client.Client, error)
	// Lister lists a source's advertised refs for `pin --poll` and
	// `force-admit`; nil means the production go-git lister.
	Lister gitpoll.Lister
	// StdinTTY reports whether stdin is an interactive terminal, i.e. whether
	// there is anybody to answer a confirmation prompt; nil means "ask
	// os.Stdin".
	StdinTTY func() bool
	// Identity names who is running the command, for the audit note; nil means
	// the kubeconfig-derived one.
	Identity func() string
	// ColorTTY reports whether the output is an interactive terminal; nil
	// means "ask os.Stdout".
	ColorTTY func() bool
	// ClusterIdent stamps a captured snapshot with the cluster it came from;
	// nil means the kubeconfig-derived one. It is a seam because the default
	// reads the ambient kubeconfig, which a test must not depend on even when
	// NewReader has replaced the client.
	ClusterIdent func() snapshot.ClusterIdent
}

// NewRootCommand builds the wfctl command tree.
//
// argv0 is the invoked path: installed as `kubectl-wavefront`, the binary is
// a kubectl plugin and must describe itself as `kubectl wavefront` in every
// usage line, because that is what the user has to type.
func NewRootCommand(argv0 string) *cobra.Command {
	return newRootCommand(argv0, NewOptions())
}

// NewOptions is the tree's default state: the real kubeconfig flags and every
// seam left at its production value.
func NewOptions() *Options {
	return &Options{
		ConfigFlags: genericclioptions.NewConfigFlags(true),
		Output:      outputTable,
		PollTimeout: snapshot.DefaultPollTimeout,
	}
}

// newRootCommand builds the tree around a given Options, so that a test can
// drive the real command tree — flags, argument parsing, exit codes and all —
// with the cluster, the terminal and the kubeconfig replaced.
func newRootCommand(argv0 string, opts *Options) *cobra.Command {
	root := &cobra.Command{
		Use:     binaryName,
		Version: Version,
		Short:   "Inspect and operate a Wavefront progressive-delivery fleet",
		Long: strings.TrimSpace(`
wfctl reports what a Wavefront is doing and why, and operates it when it is
stuck.

By default every command reads the picture the controller published in
status.members, which needs nothing but read access to wavefronts. --derive
re-derives the same picture live through the controller's own pipeline, which
answers while the controller is down and checks it while it is up; --poll adds
a ref-advertisement sweep. --from replays a captured snapshot with no cluster
at all.

Three tiers of access, each a superset of the last:

  viewer    get/list wavefronts, and get/list events — the default,
            status-backed commands, and history.
  derive    + get/list kustomizations and gitrepositories cluster-wide
            (deriving reads individual objects as well as listing them);
            --poll also reads the sources' credential secrets.
  operator  + patch gitrepositories and wavefronts, and create events.

Wavefronts are cluster-scoped and every node and source is named as
"namespace/name", so -n/--namespace is accepted for kubectl's sake but has no
effect on what wfctl reports.

Exit codes: 0 success, 1 error, 2 status found the fleet Blocked.
`),
		SilenceUsage:      true,
		SilenceErrors:     true,
		CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: true},
	}
	applyInvocationName(root, argv0)

	flags := root.PersistentFlags()
	opts.ConfigFlags.AddFlags(flags)
	flags.StringVar(&opts.Wavefront, "wavefront", "",
		"Wavefront to report on; required only when the cluster holds more than one")
	flags.StringVarP(&opts.Output, "output", "o", outputTable,
		"Output format: table, wide, json or yaml (graph also accepts dot and mermaid)")
	flags.BoolVar(&opts.NoColor, "no-color", false, "Disable coloured output")
	flags.BoolVar(&opts.Yes, "yes", false, "Skip the confirmation prompt of the write commands")
	flags.BoolVar(&opts.Derive, "derive", false,
		"Re-derive the picture live instead of reading the controller's published status")
	flags.BoolVar(&opts.Poll, "poll", false,
		"With --derive, list each source's advertised refs (reads their credential secrets)")
	flags.DurationVar(&opts.PollTimeout, "poll-timeout", snapshot.DefaultPollTimeout,
		"Timeout for one ref listing")
	flags.IntVar(&opts.PerHostConcurrency, "per-host-concurrency", 0,
		"Concurrent ref listings per git host; 0 uses the Wavefront's spec.poll.perHostConcurrency")
	flags.StringVar(&opts.From, "from", "",
		"Replay a snapshot captured by the snapshot command instead of reading a cluster")

	addReadCommands(root, opts)
	addHistoryCommand(root, opts)
	addWriteCommands(root, opts)

	return root
}

// applyInvocationName makes the tree describe itself by the name it was
// invoked under. Cobra derives a command's name from the first word of Use,
// so the two-word plugin form has to travel as a display-name annotation:
// setting Use to "kubectl wavefront" would name the command "kubectl" and
// print every subcommand as `kubectl status`.
func applyInvocationName(root *cobra.Command, argv0 string) {
	if filepath.Base(argv0) != pluginName {
		return
	}
	root.Annotations = map[string]string{
		cobra.CommandDisplayNameAnnotation: pluginUse,
	}
}

// now resolves the clock.
func (o *Options) now() time.Time {
	if o.Now == nil {
		return time.Now()
	}
	return o.Now()
}

// renderOptions builds the renderers' options. Now is always set: a renderer
// left to find its own clock would make `-o wide` unreproducible and the
// golden tests a lie.
func (o *Options) renderOptions() render.Options {
	return render.Options{
		Wide:    o.Output == outputWide,
		Palette: render.NewPalette(o.colorEnabled()),
		Now:     o.now(),
	}
}

// colorEnabled implements the three-way rule of plan B1: colour only on an
// interactive terminal, only without NO_COLOR, and only without --no-color.
//
// NO_COLOR must be set *and non-empty* to count (no-color.org): an empty
// value is how a wrapper script clears an inherited one, and treating that as
// "on" would leave colour permanently off for anyone who tried.
func (o *Options) colorEnabled() bool {
	if o.NoColor {
		return false
	}
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if o.ColorTTY != nil {
		return o.ColorTTY()
	}
	return term.IsTerminal(int(os.Stdout.Fd()))
}

// validate checks the flag combinations no single flag can check for itself.
func (o *Options) validate(formats []string) error {
	if !slices.Contains(formats, o.Output) {
		return fmt.Errorf("invalid output format %q: want one of %s", o.Output, strings.Join(formats, ", "))
	}
	if o.From != "" && (o.Derive || o.Poll) {
		return errors.New("--from replays a captured snapshot and cannot be combined with --derive or --poll")
	}
	if o.Poll && !o.Derive {
		return errors.New("--poll lists refs for a live derivation and requires --derive")
	}
	return nil
}

// reader builds the controller-runtime reader every cluster-backed provider
// reads through.
func (o *Options) reader() (client.Reader, error) {
	if o.NewReader != nil {
		return o.NewReader()
	}
	config, err := o.ConfigFlags.ToRESTConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	reader, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("connecting to the cluster: %w", err)
	}
	return reader, nil
}

// source picks the provider this invocation reads through, and reports
// whether the result should be stamped with this cluster's identity: a
// replayed file already carries the identity of the cluster it came from, and
// overwriting it with the identity of whoever is reading the file would
// falsify the evidence.
func (o *Options) source(ctx context.Context) (snapshot.Source, bool, error) {
	if o.From != "" {
		return snapshot.FileSource{Path: o.From}, false, nil
	}

	reader, err := o.reader()
	if err != nil {
		return nil, false, err
	}

	if !o.Derive {
		return &snapshot.StatusSource{Reader: reader, Wavefront: o.Wavefront, Now: o.Now}, true, nil
	}

	derive := &snapshot.DeriveSource{Reader: reader, Wavefront: o.Wavefront, Now: o.Now}
	if o.Poll {
		poll, err := o.pollOptions(ctx, reader)
		if err != nil {
			return nil, false, err
		}
		derive.Poll = poll
	}
	return derive, true, nil
}

// pollOptions resolves the sweep's bounds. A zero --per-host-concurrency
// takes the Wavefront's own value, so a CLI sweep extends git hosts exactly
// the courtesy the fleet's operator configured for the controller.
func (o *Options) pollOptions(ctx context.Context, reader client.Reader) (*snapshot.PollOptions, error) {
	poll := &snapshot.PollOptions{
		Timeout:            o.PollTimeout,
		PerHostConcurrency: o.PerHostConcurrency,
	}
	if poll.PerHostConcurrency > 0 {
		return poll, nil
	}
	wf, err := snapshot.SelectWavefront(ctx, reader, o.Wavefront)
	if err != nil {
		return nil, err
	}
	poll.PerHostConcurrency = wf.Spec.Poll.PerHostConcurrency
	return poll, nil
}

// capture runs the selected provider and stamps the result with the cluster
// it came from.
func (o *Options) capture(ctx context.Context) (*snapshot.Snapshot, error) {
	source, stamp, err := o.source(ctx)
	if err != nil {
		return nil, err
	}
	snap, err := source.Capture(ctx)
	if err != nil {
		return nil, err
	}
	if stamp {
		snap.Cluster = o.stampIdent()
	}
	return snap, nil
}

// stampIdent resolves the identity a captured snapshot is stamped with.
func (o *Options) stampIdent() snapshot.ClusterIdent {
	if o.ClusterIdent != nil {
		return o.ClusterIdent()
	}
	return o.clusterIdent()
}

// clusterIdent names the cluster a snapshot was taken from.
//
// Only the context name, the apiserver's host and wfctl's version: a snapshot
// is written to files and pasted into incident channels, so the server's path
// and any userinfo are dropped, and no credential is read here at all. Both
// lookups are best-effort — a snapshot that renders is worth more than one
// refused because the kubeconfig has no current-context name.
func (o *Options) clusterIdent() snapshot.ClusterIdent {
	ident := snapshot.ClusterIdent{WfctlVersion: Version}

	if raw, err := o.ConfigFlags.ToRawKubeConfigLoader().RawConfig(); err == nil {
		ident.Context = raw.CurrentContext
	}
	// An explicit --context overrides whatever the files call current.
	if o.ConfigFlags.Context != nil && *o.ConfigFlags.Context != "" {
		ident.Context = *o.ConfigFlags.Context
	}
	if config, err := o.ConfigFlags.ToRESTConfig(); err == nil {
		ident.Server = hostOnly(config.Host)
	}
	return ident
}

// hostOnly reduces an apiserver URL to its host. Anything that will not parse
// yields "" rather than a string that might carry credentials.
func hostOnly(raw string) string {
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return ""
	}
	return parsed.Host
}
