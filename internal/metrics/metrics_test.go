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

package metrics_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/isometry/wavefront-controller/internal/metrics"
)

// TestNewExpositionText scripts a representative sequence of calls against
// every collector and compares the resulting exposition text verbatim
// (DESIGN §6 names and labels).
func TestNewExpositionText(t *testing.T) {
	reg := prometheus.NewRegistry()
	instr, err := metrics.New(reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	instr.AdmissionsTotal.WithLabelValues("fleet", "admitted").Inc()
	instr.AdmissionsTotal.WithLabelValues("fleet", "admitted").Inc()
	instr.AdmissionsTotal.WithLabelValues("fleet", "initial").Inc()
	instr.AdmissionsTotal.WithLabelValues("fleet", "shadow").Inc()
	instr.AdmissionsTotal.WithLabelValues("fleet", "conflict").Inc()

	instr.PinLagSeconds.WithLabelValues("fleet", "Kustomization", "flux-system", "team-a").Set(42)
	instr.PinLagSeconds.WithLabelValues("fleet", "Kustomization", "flux-system", "team-b").Set(7)
	// A second, co-resident Wavefront: every fleet-wide gauge is keyed by the
	// owning Wavefront so one Wavefront's pass cannot retire another's series.
	instr.PinLagSeconds.WithLabelValues("infra", "Kustomization", "flux-system", "team-c").Set(11)

	instr.AdmissionWaitSeconds.WithLabelValues("fleet").Observe(45)
	instr.AdmissionWaitSeconds.WithLabelValues("fleet").Observe(600)

	instr.BlockedNodes.WithLabelValues("fleet", "AncestorPending").Set(3)
	instr.BlockedNodes.WithLabelValues("fleet", "SelfHeld").Set(1)
	instr.BlockedNodes.WithLabelValues("infra", "AncestorPending").Set(2)

	instr.RefListFailures.WithLabelValues("git.example.com").Add(5)

	instr.PinnedFetchFailures.WithLabelValues("fleet").Set(2)
	instr.PinnedFetchFailures.WithLabelValues("infra").Set(0)

	const want = `
# HELP wavefront_admissions_total Total pin admissions, by result (admitted, initial, shadow, conflict) and owning Wavefront.
# TYPE wavefront_admissions_total counter
wavefront_admissions_total{result="admitted",wavefront="fleet"} 2
wavefront_admissions_total{result="conflict",wavefront="fleet"} 1
wavefront_admissions_total{result="initial",wavefront="fleet"} 1
wavefront_admissions_total{result="shadow",wavefront="fleet"} 1
# HELP wavefront_blocked_nodes Number of nodes currently blocked, by owning Wavefront and reason; sum() over the wavefront label for a fleet total.
# TYPE wavefront_blocked_nodes gauge
wavefront_blocked_nodes{reason="AncestorPending",wavefront="fleet"} 3
wavefront_blocked_nodes{reason="AncestorPending",wavefront="infra"} 2
wavefront_blocked_nodes{reason="SelfHeld",wavefront="fleet"} 1
# HELP wavefront_node_pin_lag_seconds Age in seconds of a node's currently unadmitted observed revision, by owning Wavefront.
# TYPE wavefront_node_pin_lag_seconds gauge
wavefront_node_pin_lag_seconds{kind="Kustomization",name="team-a",namespace="flux-system",wavefront="fleet"} 42
wavefront_node_pin_lag_seconds{kind="Kustomization",name="team-b",namespace="flux-system",wavefront="fleet"} 7
wavefront_node_pin_lag_seconds{kind="Kustomization",name="team-c",namespace="flux-system",wavefront="infra"} 11
# HELP wavefront_pinned_fetch_failures Number of pinned sources currently reporting a fetch failure, by owning Wavefront; sum() over the wavefront label for a fleet total.
# TYPE wavefront_pinned_fetch_failures gauge
wavefront_pinned_fetch_failures{wavefront="fleet"} 2
wavefront_pinned_fetch_failures{wavefront="infra"} 0
# HELP wavefront_ref_list_failures_total Total ref-advertisement listing failures, by git host.
# TYPE wavefront_ref_list_failures_total counter
wavefront_ref_list_failures_total{host="git.example.com"} 5
`

	if err := testutil.CollectAndCompare(reg, strings.NewReader(want),
		"wavefront_admissions_total", "wavefront_blocked_nodes", "wavefront_node_pin_lag_seconds",
		"wavefront_pinned_fetch_failures", "wavefront_ref_list_failures_total"); err != nil {
		t.Fatal(err)
	}

	// The histogram is checked separately: its exposition includes generated
	// bucket boundaries, which would make the literal comparison above brittle.
	if count := testutil.CollectAndCount(instr.AdmissionWaitSeconds); count != 1 {
		t.Errorf("AdmissionWaitSeconds collected %d metrics, want 1", count)
	}
	fleetWait, ok := instr.AdmissionWaitSeconds.WithLabelValues("fleet").(prometheus.Histogram)
	if !ok {
		t.Fatalf("AdmissionWaitSeconds.WithLabelValues did not return a prometheus.Histogram")
	}
	if got := histogramSampleCount(t, fleetWait); got != 2 {
		t.Errorf("AdmissionWaitSeconds sample count = %v, want 2", got)
	}
}

// histogramSampleCount reads a Histogram's _count field directly: ToFloat64
// only supports single-value metrics (Counter, Gauge, Untyped).
func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestNewCollectAndLint asserts every collector passes promlint (naming and
// help-text conventions).
func TestNewCollectAndLint(t *testing.T) {
	instr, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	collectors := []prometheus.Collector{
		instr.AdmissionsTotal,
		instr.PinLagSeconds,
		instr.AdmissionWaitSeconds,
		instr.BlockedNodes,
		instr.RefListFailures,
		instr.PinnedFetchFailures,
		instr.CredentialReadFailures,
	}
	for _, c := range collectors {
		problems, err := testutil.CollectAndLint(c)
		if err != nil {
			t.Fatalf("CollectAndLint: %v", err)
		}
		if len(problems) > 0 {
			t.Errorf("lint problems: %+v", problems)
		}
	}
}

// TestNewNameCollisionDifferentLabelsReturnsError covers the review finding
// (6): pre-registering a same-named collector whose label set differs from
// AdmissionsTotal's own makes the registry return a plain (non-
// AlreadyRegistered) error for that name — register must surface it rather
// than silently handing back the fresh, unregistered collector.
func TestNewNameCollisionDifferentLabelsReturnsError(t *testing.T) {
	reg := prometheus.NewRegistry()

	colliding := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wavefront_admissions_total",
		Help: "A different metric that happens to share AdmissionsTotal's name.",
	}, []string{"different_label"})
	if err := reg.Register(colliding); err != nil {
		t.Fatalf("pre-registering the colliding collector: %v", err)
	}

	if _, err := metrics.New(reg); err == nil {
		t.Fatal("New(reg) error = nil, want non-nil: a same-named collector with different labels must not be swallowed")
	} else if !strings.Contains(err.Error(), "wavefront_admissions_total") {
		t.Errorf("New(reg) error = %q, want it to name the colliding metric (wavefront_admissions_total)", err.Error())
	}
}

// collidingCollector is a minimal prometheus.Collector that describes itself
// identically to AdmissionsTotal (same fully-qualified name, help text and
// label names, so its Desc has the same id and dimHash) but is not a
// *prometheus.CounterVec. Registering it first makes the registry return an
// AlreadyRegisteredError for AdmissionsTotal whose ExistingCollector fails
// the *prometheus.CounterVec type assertion inside register — the second
// swallow path from the brief. Constructing this case turned out possible:
// prometheus.Desc.id only depends on fqName and const label values (not the
// collector's Go type or variable label names), so any Collector describing
// itself with a matching Desc collides regardless of its concrete type.
type collidingCollector struct {
	desc *prometheus.Desc
}

func (c *collidingCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }
func (c *collidingCollector) Collect(chan<- prometheus.Metric)    {}

// TestNewAlreadyRegisteredWrongTypeReturnsError covers the second swallow
// path: an AlreadyRegisteredError whose ExistingCollector is not assertable
// to the metric's own concrete type (a name collision with a differently
// typed collector on the shared registry) must also surface as an error, not
// silently hand back the fresh, unregistered collector.
func TestNewAlreadyRegisteredWrongTypeReturnsError(t *testing.T) {
	reg := prometheus.NewRegistry()

	fake := &collidingCollector{
		desc: prometheus.NewDesc(
			"wavefront_admissions_total",
			"Total pin admissions, by result (admitted, initial, shadow, conflict) and owning Wavefront.",
			[]string{metrics.LabelWavefront, "result"},
			nil,
		),
	}
	if err := reg.Register(fake); err != nil {
		t.Fatalf("pre-registering the colliding collector: %v", err)
	}

	if _, err := metrics.New(reg); err == nil {
		t.Fatal("New(reg) error = nil, want non-nil: an AlreadyRegisteredError whose ExistingCollector fails the *prometheus.CounterVec assertion must not be swallowed")
	} else if !strings.Contains(err.Error(), "wavefront_admissions_total") {
		t.Errorf("New(reg) error = %q, want it to name the colliding metric (wavefront_admissions_total)", err.Error())
	}
}

// TestNewDoubleRegisterSameRegistryDoesNotPanic covers the brief's explicit
// requirement: a second controller wiring against the same Registerer (e.g.
// the shared ctrlmetrics.Registry) must not panic, and must end up recording
// against the very same series rather than a silently dropped duplicate.
func TestNewDoubleRegisterSameRegistryDoesNotPanic(t *testing.T) {
	reg := prometheus.NewRegistry()

	first, err := metrics.New(reg)
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	var second *metrics.Instruments
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("second New on the same registry panicked: %v", r)
			}
		}()
		var err2 error
		second, err2 = metrics.New(reg)
		if err2 != nil {
			t.Fatalf("second New: %v", err2)
		}
	}()

	first.AdmissionsTotal.WithLabelValues("fleet", "admitted").Inc()
	second.AdmissionsTotal.WithLabelValues("fleet", "admitted").Inc()

	if got := testutil.ToFloat64(second.AdmissionsTotal.WithLabelValues("fleet", "admitted")); got != 2 {
		t.Errorf("admissions after two increments via either handle = %v, want 2 (same underlying series)", got)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "wavefront_admissions_total" {
			continue
		}
		if got := len(family.GetMetric()); got != 1 {
			t.Errorf("wavefront_admissions_total series after double New = %d, want 1 (no duplicate registration)", got)
		}
	}
}

// TestNopIsolatesRegistries confirms Nop instances never collide with each
// other or with a caller's own registry.
func TestNopIsolatesRegistries(t *testing.T) {
	a := metrics.Nop()
	b := metrics.Nop()

	a.AdmissionsTotal.WithLabelValues("fleet", "admitted").Inc()

	if got := testutil.ToFloat64(b.AdmissionsTotal.WithLabelValues("fleet", "admitted")); got != 0 {
		t.Errorf("second Nop's counter = %v, want 0: independent registries must not share state", got)
	}
}

// TestWavefrontScopeRetireLeavesSiblings covers Retire: it deletes only the
// per-pass gauges (PinLagSeconds, BlockedNodes, PinnedFetchFailures) for the
// named Wavefront, leaving a co-resident Wavefront's series untouched.
func TestWavefrontScopeRetireLeavesSiblings(t *testing.T) {
	instr := metrics.Nop()
	instr.Wavefront("fleet").SetPinnedFetchFailures(2)
	instr.Wavefront("infra").SetPinnedFetchFailures(1)
	instr.Wavefront("fleet").SetBlocked("UnsettledAncestor", 3)
	instr.Wavefront("fleet").SetPinLag("Kustomization", "ns", "app", 42)

	instr.Wavefront("fleet").Retire()

	if got := testutil.CollectAndCount(instr.PinnedFetchFailures); got != 1 {
		t.Errorf("PinnedFetchFailures series = %d, want 1 (sibling only)", got)
	}
	if got := testutil.CollectAndCount(instr.PinLagSeconds); got != 0 {
		t.Errorf("PinLagSeconds series = %d, want 0", got)
	}
	if got := testutil.CollectAndCount(instr.BlockedNodes); got != 0 {
		t.Errorf("BlockedNodes series = %d, want 0", got)
	}
	if got := testutil.ToFloat64(instr.PinnedFetchFailures.WithLabelValues("infra")); got != 1 {
		t.Errorf("sibling PinnedFetchFailures = %v, want 1", got)
	}
}

// TestWavefrontScopeForgetRetiresCumulativeSeries covers Forget: unlike
// Retire, it also deletes the cumulative counter and histogram series for the
// named Wavefront, since no pass ever recomputes those wholesale.
func TestWavefrontScopeForgetRetiresCumulativeSeries(t *testing.T) {
	instr := metrics.Nop()
	instr.Wavefront("fleet").CountAdmission("admitted")
	instr.Wavefront("infra").CountAdmission("admitted")
	instr.Wavefront("fleet").ObserveAdmissionWait(45)

	instr.Wavefront("fleet").Forget()

	if got := testutil.CollectAndCount(instr.AdmissionsTotal); got != 1 {
		t.Errorf("AdmissionsTotal series = %d, want 1 (sibling only)", got)
	}
	if got := testutil.CollectAndCount(instr.AdmissionWaitSeconds); got != 0 {
		t.Errorf("AdmissionWaitSeconds series = %d, want 0", got)
	}
}
