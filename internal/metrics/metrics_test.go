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
	instr := metrics.New(reg)

	instr.AdmissionsTotal.WithLabelValues("admitted").Inc()
	instr.AdmissionsTotal.WithLabelValues("admitted").Inc()
	instr.AdmissionsTotal.WithLabelValues("initial").Inc()
	instr.AdmissionsTotal.WithLabelValues("shadow").Inc()
	instr.AdmissionsTotal.WithLabelValues("conflict").Inc()

	instr.PinLagSeconds.WithLabelValues("Kustomization", "flux-system", "team-a").Set(42)
	instr.PinLagSeconds.WithLabelValues("Kustomization", "flux-system", "team-b").Set(7)

	instr.AdmissionWaitSeconds.Observe(45)
	instr.AdmissionWaitSeconds.Observe(600)

	instr.BlockedNodes.WithLabelValues("AncestorPending").Set(3)
	instr.BlockedNodes.WithLabelValues("SelfHeld").Set(1)

	instr.RefListFailures.WithLabelValues("git.example.com").Add(5)

	instr.PinnedFetchFailures.Set(2)

	const want = `
# HELP wavefront_admissions_total Total pin admissions, by result (admitted, initial, shadow, conflict).
# TYPE wavefront_admissions_total counter
wavefront_admissions_total{result="admitted"} 2
wavefront_admissions_total{result="conflict"} 1
wavefront_admissions_total{result="initial"} 1
wavefront_admissions_total{result="shadow"} 1
# HELP wavefront_blocked_nodes Number of nodes currently blocked, by reason.
# TYPE wavefront_blocked_nodes gauge
wavefront_blocked_nodes{reason="AncestorPending"} 3
wavefront_blocked_nodes{reason="SelfHeld"} 1
# HELP wavefront_node_pin_lag_seconds Age in seconds of a node's currently unadmitted observed revision.
# TYPE wavefront_node_pin_lag_seconds gauge
wavefront_node_pin_lag_seconds{kind="Kustomization",name="team-a",namespace="flux-system"} 42
wavefront_node_pin_lag_seconds{kind="Kustomization",name="team-b",namespace="flux-system"} 7
# HELP wavefront_pinned_fetch_failures Number of pinned sources currently reporting a fetch failure.
# TYPE wavefront_pinned_fetch_failures gauge
wavefront_pinned_fetch_failures 2
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
	if got := histogramSampleCount(t, instr.AdmissionWaitSeconds); got != 2 {
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
	instr := metrics.New(prometheus.NewRegistry())

	collectors := []prometheus.Collector{
		instr.AdmissionsTotal,
		instr.PinLagSeconds,
		instr.AdmissionWaitSeconds,
		instr.BlockedNodes,
		instr.RefListFailures,
		instr.PinnedFetchFailures,
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

// TestNewDoubleRegisterSameRegistryDoesNotPanic covers the brief's explicit
// requirement: a second controller wiring against the same Registerer (e.g.
// the shared ctrlmetrics.Registry) must not panic, and must end up recording
// against the very same series rather than a silently dropped duplicate.
func TestNewDoubleRegisterSameRegistryDoesNotPanic(t *testing.T) {
	reg := prometheus.NewRegistry()

	first := metrics.New(reg)
	var second *metrics.Instruments
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("second New on the same registry panicked: %v", r)
			}
		}()
		second = metrics.New(reg)
	}()

	first.AdmissionsTotal.WithLabelValues("admitted").Inc()
	second.AdmissionsTotal.WithLabelValues("admitted").Inc()

	if got := testutil.ToFloat64(second.AdmissionsTotal.WithLabelValues("admitted")); got != 2 {
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

	a.AdmissionsTotal.WithLabelValues("admitted").Inc()

	if got := testutil.ToFloat64(b.AdmissionsTotal.WithLabelValues("admitted")); got != 0 {
		t.Errorf("second Nop's counter = %v, want 0: independent registries must not share state", got)
	}
}
