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

// Package metrics is the single owner of the controller's launch metric set
// (DESIGN §6): every Prometheus collector the reconciler and the poller
// record against is created and registered here, exactly once, so that no
// other package registers a metric of its own.
package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// admissionWaitBuckets spans the starvation-signal range DESIGN §6 asks for:
// 30 seconds (a fast poll interval) to 2 hours (a long-idle upstream),
// exponentially spaced so both ends stay meaningfully resolved.
var admissionWaitBuckets = prometheus.ExponentialBucketsRange(30, 7200, 10)

// LabelWavefront is the label every per-Wavefront collector carries: the
// name of the owning Wavefront. Wavefronts are cluster-scoped and may be
// co-resident, and the per-pass gauges are recomputed wholesale on each pass
// — without this label a pass would have to Reset() the collector and would
// erase a sibling's series until its next pass. With it, a pass retires
// exactly its own series (DeletePartialMatch) and a cross-fleet total is a
// PromQL sum(). WavefrontScope is the only place this label's position is
// spelled out; every collector puts it first.
const LabelWavefront = "wavefront"

// Instruments is the fixed set of collectors the controller records against
// (DESIGN §6, names and labels verbatim).
type Instruments struct {
	// AdmissionsTotal counts pin admissions by owning Wavefront and result:
	// "admitted" (an ancestor-gated advance), "initial"
	// (initial-pin-on-discovery), "shadow" (would-be admission in Shadow
	// mode, no write) or "conflict" (blocked by a foreign field-manager
	// hold). Attributed like every other per-Wavefront collector, but —
	// being cumulative — its series are never retired by a pass; only
	// Forget (on Wavefront deletion) deletes them, since there is no
	// wholesale recomputation for a pass to reconcile a counter against.
	AdmissionsTotal *prometheus.CounterVec // wavefront_admissions_total{wavefront,result}
	// PinLagSeconds is the age, in seconds, of each node's currently
	// unadmitted observed revision. Recomputed wholesale every pass, per
	// Wavefront: the owning Wavefront's name is a label so that one
	// Wavefront's pass can retire only its own series (DeletePartialMatch)
	// rather than Reset()ting a co-resident Wavefront's series away.
	PinLagSeconds *prometheus.GaugeVec // wavefront_node_pin_lag_seconds{wavefront,kind,namespace,name}
	// AdmissionWaitSeconds is observed→admitted latency at the moment an
	// admission actually executes (DESIGN D13, the starvation signal), by
	// owning Wavefront. Cumulative like AdmissionsTotal: a pass never
	// retires its series, only Forget does (on Wavefront deletion).
	AdmissionWaitSeconds *prometheus.HistogramVec // wavefront_admission_wait_seconds{wavefront}
	// BlockedNodes is the current blocked-node count by BlockedReason,
	// per Wavefront. Recomputed wholesale every pass (per-Wavefront, as
	// PinLagSeconds); a fleet total is sum() across the wavefront label.
	BlockedNodes *prometheus.GaugeVec // wavefront_blocked_nodes{wavefront,reason}
	// RefListFailures counts ref-advertisement listing failures by git host
	// (owned here; the poller only records against it).
	RefListFailures *prometheus.CounterVec // wavefront_ref_list_failures_total{host}
	// PinnedFetchFailures is the current count of pinned sources with a
	// sourcev1 FetchFailed condition (§10 force-push detection), per
	// Wavefront. Recomputed wholesale every pass; a fleet total is sum()
	// across the wavefront label.
	PinnedFetchFailures *prometheus.GaugeVec // wavefront_pinned_fetch_failures{wavefront}
	// CredentialReadFailures counts failures reading git credential Secrets
	// during ref-advertisement sweeps (owned here; the poller only records
	// against it). Not per-Wavefront: a credential read happens ahead of
	// any one Wavefront's evaluation.
	CredentialReadFailures prometheus.Counter // wavefront_credential_read_failures_total
}

// New registers the launch metric set on reg and returns the handles the
// reconciler and poller record against.
//
// Calling New more than once on the same reg (e.g. a second controller
// wiring against the shared ctrlmetrics.Registry) does not panic: each
// collector already registered is reused rather than re-registered, so every
// caller ends up recording against the same underlying series. main wires
// this exactly once regardless.
func New(reg prometheus.Registerer) *Instruments {
	return &Instruments{
		AdmissionsTotal: register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wavefront_admissions_total",
			Help: "Total pin admissions, by result (admitted, initial, shadow, conflict).",
		}, []string{LabelWavefront, "result"})),
		PinLagSeconds: register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "wavefront_node_pin_lag_seconds",
			Help: "Age in seconds of a node's currently unadmitted observed revision, by owning Wavefront.",
		}, []string{LabelWavefront, "kind", "namespace", "name"})),
		AdmissionWaitSeconds: register(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wavefront_admission_wait_seconds",
			Help:    "Seconds from first observation of a revision to its admission.",
			Buckets: admissionWaitBuckets,
		}, []string{LabelWavefront})),
		BlockedNodes: register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "wavefront_blocked_nodes",
			Help: "Number of nodes currently blocked, by owning Wavefront and reason; sum() over the wavefront label for a fleet total.",
		}, []string{LabelWavefront, "reason"})),
		RefListFailures: register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wavefront_ref_list_failures_total",
			Help: "Total ref-advertisement listing failures, by git host.",
		}, []string{"host"})),
		PinnedFetchFailures: register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "wavefront_pinned_fetch_failures",
			Help: "Number of pinned sources currently reporting a fetch failure, by owning Wavefront; sum() over the wavefront label for a fleet total.",
		}, []string{LabelWavefront})),
		CredentialReadFailures: register(reg, prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wavefront_credential_read_failures_total",
			Help: "Total failures reading git credential Secrets during ref-advertisement sweeps.",
		})),
	}
}

// Nop returns Instruments backed by a fresh, isolated registry: tests and
// fixtures that need something to record against without touching the
// process's default registry.
func Nop() *Instruments {
	return New(prometheus.NewRegistry())
}

// Wavefront returns the scope through which the reconciler and poller record
// every per-Wavefront collector for the Wavefront named name. Label order
// (wavefront first, always) lives only inside WavefrontScope's methods, not
// at each call site.
func (i *Instruments) Wavefront(name string) WavefrontScope {
	return WavefrontScope{i: i, name: name}
}

// WavefrontScope binds every per-Wavefront collector to one Wavefront's
// name.
type WavefrontScope struct {
	i    *Instruments
	name string
}

// Retire deletes s's series from every gauge a pass recomputes wholesale
// (PinLagSeconds, BlockedNodes, PinnedFetchFailures) via DeletePartialMatch
// on this Wavefront's own label — never Reset(), which would erase a
// co-resident Wavefront's series until its own next pass. Call at the start
// of a pass, before recomputing, so a node or reason that dropped out of the
// fleet since the last pass does not linger.
func (s WavefrontScope) Retire() {
	mine := prometheus.Labels{LabelWavefront: s.name}
	s.i.PinLagSeconds.DeletePartialMatch(mine)
	s.i.BlockedNodes.DeletePartialMatch(mine)
	s.i.PinnedFetchFailures.DeletePartialMatch(mine)
}

// Forget deletes every series this Wavefront has ever contributed: Retire's
// per-pass gauges, plus the cumulative counter and histogram
// (AdmissionsTotal, AdmissionWaitSeconds) that a pass leaves alone because
// there is no wholesale recomputation to reconcile a cumulative series
// against. Call once, when the Wavefront itself is deleted.
func (s WavefrontScope) Forget() {
	s.Retire()
	mine := prometheus.Labels{LabelWavefront: s.name}
	s.i.AdmissionsTotal.DeletePartialMatch(mine)
	s.i.AdmissionWaitSeconds.DeletePartialMatch(mine)
}

// SetPinLag sets s's Wavefront's pin-lag gauge for one node.
func (s WavefrontScope) SetPinLag(kind, namespace, name string, seconds float64) {
	s.i.PinLagSeconds.WithLabelValues(s.name, kind, namespace, name).Set(seconds)
}

// SetBlocked sets s's Wavefront's blocked-node count for one BlockedReason.
func (s WavefrontScope) SetBlocked(reason string, count int) {
	s.i.BlockedNodes.WithLabelValues(s.name, reason).Set(float64(count))
}

// SetPinnedFetchFailures sets s's Wavefront's pinned-fetch-failures gauge.
func (s WavefrontScope) SetPinnedFetchFailures(count int) {
	s.i.PinnedFetchFailures.WithLabelValues(s.name).Set(float64(count))
}

// CountAdmission increments s's Wavefront's admissions counter for result.
func (s WavefrontScope) CountAdmission(result string) {
	s.i.AdmissionsTotal.WithLabelValues(s.name, result).Inc()
}

// ObserveAdmissionWait records one observed→admitted latency sample for s's
// Wavefront.
func (s WavefrontScope) ObserveAdmissionWait(seconds float64) {
	s.i.AdmissionWaitSeconds.WithLabelValues(s.name).Observe(seconds)
}

// register registers c on reg, tolerating a collector already registered
// under the same name (typically a second New on one Registerer) by reusing
// the existing collector instead of panicking or silently dropping c.
func register[C prometheus.Collector](reg prometheus.Registerer, c C) C {
	if err := reg.Register(c); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(C); ok {
				return existing
			}
		}
	}
	return c
}
