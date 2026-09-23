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
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
)

// Audit reasons, one per write command. They share the vocabulary of
// the controller's own events so that `wfctl history` reads as one stream: the
// controller says what it did, wfctl says what a human did.
const (
	ReasonSuspended     = "Suspended"
	ReasonResumed       = "Resumed"
	ReasonModeChanged   = "ModeChanged"
	ReasonHandPinned    = "HandPinned"
	ReasonPinReleased   = "PinReleased"
	ReasonPinStripped   = "PinStripped"
	ReasonForceAdmitted = "ForceAdmitted"
)

const (
	// auditController is the reportingController of every wfctl event, so a
	// reader can tell an operator's write from the controller's own.
	auditController = "wfctl"
	// auditNamespace is where events about a cluster-scoped object land: the
	// same place the controller's own Wavefront events do.
	auditNamespace = "default"
	// noteLimit is the apiserver's own cap on an event note.
	noteLimit = 1024
)

// Note renders an audit note: who ran what.
//
// The note is the whole audit value of the event — the reason says what
// happened, the note says who asked for it and in exactly which words — so it
// is built in one place rather than at each of seven call sites.
func Note(identity, command string) string {
	return truncate(fmt.Sprintf("%s ran: %s", identity, command))
}

// Partial marks an audit note for a write that only partly landed.
//
// A half-applied break-glass is the single most important thing in the trail:
// the reason alone would say "PinStripped" and leave the reader to guess how
// much of the fleet it applied to.
func Partial(note string, err error) string {
	return truncate(fmt.Sprintf("%s (partial: %v)", note, err))
}

// truncate keeps a note inside the apiserver's own cap.
func truncate(note string) string {
	if len(note) <= noteLimit {
		return note
	}
	return note[:noteLimit-3] + "..."
}

// Audit records one completed write as an event on the Wavefront.
//
// Best-effort by contract: an operator whose RBAC covers patching a
// GitRepository but not creating events has still performed the write, and
// failing the command afterwards would report a lie. The caller turns the
// error into a warning; nothing here decides that.
//
// Events are at-least-once and expire with the apiserver's --event-ttl —
// this is a convenience trail, not the durable ledger. The durable record of
// a pin change is the provenance annotations.
func Audit(
	ctx context.Context,
	c client.Client,
	wf *wavefrontv1alpha1.Wavefront,
	reason, note string,
	at time.Time,
) error {
	event := &eventsv1.Event{
		// The recorder's own naming scheme: unique per object per instant,
		// which is all the apiserver requires of an event name.
		Name:                fmt.Sprintf("%s.%x", wf.Name, at.UnixNano()),
		Namespace:           auditNamespace,
		EventTime:           metav1.MicroTime{Time: at},
		ReportingController: auditController,
		ReportingInstance:   auditController,
		Action:              reason,
		Reason:              reason,
		Regarding: corev1.ObjectReference{
			APIVersion: wavefrontv1alpha1.GroupVersion.String(),
			Kind:       "Wavefront",
			Name:       wf.Name,
			UID:        wf.UID,
		},
		Note: note,
		Type: corev1.EventTypeNormal,
	}

	if err := c.Create(ctx, event); err != nil {
		return fmt.Errorf("recording the %s audit event: %w", reason, err)
	}
	return nil
}
