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
	"io"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"

	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// History renders one row per event, for `wfctl history`.
//
// TIME is how long ago the event last occurred — kubectl's own "LAST SEEN"
// convention, collapsed to a single column — anchored on
// snapshot.EventTime, which already resolves eventTime, the series'
// last-observed heartbeat, or the deprecated firstTimestamp in that order.
// COUNT is the series' occurrence count, the recorder's own way of
// collapsing a repeated event onto a single object instead of a new one each
// time. NOTE is printed verbatim except for characters a table cell cannot
// carry.
func History(w io.Writer, events []eventsv1.Event, o Options) error {
	table := NewTable(w, "TIME", "TYPE", "REASON", "COUNT", "NOTE")
	now := o.now()

	for i := range events {
		e := &events[i]

		typeCell := e.Type
		if typeCell == corev1.EventTypeWarning {
			typeCell = o.Palette.Warn(typeCell)
		}

		at := snapshot.EventTime(*e)
		table.Row(
			age(now, &at),
			typeCell,
			e.Reason,
			strconv.Itoa(int(snapshot.EventCount(*e))),
			sanitize(e.Note),
		)
	}

	return table.Flush()
}
