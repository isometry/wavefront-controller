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

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

// FileSource replays a captured Snapshot (`--from file.json`). It needs no
// cluster at all, which is the point: an incident's evidence can be attached
// to a ticket and re-rendered by anyone, and the renderers' golden tests run
// against fixtures rather than a fake apiserver.
type FileSource struct {
	Path string
}

var _ Source = (*FileSource)(nil)

// Capture implements Source.
//
// A snapshot whose apiVersion or kind is not this package's is rejected
// outright rather than decoded on a best-effort basis: a renderer that
// silently shows zero values for fields a newer schema moved would report a
// quiescent fleet that is nothing of the sort.
//
// The captured Origin is preserved and Replayed is set, so a renderer branches
// on what the snapshot proved, not on how it was delivered.
func (f FileSource) Capture(_ context.Context) (*Snapshot, error) {
	data, err := os.ReadFile(f.Path)
	if err != nil {
		return nil, fmt.Errorf("reading snapshot %s: %w", f.Path, err)
	}

	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("parsing snapshot %s: %w", f.Path, err)
	}

	if snap.Kind != KindSnapshot {
		return nil, fmt.Errorf("%s is not a wfctl snapshot: kind is %q, want %q", f.Path, snap.Kind, KindSnapshot)
	}
	if snap.APIVersion != Version {
		return nil, fmt.Errorf(
			"%s has apiVersion %q, want %q: capture it again with this version of wfctl",
			f.Path, snap.APIVersion, Version)
	}

	// Origin is left exactly as captured: replaying a derive snapshot does not
	// make it a status one, and its DERIVED columns must still render.
	// Replayed is the separate fact — that this picture came from a file and
	// is as old as its CapturedAt says.
	snap.Replayed = true
	return &snap, nil
}
