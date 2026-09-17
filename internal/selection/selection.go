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

// Package selection implements the candidate-selection seam (DESIGN §3.6,
// D10): the pluggable answer to "which SHA is the candidate?", decoupled
// from the pin mechanism itself (which is uniform across all ref styles).
// v1 ships only TrackRef; a future SemverWindow strategy is a non-breaking
// addition behind the same Strategy interface.
package selection

import (
	"errors"

	"github.com/fluxcd/pkg/git"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

// ErrUnsupportedRef marks ref styles v1 cannot sequence (semver).
var ErrUnsupportedRef = errors.New("unsupported ref style for candidate selection")

// Strategy answers "which SHA is the candidate?" (DESIGN §3.6).
type Strategy interface {
	// TrackingRef maps a GitRepository ref spec to the advertised ref name to observe.
	TrackingRef(ref *sourcev1.GitRepositoryRef) (string, error)
	// Candidate selects the candidate SHA from an advertisement listing
	// (map of ref name → SHA, including peeled "<ref>^{}" entries).
	Candidate(advertised map[string]string, trackingRef string) (sha string, ok bool)
}

// trackRef is the v1 default and only strategy (DESIGN D10).
type trackRef struct{}

// TrackRef is the v1 default and only strategy (DESIGN D10).
func TrackRef() Strategy {
	return trackRef{}
}

// defaultBranchRef is the tracking ref used when a GitRepositoryRef leaves
// every one of Name, Branch, Tag and SemVer unset (Flux's default branch is
// "master" — fluxcd/pkg/git.DefaultBranch). A ref with only Commit set also
// falls back here: a rendered ref.commit is a hand-pin concern handled by
// pin.Hold, not a tracking style, so TrackingRef deliberately considers only
// Name/Branch/Tag/SemVer.
const defaultBranchRef = "refs/heads/" + git.DefaultBranch

// TrackingRef implements Strategy.
func (trackRef) TrackingRef(ref *sourcev1.GitRepositoryRef) (string, error) {
	if ref == nil {
		return defaultBranchRef, nil
	}
	// Precedence follows Flux's verified clone dispatch order (DESIGN §7.4,
	// Implementation Notes item 4): commit → refName → tag → semver →
	// branch. TrackingRef ignores Commit (see above), so: Name > Tag >
	// SemVer > Branch. A ref with both Tag and SemVer set tracks the tag.
	switch {
	case ref.Name != "":
		return ref.Name, nil
	case ref.Tag != "":
		return "refs/tags/" + ref.Tag, nil
	case ref.SemVer != "":
		return "", ErrUnsupportedRef
	case ref.Branch != "":
		return "refs/heads/" + ref.Branch, nil
	default:
		return defaultBranchRef, nil
	}
}

// Candidate implements Strategy.
func (trackRef) Candidate(advertised map[string]string, trackingRef string) (string, bool) {
	if sha, ok := advertised[trackingRef+"^{}"]; ok {
		return sha, true
	}
	sha, ok := advertised[trackingRef]
	return sha, ok
}
