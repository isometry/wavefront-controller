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

package selection_test

import (
	"errors"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/isometry/wavefront-controller/internal/selection"
)

const (
	defaultBranchRef = "refs/heads/master"
	mainBranchRef    = "refs/heads/main"
	developBranch    = "develop"
	tagV1            = "v1.0.0"
	tagV1Ref         = "refs/tags/" + tagV1
)

func TestTrackRef_TrackingRef(t *testing.T) {
	tests := []struct {
		name    string
		ref     *sourcev1.GitRepositoryRef
		want    string
		wantErr error
	}{
		{
			name: "nil ref defaults to master branch",
			ref:  nil,
			want: defaultBranchRef,
		},
		{
			name: "empty ref defaults to master branch",
			ref:  &sourcev1.GitRepositoryRef{},
			want: defaultBranchRef,
		},
		{
			name: "name takes precedence and is used verbatim",
			ref:  &sourcev1.GitRepositoryRef{Name: mainBranchRef},
			want: mainBranchRef,
		},
		{
			name: "name verbatim even when it doesn't look like a standard ref",
			ref:  &sourcev1.GitRepositoryRef{Name: "refs/pull/420/head"},
			want: "refs/pull/420/head",
		},
		{
			name: "branch maps to refs/heads/",
			ref:  &sourcev1.GitRepositoryRef{Branch: developBranch},
			want: "refs/heads/develop",
		},
		{
			name: "tag maps to refs/tags/",
			ref:  &sourcev1.GitRepositoryRef{Tag: "v1.2.3"},
			want: "refs/tags/v1.2.3",
		},
		{
			name:    "semver is unsupported",
			ref:     &sourcev1.GitRepositoryRef{SemVer: ">=1.0.0 <2.0.0"},
			wantErr: selection.ErrUnsupportedRef,
		},
		{
			name: "commit-only ref falls back to default branch",
			ref:  &sourcev1.GitRepositoryRef{Commit: "abcdef1234567890"},
			want: defaultBranchRef,
		},
		{
			name: "name takes precedence over branch and tag",
			ref:  &sourcev1.GitRepositoryRef{Name: "refs/heads/explicit", Branch: developBranch, Tag: tagV1},
			want: "refs/heads/explicit",
		},
		{
			name: "tag takes precedence over branch",
			ref:  &sourcev1.GitRepositoryRef{Branch: developBranch, Tag: tagV1},
			want: tagV1Ref,
		},
		{
			name: "tag takes precedence over semver and branch",
			ref:  &sourcev1.GitRepositoryRef{Branch: developBranch, Tag: tagV1, SemVer: ">=1.0.0"},
			want: tagV1Ref,
		},
		{
			name:    "semver takes precedence over branch when tag is unset",
			ref:     &sourcev1.GitRepositoryRef{Branch: developBranch, SemVer: ">=1.0.0"},
			wantErr: selection.ErrUnsupportedRef,
		},
	}

	strategy := selection.TrackRef()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := strategy.TrackingRef(tt.ref)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("TrackingRef() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("TrackingRef() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TrackingRef() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTrackRef_Candidate(t *testing.T) {
	tests := []struct {
		name        string
		advertised  map[string]string
		trackingRef string
		wantSHA     string
		wantOK      bool
	}{
		{
			name: "plain branch ref resolves directly",
			advertised: map[string]string{
				mainBranchRef: "1111111111111111111111111111111111111111",
			},
			trackingRef: mainBranchRef,
			wantSHA:     "1111111111111111111111111111111111111111",
			wantOK:      true,
		},
		{
			name: "peeled entry preferred over tag object SHA",
			advertised: map[string]string{
				tagV1Ref:              "2222222222222222222222222222222222222222", // tag object SHA
				"refs/tags/v1.0.0^{}": "3333333333333333333333333333333333333333", // peeled commit SHA
			},
			trackingRef: tagV1Ref,
			wantSHA:     "3333333333333333333333333333333333333333",
			wantOK:      true,
		},
		{
			name: "lightweight tag with no peeled entry resolves directly",
			advertised: map[string]string{
				tagV1Ref: "4444444444444444444444444444444444444444",
			},
			trackingRef: tagV1Ref,
			wantSHA:     "4444444444444444444444444444444444444444",
			wantOK:      true,
		},
		{
			name:        "missing ref yields ok=false",
			advertised:  map[string]string{mainBranchRef: "5555555555555555555555555555555555555555"},
			trackingRef: "refs/heads/develop",
			wantSHA:     "",
			wantOK:      false,
		},
		{
			name:        "empty advertisement yields ok=false",
			advertised:  map[string]string{},
			trackingRef: defaultBranchRef,
			wantSHA:     "",
			wantOK:      false,
		},
	}

	strategy := selection.TrackRef()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSHA, gotOK := strategy.Candidate(tt.advertised, tt.trackingRef)
			if gotOK != tt.wantOK {
				t.Fatalf("Candidate() ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotSHA != tt.wantSHA {
				t.Errorf("Candidate() sha = %q, want %q", gotSHA, tt.wantSHA)
			}
		})
	}
}
