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

package pin_test

import (
	"reflect"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"github.com/isometry/wavefront-controller/internal/pin"
)

// kubectlPatchManager is a stand-in foreign field manager, distinct from the
// controller's own.
const kubectlPatchManager = "kubectl-patch"

// ownersCommitPath is spec.ref.commit, encoded the way the apiserver would
// in a managedFields entry's FieldsV1.
var ownersCommitPath = fieldpath.MakePathOrDie("spec", "ref", "commit")

// commitFieldsV1 builds the FieldsV1 for an entry that owns spec.ref.commit,
// exactly as the apiserver would encode it.
func commitFieldsV1(t *testing.T) *metav1.FieldsV1 {
	t.Helper()
	raw, err := fieldpath.NewSet(ownersCommitPath).ToJSON()
	if err != nil {
		t.Fatalf("encoding FieldsV1: %v", err)
	}
	return &metav1.FieldsV1{Raw: raw}
}

// TestOwners_and_Hold exercises Owners and Hold together against hand-built
// managedFields tables, without going through envtest: field-manager
// bookkeeping is just data here, not apiserver behaviour under test.
func TestOwners_and_Hold(t *testing.T) {
	tests := []struct {
		name          string
		managedFields func(t *testing.T) []metav1.ManagedFieldsEntry
		wantOwners    []pin.Owner
		wantManager   string
		wantHeld      bool
	}{
		{
			name:          "no entries",
			managedFields: func(t *testing.T) []metav1.ManagedFieldsEntry { return nil },
			wantOwners:    nil,
			wantManager:   "",
			wantHeld:      false,
		},
		{
			name: "controller entry only",
			managedFields: func(t *testing.T) []metav1.ManagedFieldsEntry {
				return []metav1.ManagedFieldsEntry{
					{
						Manager:   pin.FieldManager,
						Operation: metav1.ManagedFieldsOperationApply,
						FieldsV1:  commitFieldsV1(t),
					},
				}
			},
			wantOwners: []pin.Owner{
				{Manager: pin.FieldManager, Operation: metav1.ManagedFieldsOperationApply},
			},
			wantManager: "",
			wantHeld:    false,
		},
		{
			name: "controller and kubectl-patch, in managedFields order",
			managedFields: func(t *testing.T) []metav1.ManagedFieldsEntry {
				return []metav1.ManagedFieldsEntry{
					{
						Manager:   pin.FieldManager,
						Operation: metav1.ManagedFieldsOperationApply,
						FieldsV1:  commitFieldsV1(t),
					},
					{
						Manager:   kubectlPatchManager,
						Operation: metav1.ManagedFieldsOperationUpdate,
						FieldsV1:  commitFieldsV1(t),
					},
				}
			},
			wantOwners: []pin.Owner{
				{Manager: pin.FieldManager, Operation: metav1.ManagedFieldsOperationApply},
				{Manager: kubectlPatchManager, Operation: metav1.ManagedFieldsOperationUpdate},
			},
			wantManager: kubectlPatchManager,
			wantHeld:    true,
		},
		{
			name: "subresource entries are skipped",
			managedFields: func(t *testing.T) []metav1.ManagedFieldsEntry {
				return []metav1.ManagedFieldsEntry{
					{
						Manager:   pin.FieldManager,
						Operation: metav1.ManagedFieldsOperationApply,
						FieldsV1:  commitFieldsV1(t),
					},
					{
						Manager:     "status-writer",
						Operation:   metav1.ManagedFieldsOperationUpdate,
						Subresource: "status",
						FieldsV1:    commitFieldsV1(t),
					},
				}
			},
			wantOwners: []pin.Owner{
				{Manager: pin.FieldManager, Operation: metav1.ManagedFieldsOperationApply},
			},
			wantManager: "",
			wantHeld:    false,
		},
		{
			name: "unparseable FieldsV1 is skipped",
			managedFields: func(t *testing.T) []metav1.ManagedFieldsEntry {
				return []metav1.ManagedFieldsEntry{
					{
						Manager:   pin.FieldManager,
						Operation: metav1.ManagedFieldsOperationApply,
						FieldsV1:  commitFieldsV1(t),
					},
					{
						Manager:   kubectlPatchManager,
						Operation: metav1.ManagedFieldsOperationUpdate,
						FieldsV1:  &metav1.FieldsV1{Raw: []byte("not valid json")},
					},
				}
			},
			wantOwners: []pin.Owner{
				{Manager: pin.FieldManager, Operation: metav1.ManagedFieldsOperationApply},
			},
			wantManager: "",
			wantHeld:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &sourcev1.GitRepository{}
			repo.SetManagedFields(tt.managedFields(t))

			gotOwners := pin.Owners(repo)
			if !reflect.DeepEqual(gotOwners, tt.wantOwners) {
				t.Errorf("Owners() = %#v, want %#v", gotOwners, tt.wantOwners)
			}

			manager, held := pin.Hold(repo)
			if manager != tt.wantManager || held != tt.wantHeld {
				t.Errorf("Hold() = (%q, %v), want (%q, %v)", manager, held, tt.wantManager, tt.wantHeld)
			}

			// Hold must agree with Owners: the first owner whose Manager is
			// not FieldManager, or no hold if none exists.
			wantManager, wantHeld := "", false
			for _, owner := range gotOwners {
				if owner.Manager != pin.FieldManager {
					wantManager, wantHeld = owner.Manager, true
					break
				}
			}
			if manager != wantManager || held != wantHeld {
				t.Errorf("Hold() disagrees with Owners(): Hold() = (%q, %v), derived (%q, %v)",
					manager, held, wantManager, wantHeld)
			}
		})
	}
}

// TestOwners_nilRepo confirms Owners tolerates a nil repo, same as Hold does.
func TestOwners_nilRepo(t *testing.T) {
	if got := pin.Owners(nil); got != nil {
		t.Errorf("Owners(nil) = %#v, want nil", got)
	}

	manager, held := pin.Hold(nil)
	if manager != "" || held {
		t.Errorf("Hold(nil) = (%q, %v), want (\"\", false)", manager, held)
	}
}
