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

package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
)

const (
	kindKustomization = "Kustomization"
	managedLabelKey   = "wavefront.as-code.io/managed"
	managedLabelValue = "true"
)

// validWavefront returns a minimally valid Wavefront CR for the given name.
func validWavefront(name string) *wavefrontv1alpha1.Wavefront {
	return &wavefrontv1alpha1.Wavefront{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: wavefrontv1alpha1.WavefrontSpec{
			Nodes: wavefrontv1alpha1.NodesSpec{
				Kinds: []string{kindKustomization},
				Selector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						managedLabelKey: managedLabelValue,
					},
				},
			},
		},
	}
}

var _ = Describe("Wavefront CRD validation", func() {
	It("rejects non-Kustomization node kinds", func() {
		wf := validWavefront("wf-badkind")
		wf.Spec.Nodes.Kinds = []string{"HelmRelease"}
		Expect(k8sClient.Create(ctx, wf)).To(MatchError(ContainSubstring("only Kustomization nodes are supported")))
	})

	It("defaults mode to Shadow and poll to 90s/4", func() {
		// Built as unstructured (rather than the typed Wavefront struct) so that
		// omitted fields are genuinely absent on the wire: encoding/json's
		// `omitempty` never omits struct-kind fields (PollSpec, metav1.Duration),
		// so a typed zero-value Create would send an explicit "interval":"0s"
		// and mask the CRD default. This exercises the real minimal-manifest
		// path: `spec.mode` and `spec.poll` are both entirely omitted. `mode`
		// defaults directly (its parent, `spec`, is present); `poll` itself now
		// carries `+kubebuilder:default={}`, so the absent field is defaulted to
		// an empty object first, and its nested defaults (interval, perHostConcurrency)
		// - which only apply once their parent key exists - then fire in turn.
		name := "wf-defaults"
		u := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": wavefrontv1alpha1.GroupVersion.String(),
				"kind":       "Wavefront",
				"metadata": map[string]any{
					"name": name,
				},
				"spec": map[string]any{
					"nodes": map[string]any{
						"kinds": []any{kindKustomization},
						"selector": map[string]any{
							"matchLabels": map[string]any{
								managedLabelKey: managedLabelValue,
							},
						},
					},
					// poll intentionally omitted entirely.
				},
			},
		}
		Expect(k8sClient.Create(ctx, u)).To(Succeed())
		defer func() {
			Expect(k8sClient.Delete(ctx, u)).To(Succeed())
		}()

		got := &wavefrontv1alpha1.Wavefront{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, got)).To(Succeed())
		Expect(got.Spec.Mode).To(Equal(wavefrontv1alpha1.ModeShadow))
		Expect(got.Spec.Poll.Interval.Duration).To(Equal(90 * time.Second))
		Expect(got.Spec.Poll.PerHostConcurrency).To(Equal(4))
	})

	It("accepts a fully specified Wavefront", func() {
		// DESIGN §4.1 example, group substituted for the real API group.
		wf := &wavefrontv1alpha1.Wavefront{
			ObjectMeta: metav1.ObjectMeta{
				Name: "fleet",
			},
			Spec: wavefrontv1alpha1.WavefrontSpec{
				Nodes: wavefrontv1alpha1.NodesSpec{
					Kinds: []string{kindKustomization},
					Selector: metav1.LabelSelector{
						MatchLabels: map[string]string{
							managedLabelKey: managedLabelValue,
						},
					},
				},
				Mode:    wavefrontv1alpha1.ModeEnforce,
				Suspend: false,
				Poll: wavefrontv1alpha1.PollSpec{
					Interval:           metav1.Duration{Duration: 90 * time.Second},
					PerHostConcurrency: 4,
				},
			},
		}
		Expect(k8sClient.Create(ctx, wf)).To(Succeed())
		defer func() {
			Expect(k8sClient.Delete(ctx, wf)).To(Succeed())
		}()
	})
})
