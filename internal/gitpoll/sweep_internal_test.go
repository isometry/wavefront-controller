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

package gitpoll

import "testing"

func TestHostOf(t *testing.T) {
	for raw, want := range map[string]string{
		"https://git.example.com/org/a.git": "git.example.com",
		"://not a url":                      unknownHost,
		"relative/path":                     unknownHost,
	} {
		if got := hostOf(raw); got != want {
			t.Errorf("hostOf(%q) = %q, want %q", raw, got, want)
		}
	}
}
