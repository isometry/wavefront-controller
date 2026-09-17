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
	"encoding/json"
	"fmt"
	"io"

	"sigs.k8s.io/yaml"
)

// jsonIndent is two spaces: the snapshot is meant to be read in a terminal
// and pasted into an incident channel, and four-space JSON wraps.
const jsonIndent = "  "

// JSON writes v as indented JSON with a trailing newline.
//
// A Snapshot round-trips through this and back through snapshot.FileSource
// unchanged — `wfctl snapshot > f.json` then `--from f.json` is the contract
// the golden fixtures themselves rely on.
func JSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", jsonIndent)
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}
	return nil
}

// YAML writes v as YAML.
//
// sigs.k8s.io/yaml goes through the JSON tags, so `-o yaml` and `-o json`
// describe the same document with the same field names — anything else would
// make the two outputs disagree about a schema they share.
func YAML(w io.Writer, v any) error {
	data, err := yaml.Marshal(v)
	if err != nil {
		return fmt.Errorf("encoding YAML: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("writing YAML: %w", err)
	}
	return nil
}
