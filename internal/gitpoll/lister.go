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

import (
	"context"
	"fmt"
	"time"

	// Imported for its init() side effect alone: it registers the multi_ack
	// capabilities that protocol-v2-only hosts (Azure DevOps, AWS CodeCommit)
	// require.
	_ "github.com/fluxcd/pkg/git/gogit"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/storage/memory"
)

// Lister lists advertised refs for one repository URL.
// Returned map: full ref name → 40-hex SHA, including peeled "<ref>^{}" entries.
type Lister interface {
	List(ctx context.Context, repoURL string, auth transport.AuthMethod) (map[string]string, error)
}

// goGitLister reads ref advertisements with go-git's Remote.List. No git
// objects are ever fetched and no disk is touched: the remote is backed by an
// in-memory storer that the advertisement never writes to.
type goGitLister struct {
	timeout time.Duration
}

// NewGoGitLister returns the production Lister (go-git Remote.List, no clone).
func NewGoGitLister(timeout time.Duration) Lister {
	return &goGitLister{timeout: timeout}
}

// List implements Lister.
func (l *goGitLister) List(ctx context.Context, repoURL string, auth transport.AuthMethod) (map[string]string, error) {
	if l.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, l.timeout)
		defer cancel()
	}

	remote := gogit.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{repoURL},
	})

	// AppendPeeled keeps both "refs/tags/v1" (the tag object) and
	// "refs/tags/v1^{}" (the commit it points at), which is what candidate
	// selection needs to resolve an annotated tag to a pinnable commit.
	refs, err := remote.ListContext(ctx, &gogit.ListOptions{
		Auth:          auth,
		PeelingOption: gogit.AppendPeeled,
	})
	if err != nil {
		// The repository URL is deliberately left out of the error: it may
		// carry embedded credentials.
		return nil, fmt.Errorf("listing advertised refs: %w", err)
	}

	advertised := make(map[string]string, len(refs))
	for _, ref := range refs {
		// Symbolic references (typically HEAD) advertise no hash of their own.
		if ref.Type() != plumbing.HashReference {
			continue
		}
		advertised[ref.Name().String()] = ref.Hash().String()
	}

	return advertised, nil
}
