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

package utils

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

const (
	// GitServerNamespace is where the e2e git server (and the fixture fleet)
	// lives.
	GitServerNamespace = "wavefront-e2e"
	// GitServerService is the Service in front of the git server; its
	// in-cluster address is what the fixture GitRepositories point at.
	GitServerService = "gitserver"
	// GitServerPort is the port the git server listens on.
	GitServerPort = 8080
	// GitServerLocalPort is the host-side end of the port-forward the suite
	// pushes through.
	GitServerLocalPort = 18080

	// GitBranch is the branch every fixture repository tracks.
	GitBranch = "main"

	pushAttempts = 4
	pushBackoff  = 2 * time.Second
)

// GitServerLocalURL is the push URL of one repository on the git server, as
// reached from the host through the port-forward. The in-cluster URL that goes
// into a GitRepository spec addresses the very same repositories.
func GitServerLocalURL(repo string) string {
	return fmt.Sprintf("http://127.0.0.1:%d/%s.git", GitServerLocalPort, repo)
}

// GitServerClusterURL is the in-cluster URL of one repository.
func GitServerClusterURL(repo string) string {
	return fmt.Sprintf("http://%s.%s.svc:%d/%s.git",
		GitServerService, GitServerNamespace, GitServerPort, repo)
}

// PortForward supervises a `kubectl port-forward` for the lifetime of the
// suite. kubectl gives up on its side of a dropped connection, so the forward
// is restarted rather than trusted to survive a whole e2e run unattended.
type PortForward struct {
	namespace string
	service   string
	local     int
	remote    int

	cancel context.CancelFunc
	done   chan struct{}
}

// StartPortForward launches the supervised forward. Call WaitReady before
// using it.
func StartPortForward(namespace, service string, local, remote int) *PortForward {
	ctx, cancel := context.WithCancel(context.Background())
	f := &PortForward{
		namespace: namespace,
		service:   service,
		local:     local,
		remote:    remote,
		cancel:    cancel,
		done:      make(chan struct{}),
	}

	go func() {
		defer close(f.done)
		for ctx.Err() == nil {
			cmd := exec.CommandContext(ctx, "kubectl", "port-forward",
				"-n", f.namespace, "svc/"+f.service,
				fmt.Sprintf("%d:%d", f.local, f.remote))
			// Deliberately discarded: this outlives individual specs, and
			// GinkgoWriter is not safe to write to from a background
			// goroutine once a spec has ended.
			cmd.Stdout, cmd.Stderr = nil, nil
			_ = cmd.Run()

			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}()

	return f
}

// WaitReady blocks until the forward carries an HTTP round trip end to end.
// Any status code counts: reaching the git server at all is the signal.
func (f *PortForward) WaitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if last = f.probe(); last == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("port-forward to %s/%s not ready: %w", f.namespace, f.service, last)
}

func (f *PortForward) probe() error {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/", f.local))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

// Stop tears the forward down and waits for the supervisor to exit.
func (f *PortForward) Stop() {
	f.cancel()
	<-f.done
}

// Repo is a local working copy wired to one repository on the e2e git server.
// The server auto-creates repositories on first push, so seeding a fixture
// repository is just a commit away.
type Repo struct {
	Name string

	dir  string
	url  string
	repo *gogit.Repository
	head string
}

// Head is the SHA of the most recent commit this working copy pushed.
func (r *Repo) Head() string { return r.head }

// NewRepo initialises an empty working copy for repo name under baseDir.
func NewRepo(baseDir, name string) (*Repo, error) {
	r := &Repo{Name: name, dir: filepath.Join(baseDir, name), url: GitServerLocalURL(name)}
	if err := r.init(); err != nil {
		return nil, err
	}
	return r, nil
}

// init creates (or recreates) the working copy with an empty history.
func (r *Repo) init() error {
	if err := os.RemoveAll(r.dir); err != nil {
		return fmt.Errorf("clearing %s: %w", r.dir, err)
	}
	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", r.dir, err)
	}

	repo, err := gogit.PlainInitWithOptions(r.dir, &gogit.PlainInitOptions{
		DefaultBranch: plumbing.NewBranchReferenceName(GitBranch),
	})
	if err != nil {
		return fmt.Errorf("initialising %s: %w", r.dir, err)
	}
	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{r.url}}); err != nil {
		return fmt.Errorf("adding remote to %s: %w", r.dir, err)
	}

	r.repo = repo
	return nil
}

// Commit writes files (path relative to the repository root) and commits them.
func (r *Repo) Commit(files map[string]string, message string) (string, error) {
	worktree, err := r.repo.Worktree()
	if err != nil {
		return "", fmt.Errorf("opening worktree of %s: %w", r.Name, err)
	}

	for path, content := range files {
		full := filepath.Join(r.dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return "", fmt.Errorf("creating %s: %w", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			return "", fmt.Errorf("writing %s: %w", full, err)
		}
		if _, err := worktree.Add(path); err != nil {
			return "", fmt.Errorf("staging %s: %w", path, err)
		}
	}

	hash, err := worktree.Commit(message, &gogit.CommitOptions{
		Author: &object.Signature{Name: "wavefront e2e", Email: "e2e@example.com", When: time.Now()},
	})
	if err != nil {
		return "", fmt.Errorf("committing to %s: %w", r.Name, err)
	}
	r.head = hash.String()
	return r.head, nil
}

// Push publishes the local branch, retrying through port-forward hiccups.
func (r *Repo) Push(force bool) error {
	refspec := config.RefSpec(fmt.Sprintf("refs/heads/%s:refs/heads/%s", GitBranch, GitBranch))
	if force {
		refspec = "+" + refspec
	}

	var last error
	for attempt := range pushAttempts {
		if attempt > 0 {
			time.Sleep(pushBackoff)
		}
		last = r.repo.Push(&gogit.PushOptions{
			RemoteName: "origin",
			RefSpecs:   []config.RefSpec{refspec},
			Force:      force,
		})
		switch {
		case last == nil, errors.Is(last, gogit.NoErrAlreadyUpToDate):
			return nil
		}
	}
	return fmt.Errorf("pushing %s: %w", r.Name, last)
}

// CommitPush is the workhorse: commit files, push, return the new SHA.
func (r *Repo) CommitPush(files map[string]string, message string) (string, error) {
	sha, err := r.Commit(files, message)
	if err != nil {
		return "", err
	}
	if err := r.Push(false); err != nil {
		return "", err
	}
	return sha, nil
}

// PushFile is CommitPush for a single file (the brief's pushCommit).
func (r *Repo) PushFile(path, content, message string) (string, error) {
	return r.CommitPush(map[string]string{path: content}, message)
}

// Rewrite discards the local history, commits files onto a fresh root commit
// and force-pushes it — a real history rewrite over whatever is pinned
// (DESIGN §10).
func (r *Repo) Rewrite(files map[string]string, message string) (string, error) {
	if err := r.init(); err != nil {
		return "", err
	}
	sha, err := r.Commit(files, message)
	if err != nil {
		return "", err
	}
	if err := r.Push(true); err != nil {
		return "", err
	}
	return sha, nil
}
