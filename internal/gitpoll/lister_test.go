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

package gitpoll_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fluxcd/pkg/gittestserver"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/isometry/wavefront-controller/internal/gitpoll"
)

const (
	listTimeout = 30 * time.Second

	mainRef       = "refs/heads/main"
	tagRef        = "refs/tags/v1"
	peeledTagRef  = "refs/tags/v1^{}"
	testRepoPath  = "/test.git"
	testUsername  = "user"
	testPassword  = "pass"
	testTagName   = "v1"
	testTagAuthor = "wavefront-test"
)

// startGitServer boots an in-process HTTP git server, optionally requiring
// basic auth, and tears it down with the test.
func startGitServer(t *testing.T, username, password string) *gittestserver.GitServer {
	t.Helper()

	srv, err := gittestserver.NewTempGitServer()
	if err != nil {
		t.Fatalf("NewTempGitServer: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(srv.Root()) })

	srv.AutoCreate()
	if username != "" {
		srv.Auth(username, password)
	}
	if err := srv.StartHTTP(); err != nil {
		t.Fatalf("StartHTTP: %v", err)
	}
	t.Cleanup(srv.StopHTTP)

	return srv
}

// seedRepo authors a single-commit repository on branch main with one
// annotated tag and pushes it to repoURL. It returns the commit SHA and the
// annotated tag object SHA (which differs from the commit SHA).
func seedRepo(t *testing.T, repoURL string, auth transport.AuthMethod) (commitSHA, tagSHA string) {
	t.Helper()

	dir := t.TempDir()
	repo, err := gogit.PlainInitWithOptions(dir, &gogit.PlainInitOptions{
		DefaultBranch: plumbing.NewBranchReferenceName("main"),
	})
	if err != nil {
		t.Fatalf("PlainInitWithOptions: %v", err)
	}

	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("wavefront\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := wt.Add("README.md"); err != nil {
		t.Fatalf("Add: %v", err)
	}

	sig := &object.Signature{Name: testTagAuthor, Email: "test@example.com", When: time.Now()}
	commit, err := wt.Commit("initial commit", &gogit.CommitOptions{Author: sig, Committer: sig})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tag, err := repo.CreateTag(testTagName, commit, &gogit.CreateTagOptions{
		Tagger:  sig,
		Message: "release " + testTagName,
	})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}

	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{repoURL}}); err != nil {
		t.Fatalf("CreateRemote: %v", err)
	}
	if err := repo.Push(&gogit.PushOptions{
		RemoteName: "origin",
		RefSpecs: []config.RefSpec{
			"refs/heads/*:refs/heads/*",
			"refs/tags/*:refs/tags/*",
		},
		Auth: auth,
	}); err != nil {
		t.Fatalf("Push: %v", err)
	}

	return commit.String(), tag.Hash().String()
}

func TestGoGitListerAnonymous(t *testing.T) {
	srv := startGitServer(t, "", "")
	repoURL := srv.HTTPAddress() + testRepoPath
	commitSHA, tagSHA := seedRepo(t, repoURL, nil)

	if tagSHA == commitSHA {
		t.Fatalf("annotated tag object SHA %q must differ from commit SHA", tagSHA)
	}

	ctx, cancel := context.WithTimeout(t.Context(), listTimeout)
	defer cancel()

	refs, err := gitpoll.NewGoGitLister(listTimeout).List(ctx, repoURL, nil)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	for ref, want := range map[string]string{
		mainRef:      commitSHA,
		tagRef:       tagSHA,
		peeledTagRef: commitSHA,
	} {
		if got := refs[ref]; got != want {
			t.Errorf("refs[%q] = %q, want %q (full listing: %v)", ref, got, want, refs)
		}
	}
}

func TestGoGitListerBasicAuth(t *testing.T) {
	srv := startGitServer(t, testUsername, testPassword)
	repoURL := srv.HTTPAddress() + testRepoPath
	commitSHA, _ := seedRepo(t, repoURL, &githttp.BasicAuth{Username: testUsername, Password: testPassword})

	lister := gitpoll.NewGoGitLister(listTimeout)

	auth, err := gitpoll.AuthFromSecret(repoURL, map[string][]byte{
		keyUsername: []byte(testUsername),
		keyPassword: []byte(testPassword),
	})
	if err != nil {
		t.Fatalf("AuthFromSecret: %v", err)
	}
	if auth == nil {
		t.Fatal("AuthFromSecret returned nil auth for username/password secret")
	}

	ctx, cancel := context.WithTimeout(t.Context(), listTimeout)
	defer cancel()

	refs, err := lister.List(ctx, repoURL, auth)
	if err != nil {
		t.Fatalf("List with credentials: %v", err)
	}
	if got := refs[mainRef]; got != commitSHA {
		t.Errorf("refs[%q] = %q, want %q", mainRef, got, commitSHA)
	}

	if _, err := lister.List(ctx, repoURL, nil); err == nil {
		t.Error("List without credentials succeeded against an authenticated server, want error")
	}
}
