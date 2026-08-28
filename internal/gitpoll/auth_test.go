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
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"slices"
	"testing"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gossh "golang.org/x/crypto/ssh"

	"github.com/isometry/wavefront-controller/internal/gitpoll"
)

const (
	httpsRepoURL = "https://git.example.com/org/repo.git"
	sshRepoURL   = "ssh://git@git.example.com/org/repo.git"
	sshHost      = "git.example.com"

	// Secret data keys, as parsed by fluxcd/pkg/git.NewAuthOptions.
	keyUsername    = "username"
	keyPassword    = "password"
	keyBearerToken = "bearerToken"
	keyIdentity    = "identity"
	keyKnownHosts  = "known_hosts"
)

// sshFixture generates an ed25519 identity (PEM) and a known_hosts file
// pinning host to a second, independently generated host key.
func sshFixture(t *testing.T, host string) (identity, knownHosts []byte) {
	t.Helper()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey (identity): %v", err)
	}
	block, err := gossh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatalf("MarshalPrivateKey: %v", err)
	}

	hostPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey (host): %v", err)
	}
	hostKey, err := gossh.NewPublicKey(hostPub)
	if err != nil {
		t.Fatalf("NewPublicKey: %v", err)
	}

	return pem.EncodeToMemory(block), fmt.Appendf(nil, "%s %s", host, gossh.MarshalAuthorizedKey(hostKey))
}

func TestAuthFromSecretHTTPAnonymous(t *testing.T) {
	auth, err := gitpoll.AuthFromSecret("http://git.example.com/org/repo.git", nil)
	if err != nil {
		t.Fatalf("AuthFromSecret: %v", err)
	}
	if auth != nil {
		t.Errorf("auth = %#v, want nil for anonymous HTTP", auth)
	}
}

func TestAuthFromSecretBasic(t *testing.T) {
	auth, err := gitpoll.AuthFromSecret(httpsRepoURL, map[string][]byte{
		keyUsername: []byte("alice"),
		keyPassword: []byte("s3cret"),
	})
	if err != nil {
		t.Fatalf("AuthFromSecret: %v", err)
	}
	basic, ok := auth.(*githttp.BasicAuth)
	if !ok {
		t.Fatalf("auth = %T, want *http.BasicAuth", auth)
	}
	if basic.Username != "alice" || basic.Password != "s3cret" {
		t.Errorf("basic auth = %q/%q, want alice/s3cret", basic.Username, basic.Password)
	}
}

func TestAuthFromSecretBearerToken(t *testing.T) {
	auth, err := gitpoll.AuthFromSecret(httpsRepoURL, map[string][]byte{
		keyBearerToken: []byte("tok"),
	})
	if err != nil {
		t.Fatalf("AuthFromSecret: %v", err)
	}
	token, ok := auth.(*githttp.TokenAuth)
	if !ok {
		t.Fatalf("auth = %T, want *http.TokenAuth", auth)
	}
	if token.Token != "tok" {
		t.Errorf("token = %q, want tok", token.Token)
	}
}

func TestAuthFromSecretSSHKnownHosts(t *testing.T) {
	identity, knownHosts := sshFixture(t, sshHost)

	auth, err := gitpoll.AuthFromSecret(sshRepoURL, map[string][]byte{
		keyIdentity:   identity,
		keyKnownHosts: knownHosts,
	})
	if err != nil {
		t.Fatalf("AuthFromSecret: %v", err)
	}
	if auth == nil {
		t.Fatal("auth = nil, want an SSH public-key auth method")
	}
	if got := auth.Name(); got != "ssh-public-keys" {
		t.Errorf("auth.Name() = %q, want ssh-public-keys", got)
	}

	cc, ok := auth.(interface {
		ClientConfig() (*gossh.ClientConfig, error)
	})
	if !ok {
		t.Fatalf("auth = %T, does not expose ClientConfig", auth)
	}
	cfg, err := cc.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	if cfg.HostKeyCallback == nil {
		t.Error("HostKeyCallback = nil, want a callback derived from known_hosts")
	}
	if !slices.Contains(cfg.HostKeyAlgorithms, gossh.KeyAlgoED25519) {
		t.Errorf("HostKeyAlgorithms = %v, want it to contain %q", cfg.HostKeyAlgorithms, gossh.KeyAlgoED25519)
	}
	if cfg.User != "git" {
		t.Errorf("User = %q, want git", cfg.User)
	}
}

func TestAuthFromSecretSSHWithoutIdentity(t *testing.T) {
	if _, err := gitpoll.AuthFromSecret(sshRepoURL, nil); err == nil {
		t.Error("AuthFromSecret succeeded for SSH without an identity, want error")
	}
}

func TestAuthFromSecretInvalidURL(t *testing.T) {
	if _, err := gitpoll.AuthFromSecret("://not a url", nil); err == nil {
		t.Error("AuthFromSecret succeeded for an unparseable URL, want error")
	}
}
