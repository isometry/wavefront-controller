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

// Package gitpoll implements ref-advertisement polling: the stateless,
// checkout-free protocol read that is both the controller's change detection
// and the sole source of pin values. Credentials are parsed with the very
// function source-controller uses, making "credentials identical by
// construction" literal rather than conventional.
package gitpoll

import (
	"fmt"
	"net/url"

	"github.com/fluxcd/pkg/git"
	"github.com/fluxcd/pkg/ssh/knownhosts"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	gossh "golang.org/x/crypto/ssh"
)

// AuthFromSecret builds a go-git transport auth method from a GitRepository's
// secret, using the same parser as source-controller.
// data may be nil (anonymous HTTP).
func AuthFromSecret(repoURL string, data map[string][]byte) (transport.AuthMethod, error) {
	u, err := url.Parse(repoURL)
	if err != nil {
		return nil, fmt.Errorf("parsing repository URL: %w", err)
	}

	opts, err := git.NewAuthOptions(*u, data)
	if err != nil {
		return nil, fmt.Errorf("building auth options: %w", err)
	}

	return transportAuth(opts)
}

// transportAuth converts AuthOptions to a go-git transport.AuthMethod. It
// mirrors the gogit client's own unexported transportAuth — which is not
// importable — with one deliberate omission: there is no fallback to the
// machine's default SSH known_hosts. The controller authenticates only with
// what a GitRepository's secret carries, and touches no disk.
func transportAuth(opts *git.AuthOptions) (transport.AuthMethod, error) {
	if opts == nil {
		return nil, nil
	}

	switch opts.Transport {
	case git.HTTPS, git.HTTP:
		// Some providers (e.g. GitLab) reject empty credentials even for
		// public repositories, so prefer basic auth whenever either half is
		// present; anonymous access is signalled by a nil AuthMethod.
		switch {
		case opts.Username != "" || opts.Password != "":
			return &githttp.BasicAuth{Username: opts.Username, Password: opts.Password}, nil
		case opts.BearerToken != "":
			return &githttp.TokenAuth{Token: opts.BearerToken}, nil
		default:
			return nil, nil
		}

	case git.SSH:
		// AuthOptions.Validate has already guaranteed a non-empty identity and
		// known_hosts for the SSH transport.
		pk, err := gitssh.NewPublicKeys(opts.Username, opts.Identity, opts.Password)
		if err != nil {
			return nil, fmt.Errorf("parsing SSH identity: %w", err)
		}

		callback, hostKeyAlgos, err := knownhosts.New(opts.KnownHosts)
		if err != nil {
			return nil, fmt.Errorf("parsing known_hosts: %w", err)
		}

		return &sshPublicKeys{PublicKeys: pk, callback: callback, hostKeyAlgos: hostKeyAlgos}, nil

	case "":
		return nil, fmt.Errorf("no transport type set")

	default:
		return nil, fmt.Errorf("unknown transport %q", opts.Transport)
	}
}

// sshPublicKeys wraps go-git's PublicKeys to apply the host key material
// parsed from a secret's known_hosts, plus the SSH algorithm overrides
// fluxcd/pkg/git exposes. It is the in-controller equivalent of the gogit
// client's CustomPublicKeys, whose fields are unexported and therefore not
// reusable directly.
type sshPublicKeys struct {
	*gitssh.PublicKeys
	callback     gossh.HostKeyCallback
	hostKeyAlgos []string
}

// ClientConfig implements go-git's ssh.AuthMethod.
func (a *sshPublicKeys) ClientConfig() (*gossh.ClientConfig, error) {
	if a.callback != nil {
		a.HostKeyCallback = a.callback
	}

	config, err := a.PublicKeys.ClientConfig()
	if err != nil {
		return nil, err
	}

	if len(git.KexAlgos) > 0 {
		config.KeyExchanges = git.KexAlgos
	}

	// Whenever ssh-rsa is the only advertised host key algorithm, prioritise
	// the SHA-2 signing schemes over the SHA-1 one it implies.
	hostKeyAlgos := a.hostKeyAlgos
	if len(hostKeyAlgos) == 1 && hostKeyAlgos[0] == gossh.KeyAlgoRSA {
		hostKeyAlgos = append([]string{gossh.KeyAlgoRSASHA512, gossh.KeyAlgoRSASHA256}, hostKeyAlgos...)
	}

	config.HostKeyAlgorithms = hostKeyAlgos
	if len(git.HostKeyAlgos) > 0 {
		config.HostKeyAlgorithms = git.HostKeyAlgos
	}

	return config, nil
}
