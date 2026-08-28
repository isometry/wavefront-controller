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

// Command gitserver publishes an in-cluster HTTP git server for the e2e
// suite, backed by the very library the unit and envtest suites already use
// (fluxcd/pkg/gittestserver, which shells out to git via gitkit). Repositories
// are created on first push, so the suite seeds its fleet simply by pushing.
//
// gittestserver builds on net/http/httptest, whose listener is always an
// ephemeral loopback port; a reverse proxy in front of it is what turns that
// into the fixed, cluster-reachable address a Service can select.
package main

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"

	"github.com/fluxcd/pkg/gittestserver"
)

func main() {
	root := envOr("GIT_ROOT", "/srv/git")
	addr := envOr("LISTEN_ADDR", ":8080")

	if err := os.MkdirAll(root, 0o755); err != nil {
		log.Fatalf("creating git root %q: %v", root, err)
	}

	srv := gittestserver.NewGitServer(root).AutoCreate()
	if err := srv.StartHTTP(); err != nil {
		log.Fatalf("starting git server: %v", err)
	}
	defer srv.StopHTTP()

	backend, err := url.Parse(srv.HTTPAddress())
	if err != nil {
		log.Fatalf("parsing backend address %q: %v", srv.HTTPAddress(), err)
	}
	proxy := httputil.NewSingleHostReverseProxy(backend)
	// Git's smart-HTTP RPCs stream their packfiles; flush every write rather
	// than let the proxy's copy buffer stall a fetch.
	proxy.FlushInterval = -1

	log.Printf("serving git repositories from %s on %s (backend %s)", root, addr, backend)
	server := &http.Server{Addr: addr, Handler: proxy, ReadHeaderTimeout: 30 * time.Second}
	log.Fatal(server.ListenAndServe())
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
