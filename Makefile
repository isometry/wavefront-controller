# VERSION is derived from the nearest v* tag (git describe, v stripped); the
# release workflow overrides it from the pushed tag.
VERSION ?= $(shell git describe --tags --match 'v*' --dirty 2>/dev/null | sed 's/^v//')
ifeq ($(VERSION),)
VERSION := 0.0.0-dev
endif
IMAGE_TAG_BASE ?= ghcr.io/isometry/wavefront-controller
IMG ?= $(IMAGE_TAG_BASE):$(VERSION)
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases
	./hack/sync-chart.sh

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

FLUX_VERSION ?= v2.9.4
FLUX_INSTALL ?= test/e2e/flux-install.yaml
.PHONY: update-flux-install
update-flux-install: ## Refresh the vendored Flux install manifest used by the e2e suite.
	curl -fsSL -o $(FLUX_INSTALL) \
	  https://github.com/fluxcd/flux2/releases/download/$(FLUX_VERSION)/install.yaml

FLUX_SC_VERSION ?= v1.9.4
FLUX_KC_VERSION ?= v1.9.4
.PHONY: update-flux-crds
update-flux-crds: ## Refresh vendored Flux CRDs used by envtest.
	mkdir -p test/crds/flux
	curl -fsSL -o test/crds/flux/source.toolkit.fluxcd.io_gitrepositories.yaml \
	  https://raw.githubusercontent.com/fluxcd/source-controller/api/$(FLUX_SC_VERSION)/config/crd/bases/source.toolkit.fluxcd.io_gitrepositories.yaml
	curl -fsSL -o test/crds/flux/kustomize.toolkit.fluxcd.io_kustomizations.yaml \
	  https://raw.githubusercontent.com/fluxcd/kustomize-controller/api/$(FLUX_KC_VERSION)/config/crd/bases/kustomize.toolkit.fluxcd.io_kustomizations.yaml

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
# CertManager has nothing to do in this project (no webhooks), so the e2e run
# skips it; drop CERT_MANAGER_INSTALL_SKIP below to reinstate the scaffold's
# default.
KIND_CLUSTER ?= wavefront-controller-test-e2e
E2E_IMG ?= example.com/wavefront-controller:v0.0.1
GITSERVER_IMG ?= example.com/wavefront-gitserver:v0.0.1
E2E_TIMEOUT ?= 90m
# E2E_INSTALL selects how the suite installs the controller: `kustomize` (the
# config/ overlays, i.e. `make deploy`) or `helm` (the chart under
# $(CHART_DIR)). Both install paths are expected to pass the same specs.
E2E_INSTALL ?= kustomize

# Every e2e step addresses the kind cluster through this file and nothing else,
# so a context switch in ~/.kube/config — or another kind cluster being created
# or deleted on the same machine, which rewrites the current context — cannot
# redirect the run at a cluster it must never touch. kind honours $KUBECONFIG
# for create, export and delete, so ~/.kube/config is never read or written.
E2E_KUBECONFIG ?= $(LOCALBIN)/e2e.kubeconfig
E2E_ENV = KUBECONFIG=$(E2E_KUBECONFIG)

# The git server binary is cross-compiled on the host so its image needs no Go
# toolchain and no module download; it must therefore target the daemon's
# architecture, not the host's.
GITSERVER_ARCH ?= $(shell $(CONTAINER_TOOL) version --format '{{.Server.Arch}}' 2>/dev/null || go env GOARCH)

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@mkdir -p "$(dir $(E2E_KUBECONFIG))"
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation."; \
			$(E2E_ENV) $(KIND) export kubeconfig --name $(KIND_CLUSTER) ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(E2E_ENV) $(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac

# Checked before the cluster is built, so a typo in E2E_INSTALL — or a missing
# helm — costs a second rather than the whole setup.
.PHONY: e2e-install-guard
e2e-install-guard: ## Validate E2E_INSTALL and the tooling the chosen installer needs.
	@case "$(E2E_INSTALL)" in \
		kustomize) ;; \
		helm) command -v $(HELM) >/dev/null 2>&1 || { \
			echo "E2E_INSTALL=helm needs '$(HELM)' on PATH; install Helm or set HELM=<path>"; \
			exit 1; \
		} ;; \
		*) echo "E2E_INSTALL must be 'kustomize' or 'helm' (got '$(E2E_INSTALL)')"; exit 1 ;; \
	esac

.PHONY: e2e-guard
e2e-guard: ## Refuse to run e2e against anything but the kind cluster on loopback.
	@ctx="$$($(E2E_ENV) $(KUBECTL) config current-context 2>/dev/null)"; \
	server="$$($(E2E_ENV) $(KUBECTL) config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null)"; \
	case "$$ctx|$$server" in \
		"kind-$(KIND_CLUSTER)|https://127.0.0.1:"*) ;; \
		*) echo "refusing to run e2e against context '$$ctx' at '$$server'" \
		        "(expected kind-$(KIND_CLUSTER) on 127.0.0.1 via $(E2E_KUBECONFIG))"; \
		   exit 1 ;; \
	esac

.PHONY: gitserver-build
gitserver-build: ## Build the e2e git server image (fluxcd/pkg/gittestserver on alpine).
	CGO_ENABLED=0 GOOS=linux GOARCH=$(GITSERVER_ARCH) \
	  go build -o test/e2e/gitserver/bin/gitserver ./test/e2e/gitserver
	$(CONTAINER_TOOL) build -t $(GITSERVER_IMG) test/e2e/gitserver

.PHONY: e2e-images
e2e-images: gitserver-build ## Build the manager and git server images and load them into Kind.
	$(MAKE) ko-build-local IMG=$(E2E_IMG)
	$(KIND) load docker-image $(E2E_IMG) --name $(KIND_CLUSTER)
	$(KIND) load docker-image $(GITSERVER_IMG) --name $(KIND_CLUSTER)

.PHONY: e2e-flux
e2e-flux: e2e-guard ## Install the vendored Flux release into the e2e cluster.
	$(E2E_ENV) $(KUBECTL) apply --server-side --force-conflicts -f $(FLUX_INSTALL)
	$(E2E_ENV) $(KUBECTL) -n flux-system wait --for=condition=Available --timeout=5m \
	  deployment/source-controller deployment/kustomize-controller

.PHONY: e2e-gitserver
e2e-gitserver: e2e-guard ## Deploy the e2e git server on an empty repository store.
	# On a reused cluster, fixtures pinned to commits the restarted (and hence
	# empty) git server no longer serves would poison the run.
	-$(E2E_ENV) $(KUBECTL) delete -f test/e2e/fixtures.yaml --ignore-not-found --timeout=3m
	$(E2E_ENV) $(KUBECTL) apply -f test/e2e/gitserver/manifests.yaml
	$(E2E_ENV) $(KUBECTL) -n wavefront-e2e rollout restart deployment/gitserver
	$(E2E_ENV) $(KUBECTL) -n wavefront-e2e rollout status deployment/gitserver --timeout=3m

.PHONY: test-e2e
test-e2e: e2e-install-guard setup-test-e2e e2e-guard manifests generate fmt vet kustomize build-wfctl e2e-images e2e-flux e2e-gitserver ## Run the e2e tests. Expected an isolated environment using Kind.
	@status=0; \
	CERT_MANAGER_INSTALL_SKIP=true $(E2E_ENV) KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) E2E_IMG=$(E2E_IMG) \
	  E2E_INSTALL=$(E2E_INSTALL) HELM=$(HELM) \
	  go test -tags=e2e ./test/e2e/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) || status=$$?; \
	( cd config/manager && "$(KUSTOMIZE)" edit set image controller=controller:latest ); \
	exit $$status
	$(MAKE) cleanup-test-e2e

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(E2E_ENV) $(KIND) delete cluster --name $(KIND_CLUSTER)
	@rm -f "$(E2E_KUBECONFIG)"

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet build-wfctl ## Build manager and wfctl binaries.
	go build -o bin/manager cmd/main.go

.PHONY: build-wfctl
build-wfctl: ## Build wfctl binary.
	go build -ldflags "-X github.com/isometry/wavefront-controller/internal/wfctl/cli.Version=v$(VERSION)" -o bin/wfctl ./cmd/wfctl

# wfctl is deliberately absent from the manager image (see README, "wfctl"):
# the manager's ServiceAccount is exactly the RBAC an exec into that pod
# should not reach.
.PHONY: install-wfctl
install-wfctl: build-wfctl ## Install wfctl into GOBIN, with the kubectl-wavefront plugin symlink.
	mkdir -p "$(GOBIN)"
	install -m 0755 bin/wfctl "$(GOBIN)/wfctl"
	ln -sf wfctl "$(GOBIN)/kubectl-wavefront"

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# $(IMG) is repo:tag; ko wants the repo and the tag split apart. This breaks
# on a registry:port host (e.g. localhost:5000/x:tag) because the port's
# colon is indistinguishable from the tag separator — acceptable here since
# IMG is always a plain registry host.
.PHONY: ko-build
ko-build: ko ## Build and push the multi-arch manager image with ko (IMG=repo:tag).
	KO_DOCKER_REPO=$(firstword $(subst :, ,$(IMG))) "$(KO)" build --bare --platform=linux/amd64,linux/arm64 \
	  --tags=$(lastword $(subst :, ,$(IMG))) \
	  --image-label org.opencontainers.image.source=https://github.com/isometry/wavefront-controller ./cmd

.PHONY: ko-build-local
ko-build-local: ko ## Build the manager image for the local docker daemon with ko (IMG=repo:tag).
	KO_DOCKER_REPO=$(firstword $(subst :, ,$(IMG))) "$(KO)" build --local --bare --platform=linux/$(GITSERVER_ARCH) \
	  --tags=$(lastword $(subst :, ,$(IMG))) ./cmd

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Helm

HELM ?= helm
CHART_DIR ?= deploy/charts/wavefront-controller

.PHONY: helm-lint
helm-lint: manifests ## Lint the Helm chart (regenerates chart CRDs/RBAC first).
	$(HELM) lint $(CHART_DIR)

.PHONY: helm-template
helm-template: manifests ## Render the Helm chart to stdout (regenerates chart CRDs/RBAC first).
	$(HELM) template wavefront-controller $(CHART_DIR)

.PHONY: helm-package
helm-package: manifests ## Package the chart into dist/ with version/appVersion = $(VERSION).
	mkdir -p dist
	$(HELM) package $(CHART_DIR) --destination dist --version "$(VERSION)" --app-version "$(VERSION)"

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint
KO ?= $(LOCALBIN)/ko

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.21.0
KO_VERSION ?= v0.19.1

#ENVTEST_VERSION is the controller-runtime version to use for setup-envtest, derived from go.mod
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v")

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.13.2
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: ko
ko: $(KO) ## Download ko locally if necessary.
$(KO): $(LOCALBIN)
	$(call go-install-tool,$(KO),github.com/google/ko,$(KO_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
