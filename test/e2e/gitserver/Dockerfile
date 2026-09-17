# The gitserver binary is cross-compiled on the host by `make gitserver-build`
# (CGO_ENABLED=0, GOOS=linux) so that this image needs no Go toolchain and no
# module download: the program lives in the main module and therefore shares
# its already-resolved dependency graph.
FROM alpine:3.22

# gitkit — the git backend inside fluxcd/pkg/gittestserver — shells out to git.
RUN apk add --no-cache git ca-certificates

# Auto-created bare repositories must point HEAD at the branch the fixtures
# actually push. Without this they default to master, no such ref ever exists,
# and source-controller's commit-pinned clone (which clones the default branch
# before resolving the pinned SHA) fails with "reference not found".
RUN git config --system init.defaultBranch main

COPY bin/gitserver /usr/local/bin/gitserver

ENV GIT_ROOT=/srv/git LISTEN_ADDR=:8080
RUN mkdir -p /srv/git

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/gitserver"]
