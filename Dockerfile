# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the bond-trading-strategies image. Stage 1
# produces three static Go binaries (mcp-server, strategy-server,
# price-daemon) plus the tern migrator. Stage 2 ships them on the
# official tinygo/tinygo image, which bundles both `tinygo` and a `go`
# toolchain — both are required by the agent's WASM pipeline
# (internal/agent/tools/generate/validate/wasm.go) at request time.
# Without the toolchain, validate-on-deploy fails and the artifact
# store never gets populated, so live deployments can't load their
# .wasm modules.
#
# tern + migrations/ are kept in the image even though the service
# containers don't run them — deploy.sh's `migrate` task uses this
# same image to run `/app/tern migrate` against the live DB before
# any service task starts. The strategy-server / mcp-server /
# price-daemon containers themselves never invoke tern.
#
# Builder stage pinned to golang:1.26.5-bookworm. go.mod declares
# `go 1.26.5`; the previous `golang:1.26-alpine` resolved to
# whatever the latest 1.26.x patch on Docker Hub was, which could
# fall below the go.mod minimum and fail `go mod download` with
# "go.mod requires go >= X (running go X.Y)".
#
# Runtime stage pinned to tinygo/tinygo:0.42.0 (Debian bookworm).
# The tinygoDockerfile constant in
# internal/agent/tools/generate/validate/wasm.go is bumped in the
# same PR to match — single source of truth per the original
# dora-agent Dockerfile comment. 0.42.0 ships a recent-enough Go to
# compile the repo's go 1.26 modules and the v0.3.3 wasm framework
# the agent pins via WasmFrameworkVersion.

# ---- build stage ----
FROM golang:1.26.5-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=secret,id=github_token \
    TOKEN="$(cat /run/secrets/github_token)" && \
    test -n "$TOKEN" || (echo "github_token build secret is required" && exit 1) && \
    git config --global url."https://${TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/" && \
    go mod download && \
    rm -f /root/.gitconfig

RUN CGO_ENABLED=0 go install github.com/jackc/tern/v2@v2.4.1
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /mcp-server ./cmd/mcp-server
RUN CGO_ENABLED=0 go build -trimpath -o /strategy-server ./cmd/strategy-server
RUN CGO_ENABLED=0 go build -trimpath -o /price-daemon ./cmd/price-daemon

# ---- runtime stage ----
FROM tinygo/tinygo:0.42.0

# tinygo/tinygo:0.42.0 inherits USER tinygo (uid 1000). groupadd/useradd
# require root. Switch before mutating /etc.
USER root:root

# Debian's groupadd/useradd: -r = system account, -m = create home.
# Match dora-agent's pattern: uid/gid 65532, system user named `dora`.
#
# GOPATH=/var/lib/dora-agent/go (set in ENV below) requires the parent
# directory + the standard go subdirs (pkg/mod, pkg/mod/cache,
# pkg/sumdb, plus build/) to exist and be writable by `dora`. Without
# them, the WASM pipeline fails at request time with "could not
# create module cache: mkdir /var/lib/dora-agent/go/pkg: permission
# denied" when `go get` runs (validate/wasm.go:106). Pre-creating the
# subdirs here avoids the mkdir-at-runtime permission trap and keeps
# module downloads in a stable layer that survives across runs.
RUN groupadd -r -g 65532 dora \
 && useradd -r -g dora -u 65532 -m -d /var/lib/dora-agent dora \
 && install -d -o dora -g dora -m 0750 /var/lib/dora-agent/wasm-artifacts \
 && install -d -o dora -g dora -m 0750 /var/lib/dora-agent/go \
 && install -d -o dora -g dora -m 0750 /var/lib/dora-agent/go/build \
 && install -d -o dora -g dora -m 0750 /var/lib/dora-agent/go/pkg \
 && install -d -o dora -g dora -m 0750 /var/lib/dora-agent/go/pkg/mod \
 && install -d -o dora -g dora -m 0750 /var/lib/dora-agent/go/pkg/mod/cache \
 && install -d -o dora -g dora -m 0750 /var/lib/dora-agent/go/pkg/sumdb

# Smoke probe: catches the "mkdir permission denied" class of bug at
# BUILD time so it never reaches the request-time wasm pipeline. Runs
# as the `dora` user and performs the exact operations `go get` /
# `tinygo build` do at validate time (mkdir pkg/mod/..., touch
# GOCACHE, mkdir pkg/sumdb/...). A non-zero exit here fails the docker
# build instead of leaking into prod. Round-trip with cleanup so the
# final image is pristine.
USER dora:dora
RUN sh -c 'mkdir -p /var/lib/dora-agent/go/pkg/mod/cached_probe \
         && touch /var/lib/dora-agent/go/build/probe \
         && mkdir -p /var/lib/dora-agent/go/pkg/sumdb/probe \
         && rm -rf /var/lib/dora-agent/go/pkg/mod/cached_probe \
                    /var/lib/dora-agent/go/build/probe \
                    /var/lib/dora-agent/go/pkg/sumdb/probe \
         && echo "GOPATH setup: OK"'
USER root:root

WORKDIR /app

# Static binaries + tern migrator + source migrations. deploy.sh
# runs the migrate task against this same image before service
# tasks start; the service containers themselves do not invoke tern.
COPY --from=builder /mcp-server      /app/mcp-server
COPY --from=builder /strategy-server /app/strategy-server
COPY --from=builder /price-daemon   /app/price-daemon
COPY --from=builder /go/bin/tern    /app/tern
COPY migrations                       /app/migrations

# Per-provider model caps (used by the strategy turn driver at startup).
# Read-only supporting data; operators can rebind AGENT_MODEL_CAPS_PATH
# at runtime to point at a different file.
COPY configs /var/lib/dora-agent/configs

# Smart entrypoint: `docker run <image>` with no args runs the
# strategy-server (production default). With args, exec whatever was
# passed — e.g. `bash` for an operator shell, `tinygo version`,
# `tern migrate ...`.
RUN printf '%s\n' \
  '#!/bin/sh' \
  'set -eu' \
  'if [ "$#" -eq 0 ]; then' \
  '  exec /app/strategy-server --addr :8081' \
  'fi' \
  'exec "$@"' \
  > /usr/local/bin/entrypoint.sh \
 && chmod 0755 /usr/local/bin/entrypoint.sh

# Run as the non-root `dora` user from here on. Per-service
# entrypoints (deploy.sh) override this for price-daemon /
# strategy-server / migrate; the defaults above (CMD) use it too.
USER dora:dora

# AGENT_WASM_ARTIFACT_ROOT points at the local on-disk CAS. The
# pgstore delegate (internal/agent/wasmruntime/store/pgstore) reads
# durable bytes from agent.wasm_artifacts on a local-CAS miss and
# rehydrates into this dir, so Fargate restarts recover cleanly.
#
# HOME=/var/lib/dora-agent sets the dora user's home explicitly.
# tinygo/tinygo:0.42.0 inherits HOME=/home/tinygo for the tinygo
# user; we never reset HOME on the USER dora switch, so Go tooling
# (os.UserHomeDir, net/http DefaultTransport cookie jars, etc.) sees
# an empty HOME and falls back to /, breaking credential/caching paths
# silently. The dora account's home is /var/lib/dora-agent (set by
# useradd -d above), so point HOME there.
ENV AGENT_WASM_ARTIFACT_ROOT=/var/lib/dora-agent/wasm-artifacts \
    AGENT_MODEL_CAPS_PATH=/var/lib/dora-agent/configs/model_caps.json \
    GOPATH=/var/lib/dora-agent/go \
    GOCACHE=/var/lib/dora-agent/go/build \
    HOME=/var/lib/dora-agent \
    PATH=/var/lib/dora-agent/go/bin:$PATH

EXPOSE 8080
EXPOSE 8081

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD []
