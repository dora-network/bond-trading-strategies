ifneq (,$(wildcard .env))
	include .env
	export
endif

.DEFAULT_GOAL := help

# Image tag shared by build and the start-* targets. Bump on
# dependency changes; local dev defaults to :local, CI pushes :latest.
IMAGE ?= bond-strategy-server:local

STRATEGY_ADDR ?= :8081

MCP_ADDR ?= :8080
MCP_BASE_URL ?= http://localhost:8080
STRATEGY_BASE_URL ?= http://localhost:8081

WS_URL ?= wss://dev.dora.co
DORA_BASE_URL ?= https://dev.dora.co
PRICE_DAEMON_HTTP_ADDR ?= :8082
RECONNECT_DELAY ?= 5s

# CORS allow-list for the strategy-server. The Makefile passes this to
# --cors-allowed-origins, which the cors middleware uses as an exact-match
# list (no localhost/127.0.0.1 aliasing). The default keeps prod working
# for the dora-awsdev vercel deployment; local chatui testing overrides
# this via .env or the env (e.g. CORS_ALLOWED_ORIGINS=http://localhost:8080).
CORS_ALLOWED_ORIGINS ?= https://dora-awsdev.vercel.app

.PHONY: help build
help:
	@printf "Available targets:\n"
	@printf "  compose-up             Run docker compose services\n"
	@printf "  compose-down           Stop docker compose services\n"
	@printf "  build                  Build the strategy-server/mcp-server/price-daemon image\n"
	@printf "  start-strategy-server  Run the strategy HTTP server (in $(IMAGE))\n"
	@printf "  start-mcp-server       Run the MCP server (in $(IMAGE))\n"
	@printf "  start-price-daemon     Run the price/candle daemon (in $(IMAGE))\n"

# Build the image. Requires .secrets/github_token (per AGENTS.md) for
# private modules — see README or run `make .secrets/github_token` once.
# .PHONY: build above is duplicated intentionally so `make help` lists it
# alongside the start-* targets.
build:
	@test -f .secrets/github_token || (echo ".secrets/github_token missing — see AGENTS.md" && exit 1)
	docker build --secret id=github_token,src=.secrets/github_token -f Dockerfile -t $(IMAGE) .

compose-up:
	docker compose -f ./docker-compose.yml -p dora up --build -d

compose-down:
	docker compose -f ./docker-compose.yml -p dora down

# Common docker-run flags. --network host so containers can reach the
# tailscale DB host (DATABASE_URL points at a tailscale IP); without it
# Docker's default bridge network can't route to non-LAN ranges.
# Linux-only. --env-file .env feeds DATABASE_URL, DORA_API_KEY,
# ENCRYPTION_KEY, FRED_API_KEY, etc. without re-listing each as -e.
DOCKER_RUN_COMMON = --rm --network host --env-file .env

# start-* targets depend on build so `make start-foo` always runs
# against the latest compiled binary. Docker's layer cache makes
# incremental rebuilds cheap; the secret mount keeps private module
# fetch inside the cache.

.PHONY: start-strategy-server
start-strategy-server: build
	docker run $(DOCKER_RUN_COMMON) $(IMAGE) \
		/app/strategy-server \
		-a "$(STRATEGY_ADDR)" \
		--cors-allowed-origins "$(CORS_ALLOWED_ORIGINS)"

.PHONY: start-mcp-server
start-mcp-server: build
	docker run $(DOCKER_RUN_COMMON) $(IMAGE) \
		/app/mcp-server \
		-a "$(MCP_ADDR)" \
		-b "$(MCP_BASE_URL)" \
		-s "$(STRATEGY_BASE_URL)"

.PHONY: start-price-daemon
start-price-daemon: build
	docker run $(DOCKER_RUN_COMMON) $(IMAGE) \
		/app/price-daemon \
		-w "$(WS_URL)" \
		-b "$(DORA_BASE_URL)" \
		-a "$(ASSET_ID)" \
		-s "$(SINCE)" \
		-r "$(RECONNECT_DELAY)" \
		-A "$(PRICE_DAEMON_HTTP_ADDR)"
