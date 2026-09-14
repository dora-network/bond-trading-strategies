ifneq (,$(wildcard .env))
	include .env
	export
endif

.DEFAULT_GOAL := help

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

.PHONY: help
help:
	@printf "Available targets:\n"
	@printf "  compose-up             Run docker compose services\n"
	@printf "  compose-down           Stop docker compose services\n"
	@printf "  start-strategy-server  Run the strategy HTTP server\n"
	@printf "  start-mcp-server       Run the MCP server\n"
	@printf "  start-price-daemon     Run the price/candle daemon\n"

compose-up:
	docker compose -f ./docker-compose.yml -p dora up --build -d

compose-down:
	docker compose -f ./docker-compose.yml -p dora down

start-strategy-server:
	go run ./cmd/strategy-server -a "$(STRATEGY_ADDR)" -d "$(DATABASE_URL)" -s "$(WS_URL)" -k "$(DORA_API_KEY)" -b "$(DORA_BASE_URL)" -f "$(FRED_API_KEY)" -e "$(ENCRYPTION_KEY)" --cors-allowed-origins "$(CORS_ALLOWED_ORIGINS)"

start-mcp-server:
	go run ./cmd/mcp-server -a "$(MCP_ADDR)" -b "$(MCP_BASE_URL)" -s "$(STRATEGY_BASE_URL)" -f "$(FRED_API_KEY)" -k "$(DORA_API_KEY)"

start-price-daemon:
	go run ./cmd/price-daemon -w "$(WS_URL)" -d "$(DATABASE_URL)" -k "$(DORA_API_KEY)" -b "$(DORA_BASE_URL)" -a "$(ASSET_ID)" -s "$(SINCE)" -r "$(RECONNECT_DELAY)" -A "$(PRICE_DAEMON_HTTP_ADDR)"

.PHONY: build
build:
	docker build -f Dockerfile -t github.com/dora-network/bond-strategy-server-mcp:latest .
