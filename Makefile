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
	go run ./cmd/strategy-server -addr "$(STRATEGY_ADDR)" -db-url "$(DATABASE_URL)" -dora-base-url "$(DORA_BASE_URL)" -fred-api-key "$(FRED_API_KEY)"

start-mcp-server:
	go run ./cmd/mcp-server -a "$(MCP_ADDR)" -b "$(MCP_BASE_URL)" -s "$(STRATEGY_BASE_URL)" -f "$(FRED_API_KEY)" -k "$(DORA_API_KEY)"

start-price-daemon:
	go run ./cmd/price-daemon -ws-url "$(WS_URL)" -db-url "$(DATABASE_URL)" -api-key "$(DORA_API_KEY)" -asset-id "$(ASSET_ID)" -order-books "$(ORDER_BOOK_IDS)" -since "$(SINCE)" -reconnect-delay "$(RECONNECT_DELAY)" -http-addr "$(PRICE_DAEMON_HTTP_ADDR)"

.PHONY: build
build:
	docker build -f Dockerfile -t github.com/dora-network/bond-strategy-server-mcp:latest .
