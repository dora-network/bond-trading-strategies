# Graph Report - .  (2026-06-01)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 1227 nodes · 1952 edges · 90 communities (55 shown, 35 thin omitted)
- Extraction: 86% EXTRACTED · 14% INFERRED · 0% AMBIGUOUS · INFERRED: 266 edges (avg confidence: 0.8)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `d3bc06b1`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- [[_COMMUNITY_DORA API Client & Market API|DORA API Client & Market API]]
- [[_COMMUNITY_Strategy Server Main|Strategy Server Main]]
- [[_COMMUNITY_Mean Reversion Fakes|Mean Reversion Fakes]]
- [[_COMMUNITY_Mean Reversion Config|Mean Reversion Config]]
- [[_COMMUNITY_MCP Strategy Client|MCP Strategy Client]]
- [[_COMMUNITY_Strategy Fakes|Strategy Fakes]]
- [[_COMMUNITY_Strategy Service|Strategy Service]]
- [[_COMMUNITY_HTTP Handler & Definitions|HTTP Handler & Definitions]]
- [[_COMMUNITY_OpenAPI Path Definitions|OpenAPI Path Definitions]]
- [[_COMMUNITY_OpenAPI Info & Components|OpenAPI Info & Components]]
- [[_COMMUNITY_Rate Limiter Tests|Rate Limiter Tests]]
- [[_COMMUNITY_FRED Tenor & Yields|FRED Tenor & Yields]]
- [[_COMMUNITY_Project Architecture|Project Architecture]]
- [[_COMMUNITY_OpenAPI Responses|OpenAPI Responses]]
- [[_COMMUNITY_Project Documentation|Project Documentation]]
- [[_COMMUNITY_Price Daemon Main|Price Daemon Main]]
- [[_COMMUNITY_OpenAPI Path Operations|OpenAPI Path Operations]]
- [[_COMMUNITY_MCP FRED Tools|MCP FRED Tools]]
- [[_COMMUNITY_HTTP Handler|HTTP Handler]]
- [[_COMMUNITY_OpenAPI Request Schemas|OpenAPI Request Schemas]]
- [[_COMMUNITY_Candle Store Fakes|Candle Store Fakes]]
- [[_COMMUNITY_FRED Client Tests|FRED Client Tests]]
- [[_COMMUNITY_Historical Price Store Fakes|Historical Price Store Fakes]]
- [[_COMMUNITY_Historical Data & Benchmark|Historical Data & Benchmark]]
- [[_COMMUNITY_Strategy Strategy Fakes|Strategy Strategy Fakes]]
- [[_COMMUNITY_FRED Client Tests|FRED Client Tests]]
- [[_COMMUNITY_Backtest Store|Backtest Store]]
- [[_COMMUNITY_Strategy Summary Types|Strategy Summary Types]]
- [[_COMMUNITY_Copy Trading Config|Copy Trading Config]]
- [[_COMMUNITY_Candle Store|Candle Store]]
- [[_COMMUNITY_MCP Server|MCP Server]]
- [[_COMMUNITY_DORA User Summary|DORA User Summary]]
- [[_COMMUNITY_Strategy Interface|Strategy Interface]]
- [[_COMMUNITY_OpenAPI Response Schemas|OpenAPI Response Schemas]]
- [[_COMMUNITY_Price Stream Handler|Price Stream Handler]]
- [[_COMMUNITY_FRED Client|FRED Client]]
- [[_COMMUNITY_Tenor Tests|Tenor Tests]]
- [[_COMMUNITY_Mean Reversion Backtester|Mean Reversion Backtester]]
- [[_COMMUNITY_Strategy Config Fields|Strategy Config Fields]]
- [[_COMMUNITY_Price Store|Price Store]]
- [[_COMMUNITY_Balance Utilities|Balance Utilities]]
- [[_COMMUNITY_Historical Data|Historical Data]]
- [[_COMMUNITY_Strategy Tests|Strategy Tests]]
- [[_COMMUNITY_MCP FRED Tools|MCP FRED Tools]]
- [[_COMMUNITY_Strategy Export|Strategy Export]]
- [[_COMMUNITY_Docker Config|Docker Config]]
- [[_COMMUNITY_TODO & Roadmap|TODO & Roadmap]]
- [[_COMMUNITY_Handler Run Tests|Handler Run Tests]]
- [[_COMMUNITY_Handler Backtest Tests|Handler Backtest Tests]]
- [[_COMMUNITY_Handler Auth & Crypto|Handler Auth & Crypto]]
- [[_COMMUNITY_Copy Trading Strategy|Copy Trading Strategy]]
- [[_COMMUNITY_OpenAPI Parameters|OpenAPI Parameters]]
- [[_COMMUNITY_OpenAPI Schema Properties|OpenAPI Schema Properties]]
- [[_COMMUNITY_OpenAPI Components|OpenAPI Components]]
- [[_COMMUNITY_Counterfeiter Fakes|Counterfeiter Fakes]]
- [[_COMMUNITY_Rolling Window|Rolling Window]]
- [[_COMMUNITY_Order Book Summary|Order Book Summary]]
- [[_COMMUNITY_Migrations SQL|Migrations SQL]]
- [[_COMMUNITY_Project Config & Docs|Project Config & Docs]]
- [[_COMMUNITY_Handler Definitions|Handler Definitions]]
- [[_COMMUNITY_Price Tests|Price Tests]]
- [[_COMMUNITY_Strategy Config|Strategy Config]]
- [[_COMMUNITY_HTTP Handler Tests|HTTP Handler Tests]]
- [[_COMMUNITY_Copy Trading Config|Copy Trading Config]]
- [[_COMMUNITY_Balance Utilities|Balance Utilities]]
- [[_COMMUNITY_Price Handler|Price Handler]]
- [[_COMMUNITY_Price Handler Tests|Price Handler Tests]]
- [[_COMMUNITY_DORA API Client|DORA API Client]]
- [[_COMMUNITY_DORA API Client Tests|DORA API Client Tests]]
- [[_COMMUNITY_Mean Reversion Strategy Tests|Mean Reversion Strategy Tests]]
- [[_COMMUNITY_HTTP Crypto|HTTP Crypto]]
- [[_COMMUNITY_Project Config & Docs|Project Config & Docs]]
- [[_COMMUNITY_Price Export Tests|Price Export Tests]]
- [[_COMMUNITY_HTTP Handler Tests|HTTP Handler Tests]]
- [[_COMMUNITY_Strategy Interface|Strategy Interface]]
- [[_COMMUNITY_HTTP Run Store|HTTP Run Store]]
- [[_COMMUNITY_Mean Reversion Backtest|Mean Reversion Backtest]]
- [[_COMMUNITY_Mean Reversion Balances|Mean Reversion Balances]]
- [[_COMMUNITY_Mean Reversion DORA Client Tests|Mean Reversion DORA Client Tests]]
- [[_COMMUNITY_Mean Reversion Export Tests|Mean Reversion Export Tests]]
- [[_COMMUNITY_Mean Reversion Historical Data|Mean Reversion Historical Data]]
- [[_COMMUNITY_Mean Reversion Historical Data Tests|Mean Reversion Historical Data Tests]]
- [[_COMMUNITY_Mean Reversion Market API|Mean Reversion Market API]]
- [[_COMMUNITY_Mean Reversion Market API Tests|Mean Reversion Market API Tests]]

## God Nodes (most connected - your core abstractions)
1. `FakeMarketAPIClient` - 46 edges
2. `FakeService` - 39 edges
3. `Handler` - 38 edges
4. `New()` - 35 edges
5. `NewHandler()` - 29 edges
6. `WithDORAClient()` - 26 edges
7. `strategyClient` - 20 edges
8. `writeError()` - 20 edges
9. `defaultConfig()` - 18 edges
10. `doStrategyJSON()` - 18 edges

## Surprising Connections (you probably didn't know these)
- `Candle Stream Handler` --semantically_similar_to--> `Stream Daemon (auto-reconnect)`  [INFERRED] [semantically similar]
  candles/handler.go → streams/daemon.go
- `TestHandler_Stream()` --calls--> `newServer()`  [INFERRED]
  prices/handler_test.go → mcp/server.go
- `main()` --calls--> `NewPGBacktestStore()`  [INFERRED]
  cmd/strategy-server/main.go → strategy/http/backtest_store.go
- `newServer()` --calls--> `TestHandler_StreamSingle()`  [INFERRED]
  mcp/server.go → candles/handler_test.go
- `PostgreSQL Database` --references--> `Candles History Table`  [EXTRACTED]
  docker-compose.yml → migrations/002_add_candles_history.sql

## Hyperedges (group relationships)
- **Startup Order** — agentsmd_price_daemon, agentsmd_strategy_server, agentsmd_mcp_server [EXTRACTED 1.00]
- **Pre-commit Enforcement Pipeline** — precommitconfig_pre_commit, precommitconfig_pre_commit_hooks, precommitconfig_commitlint_hook, precommitconfig_golangci_lint_hook, precommitconfig_go_imports_hook, precommitconfig_go_mod_tidy_hook, precommitconfig_go_vet_hook, precommitconfig_go_test_hook [EXTRACTED 1.00]
- **Core Architecture Components** — agentsmd_strategy_service, agentsmd_streams_daemon, agentsmd_runstore, agentsmd_backteststore, agentsmd_handler [EXTRACTED 1.00]

## Communities (90 total, 35 thin omitted)

### Community 0 - "DORA API Client & Market API"
Cohesion: 0.06
Nodes (45): Config, doraAPIClient, BondQty(), OpenSignal(), RunWithPrices(), SetBenchmarkYieldClient(), SetHistoricalPriceStore(), SetLookupClient() (+37 more)

### Community 1 - "Strategy Server Main"
Cohesion: 0.07
Nodes (49): doraClientFunc, NewHandler(), paginateHelper(), performJSONRequest(), TestHandlerAllowsDifferentOrderBookRun(), TestHandlerAllowsRunAfterPreviousStopped(), TestHandlerBacktestOwnership(), TestHandlerBacktestSubResources() (+41 more)

### Community 3 - "Mean Reversion Config"
Cohesion: 0.04
Nodes (45): default, exclusiveMinimum, type, default, minimum, type, default, description (+37 more)

### Community 4 - "MCP Strategy Client"
Cohesion: 0.07
Nodes (23): listBacktestsArgs, doStrategyJSON(), newStrategyClient(), strategyBacktestTradesArgs, strategyCancelBacktestArgs, strategyClient, strategyCreateBacktestArgs, strategyCreateRunArgs (+15 more)

### Community 6 - "Strategy Service"
Cohesion: 0.07
Nodes (29): mcp-server main, healthChecker, newHealthHandler, healthStatus, price-daemon main, strategy-server main, FRED Client, CurvePoint (+21 more)

### Community 7 - "HTTP Handler & Definitions"
Cohesion: 0.07
Nodes (32): AssetInfo, BacktestResult, BacktestResultSummary, BacktestSummary, ClosedTrade, copyTradingConfigPayload, CreateBacktestRequest, CreateRunRequest (+24 more)

### Community 8 - "OpenAPI Path Definitions"
Cohesion: 0.12
Nodes (36): $ref, description, operationId, parameters, responses, security, summary, get (+28 more)

### Community 9 - "OpenAPI Info & Components"
Cohesion: 0.06
Nodes (34): description, in, name, type, components, parameters, securitySchemes, in (+26 more)

### Community 10 - "Rate Limiter Tests"
Cohesion: 0.11
Nodes (22): bucketEntry, Config, Limiter, hashKey(), newBucket(), NewLimiter(), TestEvictionRemovesStaleBuckets(), TestExtractIPDirect() (+14 more)

### Community 11 - "FRED Tenor & Yields"
Cohesion: 0.15
Nodes (28): CurvePoint, Tenor, TenorFromMaturity(), TestBenchmarkYield_UsesMaturityDate(), TestTenorFromMaturity_OneYear(), TestTenorFromMaturity_SameDay(), TestTenorFromMaturity_ThreeMonths(), YieldCurve (+20 more)

### Community 12 - "Project Architecture"
Cohesion: 0.09
Nodes (30): strategy/http.BacktestStore, bond-trading-strategies Project, commitlint, Conventional Commits, counterfeiter, DORA API, dora-client-go, FRED API (+22 more)

### Community 13 - "OpenAPI Responses"
Cohesion: 0.09
Nodes (25): content, description, content, description, schema, content, description, responses (+17 more)

### Community 14 - "Project Documentation"
Cohesion: 0.17
Nodes (22): API Key Authentication, Candles History Table, Commitlint Configuration, Copy Trading Strategy, Bond Trading Strategy Service Design, Docker Compose, Golangci-lint Configuration, MCP Protocol (+14 more)

### Community 15 - "Price Daemon Main"
Cohesion: 0.13
Nodes (13): isStale(), newHealthChecker(), newHealthHandler(), TestHealthCheckerStatus(), TestHealthHandler(), healthChecker, healthStatus, envOr() (+5 more)

### Community 16 - "OpenAPI Path Operations"
Cohesion: 0.14
Nodes (21): $ref, $ref, $ref, /v1/runs/{id}/pause, /v1/runs/{id}/resume, operationId, requestBody, responses (+13 more)

### Community 17 - "MCP FRED Tools"
Cohesion: 0.18
Nodes (14): SeriesID, benchmarkYieldArgs, fetchHistoricalYieldsArgs, fetchLatestArgs, fetchSeriesArgs, fetchYieldCurveArgs, fredHandler, interpolateYieldArgs (+6 more)

### Community 18 - "HTTP Handler"
Cohesion: 0.20
Nodes (5): Handler, getBacktestSubResource(), parseDateFilter(), parsePagination(), parseStatusFilter()

### Community 19 - "OpenAPI Request Schemas"
Cohesion: 0.11
Nodes (20): description, oneOf, properties, required, type, properties, required, type (+12 more)

### Community 21 - "FRED Client Tests"
Cohesion: 0.32
Nodes (15): fredResponse(), newTestClient(), TestFetchLatest_NonOKStatus(), TestFetchLatest_NoValidObservations(), TestFetchLatest_ReturnsMostRecent(), TestFetchLatest_SkipsLeadingMissingValues(), TestFetchSeries_AllMissingValues(), TestFetchSeries_EmptyObservations() (+7 more)

### Community 23 - "Historical Data & Benchmark"
Cohesion: 0.23
Nodes (8): BenchmarkTenor, benchmarkYieldClient, normalizeDate(), normalizeTenor(), parseBenchmarkTenor(), SupportedBenchmarkTenors(), historicalPriceStore, Strategy

### Community 25 - "FRED Client Tests"
Cohesion: 0.27
Nodes (13): TestFetchLatest_RequestParams(), TestFetchSeries_ZeroDateRange(), WithBaseURL(), WithHTTPClient(), TestFetchHistoricalYields_KnownTenor(), TestFetchHistoricalYields_UnknownTenor(), TestFetchYieldCurve_BuildsCurve(), TestFetchYieldCurve_SkipsMissingTenors() (+5 more)

### Community 26 - "Backtest Store"
Cohesion: 0.19
Nodes (8): NewPGBacktestStore(), paginate(), signalFromString(), tradeRecordFromHTTP(), tradeToClosedTrade(), BacktestStore, PGBacktestStore, BacktestDetail

### Community 27 - "Strategy Summary Types"
Cohesion: 0.13
Nodes (15): items, type, $ref, config_fields, status, supports_backtest, supports_run, StrategySummary (+7 more)

### Community 28 - "Copy Trading Config"
Cohesion: 0.13
Nodes (15): items, type, properties, format, type, format, type, minimum (+7 more)

### Community 29 - "Candle Store"
Cohesion: 0.18
Nodes (5): Candle, CandleStore, Config, Handler, StreamCandlesEntry

### Community 30 - "MCP Server"
Cohesion: 0.24
Nodes (6): authFromContext(), decodeJSON(), extractOrderBookID(), newBacktestResult(), NewDoraClientWithKey(), WithMarketAPIClient()

### Community 31 - "DORA User Summary"
Cohesion: 0.14
Nodes (14): type, type, properties, format, nullable, type, format, type (+6 more)

### Community 32 - "Strategy Interface"
Cohesion: 0.23
Nodes (14): Config Interface, Copy-Trading Backtester, Copy-Trading Strategy, Backtest Persistence Store, HTTP REST Handler, Run Persistence Store, Auth Middleware, AES-256-GCM Encryption (+6 more)

### Community 33 - "OpenAPI Response Schemas"
Cohesion: 0.19
Nodes (13): content, description, $ref, operationId, responses, security, summary, /v1/backtests/{id} (+5 more)

### Community 34 - "Price Stream Handler"
Cohesion: 0.19
Nodes (4): AssetPrice, Config, Handler, PriceStore

### Community 35 - "FRED Client"
Cohesion: 0.21
Nodes (6): Client, parseObservations(), ClientOption, Observation, observationsResponse, Client

### Community 36 - "Tenor Tests"
Cohesion: 0.18
Nodes (4): SeriesForTenor(), TestSeriesForTenor_KnownTenors(), TestSeriesForTenor_UnknownTenor(), TestYieldPrecision_NoNaN()

### Community 37 - "Mean Reversion Backtester"
Cohesion: 0.23
Nodes (9): Mean Reversion Backtester, Backtest PnL Computation, computePnL(), sharpe(), summarise(), Backtester, Rate Limit Config, Rate Limiter Middleware (+1 more)

### Community 38 - "Strategy Config Fields"
Cohesion: 0.18
Nodes (11): type, default, name, required, type, type, StrategyConfigField, properties (+3 more)

### Community 39 - "Price Store"
Cohesion: 0.24
Nodes (3): PGStore, scanAssetPriceRows(), Subscriber

### Community 40 - "Balance Utilities"
Cohesion: 0.31
Nodes (3): listItems(), writeJSON(), writeMethodNotAllowed()

### Community 42 - "Strategy Tests"
Cohesion: 0.20
Nodes (10): type, type, properties, required, type, base_asset_id, display_name, quote_asset_id (+2 more)

### Community 44 - "Strategy Export"
Cohesion: 0.27
Nodes (6): RunExists(), TestService_PauseStrategy(), TestService_ResumeStrategy(), TestService_RunStrategy(), TestService_RunStrategyIgnoresRequestContextCancellation(), TestService_StopStrategy()

### Community 46 - "TODO & Roadmap"
Cohesion: 0.22
Nodes (9): allOf, required, type, schemas, required, type, BacktestDetail, BacktestSummary (+1 more)

### Community 47 - "Handler Run Tests"
Cohesion: 0.22
Nodes (6): BacktestResult, ClosedTrade, Decision, Signal, TradeRecord, YieldObservation

### Community 48 - "Handler Backtest Tests"
Cohesion: 0.29
Nodes (8): Candle Data Structure, Candle Store Interface, Candles Daemon Config, Candle Stream Handler, Candle PG Store, Candle Store Subscriber, Fake Candle Store, Stream Daemon (auto-reconnect)

### Community 51 - "OpenAPI Parameters"
Cohesion: 0.25
Nodes (8): type, type, code, description, TenorSummary, properties, required, type

### Community 53 - "OpenAPI Components"
Cohesion: 0.48
Nodes (5): decryptAPIKey(), encryptAPIKey(), TestDecryptTruncatedCiphertextFails(), TestDecryptWithWrongKeyFails(), TestEncryptDecryptAPIKey()

### Community 54 - "Counterfeiter Fakes"
Cohesion: 0.29
Nodes (7): properties, required, type, format, type, id, DORAUserSummary

### Community 55 - "Rolling Window"
Cohesion: 0.29
Nodes (7): description, properties, type, total_pnl, BacktestResultSummary, description, type

### Community 56 - "Order Book Summary"
Cohesion: 0.47
Nodes (3): Config, Strategy, New()

### Community 59 - "Handler Definitions"
Cohesion: 0.33
Nodes (5): doraUserIDFromContext(), requireAuth(), authContextKey, authInfo, doraUserIDContextKey

### Community 60 - "Price Tests"
Cohesion: 0.33
Nodes (3): Config, Daemon, StreamFunc

### Community 64 - "Balance Utilities"
Cohesion: 0.83
Nodes (3): findAccountAndBalance(), findBalancesInAccounts(), initializeBalancesFromPortfolio()

## Knowledge Gaps
- **273 isolated node(s):** `@opencode-ai/plugin`, `$schema`, `plugin`, `AssetPrice`, `Config` (+268 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **35 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `NewDoraClientWithKey()` connect `MCP Server` to `DORA API Client & Market API`, `FRED Client Tests`?**
  _High betweenness centrality (0.099) - this node is a cross-community bridge._
- **Why does `newServer()` connect `FRED Client Tests` to `MCP Strategy Client`, `FRED Tenor & Yields`, `MCP FRED Tools`, `FRED Client Tests`, `Project Config & Docs`, `Copy Trading Config`?**
  _High betweenness centrality (0.096) - this node is a cross-community bridge._
- **Why does `TestDoraAPIClient_CreateMarketOrder_ErrorHandling()` connect `FRED Client Tests` to `MCP Server`?**
  _High betweenness centrality (0.084) - this node is a cross-community bridge._
- **Are the 28 inferred relationships involving `New()` (e.g. with `.AssetCollateralWeight()` and `.AssetPosition()`) actually correct?**
  _`New()` has 28 INFERRED edges - model-reasoned connections that need verification._
- **Are the 27 inferred relationships involving `NewHandler()` (e.g. with `requireAuth()` and `TestHandlerAllowsDifferentOrderBookRun()`) actually correct?**
  _`NewHandler()` has 27 INFERRED edges - model-reasoned connections that need verification._
- **What connects `@opencode-ai/plugin`, `$schema`, `plugin` to the rest of the system?**
  _273 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `DORA API Client & Market API` be split into smaller, more focused modules?**
  _Cohesion score 0.0612859097127223 - nodes in this community are weakly interconnected._
