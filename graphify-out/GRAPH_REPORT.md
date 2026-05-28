# Graph Report - .  (2026-05-28)

## Corpus Check
- 82 files · ~59,416 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1192 nodes · 1851 edges · 75 communities (50 shown, 25 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 228 edges (avg confidence: 0.8)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_HTTP Handler Tests|HTTP Handler Tests]]
- [[_COMMUNITY_DORA API Client|DORA API Client]]
- [[_COMMUNITY_Mean Reversion Strategy|Mean Reversion Strategy]]
- [[_COMMUNITY_JSON Schema Validators|JSON Schema Validators]]
- [[_COMMUNITY_Strategy REST Client|Strategy REST Client]]
- [[_COMMUNITY_Market API Fakes|Market API Fakes]]
- [[_COMMUNITY_Service Fakes|Service Fakes]]
- [[_COMMUNITY_Server Entrypoints|Server Entrypoints]]
- [[_COMMUNITY_Domain Models|Domain Models]]
- [[_COMMUNITY_OpenAPI Schema|OpenAPI Schema]]
- [[_COMMUNITY_Balance & Benchmark|Balance & Benchmark]]
- [[_COMMUNITY_API Specification|API Specification]]
- [[_COMMUNITY_FRED API Client|FRED API Client]]
- [[_COMMUNITY_FRED Tenors|FRED Tenors]]
- [[_COMMUNITY_Copy Trading Strategy|Copy Trading Strategy]]
- [[_COMMUNITY_Rate Limiter|Rate Limiter]]
- [[_COMMUNITY_Candles Store|Candles Store]]
- [[_COMMUNITY_Candles Handler|Candles Handler]]
- [[_COMMUNITY_Price Store|Price Store]]
- [[_COMMUNITY_MCP Server Tests|MCP Server Tests]]
- [[_COMMUNITY_MCP Strategy Tools|MCP Strategy Tools]]
- [[_COMMUNITY_MCP FRED Tools|MCP FRED Tools]]
- [[_COMMUNITY_Cmd daemon health|Cmd daemon health]]
- [[_COMMUNITY_Daemon Config|Daemon Config]]
- [[_COMMUNITY_Strategy Messages|Strategy Messages]]
- [[_COMMUNITY_Strategy Service|Strategy Service]]
- [[_COMMUNITY_Crypto Auth|Crypto Auth]]
- [[_COMMUNITY_Backtest Store|Backtest Store]]
- [[_COMMUNITY_Run Store|Run Store]]
- [[_COMMUNITY_Historical Data|Historical Data]]
- [[_COMMUNITY_Window Rolling|Window Rolling]]
- [[_COMMUNITY_Streams Daemon|Streams Daemon]]
- [[_COMMUNITY_Strategy Tests|Strategy Tests]]
- [[_COMMUNITY_Common Test Utils|Common Test Utils]]
- [[_COMMUNITY_Community 34|Community 34]]
- [[_COMMUNITY_Community 35|Community 35]]
- [[_COMMUNITY_Community 36|Community 36]]
- [[_COMMUNITY_Community 37|Community 37]]
- [[_COMMUNITY_Community 38|Community 38]]
- [[_COMMUNITY_Community 39|Community 39]]
- [[_COMMUNITY_Community 40|Community 40]]
- [[_COMMUNITY_Community 41|Community 41]]
- [[_COMMUNITY_Community 42|Community 42]]
- [[_COMMUNITY_Community 43|Community 43]]
- [[_COMMUNITY_Community 44|Community 44]]
- [[_COMMUNITY_Community 45|Community 45]]
- [[_COMMUNITY_Community 46|Community 46]]
- [[_COMMUNITY_Community 47|Community 47]]
- [[_COMMUNITY_Community 48|Community 48]]
- [[_COMMUNITY_Community 49|Community 49]]
- [[_COMMUNITY_Community 50|Community 50]]
- [[_COMMUNITY_Community 51|Community 51]]
- [[_COMMUNITY_Community 52|Community 52]]
- [[_COMMUNITY_Community 53|Community 53]]
- [[_COMMUNITY_Community 54|Community 54]]
- [[_COMMUNITY_Community 55|Community 55]]
- [[_COMMUNITY_Community 56|Community 56]]
- [[_COMMUNITY_Community 57|Community 57]]
- [[_COMMUNITY_Community 58|Community 58]]
- [[_COMMUNITY_Community 59|Community 59]]
- [[_COMMUNITY_Community 60|Community 60]]
- [[_COMMUNITY_Community 61|Community 61]]
- [[_COMMUNITY_Community 62|Community 62]]
- [[_COMMUNITY_Community 63|Community 63]]
- [[_COMMUNITY_Community 65|Community 65]]
- [[_COMMUNITY_Community 66|Community 66]]
- [[_COMMUNITY_Community 72|Community 72]]
- [[_COMMUNITY_Community 73|Community 73]]
- [[_COMMUNITY_Community 74|Community 74]]

## God Nodes (most connected - your core abstractions)
1. `FakeService` - 39 edges
2. `FakeMarketApiClient` - 39 edges
3. `Handler` - 38 edges
4. `NewHandler()` - 29 edges
5. `WithDORAClient()` - 26 edges
6. `strategyClient` - 20 edges
7. `writeError()` - 20 edges
8. `doStrategyJSON()` - 18 edges
9. `newTestClient()` - 18 edges
10. `fredResponse()` - 17 edges

## Surprising Connections (you probably didn't know these)
- `Candle Stream Handler` --semantically_similar_to--> `Stream Daemon (auto-reconnect)`  [INFERRED] [semantically similar]
  candles/handler.go → streams/daemon.go
- `main()` --calls--> `NewPGBacktestStore()`  [INFERRED]
  cmd/strategy-server/main.go → strategy/http/backtest_store.go
- `PostgreSQL Database` --references--> `Candles History Table`  [EXTRACTED]
  docker-compose.yml → migrations/002_add_candles_history.sql
- `PostgreSQL Database` --references--> `Price History Table`  [EXTRACTED]
  docker-compose.yml → migrations/001_create_price_history_table.sql
- `PostgreSQL Database` --references--> `Strategy Backtests Table`  [EXTRACTED]
  docker-compose.yml → migrations/004_create_strategy_backtests.sql

## Hyperedges (group relationships)
- **Core Service Triad** — price_daemon, strategy_server, mcp_server [EXTRACTED 1.00]
- **Database Schema** — price_history_table, candles_history_table, strategy_runs_table, strategy_backtests_table [EXTRACTED 1.00]
- **Strategy Management Subsystem** — mean_reversion, copy_trading, strategy_run_lifecycle, strategy_backtest_flow [INFERRED 0.85]
- **Price Stream Pipeline** — prices_Config, prices_Handler, prices_Subscriber, prices_PriceStore, prices_PGStore, prices_AssetPrice [EXTRACTED 1.00]
- **MCP Server Architecture** — mcp_MCPServer, mcp_SSEServer, mcp_strategyClient, mcp_fredHandler, cmd_mcp_server_main [EXTRACTED 1.00]
- **Strategy Lifecycle Management** — strategy_Service, strategy_service, strategy_Strategy, strategy_runState, strategy_Message [EXTRACTED 1.00]
- **Backtest Pipeline** — strategy_http_Handler, strategy_http_BacktestStore, strategy_types_types, strategy_meanreversion_Strategy, strategy_copytrading_Strategy [EXTRACTED 1.00]
- **Auth & Encryption Pipeline** — strategy_http_auth, strategy_http_crypto, strategy_http_Handler, strategy_http_doraClient [EXTRACTED 1.00]
- **Strategy Implementation Pattern** — strategy_meanreversion_Strategy, strategy_copytrading_Strategy, strategy_strategyfakes_FakeStrategy, strategy_config_Config [EXTRACTED 1.00]
- **DORA API Client Implementations** — meanreversion_doraAPIClient, dora_Client, meanreversion_fakes_FakeMarketApiClient [EXTRACTED 1.00]
- **Candle Streaming Pipeline** — candles_Handler, candles_PGStore, candles_CandleStore, candles_Subscriber, candles_Config [EXTRACTED 1.00]
- **Mean Reversion Backtesting Pipeline** — meanreversion_Strategy, meanreversion_Backtester, meanreversion_backtest_computePnL, meanreversion_backtest_sharpe, meanreversion_Config [EXTRACTED 1.00]

## Communities (75 total, 25 thin omitted)

### Community 0 - "HTTP Handler Tests"
Cohesion: 0.06
Nodes (45): TestHandler_StreamSingle(), fredResponse(), newTestClient(), TestFetchLatest_NonOKStatus(), TestFetchLatest_NoValidObservations(), TestFetchLatest_RequestParams(), TestFetchLatest_ReturnsMostRecent(), TestFetchLatest_SkipsLeadingMissingValues() (+37 more)

### Community 1 - "DORA API Client"
Cohesion: 0.07
Nodes (49): doraClientFunc, NewHandler(), paginateHelper(), performJSONRequest(), TestHandlerAllowsDifferentOrderBookRun(), TestHandlerAllowsRunAfterPreviousStopped(), TestHandlerBacktestOwnership(), TestHandlerBacktestSubResources() (+41 more)

### Community 2 - "Mean Reversion Strategy"
Cohesion: 0.07
Nodes (35): BondQty(), OpenSignal(), RunWithPrices(), SetBenchmarkYieldClient(), SetHistoricalPriceStore(), SetLookupClient(), TestStrategyGetObservations(), TestStrategyCurrentPosition() (+27 more)

### Community 3 - "JSON Schema Validators"
Cohesion: 0.04
Nodes (45): default, exclusiveMinimum, type, default, minimum, type, default, description (+37 more)

### Community 4 - "Strategy REST Client"
Cohesion: 0.07
Nodes (23): listBacktestsArgs, doStrategyJSON(), newStrategyClient(), strategyBacktestTradesArgs, strategyCancelBacktestArgs, strategyClient, strategyCreateBacktestArgs, strategyCreateRunArgs (+15 more)

### Community 7 - "Server Entrypoints"
Cohesion: 0.07
Nodes (29): mcp-server main, healthChecker, newHealthHandler, healthStatus, price-daemon main, strategy-server main, FRED Client, CurvePoint (+21 more)

### Community 8 - "Domain Models"
Cohesion: 0.07
Nodes (32): AssetInfo, BacktestResult, BacktestResultSummary, BacktestSummary, ClosedTrade, copyTradingConfigPayload, CreateBacktestRequest, CreateRunRequest (+24 more)

### Community 9 - "OpenAPI Schema"
Cohesion: 0.12
Nodes (36): $ref, description, operationId, parameters, responses, security, summary, get (+28 more)

### Community 10 - "Balance & Benchmark"
Cohesion: 0.11
Nodes (13): findAccountAndBalance(), findBalancesInAccounts(), initializeBalancesFromPortfolio(), BenchmarkTenor, benchmarkYieldClient, normalizeDate(), normalizeTenor(), parseBenchmarkTenor() (+5 more)

### Community 11 - "API Specification"
Cohesion: 0.06
Nodes (34): description, in, name, type, components, parameters, securitySchemes, in (+26 more)

### Community 12 - "FRED API Client"
Cohesion: 0.11
Nodes (22): bucketEntry, Config, Limiter, hashKey(), newBucket(), NewLimiter(), TestEvictionRemovesStaleBuckets(), TestExtractIPDirect() (+14 more)

### Community 13 - "FRED Tenors"
Cohesion: 0.10
Nodes (20): Client, parseObservations(), ClientOption, Observation, observationsResponse, SeriesID, Client, benchmarkYieldArgs (+12 more)

### Community 14 - "Copy Trading Strategy"
Cohesion: 0.09
Nodes (25): content, description, content, description, schema, content, description, responses (+17 more)

### Community 15 - "Rate Limiter"
Cohesion: 0.17
Nodes (23): API Key Authentication, Candles History Table, Commitlint Configuration, Copy Trading Strategy, Bond Trading Strategy Service Design, Docker Compose, Golangci-lint Configuration, MCP Protocol (+15 more)

### Community 16 - "Candles Store"
Cohesion: 0.13
Nodes (13): isStale(), newHealthChecker(), newHealthHandler(), TestHealthCheckerStatus(), TestHealthHandler(), healthChecker, healthStatus, envOr() (+5 more)

### Community 17 - "Candles Handler"
Cohesion: 0.14
Nodes (21): $ref, $ref, $ref, /v1/runs/{id}/pause, /v1/runs/{id}/resume, operationId, requestBody, responses (+13 more)

### Community 18 - "Price Store"
Cohesion: 0.16
Nodes (21): Config Interface, Copy-Trading Backtester, Copy-Trading Strategy, Backtest Persistence Store, HTTP REST Handler, Run Persistence Store, Auth Middleware, AES-256-GCM Encryption (+13 more)

### Community 19 - "MCP Server Tests"
Cohesion: 0.33
Nodes (19): callTool(), newStrategyMockServer(), newTestClient(), TestFREDBenchmarkYield(), TestFREDBenchmarkYieldMissingDate(), TestFREDFetchHistoricalYieldsNoAPIKey(), TestFREDFetchLatestNoAPIKey(), TestFREDFetchSeriesNoAPIKey() (+11 more)

### Community 20 - "MCP Strategy Tools"
Cohesion: 0.11
Nodes (20): description, oneOf, properties, required, type, properties, required, type (+12 more)

### Community 22 - "Cmd daemon health"
Cohesion: 0.14
Nodes (15): DORA Standalone Client, Mean Reversion Backtester, Mean Reversion Config, Mean Reversion Strategy, Backtest PnL Computation, computePnL(), sharpe(), summarise() (+7 more)

### Community 23 - "Daemon Config"
Cohesion: 0.12
Nodes (10): authFromContext(), doraUserIDFromContext(), requireAuth(), authContextKey, authInfo, doraUserIDContextKey, decodeJSON(), newBacktestResult() (+2 more)

### Community 25 - "Strategy Service"
Cohesion: 0.27
Nodes (5): getBacktestSubResource(), parsePagination(), writeError(), writeJSON(), writeMethodNotAllowed()

### Community 28 - "Run Store"
Cohesion: 0.19
Nodes (8): NewPGBacktestStore(), paginate(), signalFromString(), tradeRecordFromHTTP(), tradeToClosedTrade(), BacktestStore, PGBacktestStore, BacktestDetail

### Community 29 - "Historical Data"
Cohesion: 0.13
Nodes (15): items, type, $ref, config_fields, status, supports_backtest, supports_run, StrategySummary (+7 more)

### Community 30 - "Window Rolling"
Cohesion: 0.13
Nodes (15): items, type, properties, format, type, format, type, minimum (+7 more)

### Community 31 - "Streams Daemon"
Cohesion: 0.18
Nodes (5): Candle, CandleStore, Config, Handler, StreamCandlesEntry

### Community 32 - "Strategy Tests"
Cohesion: 0.18
Nodes (5): TestDoraAPIClient_CreateMarketOrder_ErrorHandling(), doraAPIClient, newDoraClient(), NewDoraClientWithKey(), marketAPIClient

### Community 33 - "Common Test Utils"
Cohesion: 0.14
Nodes (14): type, type, properties, format, nullable, type, format, type (+6 more)

### Community 34 - "Community 34"
Cohesion: 0.19
Nodes (13): content, description, $ref, operationId, responses, security, summary, /v1/backtests/{id} (+5 more)

### Community 35 - "Community 35"
Cohesion: 0.19
Nodes (4): AssetPrice, Config, Handler, PriceStore

### Community 36 - "Community 36"
Cohesion: 0.18
Nodes (11): type, default, name, required, type, type, StrategyConfigField, properties (+3 more)

### Community 37 - "Community 37"
Cohesion: 0.24
Nodes (3): PGStore, scanAssetPriceRows(), Subscriber

### Community 39 - "Community 39"
Cohesion: 0.20
Nodes (10): type, type, properties, required, type, base_asset_id, display_name, quote_asset_id (+2 more)

### Community 41 - "Community 41"
Cohesion: 0.27
Nodes (6): RunExists(), TestService_PauseStrategy(), TestService_ResumeStrategy(), TestService_RunStrategy(), TestService_RunStrategyIgnoresRequestContextCancellation(), TestService_StopStrategy()

### Community 43 - "Community 43"
Cohesion: 0.22
Nodes (9): allOf, required, type, schemas, required, type, BacktestDetail, BacktestSummary (+1 more)

### Community 44 - "Community 44"
Cohesion: 0.22
Nodes (6): BacktestResult, ClosedTrade, Decision, Signal, TradeRecord, YieldObservation

### Community 45 - "Community 45"
Cohesion: 0.29
Nodes (8): Candle Data Structure, Candle Store Interface, Candles Daemon Config, Candle Stream Handler, Candle PG Store, Candle Store Subscriber, Fake Candle Store, Stream Daemon (auto-reconnect)

### Community 47 - "Community 47"
Cohesion: 0.25
Nodes (8): type, type, code, description, TenorSummary, properties, required, type

### Community 49 - "Community 49"
Cohesion: 0.33
Nodes (3): filterAndSort(), listItems(), RunDetail

### Community 50 - "Community 50"
Cohesion: 0.48
Nodes (5): decryptAPIKey(), encryptAPIKey(), TestDecryptTruncatedCiphertextFails(), TestDecryptWithWrongKeyFails(), TestEncryptDecryptAPIKey()

### Community 51 - "Community 51"
Cohesion: 0.29
Nodes (7): properties, required, type, format, type, id, DORAUserSummary

### Community 52 - "Community 52"
Cohesion: 0.29
Nodes (7): description, properties, type, total_pnl, BacktestResultSummary, description, type

### Community 53 - "Community 53"
Cohesion: 0.47
Nodes (3): Config, Strategy, New()

### Community 55 - "Community 55"
Cohesion: 0.33
Nodes (3): Config, Daemon, StreamFunc

## Knowledge Gaps
- **259 isolated node(s):** `@opencode-ai/plugin`, `$schema`, `plugin`, `AssetPrice`, `Config` (+254 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **25 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `newServer()` connect `HTTP Handler Tests` to `Strategy Tests`, `MCP Server Tests`, `Strategy REST Client`, `FRED Tenors`?**
  _High betweenness centrality (0.110) - this node is a cross-community bridge._
- **Why does `NewDoraClientWithKey()` connect `Strategy Tests` to `MCP FRED Tools`, `Daemon Config`?**
  _High betweenness centrality (0.092) - this node is a cross-community bridge._
- **Why does `TestDoraAPIClient_CreateMarketOrder_ErrorHandling()` connect `Strategy Tests` to `HTTP Handler Tests`?**
  _High betweenness centrality (0.089) - this node is a cross-community bridge._
- **Are the 27 inferred relationships involving `NewHandler()` (e.g. with `requireAuth()` and `TestHandlerAllowsDifferentOrderBookRun()`) actually correct?**
  _`NewHandler()` has 27 INFERRED edges - model-reasoned connections that need verification._
- **Are the 25 inferred relationships involving `WithDORAClient()` (e.g. with `TestHandlerAllowsDifferentOrderBookRun()` and `TestHandlerAllowsRunAfterPreviousStopped()`) actually correct?**
  _`WithDORAClient()` has 25 INFERRED edges - model-reasoned connections that need verification._
- **What connects `@opencode-ai/plugin`, `$schema`, `plugin` to the rest of the system?**
  _259 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `HTTP Handler Tests` be split into smaller, more focused modules?**
  _Cohesion score 0.056189640035118525 - nodes in this community are weakly interconnected._
