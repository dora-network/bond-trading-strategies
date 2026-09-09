# Agent binary smoke test (temporary home)

This file holds the `cmd/strategy-server` binary launch smoke commands until
`docs/agent.md` lands (Plan 2 Task 1 will fold this section in).

## Verifying the install (binary smoke)

After `make start-strategy-server` (or an equivalent manual launch of
`cmd/strategy-server`) is running, verify the HTTP surface responds correctly:

```sh
# Health check — must return 200, no auth required.
curl -i http://localhost:8081/healthz

# Agent subtree auth wall — must return 401 without Authorization.
curl -i http://localhost:8081/v1/agent/sessions

# OpenAPI spec — must return 200 and list /v1/agent/* paths.
curl -i http://localhost:8081/v1/openapi | head -50
```

Expected responses:

- `GET /healthz` → `200 OK`, body `ok` (or JSON health report).
- `GET /v1/agent/sessions` (no auth) → `401 Unauthorized`. The host's
  `requireAuth` blocks the request before the agent's handler runs.
- `GET /v1/openapi` → `200 OK`, body is a JSON OpenAPI 3.1 document
  that includes paths under `/v1/agent/*` (after the OpenAPI spec merge
  lands). The `paths./v1/agent/sessions.get.operationId` should be present.

**Note on the port.** The default is `:8081` (`cmd/strategy-server/main.go`
`-a` flag, overridable via the `ADDR` env var). If `ADDR` or `STRATEGY_ADDR`
is set in the environment (the Makefile passes `STRATEGY_ADDR` to `-a`), the
curl commands must use the same port. Verify with `ss -tlnp | grep strategy-server`
or read the binary's startup log line `strategy server starting addr=...`.

**Note on the OpenAPI merge.** The `/v1/agent/*` paths appear in
`/v1/openapi` only after the OpenAPI spec merge task lands (Plan 1 Task 10,
`docs/openapi/strategy-server.json`). Before that, the spec served is the
host's pre-integration shape and won't list agent paths.
