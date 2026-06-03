#!/usr/bin/env bash
# End-to-end test for the copy-trading backtest from local trades_history.
#
# Verifies the full HTTP path: POST a backtest, poll for completion, check
# the result and error paths. Assumes the trades_history table is populated
# (sync is out of scope for this spec).
#
# Usage:
#   ./scripts/e2e/copytrading-backtest.sh                    # run all tests
#   ./scripts/e2e/copytrading-backtest.sh --no-server        # skip start
#   ./scripts/e2e/copytrading-backtest.sh --server-only      # start, don't run
#   ./scripts/e2e/copytrading-backtest.sh --cleanup          # stop server we started
#
# Required env: DATABASE_URL, DORA_API_KEY, DORA_BASE_URL, FRED_API_KEY,
#               ENCRYPTION_KEY, STRATEGY_ADDR (default :8081).
# Sourced from .env if present.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

STRATEGY_ADDR="${STRATEGY_ADDR:-:8081}"
SERVER_URL="http://localhost${STRATEGY_ADDR}"
BIN="/tmp/strategy-server-e2e"
LOG="/tmp/strategy-server-e2e.log"
PID_FILE="/tmp/strategy-server-e2e.pid"

# Counters
PASS=0
FAIL=0
FAILED_TESTS=()

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

log() { echo -e "$@"; }
ok()  { log "${GREEN}PASS${NC}: $1"; PASS=$((PASS + 1)); }
bad() { log "${RED}FAIL${NC}: $1"; FAIL=$((FAIL + 1)); FAILED_TESTS+=("$1"); }

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

check_deps() {
    for cmd in curl python3 psql go; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            log "${RED}missing dependency: $cmd${NC}" >&2
            exit 1
        fi
    done
    for var in DATABASE_URL DORA_API_KEY DORA_BASE_URL FRED_API_KEY ENCRYPTION_KEY; do
        if [ -z "${!var:-}" ]; then
            log "${RED}missing env: $var${NC}" >&2
            exit 1
        fi
    done
}

# Returns 0 if a healthy strategy-server is already listening on STRATEGY_ADDR.
server_up() {
    curl -fsS --max-time 2 "${SERVER_URL}/healthz" >/dev/null 2>&1
}

# Returns 0 if a server we previously started is still running.
own_server_running() {
    [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

start_server() {
    if own_server_running; then
        log "strategy-server already running (pid $(cat "$PID_FILE"))"
        return
    fi
    if server_up; then
        log "strategy-server already running on $STRATEGY_ADDR (not ours)"
        log "skipping start; will leave it running on exit"
        return
    fi
    log "building strategy-server..."
    go build -o "$BIN" ./cmd/strategy-server/
    log "starting strategy-server on $STRATEGY_ADDR..."
    nohup "$BIN" \
        -a "$STRATEGY_ADDR" \
        -d "$DATABASE_URL" \
        -b "$DORA_BASE_URL" \
        -f "$FRED_API_KEY" \
        -e "$ENCRYPTION_KEY" \
        --cors-allowed-origins "https://dora-awsdev.vercel.app" \
        > "$LOG" 2>&1 &
    echo $! > "$PID_FILE"
    # Wait for /healthz (max 30s).
    for _ in $(seq 1 30); do
        if server_up; then
            log "strategy-server ready (pid $(cat "$PID_FILE"))"
            return
        fi
        sleep 1
    done
    log "${RED}strategy-server failed to start within 30s; see $LOG${NC}" >&2
    exit 1
}

stop_own_server() {
    if own_server_running; then
        log "stopping strategy-server (pid $(cat "$PID_FILE"))"
        kill "$(cat "$PID_FILE")" 2>/dev/null || true
        rm -f "$PID_FILE"
    fi
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# submit_backtest <followed_trader> <start> <end> -> echoes the backtest id
submit_backtest() {
    local trader="$1" start="$2" end="$3"
    curl -fsS -X POST "${SERVER_URL}/v1/backtests" \
        -H "Authorization: ApiKey $DORA_API_KEY" \
        -H "Content-Type: application/json" \
        -d "$(cat <<JSON
{
    "strategy_type": "copytrading",
    "config": {
        "followed_trader": "$trader",
        "percentage_of_available": 0.1,
        "leverage": 1.0,
        "min_order_size": 0,
        "max_order_size": 0,
        "disallowed_bonds": []
    },
    "start": "$start",
    "end": "$end"
}
JSON
    )" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])"
}

# wait_for_done <id> <max_seconds> -> echoes the final status, or "timeout"
wait_for_done() {
    local id="$1" max="$2"
    local deadline=$(( $(date +%s) + max ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local status
        status=$(curl -fsS -H "Authorization: ApiKey $DORA_API_KEY" \
            "${SERVER_URL}/v1/backtests/$id" \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))")
        case "$status" in
            completed|failed) echo "$status"; return ;;
        esac
        sleep 1
    done
    echo "timeout"
}

# wait_for_result <id> <max_seconds> -> 0 if trades endpoint returns >=1 item, 1 otherwise
# Closes the race where status flips to "completed" before the result JSON is
# persisted to strategy_backtests.
wait_for_result() {
    local id="$1" max="$2"
    local deadline=$(( $(date +%s) + max ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local n
        n=$(curl -fsS -H "Authorization: ApiKey $DORA_API_KEY" \
            "${SERVER_URL}/v1/backtests/$id/trades?limit=1" \
            | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('items',[])))")
        if [ "$n" -ge 1 ]; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# get_backtest_field <id> <jsonpath-like-key> -> echoes the field value
get_backtest_field() {
    local id="$1" key="$2"
    curl -fsS -H "Authorization: ApiKey $DORA_API_KEY" \
        "${SERVER_URL}/v1/backtests/$id" \
        | python3 -c "import json,sys; print(json.load(sys.stdin).get('$key',''))"
}

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test_valid_3day() {
    local name="valid_3day_window"
    local id elapsed start
    start=$(date +%s)
    id=$(submit_backtest "019c4d37-311e-7a2f-8d58-f17c39170865" \
        "2026-05-30T00:00:00Z" "2026-06-02T00:00:00Z")
    local status
    status=$(wait_for_done "$id" 120)
    elapsed=$(( $(date +%s) - start ))

    if [ "$status" != "completed" ]; then
        bad "$name: expected completed, got $status (id=$id, elapsed=${elapsed}s)"
        return
    fi
    # Close the status->persist race.
    if ! wait_for_result "$id" 30; then
        bad "$name: status=completed but trades endpoint empty after 30s (id=$id)"
        return
    fi

    local pnl wins losses
    pnl=$(get_backtest_field "$id" "total_pnl")
    wins=$(get_backtest_field "$id" "win_count")
    losses=$(get_backtest_field "$id" "loss_count")
    if [ -z "$pnl" ] || [ "$pnl" = "0" ] || [ "$wins" -lt 1 ]; then
        bad "$name: expected positive PnL with wins, got pnl=$pnl wins=$wins losses=$losses (elapsed=${elapsed}s)"
        return
    fi

    # Count followed-trader trades and following-trader trade records.
    local followed_trades following_records
    followed_trades=$(psql "$DATABASE_URL" -tA -c \
        "SELECT COUNT(*) FROM trades_history WHERE user_id = '019c4d37-311e-7a2f-8d58-f17c39170865' AND created_at >= '2026-05-30T00:00:00Z' AND created_at <= '2026-06-02T00:00:00Z';")
    following_records=$(psql "$DATABASE_URL" -tA -c \
        "SELECT jsonb_array_length(result->'trade_records') FROM strategy_backtests WHERE id = '$id';")

    ok "$name: followed_trader_trades=$followed_trades, following_trader_records=$following_records, pnl=$pnl, wins=$wins, losses=$losses, elapsed=${elapsed}s (id=$id)"
}

test_perf_1day() {
    local name="perf_1day_window"
    local id elapsed start
    start=$(date +%s)
    id=$(submit_backtest "019c4d37-311e-7a2f-8d58-f17c39170865" \
        "2026-06-01T00:00:00Z" "2026-06-02T00:00:00Z")
    local status
    status=$(wait_for_done "$id" 120)
    elapsed=$(( $(date +%s) - start ))

    if [ "$status" != "completed" ]; then
        bad "$name: expected completed, got $status (id=$id, elapsed=${elapsed}s)"
        return
    fi
    if [ "$elapsed" -gt 60 ]; then
        bad "$name: 1-day window took ${elapsed}s, expected <60s (id=$id)"
        return
    fi
    local wins losses
    wins=$(get_backtest_field "$id" "win_count")
    losses=$(get_backtest_field "$id" "loss_count")
    ok "$name: status=completed, wins=$wins, losses=$losses, elapsed=${elapsed}s (id=$id)"
}

test_trades_pagination() {
    local name="trades_and_closed_trades_pagination"
    # Reuse the 3-day backtest if it exists, otherwise submit a fresh one.
    local id
    id=$(submit_backtest "019c4d37-311e-7a2f-8d58-f17c39170865" \
        "2026-05-30T00:00:00Z" "2026-06-02T00:00:00Z")
    local status
    status=$(wait_for_done "$id" 120)
    if [ "$status" != "completed" ]; then
        bad "$name: backtest did not complete (status=$status, id=$id)"
        return
    fi

    local trades_count first_trade_time first_trade_signal
    # Close the race: status="completed" is set before the result JSON is
    # persisted to the DB. Wait until the trades endpoint actually returns rows.
    if ! wait_for_result "$id" 30; then
        bad "$name: status=completed but trades endpoint returned 0 items within 30s (id=$id)"
        return
    fi
    trades_count=$(curl -fsS -H "Authorization: ApiKey $DORA_API_KEY" \
        "${SERVER_URL}/v1/backtests/$id/trades?limit=3" \
        | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('items',[])))")
    first_trade_time=$(curl -fsS -H "Authorization: ApiKey $DORA_API_KEY" \
        "${SERVER_URL}/v1/backtests/$id/trades?limit=1" \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['items'][0]['time'] if d.get('items') else '')")
    first_trade_signal=$(curl -fsS -H "Authorization: ApiKey $DORA_API_KEY" \
        "${SERVER_URL}/v1/backtests/$id/trades?limit=1" \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['items'][0]['signal'] if d.get('items') else '')")
    local closed_count
    closed_count=$(curl -fsS -H "Authorization: ApiKey $DORA_API_KEY" \
        "${SERVER_URL}/v1/backtests/$id/closed-trades?limit=3" \
        | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('items',[])))")

    if [ "$trades_count" -ne 3 ]; then
        bad "$name: expected 3 trades on page 1, got $trades_count (id=$id)"
        return
    fi
    if [ "$closed_count" -ne 3 ]; then
        bad "$name: expected 3 closed trades on page 1, got $closed_count (id=$id)"
        return
    fi
    if [ -z "$first_trade_signal" ] || { [ "$first_trade_signal" != "BUY" ] && [ "$first_trade_signal" != "SELL" ]; }; then
        bad "$name: first trade has unexpected signal '$first_trade_signal' (id=$id)"
        return
    fi
    ok "$name: trades page=$trades_count, closed-trades page=$closed_count, first_trade=$first_trade_time $first_trade_signal (id=$id)"
}

test_no_data_for_user() {
    local name="no_data_for_user"
    local id
    id=$(submit_backtest "00000000-0000-0000-0000-000000000000" \
        "2026-05-30T00:00:00Z" "2026-06-02T00:00:00Z")
    local status
    status=$(wait_for_done "$id" 30)
    if [ "$status" != "failed" ]; then
        bad "$name: expected failed, got $status (id=$id)"
        return
    fi
    # Wait for the error field to be populated (closes the same status->save race
    # as wait_for_result).
    local err=""
    for _ in $(seq 1 10); do
        err=$(get_backtest_field "$id" "error")
        [ -n "$err" ] && break
        sleep 1
    done
    case "$err" in
        *"no trades in trades_history"*"sync required"*)
            ok "$name: failed with expected error (id=$id)"
            ;;
        *)
            bad "$name: failed but error did not match: '$err' (id=$id)"
            ;;
    esac
}

test_window_before_data() {
    local name="window_before_data"
    local id
    id=$(submit_backtest "019c4d37-311e-7a2f-8d58-f17c39170865" \
        "2020-01-01T00:00:00Z" "2020-01-02T00:00:00Z")
    local status
    status=$(wait_for_done "$id" 30)
    if [ "$status" != "failed" ]; then
        bad "$name: expected failed, got $status (id=$id)"
        return
    fi
    local err=""
    for _ in $(seq 1 10); do
        err=$(get_backtest_field "$id" "error")
        [ -n "$err" ] && break
        sleep 1
    done
    case "$err" in
        *"outside available data"*)
            ok "$name: failed with expected error (id=$id)"
            ;;
        *)
            bad "$name: failed but error did not match: '$err' (id=$id)"
            ;;
    esac
}

test_window_after_data() {
    local name="window_after_data"
    local id
    id=$(submit_backtest "019c4d37-311e-7a2f-8d58-f17c39170865" \
        "2026-06-03T12:00:00Z" "2026-06-03T12:01:00Z")
    local status
    status=$(wait_for_done "$id" 30)
    if [ "$status" != "failed" ]; then
        bad "$name: expected failed, got $status (id=$id)"
        return
    fi
    local err=""
    for _ in $(seq 1 10); do
        err=$(get_backtest_field "$id" "error")
        [ -n "$err" ] && break
        sleep 1
    done
    case "$err" in
        *"outside available data"*)
            ok "$name: failed with expected error (id=$id)"
            ;;
        *)
            bad "$name: failed but error did not match: '$err' (id=$id)"
            ;;
    esac
}

test_empty_in_bounds() {
    local name="empty_in_bounds"
    # 019c485f-9bb7-715e-a6f6-bd4e3eefe51e has data 2026-02-23 to 2026-05-29.
    # Pick a 1-minute slice inside that range where the trader had no activity.
    local id
    id=$(submit_backtest "019c485f-9bb7-715e-a6f6-bd4e3eefe51e" \
        "2026-04-15T12:00:00Z" "2026-04-15T12:01:00Z")
    local status
    status=$(wait_for_done "$id" 30)
    if [ "$status" != "completed" ]; then
        bad "$name: expected completed, got $status (id=$id)"
        return
    fi
    local pnl wins losses
    pnl=$(get_backtest_field "$id" "total_pnl")
    wins=$(get_backtest_field "$id" "win_count")
    losses=$(get_backtest_field "$id" "loss_count")
    if [ "$wins" -ne 0 ] || [ "$losses" -ne 0 ] || [ "$pnl" != "0" ]; then
        bad "$name: expected zero result, got pnl=$pnl wins=$wins losses=$losses (id=$id)"
        return
    fi
    ok "$name: zero result as expected (id=$id)"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    check_deps

    local start_server=true
    local run_tests=true
    local cleanup_only=false

    while [ $# -gt 0 ]; do
        case "$1" in
            --no-server)   start_server=false ;;
            --server-only) run_tests=false ;;
            --cleanup)     cleanup_only=true ;;
            -h|--help)
                sed -n '2,20p' "$0" | sed 's/^# \?//'
                exit 0
                ;;
            *) log "unknown flag: $1" >&2; exit 1 ;;
        esac
        shift
    done

    if $cleanup_only; then
        stop_own_server
        exit 0
    fi

    trap 'stop_own_server' EXIT

    if $start_server; then
        start_server
    else
        if ! server_up; then
            log "${RED}--no-server set but no server is running on $STRATEGY_ADDR${NC}" >&2
            exit 1
        fi
        log "using existing strategy-server on $STRATEGY_ADDR"
    fi

    if ! $run_tests; then
        log "server-only mode: server up, not running tests"
        log "press Ctrl-C to stop (or run with --cleanup)"
        wait
    fi

    log ""
    log "=== copy-trading backtest e2e ==="
    log ""

    test_valid_3day
    test_perf_1day
    test_trades_pagination
    test_no_data_for_user
    test_window_before_data
    test_window_after_data
    test_empty_in_bounds

    log ""
    log "=== summary ==="
    log "${GREEN}passed: $PASS${NC}"
    if [ "$FAIL" -gt 0 ]; then
        log "${RED}failed: $FAIL${NC}"
        for t in "${FAILED_TESTS[@]}"; do
            log "  - $t"
        done
        exit 1
    fi
    log "${GREEN}all tests passed${NC}"
}

main "$@"
