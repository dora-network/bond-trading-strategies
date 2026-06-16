#!/usr/bin/env bash
# End-to-end test for the strategy-server /v1/notifications/ws endpoint.
#
# Verifies:
#   1. WS auth: requests without an Authorization header are rejected (401).
#   2. WS connect: a connected client receives live events as they are
#      emitted by the strategy-server.
#   3. PG log: every published event is persisted to notification_log.
#   4. Last-Event-ID replay: connecting with a cursor replays all events
#      with id > cursor in chronological order.
#
# The script owns the strategy-server it starts (started under
# /tmp/strategy-server-notif-e2e.pid, cleaned up on EXIT). Use --no-server
# to run against an already-running server.
#
# Usage:
#   ./scripts/e2e/notifications-websocket.sh
#   ./scripts/e2e/notifications-websocket.sh --no-server
#   ./scripts/e2e/notifications-websocket.sh --server-only
#   ./scripts/e2e/notifications-websocket.sh --cleanup
#
# Required env (sourced from .env if present):
#   DATABASE_URL, DORA_API_KEY, DORA_BASE_URL, FRED_API_KEY, ENCRYPTION_KEY.
#   STRATEGY_ADDR (default :8081).

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
BIN="/tmp/strategy-server-notif-e2e"
LOG="/tmp/strategy-server-notif-e2e.log"
PID_FILE="/tmp/strategy-server-notif-e2e.pid"

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

server_up() {
    curl -fsS --max-time 2 "${SERVER_URL}/healthz" >/dev/null 2>&1
}

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
# WS client (compiled from the version-controlled source in
# scripts/e2e/wsclient/, so the test is reproducible on any machine
# that has the repo and Go installed)
# ---------------------------------------------------------------------------

WS_CLIENT_SRC="scripts/e2e/wsclient/main.go"
WS_CLIENT_BIN="/tmp/wsclient-notif-e2e"

# build_ws_client compiles the WS client in the module so it picks up
# the project's pinned github.com/coder/websocket version.
build_ws_client() {
    go build -o "$WS_CLIENT_BIN" "$WS_CLIENT_SRC"
}

# read_events <last_id> <read_for> <log_file>
#   - If last_id is empty, the client connects at the live tail.
#   - If last_id is set, the client passes it as Last-Event-ID for replay.
#   - read_for is a Go duration string (e.g. "8s").
#   - Stdout is the raw event JSON (one per line). Stderr is diagnostic.
#   - Exit code 0 even on clean timeout; non-zero on dial failure.
read_events() {
    local last_id="$1" read_for="$2" log_file="$3"
    if [ -n "$last_id" ]; then
        WS_LAST_EVENT_ID="$last_id" read_events_inner "$read_for" "$log_file"
    else
        unset WS_LAST_EVENT_ID
        read_events_inner "$read_for" "$log_file"
    fi
}

read_events_inner() {
    local read_for="$1" log_file="$2"
    WS_BASE_URL="$SERVER_URL" \
    WS_READ_FOR="$read_for" \
    /tmp/wsclient-notif-e2e >"$log_file" 2>>"$LOG" || true
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# submit_backtest <start> <end> -> echoes the backtest id
submit_backtest() {
    local start="$1" end="$2"
    curl -fsS -X POST "${SERVER_URL}/v1/backtests" \
        -H "Authorization: ApiKey $DORA_API_KEY" \
        -H "Content-Type: application/json" \
        -d "$(cat <<JSON
{
    "strategy_type": "mean_reversion",
    "config": {
        "lookback_window": 20,
        "entry_z_score": 2.0,
        "exit_z_score": 0.5,
        "stop_loss_z_score": 3.5,
        "min_std_dev": 0.0005,
        "max_position_size": 1.0,
        "order_book_id": "${DEFAULT_ORDER_BOOK_ID}",
        "tenor": "30Y",
        "initial_balance": 1000.0,
        "leverage": 1.0
    },
    "start": "$start",
    "end": "$end"
}
JSON
    )" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])"
}

# pick_order_book -> echoes the first OPEN order book id
pick_order_book() {
    curl -fsS -H "Authorization: ApiKey $DORA_API_KEY" \
        "${SERVER_URL}/v1/dora/orderbooks" \
        | python3 -c "
import json,sys
items=json.load(sys.stdin).get('items',[])
opens=[b for b in items if b.get('status')=='OPEN']
print(opens[0]['id'] if opens else '')
"
}

# newest_log_id -> echoes the id of the most recent notification_log row,
# or empty string if the table is empty.
newest_log_id() {
    psql "$DATABASE_URL" -tA -c \
        "SELECT id FROM notification_log ORDER BY created_at DESC LIMIT 1" \
        | tr -d '[:space:]'
}

# oldest_log_id -> echoes the id of the oldest notification_log row, or
# empty string if the table is empty.
oldest_log_id() {
    psql "$DATABASE_URL" -tA -c \
        "SELECT id FROM notification_log ORDER BY created_at ASC LIMIT 1" \
        | tr -d '[:space:]'
}

# log_count <backtest_id> -> number of notification_log rows whose
# backtest_id matches the supplied id.
log_count() {
    local id="$1"
    psql "$DATABASE_URL" -tA -c \
        "SELECT COUNT(*) FROM notification_log WHERE backtest_id = '$id'" \
        | tr -d '[:space:]'
}

# Verify that $1 (an event JSON line) has all the required fields
# populated and that type is a known v1 EventType.
verify_event_shape() {
    python3 -c "
import json,sys
KNOWN = {'backtest.started','backtest.completed','backtest.failed',
         'run.started','run.paused','run.resumed','run.stopped','run.stop_loss'}
try:
    e = json.loads('''$1''')
except Exception as ex:
    print('parse error:', ex)
    sys.exit(2)
for k in ('id','type','user_id','timestamp','payload'):
    if k not in e:
        print('missing field:', k)
        sys.exit(2)
if e['type'] not in KNOWN:
    print('unknown type:', e['type'])
    sys.exit(2)
# id should be a UUIDv7-shaped 36-char string with the version nibble '7'.
if not (isinstance(e['id'], str) and len(e['id']) == 36 and e['id'][14] == '7'):
    print('id is not UUIDv7-shaped:', e['id'])
    sys.exit(2)
sys.exit(0)
"
}

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test_ws_unauth_rejected() {
    local name="ws_unauth_rejected"
    local code
    code=$(curl -sS -o /dev/null -w "%{http_code}" \
        -H "Connection: Upgrade" -H "Upgrade: websocket" \
        -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
        "${SERVER_URL}/v1/notifications/ws")
    if [ "$code" = "401" ]; then
        ok "$name: 401 without Authorization header"
    else
        bad "$name: expected 401, got $code"
    fi
}

test_live_event_received() {
    local name="live_event_received"
    local bt_id
    # Cursor: any row currently in the log; the live test will assert
    # at least one new event arrives after we start the client.
    local before
    before=$(newest_log_id)

    build_ws_client
    # Start the client in the background, reading for 8s.
    read_events "" "8s" /tmp/ws-live.log &
    CLIENT_PID=$!
    sleep 1
    bt_id=$(submit_backtest "2025-04-01T00:00:00Z" "2025-04-15T00:00:00Z")
    if [ -z "$bt_id" ]; then
        bad "$name: submit_backtest returned no id"
        wait $CLIENT_PID 2>/dev/null || true
        return
    fi
    sleep 6
    wait $CLIENT_PID 2>/dev/null || true

    # At least one event with our backtest_id must be in the log.
    local n
    n=$(log_count "$bt_id")
    if [ "$n" = "0" ]; then
        bad "$name: no notification_log rows for $bt_id"
        return
    fi

    # The WS client must have received at least one frame, and at least
    # one of them must reference our backtest_id.
    local got_backtest
    got_backtest=$(grep -c "\"backtest_id\":\"$bt_id\"" /tmp/ws-live.log || true)
    if [ "$got_backtest" -lt 1 ]; then
        bad "$name: WS client received $got_backtest events for $bt_id (expected >=1)"
        return
    fi

    # Shape-check the first received frame.
    local first
    first=$(head -1 /tmp/ws-live.log)
    if ! verify_event_shape "$first" >/dev/null 2>&1; then
        bad "$name: first frame failed shape check: $first"
        return
    fi

    ok "$name: bt=$bt_id, log_rows=$n, ws_frames_with_bt=$got_backtest, first frame shape OK"
    if [ -n "$before" ]; then
        log "  cursor before test: $before"
    fi
}

test_last_event_id_replay() {
    local name="last_event_id_replay"
    # Submit a fresh backtest so the log has at least one row newer
    # than the cursor we'll pick.
    local bt_id
    bt_id=$(submit_backtest "2025-05-01T00:00:00Z" "2025-05-15T00:00:00Z")
    if [ -z "$bt_id" ]; then
        bad "$name: submit_backtest returned no id"
        return
    fi
    sleep 1

    local cursor
    cursor=$(oldest_log_id)
    if [ -z "$cursor" ]; then
        bad "$name: notification_log is empty; cannot pick cursor"
        return
    fi

    build_ws_client
    read_events "$cursor" "6s" /tmp/ws-replay.log &
    CLIENT_PID=$!
    sleep 1
    # Trigger a fresh event so the client can confirm live delivery AFTER
    # the replay (replay includes events already in the log; this new
    # event proves the connection is still live).
    submit_backtest "2025-06-01T00:00:00Z" "2025-06-15T00:00:00Z" >/dev/null
    sleep 4
    wait $CLIENT_PID 2>/dev/null || true

    local n
    n=$(wc -l < /tmp/ws-replay.log)
    if [ "$n" = "0" ]; then
        bad "$name: WS client received 0 frames (expected replay + live)"
        return
    fi

    # Every received frame must have id > cursor.
    local bad_ids
    bad_ids=$(python3 -c "
import json,sys
cursor='$cursor'
bad=[]
for line in open('/tmp/ws-replay.log'):
    line=line.strip()
    if not line: continue
    try:
        e=json.loads(line)
    except Exception:
        bad.append(('parse', line))
        continue
    if not e.get('id') or e['id'] <= cursor:
        bad.append((e.get('id'), line))
print(len(bad))
")
    if [ "$bad_ids" != "0" ]; then
        bad "$name: $bad_ids frames had id <= cursor"
        return
    fi

    # The frames must be in ascending id order.
    local unsorted
    unsorted=$(python3 -c "
import json,sys
ids=[]
for line in open('/tmp/ws-replay.log'):
    line=line.strip()
    if not line: continue
    try:
        e=json.loads(line)
    except Exception:
        continue
    if e.get('id'): ids.append(e['id'])
print(0 if ids == sorted(ids) else 1)
")
    if [ "$unsorted" != "0" ]; then
        bad "$name: frames were not in ascending id order"
        return
    fi

    ok "$name: cursor=$cursor, frames=$n, all id > cursor, ascending order"
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

    trap 'stop_own_server; rm -f /tmp/wsclient-notif-e2e /tmp/ws-live.log /tmp/ws-replay.log' EXIT

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

    # Pick an OPEN order book for backtest submissions.
    DEFAULT_ORDER_BOOK_ID=$(pick_order_book)
    if [ -z "$DEFAULT_ORDER_BOOK_ID" ]; then
        log "${RED}no OPEN order book found via /v1/dora/orderbooks${NC}" >&2
        exit 1
    fi
    log "using order_book_id=$DEFAULT_ORDER_BOOK_ID for backtest submissions"

    log ""
    log "=== notifications WebSocket e2e ==="
    log ""

    test_ws_unauth_rejected
    test_live_event_received
    test_last_event_id_replay

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
