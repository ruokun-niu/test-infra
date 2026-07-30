#!/usr/bin/env bash
# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# DYNAMIC building_comfort E2E driver.
#
# Instead of booting Drasi Server from a fully-populated static YAML (see
# local/drasi_server_grpc), this driver boots a BARE server from
# base/drasi_server.empty.yaml and then applies the scenario at runtime via
# the Drasi Server admin REST API (POST /api/v1/{sources,queries,reactions}),
# composing it from reusable building blocks under components/server/.
#
# This makes it cheap to test many component-config combinations: swap a
# component JSON (or point the *_FILE env vars at a different one) instead of
# maintaining a whole new server YAML per variant.
#
# The test-service side still loads a normal config.json (the framework's
# source dispatcher and reaction handler live in the test definition, which
# the framework does not let us inject over its own REST API).
#
# Flow:
#   1. Download the drasi-server release binary (unless DRASI_SERVER_BIN set).
#   2. Patch configs for CI safety (admin port, keep artifacts, data path).
#   3. Start the bare drasi-server and wait for its admin/REST API.
#   4. Apply source -> queries -> reactions via REST (order matters).
#   5. Wait for the source's ingress port to listen.
#   6. Start the test-service and wait for the completion signal.
#   7. Snapshot reactions, verify determinism, write summary.
#
# Env vars (defaults in parens):
#   DRASI_REPO            GitHub repo (owner/name) for the release download or
#                         the source build (drasi-project/drasi-server). Point
#                         at a fork to build a fork branch.
#   DRASI_SERVER_VERSION  release tag ("" = latest). Release mode only.
#   DRASI_SERVER_REF      branch/tag/SHA to BUILD drasi-server from source with
#                         cargo. When set, overrides the release download.
#                         Empty = download the release binary (default).
#   DRASI_SERVER_BIN      pre-built binary (skips download and source build)
#   DRASI_ADMIN_PORT      admin/REST port patched into empty.yaml (8090)
#   DRASI_SOURCE_PORT     source ingress port to wait for (50051)
#   SERVER_SOURCE_FILE    components/server/ file (source_grpc.json)
#   SERVER_QUERIES_FILE   components/server/ file (queries.json)
#   SERVER_REACTIONS_FILE components/server/ file (reactions_grpc.json)
#   TEST_CFG_SRC          test-service config ($SCRIPT_DIR/config.json)
#   TEST_SERVICE_PORT     test-service REST port (63123)
#   TEST_RUN_ID           full run id (drasi_server_dev_repo.building_comfort.test_run_001)
#   TEST_REACTION_IDS     reactions to snapshot ("building-comfort building-comfort-floor-agg")
#   TIMEOUT_SECS          completion timeout (1800)
#   POLL_INTERVAL_SECS    status poll interval (10)
#   BATCHING_SPEED        adaptive batching preset: low|medium|high (medium).
#                         Only affects adaptive components (gRPC adaptive
#                         dispatcher batch_size/batch_timeout_ms and HTTP
#                         adaptive source adaptiveMax*); standard variants are
#                         left untouched.
#   QUERY_TUNING          query capacity preset: low|medium|high (medium).
#                         Sets priorityQueueCapacity / dispatchBufferCapacity /
#                         bootstrapBufferSize on every server query component.
#                         Perf/backpressure only; results (determinism SHAs)
#                         must not change. 'medium' == server defaults.
#   PERSIST_INDEX         true|false (false). Selects a base yaml with instance-
#                         level persistIndex: true (built-in RocksDB index). The
#                         driver also enables source WAL durability, since a
#                         persistent query requires replay-capable sources.
#   STATE_STORE           true|false (false). Selects a base yaml with an
#                         instance-level redb stateStore. Independent of
#                         PERSIST_INDEX — all four combinations have a committed
#                         base yaml under base/. These are NOT REST-creatable.
#   WAL_MAX_EVENTS        WAL retention cap when PERSIST_INDEX forces durability
#                         on (500000). Must exceed the scenario's total events so
#                         the default RejectIncoming policy never drops any.
#   ARTIFACTS_DIR / WORK_DIR  outputs / scratch

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Script lives at examples/building_comfort/dynamic/ — four levels below the repo root.
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

DRASI_REPO="${DRASI_REPO:-drasi-project/drasi-server}"
DRASI_SERVER_VERSION="${DRASI_SERVER_VERSION:-}"
DRASI_SERVER_REF="${DRASI_SERVER_REF:-}"
DRASI_ADMIN_PORT="${DRASI_ADMIN_PORT:-8090}"
DRASI_SOURCE_PORT="${DRASI_SOURCE_PORT:-50051}"

SERVER_SOURCE_FILE="${SERVER_SOURCE_FILE:-source_grpc.json}"
SERVER_QUERIES_FILE="${SERVER_QUERIES_FILE:-queries.json}"
SERVER_REACTIONS_FILE="${SERVER_REACTIONS_FILE:-reactions_grpc.json}"
COMPONENTS_DIR="$SCRIPT_DIR/components/server"

TEST_SERVICE_PORT="${TEST_SERVICE_PORT:-63123}"
TEST_RUN_ID="${TEST_RUN_ID:-drasi_server_dev_repo.building_comfort.test_run_001}"
TEST_REACTION_IDS="${TEST_REACTION_IDS:-building-comfort building-comfort-floor-agg}"
TIMEOUT_SECS="${TIMEOUT_SECS:-1800}"
POLL_INTERVAL_SECS="${POLL_INTERVAL_SECS:-10}"
BATCHING_SPEED="${BATCHING_SPEED:-medium}"
QUERY_TUNING="${QUERY_TUNING:-medium}"
SERVER_PROFILE_PERSIST_INDEX="${PERSIST_INDEX:-false}"
SERVER_PROFILE_STATE_STORE="${STATE_STORE:-false}"
# WAL retention cap used when PERSIST_INDEX forces source durability on. Must
# exceed the total events this scenario emits so the default RejectIncoming
# capacity policy never drops events.
WAL_MAX_EVENTS="${WAL_MAX_EVENTS:-500000}"
ARTIFACTS_DIR="${ARTIFACTS_DIR:-$SCRIPT_DIR/ci_artifacts}"
WORK_DIR="${WORK_DIR:-$SCRIPT_DIR/.ci_work}"

LOG_DIR="$WORK_DIR/logs"
DOWNLOAD_DIR="$WORK_DIR/drasi-server-download"
SRC_BUILD_DIR="$WORK_DIR/drasi-server-src"
DATA_CACHE="$WORK_DIR/test_data_cache"
DRASI_CFG_SRC="$SCRIPT_DIR/base/drasi_server.empty.yaml"
TEST_CFG_SRC="${TEST_CFG_SRC:-$SCRIPT_DIR/config.json}"
DRASI_CFG_CI="$WORK_DIR/drasi_server.empty.ci.yaml"
TEST_CFG_CI="$WORK_DIR/config.ci.json"

DRASI_API="http://127.0.0.1:${DRASI_ADMIN_PORT}/api/v1"

mkdir -p "$WORK_DIR" "$LOG_DIR" "$ARTIFACTS_DIR"

DRASI_PID=""
SERVICE_PID=""
# Human-readable description of where DRASI_SERVER_BIN came from (release tag,
# source build, or preset). Surfaced in the step summary for result labeling.
DRASI_BUILD_SOURCE=""

log() { echo "[dyn] $*"; }

cleanup() {
    local exit_code=$?
    set +e
    for pid_name in SERVICE_PID DRASI_PID; do
        pid="${!pid_name}"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            log "Stopping $pid_name (pid=$pid)"
            kill -TERM "$pid" 2>/dev/null
            for _ in $(seq 1 30); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -KILL "$pid" 2>/dev/null
        fi
    done
    [[ -d "$DATA_CACHE" ]] && { rm -rf "$ARTIFACTS_DIR/test_data_cache"; cp -R "$DATA_CACHE" "$ARTIFACTS_DIR/test_data_cache" 2>/dev/null; }
    [[ -d "$LOG_DIR"   ]] && { rm -rf "$ARTIFACTS_DIR/logs";            cp -R "$LOG_DIR"   "$ARTIFACTS_DIR/logs" 2>/dev/null; }
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

wait_for_port() {
    local host="$1" port="$2" name="$3" timeout="${4:-120}"
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        if (echo > "/dev/tcp/$host/$port") >/dev/null 2>&1; then
            log "$name is listening on $host:$port"
            return 0
        fi
        sleep 1
    done
    log "ERROR: $name did not start listening on $host:$port within ${timeout}s"
    return 1
}

wait_for_http() {
    local url="$1" name="$2" timeout="${3:-120}"
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        if curl -fsS "$url" >/dev/null 2>&1; then
            log "$name is responding at $url"
            return 0
        fi
        sleep 1
    done
    log "ERROR: $name did not respond at $url within ${timeout}s"
    return 1
}

download_drasi_server() {
    if [[ -n "${DRASI_SERVER_BIN:-}" ]]; then
        log "Using pre-set DRASI_SERVER_BIN=$DRASI_SERVER_BIN"
        DRASI_BUILD_SOURCE="preset ${DRASI_SERVER_BIN}"
        return 0
    fi

    if [[ -n "$DRASI_SERVER_REF" ]]; then
        build_drasi_server_from_source
        return 0
    fi

    download_drasi_server_release
}

# Build drasi-server from a branch/tag/SHA of $DRASI_REPO using cargo, then set
# DRASI_SERVER_BIN to the freshly built binary. Point DRASI_REPO at a fork to
# build fork branches; $DRASI_SERVER_REF may be a branch, tag, or commit SHA.
build_drasi_server_from_source() {
    local ref="$DRASI_SERVER_REF"
    local repo_url="https://github.com/${DRASI_REPO}.git"
    log "Building drasi-server from source: repo=$DRASI_REPO ref=$ref"

    rm -rf "$SRC_BUILD_DIR"
    # Shallow branch/tag clone is fastest; fall back to a full clone + checkout
    # when $ref is a commit SHA (which --branch does not accept).
    if ! git clone --depth 1 --branch "$ref" "$repo_url" "$SRC_BUILD_DIR" 2>/dev/null; then
        log "Shallow clone of ref '$ref' failed; retrying with full clone + checkout"
        rm -rf "$SRC_BUILD_DIR"
        git clone "$repo_url" "$SRC_BUILD_DIR"
        git -C "$SRC_BUILD_DIR" checkout "$ref"
    fi

    local built_sha
    built_sha="$(git -C "$SRC_BUILD_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    log "Checked out $DRASI_REPO @ $ref ($built_sha); running cargo build --release"

    if ! ( cd "$SRC_BUILD_DIR" && cargo build --release --bin drasi-server ); then
        log "cargo build --bin drasi-server failed; retrying default release build"
        ( cd "$SRC_BUILD_DIR" && cargo build --release )
    fi

    local built_bin="$SRC_BUILD_DIR/target/release/drasi-server"
    if [[ ! -x "$built_bin" ]]; then
        # Bin name may differ from the crate default; locate it under target/release.
        built_bin="$(find "$SRC_BUILD_DIR/target/release" -maxdepth 1 -type f -name 'drasi-server*' -perm -u+x 2>/dev/null | head -n1)"
    fi
    [[ -n "$built_bin" && -x "$built_bin" ]] || { log "ERROR: cargo build did not produce a drasi-server binary"; return 1; }

    DRASI_SERVER_BIN="$built_bin"
    export DRASI_SERVER_BIN
    DRASI_BUILD_SOURCE="source ${DRASI_REPO}@${ref} (${built_sha})"
    log "DRASI_SERVER_BIN=$DRASI_SERVER_BIN"
    "$DRASI_SERVER_BIN" --version || true
}

download_drasi_server_release() {
    mkdir -p "$DOWNLOAD_DIR"
    cd "$DOWNLOAD_DIR"
    local tag="$DRASI_SERVER_VERSION"
    if [[ -z "$tag" ]]; then
        if command -v gh >/dev/null 2>&1; then
            tag="$(gh release view --repo "$DRASI_REPO" --json tagName -q .tagName)"
        else
            tag="$(curl -fsSL "https://api.github.com/repos/${DRASI_REPO}/releases/latest" | jq -r '.tag_name')"
        fi
    fi
    log "drasi-server release tag: $tag"
    local target="${DRASI_TARGET:-x86_64-linux-gnu}"
    local asset_name="drasi-server-${target}"
    log "Selected asset: $asset_name"
    if command -v gh >/dev/null 2>&1; then
        gh release download "$tag" --repo "$DRASI_REPO" --pattern "$asset_name"
    else
        curl -fsSL -O "https://github.com/${DRASI_REPO}/releases/download/${tag}/${asset_name}"
    fi
    [[ -f "$asset_name" ]] || { log "ERROR: download did not produce $asset_name"; ls -la; return 1; }
    chmod +x "$asset_name"
    mv "$asset_name" drasi-server
    DRASI_SERVER_BIN="$DOWNLOAD_DIR/drasi-server"
    export DRASI_SERVER_BIN
    DRASI_BUILD_SOURCE="release ${tag} (${DRASI_REPO})"
    log "DRASI_SERVER_BIN=$DRASI_SERVER_BIN"
    "$DRASI_SERVER_BIN" --version || true
    cd - >/dev/null
}

# Map the BATCHING_SPEED preset to concrete batch-size / wait knobs shared by
# the gRPC adaptive dispatcher (batch_size / batch_timeout_ms) and the HTTP
# adaptive source (adaptiveMaxBatchSize / adaptiveMaxWaitMs). 'medium' matches
# the values checked into the component/config files.
resolve_batching_preset() {
    case "$BATCHING_SPEED" in
        low)    BATCH_SIZE=100;  BATCH_WAIT_MS=10  ;;
        medium) BATCH_SIZE=1000; BATCH_WAIT_MS=50  ;;
        high)   BATCH_SIZE=5000; BATCH_WAIT_MS=200 ;;
        *)
            log "ERROR: invalid BATCHING_SPEED='$BATCHING_SPEED' (expected low|medium|high)"
            return 1
            ;;
    esac
    log "Batching speed '$BATCHING_SPEED' -> batch_size=$BATCH_SIZE, wait_ms=$BATCH_WAIT_MS"
}

# Map the QUERY_TUNING preset to server query capacity knobs applied to every
# query component (priorityQueueCapacity / dispatchBufferCapacity /
# bootstrapBufferSize). These are perf/backpressure only, so all presets must
# yield identical determinism SHAs. 'medium' matches the drasi-server defaults
# (priorityQueueCapacity 10000, dispatchBufferCapacity 1000, bootstrapBufferSize
# 10000), so it is effectively a no-op made explicit.
resolve_query_tuning() {
    case "$QUERY_TUNING" in
        low)    PRIORITY_QUEUE_CAP=1000;   DISPATCH_BUFFER_CAP=100;   BOOTSTRAP_BUFFER_SIZE=1000   ;;
        medium) PRIORITY_QUEUE_CAP=10000;  DISPATCH_BUFFER_CAP=1000;  BOOTSTRAP_BUFFER_SIZE=10000  ;;
        high)   PRIORITY_QUEUE_CAP=100000; DISPATCH_BUFFER_CAP=10000; BOOTSTRAP_BUFFER_SIZE=100000 ;;
        *)
            log "ERROR: invalid QUERY_TUNING='$QUERY_TUNING' (expected low|medium|high)"
            return 1
            ;;
    esac
    log "Query tuning '$QUERY_TUNING' -> priorityQueueCapacity=$PRIORITY_QUEUE_CAP, dispatchBufferCapacity=$DISPATCH_BUFFER_CAP, bootstrapBufferSize=$BOOTSTRAP_BUFFER_SIZE"
}

# Select the committed base server yaml from the two INDEPENDENT instance-config
# toggles (PERSIST_INDEX, STATE_STORE). Each of the four combinations has its own
# committed yaml under base/, so the yaml stays the source of truth for
# instance-level config (persistIndex, stateStore) — which is NOT REST-creatable.
# The driver derives SERVER_PERSIST_INDEX from the selected yaml so it can also
# enable source WAL durability (persistent queries reject non-replay sources) and
# pre-create the RocksDB index dir.
resolve_server_config() {
    local pi="$SERVER_PROFILE_PERSIST_INDEX" ss="$SERVER_PROFILE_STATE_STORE" base
    case "$pi:$ss" in
        false:false) base="drasi_server.empty.yaml" ;;
        true:false)  base="drasi_server.persist_index.yaml" ;;
        false:true)  base="drasi_server.state_store.yaml" ;;
        true:true)   base="drasi_server.persist_index_state_store.yaml" ;;
        *)
            log "ERROR: PERSIST_INDEX/STATE_STORE must be true|false (got '$pi'/'$ss')"
            return 1
            ;;
    esac
    DRASI_CFG_SRC="$SCRIPT_DIR/base/$base"
    if [[ ! -f "$DRASI_CFG_SRC" ]]; then
        log "ERROR: base server config not found: $DRASI_CFG_SRC"
        return 1
    fi
    if grep -qE '^persistIndex:[[:space:]]*true([[:space:]]|$)' "$DRASI_CFG_SRC"; then
        SERVER_PERSIST_INDEX=true
    else
        SERVER_PERSIST_INDEX=false
    fi
    log "Server config -> base=$base (persistIndex=$pi, stateStore=$ss)"
}

patch_configs() {
    log "Patching empty server config admin port -> $DRASI_ADMIN_PORT"
    sed -E "s/^port:[[:space:]]*8080\$/port: ${DRASI_ADMIN_PORT}/" "$DRASI_CFG_SRC" > "$DRASI_CFG_CI"
    grep -E '^(host|port):' "$DRASI_CFG_CI"

    log "Patching config.json: delete_on_start/stop=false, data_store_path=$DATA_CACHE"
    jq --arg cache "$DATA_CACHE" \
        '.data_store.data_store_path = $cache
         | .data_store.delete_on_start = false
         | .data_store.delete_on_stop = false' \
        "$TEST_CFG_SRC" > "$TEST_CFG_CI"

    local seed_count
    seed_count="$(jq '[.data_store.test_repos[]?.local_tests[]?.sources[]? | select(.kind == "Model") | .model_data_generator.seed? | select(. != null)] | length' "$TEST_CFG_CI")"
    if [[ "$seed_count" -eq 0 ]]; then
        log "ERROR: No model_data_generator.seed configured in $TEST_CFG_CI"
        return 1
    fi

    # Apply the batching-speed preset to any *adaptive* gRPC dispatcher. Only
    # dispatchers with adaptive_enabled==true are touched, so standard gRPC
    # variants keep their config verbatim.
    local patched
    patched="$(jq --argjson bs "$BATCH_SIZE" --argjson wt "$BATCH_WAIT_MS" '
        (.data_store.test_repos[]?.local_tests[]?.sources[]?.source_change_dispatchers[]?
          | select(.adaptive_enabled == true))
          |= (.batch_size = $bs | .batch_timeout_ms = $wt)
    ' "$TEST_CFG_CI")"
    printf '%s\n' "$patched" > "$TEST_CFG_CI"
    if jq -e '[.data_store.test_repos[]?.local_tests[]?.sources[]?.source_change_dispatchers[]? | select(.adaptive_enabled == true)] | length > 0' "$TEST_CFG_CI" >/dev/null 2>&1; then
        log "Applied batching preset '$BATCHING_SPEED' to adaptive gRPC dispatcher (batch_size=$BATCH_SIZE, batch_timeout_ms=$BATCH_WAIT_MS)"
    fi
}

# Pre-create the RocksDB index base directory. The server's built-in RocksDB
# index opens each query's DB at ./data/<safe_id>/index/<query_id> but only
# create_if_missing's the leaf; nothing creates the parent ./data/<safe_id>/index
# dir (the WAL provider creates the sibling ./data/<safe_id>/wal, so the
# <safe_id> parent exists but not `index`). Without this, the first persistent
# query's POST /queries fails with HTTP 500. No-op unless the persist_index
# profile is active (SERVER_PERSIST_INDEX=true).
#
# safe_id = "id-" + lowercase hex of each instance-id byte (server's
# instance_storage_key). We compute it from the instance id in the CI yaml and
# also scan any data dirs the server already created, for robustness.
prepare_rocksdb_index_dirs() {
    [[ "$SERVER_PERSIST_INDEX" == "true" ]] || return 0
    local iid hex safe
    iid="$(grep -E '^id:' "$DRASI_CFG_CI" | head -1 | sed -E 's/^id:[[:space:]]*//; s/^["'\'']//; s/["'\'']$//; s/[[:space:]]+$//')"
    if [[ -n "$iid" ]]; then
        hex="$(printf '%s' "$iid" | od -An -v -tx1 | tr -d ' \n')"
        safe="id-$hex"
        mkdir -p "$WORK_DIR/data/$safe/index"
        log "Pre-created RocksDB index dir: $WORK_DIR/data/$safe/index (instance '$iid')"
    fi
    # Belt-and-suspenders: create an `index` sibling for every data dir the
    # server has already materialized (e.g. the WAL dir's <safe_id> parent).
    shopt -s nullglob
    local d
    for d in "$WORK_DIR"/data/*/; do
        mkdir -p "${d}index"
    done
    shopt -u nullglob
}

start_drasi_server() {
    log "Starting bare drasi-server (components applied via REST)"
    # Run from WORK_DIR so the built-in RocksDB index/WAL (written to ./data
    # relative to cwd) lands in scratch, not the source tree. Plugins resolve
    # relative to the binary dir, not cwd, so this is safe for plugin loading.
    rm -rf "$WORK_DIR/data"
    # ./data must exist before the server starts: the redb state store writes
    # ./data/state.redb and the RocksDB index writes ./data/<id>/index, and
    # neither create_dir_all's the base dir.
    mkdir -p "$WORK_DIR/data"
    (
        cd "$WORK_DIR"
        "$DRASI_SERVER_BIN" --config "$DRASI_CFG_CI" \
            > "$LOG_DIR/drasi-server.log" 2>&1
    ) &
    DRASI_PID=$!
    log "drasi-server pid=$DRASI_PID"
    if ! wait_for_http "http://127.0.0.1:${DRASI_ADMIN_PORT}/health" "drasi-server admin API" 120; then
        log "--- drasi-server.log (last 200 lines) ---"
        tail -n 200 "$LOG_DIR/drasi-server.log" || true
        return 1
    fi
    prepare_rocksdb_index_dirs
}

# drasi_apply <resource-path> <json-body>
# POSTs one component and fails on transport error or {success:false}.
drasi_apply() {
    local path="$1" body="$2"
    local resp http_code json_body ok
    resp="$(curl -sS -w $'\n%{http_code}' -X POST "${DRASI_API}${path}" \
        -H 'Content-Type: application/json' --data-binary "$body" 2>&1)" || {
        log "ERROR: POST ${path} failed (curl): $resp"
        return 1
    }
    http_code="${resp##*$'\n'}"
    json_body="${resp%$'\n'*}"
    if [[ "$http_code" != "2"* ]]; then
        log "ERROR: POST ${path} returned HTTP ${http_code}: ${json_body}"
        tail -n 40 "$LOG_DIR/drasi-server.log" 2>/dev/null | sed 's/^/[drasi-server] /' || true
        return 1
    fi
    # Drasi Server sometimes returns HTTP 200 with {"success": false, ...}.
    ok="$(printf '%s' "$json_body" | jq -r 'if type=="object" then (.success // "true") else "true" end' 2>/dev/null || echo true)"
    if [[ "$ok" == "false" ]]; then
        log "ERROR: POST ${path} reported failure: ${json_body}"
        tail -n 40 "$LOG_DIR/drasi-server.log" 2>/dev/null | sed 's/^/[drasi-server] /' || true
        return 1
    fi
    return 0
}

# Preflight: confirm the plugins our components need actually loaded. The
# server auto-installs the plugins listed in the base yaml from the OCI
# registry, but install failures are non-fatal (logged as warnings) — so a
# missing plugin only surfaces later as "Unknown source kind". This turns
# that into an early, actionable failure. Common cause: the registry has no
# build for the current platform (e.g. darwin-arm64); use a Linux runner or
# point pluginRegistry at a local plugins dir.
check_plugins() {
    local src_file="$1" rxn_file="$2"
    local kinds_json
    kinds_json="$(curl -fsS "${DRASI_API}/plugins/kinds" 2>/dev/null || echo '{}')"
    local have_sources have_reactions
    have_sources="$(printf '%s' "$kinds_json" | jq -r '[.sources[]?.kind] | join(",")' 2>/dev/null || echo '')"
    have_reactions="$(printf '%s' "$kinds_json" | jq -r '[.reactions[]?.kind] | join(",")' 2>/dev/null || echo '')"
    log "Available source kinds: [${have_sources}]  reaction kinds: [${have_reactions}]"

    local missing=0 kind
    kind="$(jq -r '.kind' "$src_file")"
    if ! printf '%s' "$kinds_json" | jq -e --arg k "$kind" '[.sources[]?.kind] | index($k)' >/dev/null 2>&1; then
        log "ERROR: source plugin kind '$kind' not loaded"; missing=1
    fi
    while IFS= read -r kind; do
        [[ -z "$kind" ]] && continue
        if ! printf '%s' "$kinds_json" | jq -e --arg k "$kind" '[.reactions[]?.kind] | index($k)' >/dev/null 2>&1; then
            log "ERROR: reaction plugin kind '$kind' not loaded"; missing=1
        fi
    done < <(jq -r '.[].kind' "$rxn_file" | sort -u)

    if (( missing )); then
        log "ERROR: required plugins are not available. The server could not"
        log "       resolve them from the registry ($([[ -n "$have_sources$have_reactions" ]] && echo 'partial' || echo 'none available'))."
        log "       On darwin-arm64 the OCI registry may not publish plugin builds;"
        log "       run on a Linux x86_64 host/CI, or set pluginRegistry to a local dir."
        return 3
    fi
}

apply_server_components() {
    local src_file="$COMPONENTS_DIR/$SERVER_SOURCE_FILE"
    local qry_file="$COMPONENTS_DIR/$SERVER_QUERIES_FILE"
    local rxn_file="$COMPONENTS_DIR/$SERVER_REACTIONS_FILE"
    for f in "$src_file" "$qry_file" "$rxn_file"; do
        [[ -s "$f" ]] || { log "ERROR: component file missing: $f"; return 1; }
    done

    check_plugins "$src_file" "$rxn_file" || return $?

    # Order matters: source before queries that subscribe to it, queries
    # before reactions that consume them.
    log "Applying source from $SERVER_SOURCE_FILE"
    # Apply the batching-speed preset to an *adaptive* HTTP source only
    # (adaptiveEnabled==true). Non-adaptive sources pass through unchanged.
    local src_body
    src_body="$(jq --argjson bs "$BATCH_SIZE" --argjson wt "$BATCH_WAIT_MS" '
        if .adaptiveEnabled == true then
            .adaptiveMaxBatchSize = $bs | .adaptiveMaxWaitMs = $wt
        else . end' "$src_file")"
    if printf '%s' "$src_body" | jq -e '.adaptiveEnabled == true' >/dev/null 2>&1; then
        log "Applied batching preset '$BATCHING_SPEED' to adaptive HTTP source (adaptiveMaxBatchSize=$BATCH_SIZE, adaptiveMaxWaitMs=$BATCH_WAIT_MS)"
    fi

    # A persistent (RocksDB) query rejects "volatile" sources — those whose
    # supports_replay() is false. The http/gRPC sources become replay-capable
    # only when WAL durability is enabled (drasi-core PR #465 / issue #368:
    # "Transient Source WAL Integration"). So when the persist_index profile is
    # active we must turn durability on, else POST /queries fails with a
    # compatibility error. DurabilityConfig fields are snake_case (no
    # rename_all); the source DTO passes unknown fields straight to the plugin.
    # max_events must exceed the total events this scenario emits (~100k) so the
    # default RejectIncoming capacity policy never drops events (which would
    # break determinism / the stop trigger). WAL is write-ahead, so results are
    # unchanged — determinism SHAs must still match the memory baselines.
    if [[ "$SERVER_PERSIST_INDEX" == "true" ]]; then
        src_body="$(printf '%s' "$src_body" | jq --argjson me "$WAL_MAX_EVENTS" \
            '.durability = {enabled: true, max_events: $me}')"
        log "Enabled source WAL durability for persistent index (max_events=$WAL_MAX_EVENTS)"
    fi
    drasi_apply "/sources" "$src_body"

    log "Applying queries from $SERVER_QUERIES_FILE (query tuning '$QUERY_TUNING')"
    local q
    while IFS= read -r q; do
        [[ -z "$q" ]] && continue
        local qid; qid="$(printf '%s' "$q" | jq -r '.id')"
        # Apply the query-tuning preset (camelCase; the server DTO uses
        # rename_all=camelCase + deny_unknown_fields, so exact casing matters).
        local q_body
        q_body="$(printf '%s' "$q" | jq \
            --argjson pq "$PRIORITY_QUEUE_CAP" \
            --argjson db "$DISPATCH_BUFFER_CAP" \
            --argjson bb "$BOOTSTRAP_BUFFER_SIZE" '
            .priorityQueueCapacity = $pq
            | .dispatchBufferCapacity = $db
            | .bootstrapBufferSize = $bb')"
        log "  -> query $qid (priorityQueueCapacity=$PRIORITY_QUEUE_CAP, dispatchBufferCapacity=$DISPATCH_BUFFER_CAP, bootstrapBufferSize=$BOOTSTRAP_BUFFER_SIZE)"
        drasi_apply "/queries" "$q_body"
    done < <(jq -c '.[]' "$qry_file")

    log "Applying reactions from $SERVER_REACTIONS_FILE"
    local r
    while IFS= read -r r; do
        [[ -z "$r" ]] && continue
        local rid; rid="$(printf '%s' "$r" | jq -r '.id')"
        log "  -> reaction $rid"
        drasi_apply "/reactions" "$r"
    done < <(jq -c '.[]' "$rxn_file")

    log "Component snapshot:"
    curl -fsS "${DRASI_API}/sources"   | jq -c '.' || true
    curl -fsS "${DRASI_API}/queries"   | jq -c '.' || true
    curl -fsS "${DRASI_API}/reactions" | jq -c '.' || true

    # The source's ingress port must be listening before the test-service
    # starts dispatching change events to it.
    if ! wait_for_port 127.0.0.1 "$DRASI_SOURCE_PORT" "drasi-server source ingress" 120; then
        log "--- drasi-server.log (last 200 lines) ---"
        tail -n 200 "$LOG_DIR/drasi-server.log" || true
        return 1
    fi
}

start_test_service() {
    log "Building & starting test-service"
    (
        cd "$REPO_ROOT/e2e-test-framework"
        RUST_LOG='info,drasi_core::query::continuous_query=error,drasi_core::path_solver=error' \
        cargo run --release --manifest-path "test-service/Cargo.toml" -- --config "$TEST_CFG_CI" \
            > "$LOG_DIR/test-service.log" 2>&1
    ) &
    SERVICE_PID=$!
    log "test-service pid=$SERVICE_PID"
    if ! wait_for_port 127.0.0.1 "$TEST_SERVICE_PORT" "test-service API" 600; then
        log "--- test-service.log (last 200 lines) ---"
        tail -n 200 "$LOG_DIR/test-service.log" || true
        return 1
    fi
}

fetch_final_reaction_state() {
    local reaction_id="$1"
    local state_file="$ARTIFACTS_DIR/final_reaction_state__${reaction_id}.json"
    local url="http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}/reactions/${reaction_id}"
    local body status
    body="$(curl -sS "$url" 2>/dev/null || true)"
    if [[ -z "$body" ]]; then
        log "WARNING: [$reaction_id] empty response from $url"
        return 1
    fi
    echo "$body" > "$state_file"
    status="$(echo "$body" | jq -r '.reaction_observer.status // "Unknown"')"
    case "$status" in
        Stopped) log "[$reaction_id] final state: Stopped"; return 0 ;;
        Error)   log "ERROR: [$reaction_id] final state: Error"; return 1 ;;
        *)       log "ERROR: [$reaction_id] final state: $status (expected Stopped)"; return 1 ;;
    esac
}

wait_for_completion_signal() {
    local log_file="$LOG_DIR/test-service.log"
    local marker="TestRun '${TEST_RUN_ID}' completed:"
    log "Waiting for completion-tracker signal in $log_file"
    log "  marker: $marker  (timeout=${TIMEOUT_SECS}s interval=${POLL_INTERVAL_SECS}s)"
    local deadline=$(( $(date +%s) + TIMEOUT_SECS ))
    local start_ts=$(( $(date +%s) ))
    local last_log_ts=0
    while (( $(date +%s) < deadline )); do
        if ! kill -0 "$SERVICE_PID" 2>/dev/null; then
            log "ERROR: test-service exited unexpectedly"; return 1
        fi
        if ! kill -0 "$DRASI_PID" 2>/dev/null; then
            log "ERROR: drasi-server exited unexpectedly"; return 1
        fi
        if [[ -s "$log_file" ]] && grep -qF "$marker" "$log_file"; then
            log "Completion signal observed for $TEST_RUN_ID"
            grep -F "$marker" "$log_file" | tail -n1 | sed 's/^/[completion] /'
            return 0
        fi
        local now elapsed
        now=$(date +%s); elapsed=$(( now - start_ts ))
        if (( now - last_log_ts >= 30 )); then
            log "waiting for completion t=${elapsed}s (no marker yet)"; last_log_ts=$now
        fi
        sleep "$POLL_INTERVAL_SECS"
    done
    log "ERROR: completion signal not observed within ${TIMEOUT_SECS}s"
    log "--- test-service.log (last 100 lines) ---"; tail -n 100 "$log_file" || true
    log "--- drasi-server.log (last 100 lines) ---"; tail -n 100 "$LOG_DIR/drasi-server.log" || true
    local id
    for id in $TEST_REACTION_IDS; do fetch_final_reaction_state "$id" || true; done
    return 1
}

print_summary() {
    local id state_file
    for id in $TEST_REACTION_IDS; do
        state_file="$ARTIFACTS_DIR/final_reaction_state__${id}.json"
        echo "::group::Final reaction state [$id]"
        if [[ -s "$state_file" ]]; then
            jq '{ id: .id, status: .reaction_observer.status,
                  result_summary: .reaction_observer.result_summary,
                  logger_results: .reaction_observer.logger_results }' "$state_file" 2>/dev/null || cat "$state_file"
        else
            log "[$id] No final_reaction_state file available"
        fi
        echo "::endgroup::"
    done
}

verify_test_run_status() {
    local url="http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}"
    local status_file="$ARTIFACTS_DIR/final_test_run_status.json"
    local deadline=$(( $(date +%s) + 30 ))
    local body status
    while (( $(date +%s) < deadline )); do
        body="$(curl -sS "$url" 2>/dev/null || true)"
        if [[ -n "$body" ]]; then
            echo "$body" > "$status_file"
            status="$(echo "$body" | jq -r '.status // "Unknown"')"
            case "$status" in
                Error:*)          log "ERROR: test-run status: $status"; return 1 ;;
                Stopped|Running)  log "test-run status: $status"; return 0 ;;
            esac
        fi
        sleep 1
    done
    log "WARNING: could not confirm final test-run status from $url"
    [[ -s "$status_file" ]] && cat "$status_file"
    return 0
}

copy_determinism_verdict() {
    local verdict
    verdict="$(find "$DATA_CACHE" -name 'determinism_verdict.json' -type f -print -quit 2>/dev/null || true)"
    if [[ -n "$verdict" && -f "$verdict" ]]; then
        cp "$verdict" "$ARTIFACTS_DIR/determinism_verdict.json"
        echo "::group::Determinism verdict"
        jq '.' "$ARTIFACTS_DIR/determinism_verdict.json" 2>/dev/null || cat "$ARTIFACTS_DIR/determinism_verdict.json"
        echo "::endgroup::"
    fi
}

# Render a markdown summary into $GITHUB_STEP_SUMMARY so it shows up on the
# workflow run page. Local runs (no GITHUB_STEP_SUMMARY env var) skip this.
write_step_summary() {
    if [[ -z "${GITHUB_STEP_SUMMARY:-}" ]]; then
        return 0
    fi

    local out="$GITHUB_STEP_SUMMARY"
    local drasi_source="${DRASI_BUILD_SOURCE:-unknown}"
    local server_version
    server_version="$("$DRASI_SERVER_BIN" --version 2>/dev/null | head -n1 || echo unknown)"

    {
        echo "## E2E test summary — \`${VARIANT:-dynamic}\`"
        echo
        echo "- test run: \`$TEST_RUN_ID\`"
        echo "- transport: dynamic (bare drasi-server, components applied via REST)"
        echo "- drasi-server source: \`$drasi_source\`"
        echo "- drasi-server binary: \`$server_version\`"
        echo "- source component: \`$SERVER_SOURCE_FILE\` (ingress port $DRASI_SOURCE_PORT)"
        echo "- reactions component: \`$SERVER_REACTIONS_FILE\`"
        echo "- test config: \`$(basename "$TEST_CFG_SRC")\`"
        echo "- batching speed: \`$BATCHING_SPEED\` (batch_size=$BATCH_SIZE, wait_ms=$BATCH_WAIT_MS)"
        echo "- query tuning: \`$QUERY_TUNING\` (priorityQueueCapacity=$PRIORITY_QUEUE_CAP, dispatchBufferCapacity=$DISPATCH_BUFFER_CAP, bootstrapBufferSize=$BOOTSTRAP_BUFFER_SIZE)"
        echo "- server config: persistIndex=\`$SERVER_PROFILE_PERSIST_INDEX\`, stateStore=\`$SERVER_PROFILE_STATE_STORE\`$([[ "$SERVER_PERSIST_INDEX" == "true" ]] && echo " (source WAL durability on, max_events=$WAL_MAX_EVENTS)")"
        echo

        echo "### Reactions"
        echo
        echo "| Reaction | Status | Records | Runtime | SHA-256 | Determinism |"
        echo "| --- | --- | ---: | --- | --- | --- |"

        local verdict_file="$ARTIFACTS_DIR/determinism_verdict.json"
        local id state_file status invocations runtime sha verdict_passed verdict_cell
        for id in $TEST_REACTION_IDS; do
            state_file="$ARTIFACTS_DIR/final_reaction_state__${id}.json"
            status="n/a"; invocations="n/a"; runtime="n/a"; sha="n/a"
            if [[ -s "$state_file" ]]; then
                status="$(jq -r '.reaction_observer.status // "n/a"' "$state_file" 2>/dev/null)"
                invocations="$(jq -r '.reaction_observer.result_summary.reaction_invocation_count // "n/a"' "$state_file" 2>/dev/null)"
                runtime="$(jq -r '.reaction_observer.result_summary.observer_runtime_s // "n/a"' "$state_file" 2>/dev/null)"
                sha="$(jq -r '
                    (.reaction_observer.logger_results[]?
                        | select(.logger_name == "DeterminismHash")
                        | .summary.sha256) // "n/a"' "$state_file" 2>/dev/null)"
            fi

            verdict_cell="-"
            if [[ -s "$verdict_file" ]]; then
                verdict_passed="$(jq -r --arg id "$id" '.results[$id].passed // empty' "$verdict_file" 2>/dev/null)"
                case "$verdict_passed" in
                    true)  verdict_cell="✅ pass" ;;
                    false) verdict_cell="❌ fail" ;;
                esac
            fi

            local sha_short="${sha:0:12}"
            [[ "$sha" == "n/a" ]] && sha_short="n/a"
            echo "| \`$id\` | $status | $invocations | $runtime | \`$sha_short\` | $verdict_cell |"
        done
        echo

        echo "### Throughput"
        echo
        echo "| Reaction | Records | Duration (s) | Records/sec |"
        echo "| --- | ---: | ---: | ---: |"
        local metrics_file rid records duration rps
        while IFS= read -r -d '' metrics_file; do
            rid="$(jq -r '.test_run_reaction_id // "unknown"' "$metrics_file" 2>/dev/null | awk -F'.' '{print $NF}')"
            records="$(jq -r '.record_count // "n/a"' "$metrics_file" 2>/dev/null)"
            duration="$(jq -r '(.duration_ns // 0) / 1e9 | . * 1000 | round / 1000' "$metrics_file" 2>/dev/null)"
            rps="$(jq -r '.records_per_second // "n/a" | if type == "number" then . * 100 | round / 100 else . end' "$metrics_file" 2>/dev/null)"
            echo "| \`$rid\` | $records | $duration | $rps |"
        done < <(find "$DATA_CACHE" -path '*output_log/performance_metrics/*.json' -type f -print0 2>/dev/null || true)
        echo

        if [[ -s "$verdict_file" ]]; then
            echo "### Determinism verdict"
            echo
            echo '```json'
            jq '.' "$verdict_file" 2>/dev/null || cat "$verdict_file"
            echo '```'
        fi
    } >> "$out"
}

download_drasi_server
resolve_batching_preset
resolve_query_tuning
resolve_server_config
patch_configs
start_drasi_server
apply_server_components
start_test_service

poll_rc=0
if wait_for_completion_signal; then
    for id in $TEST_REACTION_IDS; do
        fetch_final_reaction_state "$id" || poll_rc=1
    done
else
    poll_rc=1
fi
print_summary

determinism_rc=0
verify_test_run_status || determinism_rc=$?
copy_determinism_verdict
write_step_summary

if (( poll_rc != 0 )); then
    exit "$poll_rc"
fi
if (( determinism_rc != 0 )); then
    exit "$determinism_rc"
fi
exit 0
