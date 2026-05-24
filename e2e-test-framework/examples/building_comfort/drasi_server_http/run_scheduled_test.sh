#!/usr/bin/env bash
# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Scheduled / CI runner for the building_comfort / drasi_server_http E2E test.
#
# Responsibilities:
#   1. Download the latest (or pinned) drasi-server release binary, unless
#      DRASI_SERVER_BIN is already set.
#   2. Patch the example configs so the run is CI-safe (port collision, keep
#      artifacts on shutdown).
#   3. Start drasi-server and the test-service as background processes.
#   4. Poll the test-service REST API for the reaction's stop trigger.
#   5. Tear down both processes and copy artifacts to $ARTIFACTS_DIR.
#
# Required tools: bash, jq, curl, tar/unzip, cargo. Either `gh` (preferred)
# or `curl` is used to fetch the release.
#
# Environment variables (with defaults):
#   DRASI_REPO            GitHub repo for releases. Default: drasi-project/drasi-server
#   DRASI_SERVER_VERSION  Release tag to download. Default: latest
#   DRASI_SERVER_BIN      Pre-downloaded binary; skips the download step.
#   DRASI_ADMIN_PORT      Admin port to patch into drasi_server_config.yaml. Default: 8090
#   DRASI_SOURCE_PORT     HTTP source port. Default: 9000
#   TEST_SERVICE_PORT     test-service REST API port. Default: 8080
#   TEST_RUN_ID           Must match config.json. Default: test_run_001
#   TEST_REACTION_ID      Must match config.json. Default: building-comfort
#   TIMEOUT_SECS          Max seconds to wait for Stopped state. Default: 1800
#   POLL_INTERVAL_SECS    Seconds between status polls. Default: 10
#   ARTIFACTS_DIR         Where to copy outputs. Default: ./ci_artifacts
#   WORK_DIR              Scratch dir. Default: ./.ci_work

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

DRASI_REPO="${DRASI_REPO:-drasi-project/drasi-server}"
DRASI_SERVER_VERSION="${DRASI_SERVER_VERSION:-}"
DRASI_ADMIN_PORT="${DRASI_ADMIN_PORT:-8090}"
DRASI_SOURCE_PORT="${DRASI_SOURCE_PORT:-9000}"
TEST_SERVICE_PORT="${TEST_SERVICE_PORT:-8080}"
TEST_RUN_ID="${TEST_RUN_ID:-test_run_001}"
TEST_REACTION_ID="${TEST_REACTION_ID:-building-comfort}"
TIMEOUT_SECS="${TIMEOUT_SECS:-1800}"
POLL_INTERVAL_SECS="${POLL_INTERVAL_SECS:-10}"
ARTIFACTS_DIR="${ARTIFACTS_DIR:-$SCRIPT_DIR/ci_artifacts}"
WORK_DIR="${WORK_DIR:-$SCRIPT_DIR/.ci_work}"

LOG_DIR="$WORK_DIR/logs"
DOWNLOAD_DIR="$WORK_DIR/drasi-server-download"
DATA_CACHE="$WORK_DIR/test_data_cache"
DRASI_CFG_SRC="$SCRIPT_DIR/drasi_server_config.yaml"
TEST_CFG_SRC="$SCRIPT_DIR/config.json"
DRASI_CFG_CI="$WORK_DIR/drasi_server_config.ci.yaml"
TEST_CFG_CI="$WORK_DIR/config.ci.json"

mkdir -p "$WORK_DIR" "$LOG_DIR" "$ARTIFACTS_DIR"

DRASI_PID=""
SERVICE_PID=""

log() { echo "[ci] $*"; }

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

    # Best-effort artifact collection.
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

download_drasi_server() {
    if [[ -n "${DRASI_SERVER_BIN:-}" ]]; then
        log "Using pre-set DRASI_SERVER_BIN=$DRASI_SERVER_BIN"
        return 0
    fi

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

    # drasi-server publishes raw, unarchived per-target binaries named
    # `drasi-server-<arch>-<os>-<libc>`. Pick by $DRASI_TARGET (default
    # x86_64-linux-gnu) so this script also works on ARM runners.
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
    log "DRASI_SERVER_BIN=$DRASI_SERVER_BIN"
    "$DRASI_SERVER_BIN" --version || true
    cd - >/dev/null
}

patch_configs() {
    log "Patching drasi_server_config.yaml admin port -> $DRASI_ADMIN_PORT"
    sed -E "s/^port:[[:space:]]*8080\$/port: ${DRASI_ADMIN_PORT}/" "$DRASI_CFG_SRC" > "$DRASI_CFG_CI"
    grep -E '^(host|port):' "$DRASI_CFG_CI"

    log "Patching config.json: delete_on_start/stop=false, data_store_path=$DATA_CACHE"
    jq --arg cache "$DATA_CACHE" \
        '.data_store.data_store_path = $cache
         | .data_store.delete_on_start = false
         | .data_store.delete_on_stop = false' \
        "$TEST_CFG_SRC" > "$TEST_CFG_CI"
}

start_drasi_server() {
    log "Starting drasi-server"
    (
        cd "$SCRIPT_DIR"
        "$DRASI_SERVER_BIN" --config "$DRASI_CFG_CI" \
            > "$LOG_DIR/drasi-server.log" 2>&1
    ) &
    DRASI_PID=$!
    log "drasi-server pid=$DRASI_PID"
    wait_for_port 127.0.0.1 "$DRASI_SOURCE_PORT" "drasi-server source"
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
    wait_for_port 127.0.0.1 "$TEST_SERVICE_PORT" "test-service API" 600
}

poll_until_stopped() {
    local url="http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}/reactions/${TEST_REACTION_ID}"
    log "Polling $url (timeout=${TIMEOUT_SECS}s interval=${POLL_INTERVAL_SECS}s)"
    local deadline=$(( $(date +%s) + TIMEOUT_SECS ))
    local last_count="-"
    while (( $(date +%s) < deadline )); do
        if ! kill -0 "$SERVICE_PID" 2>/dev/null; then
            log "ERROR: test-service exited unexpectedly"
            return 1
        fi
        if ! kill -0 "$DRASI_PID" 2>/dev/null; then
            log "ERROR: drasi-server exited unexpectedly"
            return 1
        fi
        local body status count
        body="$(curl -fsS "$url" 2>/dev/null || true)"
        if [[ -n "$body" ]]; then
            status="$(echo "$body" | jq -r '.reaction_observer.status // "Unknown"')"
            count="$(echo "$body"  | jq -r '.reaction_observer.result_summary.record_count // .reaction_observer.result_summary.reaction_invocation_count // "?"')"
            if [[ "$count" != "$last_count" ]]; then
                log "status=$status records=$count"
                last_count="$count"
            fi
            if [[ "$status" == "Stopped" ]]; then
                echo "$body" > "$ARTIFACTS_DIR/final_reaction_state.json"
                log "Reaction reached Stopped state"
                return 0
            fi
            if [[ "$status" == "Error" ]]; then
                echo "$body" > "$ARTIFACTS_DIR/final_reaction_state.json"
                log "ERROR: reaction entered Error state"
                return 1
            fi
        fi
        sleep "$POLL_INTERVAL_SECS"
    done
    log "ERROR: test did not complete within ${TIMEOUT_SECS}s"
    curl -fsS "$url" > "$ARTIFACTS_DIR/final_reaction_state.json" 2>/dev/null || true
    return 1
}

download_drasi_server
patch_configs
start_drasi_server
start_test_service
poll_until_stopped
