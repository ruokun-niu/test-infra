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
#   TEST_SERVICE_PORT     test-service REST API port. Default: 63123
#   TEST_RUN_ID           Full run id used by the API: test_repo_id.test_id.test_run_id
#                         Default: drasi_server_dev_repo.building_comfort.test_run_001
#   TEST_REACTION_ID      Must match config.json. Default: building-comfort
#   EXPECTED_REACTION_SHA256
#                         Optional expected SHA-256 fingerprint of canonical
#                         reaction JsonlFile output. If set, mismatch fails CI.
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
TEST_SERVICE_PORT="${TEST_SERVICE_PORT:-63123}"
TEST_RUN_ID="${TEST_RUN_ID:-drasi_server_dev_repo.building_comfort.test_run_001}"
TEST_REACTION_ID="${TEST_REACTION_ID:-building-comfort}"
EXPECTED_REACTION_SHA256="${EXPECTED_REACTION_SHA256:-}"
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

    # Enforce deterministic inputs by requiring explicit seed(s) for model sources.
    local seed_count
    seed_count="$(jq '[.data_store.test_repos[]?.local_tests[]?.sources[]? | select(.kind == "Model") | .model_data_generator.seed? | select(. != null)] | length' "$TEST_CFG_CI")"
    if [[ "$seed_count" -eq 0 ]]; then
        log "ERROR: No model_data_generator.seed configured in $TEST_CFG_CI"
        return 1
    fi
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
    if ! wait_for_port 127.0.0.1 "$DRASI_SOURCE_PORT" "drasi-server source"; then
        log "--- drasi-server.log (last 200 lines) ---"
        tail -n 200 "$LOG_DIR/drasi-server.log" || true
        log "--- end drasi-server.log ---"
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
        log "--- end test-service.log ---"
        return 1
    fi
}

poll_until_stopped() {
    local url="http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}/reactions/${TEST_REACTION_ID}"
    log "Polling $url (timeout=${TIMEOUT_SECS}s interval=${POLL_INTERVAL_SECS}s)"
    local deadline=$(( $(date +%s) + TIMEOUT_SECS ))
    local start_ts=$(( $(date +%s) ))
    local last_log_ts=0

    while (( $(date +%s) < deadline )); do
        if ! kill -0 "$SERVICE_PID" 2>/dev/null; then
            log "ERROR: test-service exited unexpectedly"
            return 1
        fi
        if ! kill -0 "$DRASI_PID" 2>/dev/null; then
            log "ERROR: drasi-server exited unexpectedly"
            return 1
        fi

        local http_code body status count now elapsed
        http_code="$(curl -sS -o /tmp/poll_body.$$ -w '%{http_code}' "$url" 2>/dev/null || echo '000')"
        body="$(cat /tmp/poll_body.$$ 2>/dev/null || true)"
        rm -f /tmp/poll_body.$$

        status="Unknown"
        count="?"
        if [[ "$http_code" == "200" && -n "$body" ]]; then
            status="$(echo "$body" | jq -r '.reaction_observer.status // "Unknown"')"
            count="$(echo "$body" | jq -r '.reaction_observer.result_summary.record_count // .reaction_observer.result_summary.reaction_invocation_count // "?"')"
        fi

        now=$(date +%s)
        elapsed=$(( now - start_ts ))
        if (( now - last_log_ts >= 30 )); then
            log "poll t=${elapsed}s http=${http_code} status=${status} records=${count}"
            last_log_ts=$now
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

        sleep "$POLL_INTERVAL_SECS"
    done

    log "ERROR: test did not complete within ${TIMEOUT_SECS}s"
    log "--- test-service.log (last 100 lines) ---"
    tail -n 100 "$LOG_DIR/test-service.log" || true
    log "--- end test-service.log ---"
    log "--- drasi-server.log (last 100 lines) ---"
    tail -n 100 "$LOG_DIR/drasi-server.log" || true
    log "--- end drasi-server.log ---"
    curl -sS "$url" > "$ARTIFACTS_DIR/final_reaction_state.json" 2>/dev/null || true
    return 1
}

print_summary() {
    local state_file="$ARTIFACTS_DIR/final_reaction_state.json"

    echo "::group::Final reaction state"
    if [[ -s "$state_file" ]]; then
        jq '{
            id: .id,
            status: .reaction_observer.status,
            handler_status: .reaction_observer.handler_status,
            error_message: .reaction_observer.error_message,
            result_summary: .reaction_observer.result_summary,
            logger_results: .reaction_observer.logger_results
        }' "$state_file" 2>/dev/null || cat "$state_file"

        local runtime invocations
        runtime="$(jq -r '.reaction_observer.result_summary.observer_runtime_s // "unknown"' "$state_file" 2>/dev/null || echo unknown)"
        invocations="$(jq -r '.reaction_observer.result_summary.reaction_invocation_count // "unknown"' "$state_file" 2>/dev/null || echo unknown)"
        log "Observer runtime: $runtime  Reaction invocations: $invocations"
    else
        log "No final_reaction_state.json available"
    fi
    echo "::endgroup::"

    echo "::group::Performance metrics output"
    local found=0
    while IFS= read -r -d '' metrics_file; do
        found=1
        log "--- $metrics_file ---"
        jq '.' "$metrics_file" 2>/dev/null || cat "$metrics_file"
    done < <(find "$DATA_CACHE" -path '*output_log/performance_metrics/*.json' -type f -print0 2>/dev/null || true)

    if (( found == 0 )); then
        log "No performance_metrics JSON files found under $DATA_CACHE"
    fi
    echo "::endgroup::"
}

verify_deterministic_result() {
    local state_file="$ARTIFACTS_DIR/final_reaction_state.json"
    local jsonl_dir
    local first_jsonl
    local reaction_sha

    if [[ ! -s "$state_file" ]]; then
        log "WARNING: No final reaction state; skipping deterministic output hash"
        return 0
    fi

    jsonl_dir="$(jq -r '.reaction_observer.logger_results[]? | select(.logger_name == "JsonlFile" and .has_output == true) | .output_folder_path' "$state_file" | head -n1)"
    if [[ -z "$jsonl_dir" || ! -d "$jsonl_dir" ]]; then
        log "WARNING: JsonlFile output folder not found from reaction state; skipping hash check"
        return 0
    fi

    first_jsonl="$(find "$jsonl_dir" -name '*.jsonl' -type f -print -quit)"
    if [[ -z "$first_jsonl" ]]; then
        log "WARNING: No reaction JSONL files found in $jsonl_dir; skipping hash check"
        return 0
    fi

    # Hash only canonical reaction payload content (not timestamps/trace metadata)
    # so equal seeded runs are compared on actual result data.
    reaction_sha="$(find "$jsonl_dir" -name '*.jsonl' -type f -print0 \
        | sort -z \
        | xargs -0 cat \
        | jq -cS 'if .payload.type == "ReactionInvocation" then .payload.request_body elif .payload.type == "ReactionOutput" then .payload.reaction_output else .payload end' \
        | sha256sum \
        | awk '{print $1}')"
    if [[ -z "$reaction_sha" ]]; then
        log "WARNING: Failed to compute reaction output hash"
        return 0
    fi

    printf '%s\n' "$reaction_sha" > "$ARTIFACTS_DIR/reaction_output_sha256.txt"
    printf '%s\n' "$jsonl_dir" > "$ARTIFACTS_DIR/reaction_output_jsonl_dir.txt"
    log "Reaction output SHA-256: $reaction_sha"

    if [[ -n "$EXPECTED_REACTION_SHA256" ]]; then
        if [[ "$reaction_sha" != "$EXPECTED_REACTION_SHA256" ]]; then
            log "ERROR: Determinism check failed. expected=$EXPECTED_REACTION_SHA256 actual=$reaction_sha"
            return 1
        fi
        log "Determinism check passed against EXPECTED_REACTION_SHA256"
    else
        log "No EXPECTED_REACTION_SHA256 provided; fingerprint emitted for baseline setup"
    fi
}

download_drasi_server
patch_configs
start_drasi_server
start_test_service

poll_rc=0
poll_until_stopped || poll_rc=$?
print_summary

determinism_rc=0
verify_deterministic_result || determinism_rc=$?

if (( poll_rc != 0 )); then
    exit "$poll_rc"
fi

if (( determinism_rc != 0 )); then
    exit "$determinism_rc"
fi

exit 0
