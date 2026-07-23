#!/usr/bin/env bash
# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Scheduled / CI runner for the building_comfort / drasi_server_grpc E2E test.
#
# Responsibilities:
#   1. Download the latest (or pinned) drasi-server release binary, unless
#      DRASI_SERVER_BIN is already set.
#   2. Patch the example configs so the run is CI-safe (port collision, keep
#      artifacts on shutdown).
#   3. Start drasi-server and the test-service as background processes.
#   4. Poll the test-service REST API until each reaction reaches Stopped.
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
#   DRASI_SOURCE_PORT     gRPC source port. Default: 50051
#   DRASI_INDEX           Query index backend: memory (default) | rocksdb.
#                         `rocksdb` patches `persistIndex: true` into the CI
#                         drasi-server config and clears ./data for a clean
#                         cold start. (drasi-server only compiles in these two
#                         backends; Redis indexes exist only on the Platform.)
#   TEST_SERVICE_PORT     test-service REST API port. Default: 63123
#   TEST_RUN_ID           Full run id used by the API: test_repo_id.test_id.test_run_id
#                         Default: drasi_server_dev_repo.building_comfort.test_run_001
#   TEST_REACTION_IDS     Space-separated list of test_reaction_id values to
#                         snapshot at completion.
#                         Default: "building-comfort building-comfort-floor-agg"
#   TIMEOUT_SECS          Max seconds to wait for the completion signal.
#                         Default: 1800
#   POLL_INTERVAL_SECS    Seconds between status polls. Default: 10
#   ARTIFACTS_DIR         Where to copy outputs. Default: ./ci_artifacts
#   WORK_DIR              Scratch dir. Default: ./.ci_work

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Script lives at examples/building_comfort/ci/drasi_server_grpc/ — five levels
# below the repo root.
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"

DRASI_REPO="${DRASI_REPO:-drasi-project/drasi-server}"
DRASI_SERVER_VERSION="${DRASI_SERVER_VERSION:-}"
DRASI_ADMIN_PORT="${DRASI_ADMIN_PORT:-8090}"
DRASI_SOURCE_PORT="${DRASI_SOURCE_PORT:-50051}"
DRASI_INDEX="${DRASI_INDEX:-memory}"
TEST_SERVICE_PORT="${TEST_SERVICE_PORT:-63123}"
TEST_RUN_ID="${TEST_RUN_ID:-drasi_server_dev_repo.building_comfort.test_run_001}"
TEST_REACTION_IDS="${TEST_REACTION_IDS:-building-comfort building-comfort-floor-agg}"
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

    log "Selecting query index backend -> $DRASI_INDEX"
    case "$DRASI_INDEX" in
        memory)
            log "Index backend: memory (in-memory query indexes)"
            ;;
        rocksdb)
            if grep -qE '^persistIndex:' "$DRASI_CFG_CI"; then
                log "Index backend: rocksdb (config already sets persistIndex)"
            elif grep -qE '^persistConfig:' "$DRASI_CFG_CI"; then
                awk '{ print } /^persistConfig:/ { print "persistIndex: true" }' \
                    "$DRASI_CFG_CI" > "${DRASI_CFG_CI}.tmp" && mv "${DRASI_CFG_CI}.tmp" "$DRASI_CFG_CI"
            else
                printf '\npersistIndex: true\n' >> "$DRASI_CFG_CI"
            fi
            if ! grep -qE '^persistIndex: true' "$DRASI_CFG_CI"; then
                log "ERROR: failed to patch persistIndex into $DRASI_CFG_CI"
                return 1
            fi
            log "Index backend: rocksdb (persistIndex: true)"
            grep -E '^persistIndex:' "$DRASI_CFG_CI"
            # Cold start: clear any RocksDB index / redb WAL from a prior run.
            rm -rf "$SCRIPT_DIR/data"
            ;;
        *)
            log "ERROR: DRASI_INDEX='$DRASI_INDEX' not supported (use memory|rocksdb)"
            return 1
            ;;
    esac

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

fetch_final_reaction_state() {
    # fetch_final_reaction_state <test_reaction_id>
    # Snapshots the reaction's current state to $ARTIFACTS_DIR/final_reaction_state__<id>.json.
    # Returns 0 if the reaction is Stopped, 1 on Error/anything else (the completion
    # tracker only fires when every reaction is Stopped/Error, so Running here means
    # the run aborted before the tracker could fire).
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
    # Waits for the test-run's completion tracker to fire, signaled by the
    # test-service log line emitted from test_run_completion::completion_handlers
    # ("TestRun '<id>' completed:") which appears once every component reaches a
    # terminal state. Uses TEST_RUN_ID as the discriminator so multiple runs sharing
    # the same log don't false-positive.
    local log_file="$LOG_DIR/test-service.log"
    local marker="TestRun '${TEST_RUN_ID}' completed:"
    log "Waiting for completion-tracker signal in $log_file"
    log "  marker: $marker  (timeout=${TIMEOUT_SECS}s interval=${POLL_INTERVAL_SECS}s)"

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

        if [[ -s "$log_file" ]] && grep -qF "$marker" "$log_file"; then
            log "Completion signal observed for $TEST_RUN_ID"
            grep -F "$marker" "$log_file" | tail -n1 | sed 's/^/[completion] /'
            return 0
        fi

        local now elapsed
        now=$(date +%s)
        elapsed=$(( now - start_ts ))
        if (( now - last_log_ts >= 30 )); then
            log "waiting for completion t=${elapsed}s (no marker yet)"
            last_log_ts=$now
        fi

        sleep "$POLL_INTERVAL_SECS"
    done

    log "ERROR: completion signal not observed within ${TIMEOUT_SECS}s"
    log "--- test-service.log (last 100 lines) ---"
    tail -n 100 "$log_file" || true
    log "--- end test-service.log ---"
    log "--- drasi-server.log (last 100 lines) ---"
    tail -n 100 "$LOG_DIR/drasi-server.log" || true
    log "--- end drasi-server.log ---"
    # Best-effort snapshot of each reaction for debugging.
    local id
    for id in $TEST_REACTION_IDS; do
        fetch_final_reaction_state "$id" || true
    done
    return 1
}

print_summary() {
    local id state_file
    for id in $TEST_REACTION_IDS; do
        state_file="$ARTIFACTS_DIR/final_reaction_state__${id}.json"

        echo "::group::Final reaction state [$id]"
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
            log "[$id] Observer runtime: $runtime  Reaction invocations: $invocations"
        else
            log "[$id] No final_reaction_state file available"
        fi
        echo "::endgroup::"
    done

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

# Verifies the test-run's final status via the test-service REST API. The
# framework's Sha256Determinism completion handler flips the run status to
# Error if any reaction's DeterminismHash logger SHA doesn't match the
# baseline declared in the test definition; otherwise the run stays at
# Running once all components have finished. We give the handler chain a
# short grace period after the completion marker to settle.
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
                Error:*)
                    log "ERROR: test-run status: $status"
                    return 1
                    ;;
                Stopped|Running)
                    log "test-run status: $status"
                    return 0
                    ;;
            esac
        fi
        sleep 1
    done

    log "WARNING: could not confirm final test-run status from $url"
    [[ -s "$status_file" ]] && cat "$status_file"
    return 0
}

# Best-effort copy of the determinism verdict file (written by the
# Sha256Determinism completion handler under the test-run storage path).
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
    local drasi_version="${DRASI_SERVER_VERSION:-latest}"
    local server_version
    server_version="$("$DRASI_SERVER_BIN" --version 2>/dev/null | head -n1 || echo unknown)"

    {
        echo "## E2E test summary — \`$TEST_RUN_ID\`"
        echo
        echo "- drasi-server tag: \`$drasi_version\`"
        echo "- drasi-server binary: \`$server_version\`"
        echo "- index backend: \`$DRASI_INDEX\`"
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
patch_configs
start_drasi_server
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
