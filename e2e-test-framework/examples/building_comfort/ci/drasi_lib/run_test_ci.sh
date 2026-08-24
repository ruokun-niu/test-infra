#!/usr/bin/env bash
# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Scheduled / CI runner for the building_comfort / drasi_lib E2E test.
#
# Unlike the HTTP/gRPC server-based variants (the dynamic driver), this test
# uses an *embedded* drasi-lib instance hosted in-process by the
# test-service — there is no separate drasi-server process and no plugin
# install step. The test-service crate must therefore be built with the
# `drasi-lib` instance support compiled in (which it is by default).
#
# Responsibilities:
#   1. Patch the example config so the run is CI-safe (clear stale data
#      cache, keep artifacts on shutdown, redirect data_store_path into
#      $WORK_DIR, enforce that the model source has a `seed`).
#   2. Start the test-service as a background process. The embedded
#      drasi-lib instance never reaches a terminal state on its own, so we
#      do NOT wait for the framework's test-run completion tracker.
#   3. Wait for every configured reaction to reach Stopped via the REST API.
#   4. Snapshot each reaction's final state, do an inline SHA-256
#      determinism check against the `expected` map in config.json (if a
#      Sha256Determinism handler is declared), and write a markdown
#      summary into $GITHUB_STEP_SUMMARY.
#
#      The `DeterminismHash` output logger skips empty-results heartbeats
#      so the per-reaction SHA reflects only the data records the
#      reaction's query actually emitted. Cross-reaction interleaving is
#      still scheduler-dependent and is intentionally not hashed.
#
# Required tools: bash, jq, curl, cargo.
#
# Environment variables (with defaults):
#   TEST_SERVICE_PORT     test-service REST API port. Default: 63123
#   TEST_RUN_ID           Full run id used by the API:
#                         Default: drasi_lib_dev_repo.building_comfort.test_run_001
#   TEST_REACTION_IDS     Space-separated list of test_reaction_id values to
#                         snapshot at completion.
#                         Default: "building-comfort building-comfort-floor-agg"
#   TIMEOUT_SECS          Max seconds to wait for all reactions to Stop.
#                         Default: 1800
#   POLL_INTERVAL_SECS    Seconds between status polls. Default: 10
#   ARTIFACTS_DIR         Where to copy outputs. Default: ./ci_artifacts
#   WORK_DIR              Scratch dir. Default: ./.ci_work

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Script lives at examples/building_comfort/ci/drasi_lib/ — five levels
# below the repo root.
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"

TEST_SERVICE_PORT="${TEST_SERVICE_PORT:-63123}"
TEST_RUN_ID="${TEST_RUN_ID:-drasi_lib_dev_repo.building_comfort.test_run_001}"
TEST_REACTION_IDS="${TEST_REACTION_IDS:-building-comfort building-comfort-floor-agg}"
TIMEOUT_SECS="${TIMEOUT_SECS:-1800}"
POLL_INTERVAL_SECS="${POLL_INTERVAL_SECS:-10}"
ARTIFACTS_DIR="${ARTIFACTS_DIR:-$SCRIPT_DIR/ci_artifacts}"
WORK_DIR="${WORK_DIR:-$SCRIPT_DIR/.ci_work}"

LOG_DIR="$WORK_DIR/logs"
DATA_CACHE="$WORK_DIR/test_data_cache"
TEST_CFG_SRC="$SCRIPT_DIR/config.json"
TEST_CFG_CI="$WORK_DIR/config.ci.json"

mkdir -p "$WORK_DIR" "$LOG_DIR" "$ARTIFACTS_DIR"

SERVICE_PID=""

log() { echo "[ci] $*"; }

cleanup() {
    local exit_code=$?
    set +e
    if [[ -n "$SERVICE_PID" ]] && kill -0 "$SERVICE_PID" 2>/dev/null; then
        log "Stopping SERVICE_PID (pid=$SERVICE_PID)"
        kill -TERM "$SERVICE_PID" 2>/dev/null
        for _ in $(seq 1 30); do
            kill -0 "$SERVICE_PID" 2>/dev/null || break
            sleep 1
        done
        kill -KILL "$SERVICE_PID" 2>/dev/null
    fi

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

patch_configs() {
    # Wipe any stale data cache from a previous local run. The framework's
    # `add_test_repo` only registers the repo in its in-memory map when the
    # on-disk directory is newly created, so reusing an existing
    # `test_data_cache/<repo_id>/` directory leads to a later panic in
    # `get_test_repo_storage(...).unwrap()`. We keep `delete_on_start=false`
    # in the patched config so completion handlers can write verdict files
    # before the framework clears anything; clearing here at script start
    # gives the framework a clean slate.
    if [[ -d "$DATA_CACHE" ]]; then
        log "Wiping stale data cache at $DATA_CACHE"
        rm -rf "$DATA_CACHE"
    fi

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

wait_for_reactions_stopped() {
    # Embedded drasi-lib runs never fire the framework's test-run completion
    # tracker because the in-process drasi-lib instance stays Running until
    # the host process exits. We treat "every reaction Stopped" as the
    # authoritative done signal.
    local url="http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}"
    log "Waiting for all reactions to reach Stopped (timeout=${TIMEOUT_SECS}s interval=${POLL_INTERVAL_SECS}s)"
    log "  reactions: $TEST_REACTION_IDS"

    local deadline=$(( $(date +%s) + TIMEOUT_SECS ))
    local start_ts=$(( $(date +%s) ))
    local last_log_ts=0

    while (( $(date +%s) < deadline )); do
        if ! kill -0 "$SERVICE_PID" 2>/dev/null; then
            log "ERROR: test-service exited unexpectedly"
            return 1
        fi

        if all_reactions_stopped; then
            log "All reactions reached Stopped state"
            return 0
        fi

        local now elapsed
        now=$(date +%s)
        elapsed=$(( now - start_ts ))
        if (( now - last_log_ts >= 30 )); then
            # Heartbeat: live reaction-invocation counts so a stuck run is
            # debuggable from the workflow log alone (no artifact required).
            local id summary
            summary=""
            for id in $TEST_REACTION_IDS; do
                local count
                count="$(curl -sS --max-time 5 "${url}/reactions/${id}" 2>/dev/null \
                    | jq -r '.reaction_observer.result_summary.reaction_invocation_count // "?"' 2>/dev/null || echo '?')"
                summary+="${id}=${count} "
            done
            log "waiting t=${elapsed}s ${summary}(no Stopped yet)"
            last_log_ts=$now
        fi

        sleep "$POLL_INTERVAL_SECS"
    done

    log "ERROR: not all reactions reached Stopped within ${TIMEOUT_SECS}s"
    log "--- test-service.log (last 100 lines) ---"
    tail -n 100 "$LOG_DIR/test-service.log" || true
    log "--- end test-service.log ---"
    local id
    for id in $TEST_REACTION_IDS; do
        fetch_final_reaction_state "$id" || true
    done
    return 1
}

# True if every reaction in $TEST_REACTION_IDS is currently in Stopped state.
all_reactions_stopped() {
    local url="http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}"
    local id status
    for id in $TEST_REACTION_IDS; do
        status="$(curl -sS --max-time 5 "${url}/reactions/${id}" 2>/dev/null \
            | jq -r '.reaction_observer.status // "Unknown"' 2>/dev/null || echo 'Unknown')"
        [[ "$status" == "Stopped" ]] || return 1
    done
    return 0
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

verify_test_run_status() {
    # Best-effort snapshot of the overall test-run state. Useful for
    # debugging but not authoritative for embedded drasi-lib runs (see the
    # note on wait_for_reactions_stopped).
    local url="http://127.0.0.1:${TEST_SERVICE_PORT}/api/test_runs/${TEST_RUN_ID}"
    local body
    body="$(curl -sS --max-time 5 "$url" 2>/dev/null || true)"
    [[ -n "$body" ]] && echo "$body" > "$ARTIFACTS_DIR/final_test_run_status.json"
    return 0
}

# Compare each reaction's DeterminismHash SHA-256 from its logger summary to
# the `expected` map declared on the Sha256Determinism completion handler in
# config.json. Returns 1 on mismatch (when an `expected` value is set);
# returns 0 (with a log line) when no baseline is configured yet.
verify_determinism_inline() {
    local expected_map
    expected_map="$(jq -c '
        .data_store.test_repos[]?.local_tests[]?.completion_handlers[]?
        | select(.kind == "Sha256Determinism")
        | .expected // {}
    ' "$TEST_CFG_CI" 2>/dev/null | head -n1)"
    if [[ -z "$expected_map" || "$expected_map" == "null" ]]; then
        log "No Sha256Determinism handler configured; skipping inline SHA check"
        return 0
    fi

    local verdict_file="$ARTIFACTS_DIR/determinism_verdict.json"
    local results="{}"
    local fail=0
    local id state_file actual expected passed

    for id in $TEST_REACTION_IDS; do
        state_file="$ARTIFACTS_DIR/final_reaction_state__${id}.json"
        actual="$(jq -r '
            (.reaction_observer.logger_results[]?
                | select(.logger_name == "DeterminismHash")
                | .summary.sha256) // empty
        ' "$state_file" 2>/dev/null)"
        expected="$(echo "$expected_map" | jq -r --arg id "$id" '.[$id] // empty' 2>/dev/null)"

        if [[ -z "$actual" ]]; then
            log "WARNING: [$id] no DeterminismHash logger summary; cannot verify"
            passed="false"
            fail=1
        elif [[ -z "$expected" ]]; then
            log "[$id] no expected baseline; actual=$actual (treating as pass)"
            passed="true"
        elif [[ "$actual" == "$expected" ]]; then
            log "[$id] determinism check passed (sha256=${actual:0:12}…)"
            passed="true"
        else
            log "ERROR: [$id] determinism mismatch expected=$expected actual=$actual"
            passed="false"
            fail=1
        fi

        results="$(echo "$results" | jq --arg id "$id" --arg a "$actual" --arg e "$expected" --argjson p "$passed" \
            '.[$id] = {actual: ($a // null), expected: ($e // null), passed: $p}')"
    done

    jq --arg run "$TEST_RUN_ID" --argjson results "$results" \
        '{test_run_id: $run, results: $results}' \
        <<<'{}' > "$verdict_file"
    echo "::group::Determinism verdict (inline)"
    jq '.' "$verdict_file" 2>/dev/null || cat "$verdict_file"
    echo "::endgroup::"

    return "$fail"
}

write_step_summary() {
    if [[ -z "${GITHUB_STEP_SUMMARY:-}" ]]; then
        return 0
    fi

    local out="$GITHUB_STEP_SUMMARY"

    {
        echo "## E2E test summary — \`$TEST_RUN_ID\`"
        echo
        echo "- transport: embedded drasi-lib (in-process, no external drasi-server)"
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

patch_configs
start_test_service

poll_rc=0
if wait_for_reactions_stopped; then
    for id in $TEST_REACTION_IDS; do
        fetch_final_reaction_state "$id" || poll_rc=1
    done
else
    poll_rc=1
fi
print_summary

determinism_rc=0
verify_test_run_status || true
verify_determinism_inline || determinism_rc=$?
write_step_summary

if (( poll_rc != 0 )); then
    exit "$poll_rc"
fi

if (( determinism_rc != 0 )); then
    exit "$determinism_rc"
fi

exit 0
