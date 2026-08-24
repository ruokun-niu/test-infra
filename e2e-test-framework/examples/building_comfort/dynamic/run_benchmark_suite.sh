#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_SCRIPT="${RUN_SCRIPT:-$SCRIPT_DIR/run_dynamic.sh}"

VARIANTS="${VARIANTS:-http_standard http_adaptive grpc_standard grpc_adaptive}"
SUITE_WORK_DIR="${SUITE_WORK_DIR:-$SCRIPT_DIR/.benchmark_suite}"
SUITE_ARTIFACTS_DIR="${SUITE_ARTIFACTS_DIR:-$SCRIPT_DIR/benchmark_artifacts}"
PERF_PROFILE_ID="${PERF_PROFILE_ID:-unknown}"

: "${DRASI_SERVER_BIN:?DRASI_SERVER_BIN must point to a pinned drasi-server binary}"
: "${TEST_SERVICE_BIN:?TEST_SERVICE_BIN must point to a pre-built test-service binary}"

if [[ ! -x "$DRASI_SERVER_BIN" ]]; then
    echo "DRASI_SERVER_BIN is not executable: $DRASI_SERVER_BIN" >&2
    exit 1
fi
if [[ ! -x "$TEST_SERVICE_BIN" ]]; then
    echo "TEST_SERVICE_BIN is not executable: $TEST_SERVICE_BIN" >&2
    exit 1
fi

read -r -a variant_list <<< "${VARIANTS//,/ }"
if (( ${#variant_list[@]} == 0 )); then
    echo "At least one benchmark variant is required" >&2
    exit 1
fi

mkdir -p "$SUITE_WORK_DIR" "$SUITE_ARTIFACTS_DIR"
results_jsonl="$SUITE_ARTIFACTS_DIR/suite-results.jsonl"
: > "$results_jsonl"
suite_rc=0

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

clear_benchmark_ports() {
    local port pid
    local ports=(8090 9000 50051 50052 50053 63123)
    local pids=()

    for port in "${ports[@]}"; do
        while IFS= read -r pid; do
            [[ -n "$pid" ]] && pids+=("$pid")
        done < <(lsof -ti "tcp:$port" 2>/dev/null || true)
    done

    if (( ${#pids[@]} == 0 )); then
        return 0
    fi

    local unique_pids=()
    while IFS= read -r pid; do
        unique_pids+=("$pid")
    done < <(printf '%s\n' "${pids[@]}" | sort -un)
    pids=("${unique_pids[@]}")
    echo "Stopping ${#pids[@]} process(es) left on benchmark ports: ${pids[*]}"
    kill -TERM "${pids[@]}" 2>/dev/null || true

    for _ in $(seq 1 30); do
        local alive=()
        for pid in "${pids[@]}"; do
            kill -0 "$pid" 2>/dev/null && alive+=("$pid")
        done
        (( ${#alive[@]} == 0 )) && return 0
        pids=("${alive[@]}")
        sleep 1
    done

    echo "Force-stopping benchmark processes that did not exit: ${pids[*]}"
    kill -KILL "${pids[@]}" 2>/dev/null || true
}

configure_variant() {
    local variant="$1"
    SERVER_QUERIES_FILE="queries.json"

    case "$variant" in
        http_standard)
            SERVER_SOURCE_FILE="source_http.json"
            SERVER_REACTIONS_FILE="reactions_http.json"
            DRASI_SOURCE_PORT=9000
            TEST_CFG_SRC="$SCRIPT_DIR/config.http.json"
            ;;
        http_adaptive)
            SERVER_SOURCE_FILE="source_http_adaptive.json"
            SERVER_REACTIONS_FILE="reactions_http.json"
            DRASI_SOURCE_PORT=9000
            TEST_CFG_SRC="$SCRIPT_DIR/config.http.json"
            ;;
        grpc_standard)
            SERVER_SOURCE_FILE="source_grpc.json"
            SERVER_REACTIONS_FILE="reactions_grpc.json"
            DRASI_SOURCE_PORT=50051
            TEST_CFG_SRC="$SCRIPT_DIR/config.json"
            ;;
        grpc_adaptive)
            SERVER_SOURCE_FILE="source_grpc.json"
            SERVER_REACTIONS_FILE="reactions_grpc.json"
            DRASI_SOURCE_PORT=50051
            TEST_CFG_SRC="$SCRIPT_DIR/config.grpc_adaptive.json"
            ;;
        *)
            echo "Unsupported building comfort benchmark variant: $variant" >&2
            return 1
            ;;
    esac
}

for variant in "${variant_list[@]}"; do
    [[ -n "$variant" ]] || continue
    configure_variant "$variant"

    clear_benchmark_ports
    run_work_dir="$SUITE_WORK_DIR/$variant"
    run_artifacts_dir="$SUITE_ARTIFACTS_DIR/$variant"
    mkdir -p "$run_work_dir" "$run_artifacts_dir"

    echo "::group::$variant"
    started_at="$(date -u +%FT%TZ)"
    run_rc=0
    env \
        VARIANT="$variant" \
        DRASI_SERVER_BIN="$DRASI_SERVER_BIN" \
        TEST_SERVICE_BIN="$TEST_SERVICE_BIN" \
        SERVER_SOURCE_FILE="$SERVER_SOURCE_FILE" \
        SERVER_QUERIES_FILE="$SERVER_QUERIES_FILE" \
        SERVER_REACTIONS_FILE="$SERVER_REACTIONS_FILE" \
        DRASI_SOURCE_PORT="$DRASI_SOURCE_PORT" \
        TEST_CFG_SRC="$TEST_CFG_SRC" \
        ARTIFACTS_DIR="$run_artifacts_dir" \
        WORK_DIR="$run_work_dir" \
        bash "$RUN_SCRIPT" || run_rc=$?
    completed_at="$(date -u +%FT%TZ)"
    echo "::endgroup::"

    metrics_json="$(
        find "$run_artifacts_dir" -path '*output_log/performance_metrics/*.json' -type f -print0 |
            sort -z |
            xargs -0 -r jq -s '[
                .[] | {
                    reaction: (.test_run_reaction_id | split(".") | last),
                    record_count,
                    duration_ns,
                    records_per_second,
                    bootstrap,
                    steady_state
                }
            ]'
    )"
    [[ -n "$metrics_json" ]] || metrics_json="[]"

    jq -cn \
        --arg profile_id "$PERF_PROFILE_ID" \
        --arg variant "$variant" \
        --arg started_at "$started_at" \
        --arg completed_at "$completed_at" \
        --argjson exit_code "$run_rc" \
        --argjson metrics "$metrics_json" \
        '{
            profile_id: $profile_id,
            variant: $variant,
            started_at: $started_at,
            completed_at: $completed_at,
            exit_code: $exit_code,
            metrics: $metrics
        }' >> "$results_jsonl"

    if (( run_rc != 0 )); then
        suite_rc=1
    fi
done

clear_benchmark_ports

plugins_dir="$(dirname "$DRASI_SERVER_BIN")/plugins"
plugin_manifest="$SUITE_ARTIFACTS_DIR/plugin-manifest.json"
if [[ -d "$plugins_dir" ]]; then
    find "$plugins_dir" -maxdepth 1 \( -name '*.so' -o -name '*.dylib' \) -type f -print0 |
        sort -z |
        while IFS= read -r -d '' plugin_file; do
            jq -cn \
                --arg file "$(basename "$plugin_file")" \
                --arg sha256 "$(sha256_file "$plugin_file")" \
                '{file: $file, sha256: $sha256}'
        done |
        jq -s '.' > "$plugin_manifest"
else
    echo '[]' > "$plugin_manifest"
fi

jq -s \
    --arg profile_id "$PERF_PROFILE_ID" \
    --arg drasi_server_version "$("$DRASI_SERVER_BIN" --version 2>/dev/null | head -n 1 || echo unknown)" \
    --arg drasi_server_sha256 "$(sha256_file "$DRASI_SERVER_BIN")" \
    --arg test_service_sha256 "$(sha256_file "$TEST_SERVICE_BIN")" \
    --slurpfile plugins "$plugin_manifest" \
    '{
        profile_id: $profile_id,
        drasi_server: {
            version: $drasi_server_version,
            sha256: $drasi_server_sha256
        },
        test_service: {
            sha256: $test_service_sha256
        },
        plugins: $plugins[0],
        runs: .,
        aggregates: (
            [
                .[]
                | select(.exit_code == 0)
                | .variant as $variant
                | .metrics[]
                | {
                    variant: $variant,
                    reaction,
                    duration_ns,
                    records_per_second
                }
            ]
            | sort_by(.variant, .reaction)
            | group_by([.variant, .reaction])
            | map({
                variant: .[0].variant,
                reaction: .[0].reaction,
                samples: length,
                duration_seconds: {
                    mean: (map(.duration_ns / 1e9) | add / length),
                    min: (map(.duration_ns / 1e9) | min),
                    max: (map(.duration_ns / 1e9) | max)
                },
                records_per_second: {
                    mean: (map(.records_per_second) | add / length),
                    min: (map(.records_per_second) | min),
                    max: (map(.records_per_second) | max)
                }
            })
        )
    }' "$results_jsonl" > "$SUITE_ARTIFACTS_DIR/suite-results.json"

{
    echo "## Fixed-VM building comfort benchmark"
    echo
    echo "- profile: \`$PERF_PROFILE_ID\`"
    echo "- drasi-server: \`$("$DRASI_SERVER_BIN" --version 2>/dev/null | head -n 1 || echo unknown)\`"
    echo
    echo "| Variant | Reaction | Records | Duration (s) | Records/sec | Result |"
    echo "| --- | --- | ---: | ---: | ---: | --- |"
    jq -r '
        .runs[] as $run
        | if ($run.metrics | length) == 0 then
            "| `\($run.variant)` | n/a | n/a | n/a | n/a | \(if $run.exit_code == 0 then "pass" else "fail" end) |"
          else
            $run.metrics[]
            | "| `\($run.variant)` | `\(.reaction)` | \(.record_count) | \((.duration_ns / 1e9 * 1000 | round) / 1000) | \((.records_per_second * 100 | round) / 100) | \(if $run.exit_code == 0 then "pass" else "fail" end) |"
          end
    ' "$SUITE_ARTIFACTS_DIR/suite-results.json"
    echo
    echo "### Aggregate throughput"
    echo
    echo "| Variant | Reaction | Samples | Mean records/sec | Min | Max |"
    echo "| --- | --- | ---: | ---: | ---: | ---: |"
    jq -r '
        .aggregates[]
        | "| `\(.variant)` | `\(.reaction)` | \(.samples) | \((.records_per_second.mean * 100 | round) / 100) | \((.records_per_second.min * 100 | round) / 100) | \((.records_per_second.max * 100 | round) / 100) |"
    ' "$SUITE_ARTIFACTS_DIR/suite-results.json"
} | tee "$SUITE_ARTIFACTS_DIR/summary.md"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    cat "$SUITE_ARTIFACTS_DIR/summary.md" >> "$GITHUB_STEP_SUMMARY"
fi

exit "$suite_rc"
