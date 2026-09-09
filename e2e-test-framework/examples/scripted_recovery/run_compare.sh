#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRAMEWORK="$(cd "$HERE/../.." && pwd)"
TEST_SERVICE_BIN="${TEST_SERVICE_BIN:-$FRAMEWORK/target/debug/test-service}"
DRASI_SERVER_BIN="${DRASI_SERVER_BIN:-}"
SERVICE_PORT="${SERVICE_PORT:-63125}"
ADMIN_PORT="${ADMIN_PORT:-8092}"
SOURCE_PORT="${SOURCE_PORT:-50063}"
REACTION_PORT="${REACTION_PORT:-50064}"
TIMEOUT_SECS="${TIMEOUT_SECS:-300}"
PAUSE_SECS="${PAUSE_SECS:-5}"
VERIFY_PLUGINS=true
VARIANT=pause
CRASH_INJECTED=false
SERVICE_PID=""
SERVER_PID=""
RUN_ID=local.scripted_recovery.run

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

request() {
    curl -fsS --connect-timeout 2 --max-time 10 --request "${2:-GET}" "$1"
}

stop_children() {
    local pid deadline
    for pid in "$SERVICE_PID" "$SERVER_PID"; do
        [[ -n "$pid" ]] || continue
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            deadline=$((SECONDS + 10))
            while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do sleep 0.1; done
            if kill -0 "$pid" 2>/dev/null; then
                printf 'Cleanup: process %s did not exit on TERM; forcing shutdown.\n' "$pid" >&2
                kill -KILL "$pid" 2>/dev/null || true
            fi
        fi
        wait "$pid" 2>/dev/null || true
    done
    SERVICE_PID="" SERVER_PID=""
}

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    if (( exit_code != 0 )); then
        jq -n --argjson code "$exit_code" '{passed:false, exit_code:$code}' > "$ROOT/verdict.json"
        printf 'Failure artifacts: %s\n' "$ROOT" >&2
    fi
    stop_children
    exit "$exit_code"
}

wait_for() {
    local description="$1" deadline=$((SECONDS + TIMEOUT_SECS)) pid ret
    shift
    while (( SECONDS < deadline )); do
        for pid in "$SERVICE_PID" "$SERVER_PID"; do
            [[ -z "$pid" ]] || kill -0 "$pid" 2>/dev/null || fail "Process $pid exited; see $WORK logs"
        done
        ret=0
        "$@" || ret=$?
        (( ret != 0 )) || return 0
        (( ret == 1 )) || fail "Invalid data while waiting for $description"
        sleep 0.2
    done
    fail "Timed out waiting for $description"
}

generator() {
    request "$SOURCE_URL" > "$WORK/source-current.json" || return 1
    jq -e '.source_change_generator |
        if .status == "Error" or .status == "Stopped" or (.state | type) != "object"
        then error("Generator failed") else .state end' \
        "$WORK/source-current.json" > "$WORK/generator-current.json" || return 2
}

pause_probe() {
    generator || return $?
    jq -e --arg label "after-$1" '.status == "Paused" and
        .previous_record.scripted.record.label == $label' "$WORK/generator-current.json" >/dev/null
}

finished_probe() {
    generator || return $?
    jq -e '.status == "Finished"' "$WORK/generator-current.json" >/dev/null
}

count_probe() {
    request "$SERVICE_URL/reactions/items" > "$WORK/reaction-current.json" || return 1
    jq -e --argjson expected "$1" '.reaction_observer |
        if .error_message != null or .status == "Error" then error("Reaction failed")
        elif (.result_summary.reaction_invocation_count | type) != "number" then error("Missing count")
        elif .result_summary.reaction_invocation_count > $expected then error("Extra results")
        else .result_summary.reaction_invocation_count == $expected end' \
        "$WORK/reaction-current.json" >/dev/null
}

snapshot_probe() {
    request "$ADMIN_URL/api/v1/queries/items/results" > "$WORK/snapshot-current.json" || return 1
    jq -e --argjson count "$1" '
        if .success != true or (.data | type) != "array" then error("Invalid snapshot")
        else (.data | map(.Ordinal) | sort) == [range(1; $count + 1)] end
    ' "$WORK/snapshot-current.json" >/dev/null
}

stopped_probe() {
    request "$SERVICE_URL/reactions/items" > "$WORK/reaction-final.json" || return 1
    jq -e '.reaction_observer.status == "Stopped"' "$WORK/reaction-final.json" >/dev/null
}

health_probe() { request "$ADMIN_URL/health" > "$WORK/health.json" || return 1; }

start_server() {
    (
        cd "$WORK"
        export RUST_LOG="${DRASI_RUST_LOG:-info,drasi_lib::queries=debug,drasi_lib::reactions=debug}"
        exec "$DRASI_SERVER_BIN" --config "$WORK/server.yaml"
    ) >> "$WORK/drasi-server.log" 2>&1 &
    SERVER_PID=$!
    wait_for 'Drasi health' health_probe
}

restart_server() {
    local killed_pid="$SERVER_PID" killed_at
    [[ -n "$killed_pid" ]] && kill -0 "$killed_pid" 2>/dev/null || fail 'Server exited before crash injection'
    kill -KILL "$killed_pid"
    wait "$killed_pid" 2>/dev/null || true
    SERVER_PID=""
    CRASH_INJECTED=true
    killed_at="$(date -u +%FT%TZ)"
    jq -n --arg variant "$VARIANT" --argjson pid "$killed_pid" --arg date "$killed_at" \
        '{variant:$variant, killed_pid:$pid, killed_at_utc:$date, input_boundary:4000,
          pending_query_work_confirmed:null}' > "$WORK/crash.json"
    printf '\n=== Restart after SIGKILL (%s) %s ===\n' "$VARIANT" "$killed_at" >> "$WORK/drasi-server.log"
    printf '%s: restarting Drasi with saved data; generator stays paused\n' "$VARIANT"
    start_server
    jq --argjson pid "$SERVER_PID" '. + {restarted_pid:$pid}' "$WORK/crash.json" > "$WORK/crash-restarted.json"
}

verify_paused_recovery() {
    local boundary="$1"
    generator || fail 'Cannot read generator after restart'
    cp "$WORK/generator-current.json" "$WORK/generator-after-restart.json"
    jq -e -s '.[0] == .[1]' "$WORK/generator-pause-$boundary.json" \
        "$WORK/generator-after-restart.json" >/dev/null || fail 'Generator cursor changed during restart'
    wait_for "restored $boundary query rows before resume" snapshot_probe "$boundary"
    cp "$WORK/snapshot-current.json" "$WORK/snapshot-restored.json"
    wait_for "recovered $boundary deliveries before resume" count_probe "$boundary"
    cp "$WORK/reaction-current.json" "$WORK/reaction-recovered.json"
}

maybe_restart() {
    local phase="$1" boundary="$2" mode="$3"
    [[ "$mode" != uninterrupted && "$boundary" == 4000 ]] || return 0
    case "$VARIANT:$phase" in
        restart-immediate:paused|restart-caught-up:caught-up)
            restart_server
            verify_paused_recovery "$boundary"
            ;;
    esac
}

prepare_config() {
    jq --arg repo "$HERE/dev_repo" --argjson ignored "$1" \
        --argjson source_port "$SOURCE_PORT" --argjson reaction_port "$REACTION_PORT" '
        .data_store.test_repos[0].source_path = $repo |
        .data_store.test_repos[0].local_tests[0].test_folder = "pause_comparison" |
        .data_store.test_repos[0].local_tests[0].description = "12000-change pause comparison" |
        .data_store.test_repos[0].local_tests[0].sources[0].source_change_generator.ignore_scripted_pause_commands = $ignored |
        .data_store.test_repos[0].local_tests[0].sources[0].source_change_dispatchers = [
            {kind:"Grpc", host:"127.0.0.1", port:$source_port, source_id:"script-db",
             tls:false, batch_events:false, timeout_seconds:30},
            {kind:"JsonlFile", max_events_per_file:4000}] |
        .data_store.test_repos[0].local_tests[0].reactions = [{
            test_reaction_id:"items", stop_triggers:[], output_handler:{
                kind:"Grpc", host:"127.0.0.1", port:$reaction_port,
                correlation_metadata_key:"x-query-sequence", query_ids:["items"], include_initial_state:false}}] |
        .test_run_host.test_runs[0].reactions = [{test_reaction_id:"items", start_immediately:true,
            output_loggers:[{kind:"JsonlFile",max_lines_per_file:4000}]}]
    ' "$HERE/config.json" > "$WORK/config.json"
    jq --argjson admin "$ADMIN_PORT" --argjson source "$SOURCE_PORT" \
        --arg endpoint "grpc://127.0.0.1:$REACTION_PORT" --argjson verify "$VERIFY_PLUGINS" '
        .port=$admin | .sources[0].port=$source | .reactions[0].endpoint=$endpoint |
        .verifyPlugins=$verify' "$HERE/server.json" > "$WORK/server.yaml"
}

collect_records() {
    local filename
    while IFS= read -r filename; do cat "$filename"; done < <(find "$1" -type f -name "$2" | LC_ALL=C sort)
}

run_case() {
    local mode="$1" ignored="$2" boundary hold_until
    WORK="$ROOT/$mode"
    mkdir -p "$WORK/data"
    prepare_config "$ignored"
    start_server
    (
        cd "$WORK"
        export RUST_LOG="${TEST_SERVICE_RUST_LOG:-info}"
        exec "$TEST_SERVICE_BIN" --config "$WORK/config.json" --port "$SERVICE_PORT"
    ) > "$WORK/test-service.log" 2>&1 &
    SERVICE_PID=$!
    wait_for 'generator API' generator
    printf '%s: starting 12000 changes\n' "$mode"
    request "$SOURCE_URL/start" POST
    if [[ "$ignored" == false ]]; then
        for boundary in 4000 8000; do
            wait_for "pause after $boundary" pause_probe "$boundary"
            cp "$WORK/generator-current.json" "$WORK/generator-pause-$boundary.json"
            jq -e --argjson next "$((boundary + 1))" \
                '.next_record.record.source_change_event.payload.source.lsn == $next' \
                "$WORK/generator-current.json" >/dev/null
            maybe_restart paused "$boundary" "$mode"
            wait_for "$boundary received results" count_probe "$boundary"
            wait_for "$boundary query rows" snapshot_probe "$boundary"
            cp "$WORK/reaction-current.json" "$WORK/reaction-pause-$boundary.json"
            hold_until=$((SECONDS + PAUSE_SECS))
            while (( SECONDS < hold_until )); do
                pause_probe "$boundary" || fail 'Generator did not remain paused'
                count_probe "$boundary" || fail 'Receiver count changed during pause'
                jq -e -s '.[0] == .[1]' "$WORK/generator-pause-$boundary.json" \
                    "$WORK/generator-current.json" >/dev/null || fail 'Paused cursor changed'
                sleep 0.2
            done
            maybe_restart caught-up "$boundary" "$mode"
            printf '%s: paused at %s results; resuming at input %s\n' "$mode" "$boundary" "$((boundary + 1))"
            request "$SOURCE_URL/start" POST
        done
    fi
    wait_for 'generator finish' finished_probe
    cp "$WORK/generator-current.json" "$WORK/generator-final.json"
    wait_for '12000 received results' count_probe 12000
    wait_for '12000 query rows' snapshot_probe 12000
    cp "$WORK/snapshot-current.json" "$WORK/snapshot-final.json"
    request "$SERVICE_URL/reactions/items/stop" POST
    wait_for 'reaction log finalization' stopped_probe
    local storage="$WORK/cache/test_runs/$RUN_ID"
    collect_records "$storage/sources/script-db" '*.jsonl' |
        jq -s '[.[].payload.after.properties.ordinal]' > "$WORK/input-ordinals.json"
    collect_records "$storage/reactions/items" 'outputs*.jsonl' | jq -s '
        [.[].payload.request_body | select(has("result")) | .result |
            if .type != "ADD" or (.after.Ordinal | type) != "number"
            then error("Unexpected result") else .after.Ordinal end]
    ' > "$WORK/delivered-ordinals.json"
    jq -e '. == [range(1;12001)]' "$WORK/input-ordinals.json" >/dev/null || fail 'Input sequence mismatch'
    jq -e '(sort) == [range(1;12001)]' "$WORK/delivered-ordinals.json" >/dev/null || fail 'Missing or duplicate delivery'
    jq -e '.reaction_observer.result_summary.reaction_invocation_count == 12000' \
        "$WORK/reaction-final.json" >/dev/null
    jq '.data | sort_by(.Ordinal)' "$WORK/snapshot-final.json" > "$WORK/rows.json"
        jq -n --arg mode "$mode" --argjson injected "$CRASH_INJECTED" \
                '{passed:true, mode:$mode, input_count:12000, receiver_count:12000,
                    injected_crash:$injected, pending_query_work_confirmed:null}' > "$WORK/verdict.json"
    printf 'PASS: %s, 12000 inputs, 12000 received results, 12000 query rows\n' "$mode"
    stop_children
}

main() {
    local tool port seen=" " digest_service digest_server case_name
    while (( $# > 0 )); do
        case "$1" in
            --drasi-server-bin|--test-service-bin)
                (( $# >= 2 )) || fail "Missing value for $1"
                if [[ "$1" == --drasi-server-bin ]]; then DRASI_SERVER_BIN="$2"; else TEST_SERVICE_BIN="$2"; fi
                shift 2 ;;
            --allow-local-plugins) VERIFY_PLUGINS=false; shift ;;
            --variant)
                (( $# >= 2 )) || fail 'Missing value for --variant'
                VARIANT="$2"; shift 2 ;;
            --help) printf 'Usage: bash run_compare.sh --drasi-server-bin PATH [--test-service-bin PATH] [--allow-local-plugins] [--variant pause|restart-caught-up|restart-immediate]\n'; return ;;
            *) fail "Unknown option: $1" ;;
        esac
    done
    case "$VARIANT" in pause|restart-caught-up|restart-immediate) ;; *) fail "Unknown variant: $VARIANT" ;; esac
    for tool in jq curl lsof shasum; do command -v "$tool" >/dev/null || fail "Missing command: $tool"; done
    [[ "$TIMEOUT_SECS" =~ ^[1-9][0-9]*$ && "$PAUSE_SECS" =~ ^[1-9][0-9]*$ ]] || fail 'Timeout/pause seconds must be positive integers'
    for port in "$SERVICE_PORT" "$ADMIN_PORT" "$SOURCE_PORT" "$REACTION_PORT"; do
        [[ "$port" =~ ^[1-9][0-9]*$ ]] && (( port < 65536 )) || fail "Invalid port: $port"
        [[ "$seen" != *" $port "* ]] || fail 'Ports must be distinct'
        seen="$seen$port "
        if lsof -nP -iTCP:"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then fail "Port $port occupied"; fi
    done
    [[ -f "$TEST_SERVICE_BIN" && -x "$TEST_SERVICE_BIN" ]] || fail "Build test-service first: $TEST_SERVICE_BIN"
    [[ -f "$DRASI_SERVER_BIN" && -x "$DRASI_SERVER_BIN" ]] || fail 'Set DRASI_SERVER_BIN or --drasi-server-bin'
    TEST_SERVICE_BIN="$(cd "$(dirname "$TEST_SERVICE_BIN")" && pwd)/$(basename "$TEST_SERVICE_BIN")"
    DRASI_SERVER_BIN="$(cd "$(dirname "$DRASI_SERVER_BIN")" && pwd)/$(basename "$DRASI_SERVER_BIN")"
    ROOT="$(mktemp -d "${TMPDIR:-/tmp}/scripted-pause-compare.XXXXXX")"
    ROOT="$(cd "$ROOT" && pwd)"
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    printf 'Artifacts: %s\n' "$ROOT"
    [[ -d "$HERE/dev_repo/pause_comparison/sources/script-db/source_change_scripts" ]] || fail 'Example script files are missing'
    digest_service="$(shasum -a 256 "$TEST_SERVICE_BIN")"
    digest_server="$(shasum -a 256 "$DRASI_SERVER_BIN")"
    jq -n --arg service "$TEST_SERVICE_BIN" --arg server "$DRASI_SERVER_BIN" \
        --arg service_sha "${digest_service%% *}" --arg server_sha "${digest_server%% *}" \
                --arg date "$(date -u +%FT%TZ)" --argjson hold "$PAUSE_SECS" --argjson verify "$VERIFY_PLUGINS" --arg variant "$VARIANT" \
        '{test_service:$service, drasi_server:$server, service_sha256:$service_sha,
                    server_sha256:$server_sha, started_at_utc:$date, pause_seconds:$hold, verify_plugins:$verify,
                    variant:$variant}' > "$ROOT/run.json"
    SERVICE_URL="http://127.0.0.1:$SERVICE_PORT/api/test_runs/$RUN_ID"
    SOURCE_URL="$SERVICE_URL/sources/script-db"
    ADMIN_URL="http://127.0.0.1:$ADMIN_PORT"
    case_name="$VARIANT"
    [[ "$VARIANT" != pause ]] || case_name=paused
    run_case uninterrupted true
    run_case "$case_name" false
    jq -e -s '.[0] == .[1]' "$ROOT/uninterrupted/rows.json" "$ROOT/$case_name/rows.json" >/dev/null
    jq -e -s '(.[0] | sort) == (.[1] | sort)' "$ROOT/uninterrupted/delivered-ordinals.json" \
        "$ROOT/$case_name/delivered-ordinals.json" >/dev/null
    [[ "$VARIANT" == pause || "$CRASH_INJECTED" == true ]] || fail 'Requested crash was not injected'
    jq -n --arg variant "$VARIANT" --argjson injected "$CRASH_INJECTED" \
        '{passed:true, variant:$variant, changes:12000, script_files:3, pause_after:[4000,8000],
          uninterrupted_count:12000, variant_count:12000, injected_crash:$injected,
          pending_query_work_confirmed:null,
          pending_work_coverage:(if $variant == "restart-immediate" then "unverified" else "not_targeted" end)}' > "$ROOT/verdict.json"
    printf 'PASS: %s preserved the 12000-result count and identities. Artifacts: %s\n' "$VARIANT" "$ROOT"
    if [[ "$VARIANT" == restart-immediate ]]; then
        printf 'INCONCLUSIVE COVERAGE: immediate kill does not prove query work was pending; inspect replay/checkpoint logs.\n'
    fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then main "$@"; fi