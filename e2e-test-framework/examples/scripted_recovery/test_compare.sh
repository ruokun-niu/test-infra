#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/run_compare.sh"
TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/scripted-compare-tests.XXXXXX")"
WORK="$TEST_DIR"
bash "$HERE/generate_scripts.sh" "$TEST_DIR/scripts"
files=("$HERE"/dev_repo/pause_comparison/sources/script-db/source_change_scripts/*.jsonl)
[[ ${#files[@]} == 3 ]]
for filename in "${files[@]}"; do
    cmp "$filename" "$TEST_DIR/scripts/$(basename "$filename")"
done
jq -se '
    ([.[] | select(.kind == "SourceChange") | .source_change_event.payload.source.lsn] == [range(1;12001)]) and
    ([.[] | select(.kind == "PauseCommand") | .label] == ["after-4000","after-8000"]) and
    ([.[] | select(.kind == "Header")] | length == 1) and (last.kind == "Finish") and
    ([.[] | select(.kind != "Header") | .offset_ns] as $offsets | $offsets == ($offsets | sort))
' "${files[@]}" >/dev/null
jq -se 'last.kind == "PauseCommand" and last.label == "after-4000"' "${files[0]}" >/dev/null
jq -se 'first.source_change_event.payload.source.lsn == 4001 and last.label == "after-8000"' "${files[1]}" >/dev/null
jq -se 'first.source_change_event.payload.source.lsn == 8001 and last.kind == "Finish"' "${files[2]}" >/dev/null
if bash "$HERE/generate_scripts.sh" "$TEST_DIR/scripts" 2>/dev/null; then
    fail 'Generator overwrote existing scripts'
fi

request() { printf '%s\n' "$RESPONSE"; }
ADMIN_URL=http://unused
SERVICE_URL=http://unused
RESPONSE='{"success":true,"data":[{"Ordinal":2},{"Ordinal":1}]}'
snapshot_probe 2
for RESPONSE in \
    '{"success":true,"data":[]}' \
    '{"success":true,"data":[{"Ordinal":1}]}' \
    '{"success":true,"data":[{"Ordinal":1},{"Ordinal":1}]}' \
    '{"success":true,"data":[{"Ordinal":1},{"Ordinal":2},{"Ordinal":3}]}' \
    '{"success":false,"data":[]}'; do
    if snapshot_probe 2 2>/dev/null; then fail 'Invalid snapshot accepted'; fi
done
RESPONSE='{"reaction_observer":{"status":"Running","error_message":null,"result_summary":{"reaction_invocation_count":2}}}'
count_probe 2
if count_probe 1 2>/dev/null; then fail 'Extra results accepted'; fi
if count_probe 3; then fail 'Missing results accepted'; fi
(
    restart_server() { printf 'kill-restart\n' >> "$WORK/actions"; }
    verify_paused_recovery() { printf 'verify-%s\n' "$1" >> "$WORK/actions"; }
    for VARIANT in pause restart-caught-up restart-immediate; do
        : > "$WORK/actions"
        maybe_restart paused 4000 uninterrupted
        maybe_restart caught-up 4000 uninterrupted
        maybe_restart paused 8000 "$VARIANT"
        maybe_restart caught-up 8000 "$VARIANT"
        [[ ! -s "$WORK/actions" ]] || fail 'Crash triggered outside selected case/first boundary'
        maybe_restart paused 4000 "$VARIANT"
        if [[ "$VARIANT" == restart-immediate ]]; then
            [[ "$(cat "$WORK/actions")" == $'kill-restart\nverify-4000' ]] || fail 'Immediate restart order incorrect'
        else
            [[ ! -s "$WORK/actions" ]] || fail 'Restart occurred before catch-up'
        fi
        maybe_restart caught-up 4000 "$VARIANT"
        if [[ "$VARIANT" == pause ]]; then
            [[ ! -s "$WORK/actions" ]] || fail 'Default variant injected a crash'
        else
            [[ "$(cat "$WORK/actions")" == $'kill-restart\nverify-4000' ]] || fail 'Expected exactly one restart then recovery check'
        fi
    done
)

(
    generator() { printf '{"status":"Paused","next":4001}\n' > "$WORK/generator-current.json"; }
    printf '{"status":"Paused","next":4001}\n' > "$WORK/generator-pause-4000.json"
    : > "$WORK/recovery-checks"
    wait_for() {
        printf '%s %s\n' "$2" "$3" >> "$WORK/recovery-checks"
        printf '{}\n' > "$WORK/snapshot-current.json"
        printf '{}\n' > "$WORK/reaction-current.json"
    }
    verify_paused_recovery 4000
    [[ "$(cat "$WORK/recovery-checks")" == $'snapshot_probe 4000\ncount_probe 4000' ]] || fail 'Recovery checks missing'
    printf '{"status":"Paused","next":4002}\n' > "$WORK/generator-pause-4000.json"
    if (verify_paused_recovery 4000) 2>/dev/null; then fail 'Changed generator cursor accepted'; fi
)

(
    SERVER_PID=12345
    VARIANT=restart-immediate
    mkdir -p "$WORK/data"
    printf 'retained\n' > "$WORK/data/sentinel"
    printf 'config\n' > "$WORK/server.yaml"
    : > "$WORK/restart-actions"
    kill() { printf 'kill %s\n' "$*" >> "$WORK/restart-actions"; }
    wait() { printf 'wait %s\n' "$*" >> "$WORK/restart-actions"; }
    start_server() {
        [[ -z "$SERVER_PID" ]] || fail 'Stale PID during restart'
        [[ "$(cat "$WORK/data/sentinel")" == retained ]] || fail 'Data not retained'
        [[ "$(cat "$WORK/server.yaml")" == config ]] || fail 'Config changed'
        printf 'start\n' >> "$WORK/restart-actions"
        SERVER_PID=12346
    }
    restart_server
    [[ "$CRASH_INJECTED" == true && "$SERVER_PID" == 12346 ]]
    [[ "$(cat "$WORK/restart-actions")" == $'kill -0 12345\nkill -KILL 12345\nwait 12345\nstart' ]] || fail 'SIGKILL/reap/restart order incorrect'
    jq -e '.killed_pid == 12345 and .restarted_pid == 12346 and .pending_query_work_confirmed == null' \
        "$WORK/crash-restarted.json" >/dev/null
)
printf 'PASS: fixture, result checks, restart routing, cursor checks, and mocked process lifecycle. Artifacts: %s\n' "$TEST_DIR"