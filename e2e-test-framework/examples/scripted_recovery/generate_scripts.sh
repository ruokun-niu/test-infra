#!/usr/bin/env bash
set -euo pipefail

destination="${1:?Usage: bash generate_scripts.sh NEW_DIRECTORY}"
[[ ! -e "$destination" ]] || { printf 'Refusing to overwrite %s\n' "$destination" >&2; exit 1; }
mkdir -p "$destination"
for part in 0 1 2; do
    jq -cn --argjson part "$part" '
        (if $part == 0 then
            {kind: "Header", start_time: "2025-01-01T00:00:00Z",
             description: "12000 inserts, pauses after 4000 and 8000; three files"}
         else empty end),
        (range($part * 4000 + 1; ($part + 1) * 4000 + 1) as $ordinal |
            {kind: "SourceChange", offset_ns: ($ordinal * 1000000),
             source_change_event: {op: "i", reactivatorStart_ns: 0, reactivatorEnd_ns: 0,
                payload: {source: {db: "script-db", table: "node", ts_ns: ($ordinal * 1000000), lsn: $ordinal},
                          before: null, after: {id: ("item-" + ($ordinal | tostring)),
                          labels: ["Item"], properties: {ordinal: $ordinal}}}}}),
        (if $part < 2 then
            {kind: "PauseCommand", offset_ns: (($part + 1) * 4000 * 1000000),
             label: ("after-" + (($part + 1) * 4000 | tostring))}
         else {kind: "Finish", offset_ns: 12000000000} end)
    ' > "$destination/changes_00${part}.jsonl"
done