# Building Comfort &mdash; Internal drasi-lib instance

End-to-end test that runs **drasi-lib in-process** inside the E2E test
service. No external Drasi Server is required.

## What this test does

1. The test service generates change events for a `BuildingHierarchy` model
   (Building &rarr; Floor &rarr; Room with `temperature` / `humidity` /
   `co2` sensor properties).
2. Events are delivered via an **in-process channel** directly to a
   drasi-lib instance hosted by the test service (`source_change_dispatchers
   .kind = "DrasiLibInstanceChannel"`).
3. The drasi-lib instance evaluates a continuous Cypher query
   (`all-rooms`: `MATCH (r:Room) RETURN ...`) and pushes results back to
   the test service over the same in-process channel
   (`output_handler.kind = "DrasiLibInstanceChannel"`).
4. Results are written through the configured loggers (JSONL file +
   performance metrics) and the test stops after **100,000** events
   (`stop_triggers.RecordCount`).

```
test-service ── in-process channel ──> drasi-lib ── in-process channel ──> test-service
   (Model source data generator)        (queries + reactions)              (loggers)
```

This is the lowest-friction way to exercise drasi-lib end-to-end: there is
no network hop, no separate process to start, and no plugin install step.

## Configuration highlights

- `data_store.test_repos[0].local_tests[0].drasi_lib_instances[0]`: the
  embedded drasi-lib instance, with one source (`facilities-db`,
  `kind: application`), one query (`all-rooms`), and one reaction
  (`building-comfort-alerts`).
- `data_store_path: "./test_data_cache"` and `source_path: "./dev_repo"`
  are resolved relative to this folder, so paths stay valid when the test
  is launched from inside the folder.

## Prerequisites

- This repository buildable via `cargo build --release`.
- No external services.

## Run the test

All commands assume you are in this folder.

### Option A: helper script

```bash
./run_test.sh
```

Use `./run_test_debug.sh` for verbose tracing.

### Option B: cargo run directly

The script is just a wrapper around `cargo run` against the workspace's
`test-service` crate. The equivalent invocation from this folder is:

```bash
cargo run --release \
  --manifest-path ../../../test-service/Cargo.toml \
  -- --config config.json
```

Tune `RUST_LOG` to control log verbosity, e.g.:

```bash
RUST_LOG="off,test_run_host=info,test_run_service=info,test_data_store=info" \
  cargo run --release \
    --manifest-path ../../../test-service/Cargo.toml \
    -- --config config.json
```

## Inspect / control while running

The test service exposes a REST API on `http://localhost:8080`. The
`web_api_drasi_lib_instance.http`, `web_api_source.http`,
`web_api_query.http`, and `web_api_reaction.http` files in this folder
contain ready-to-run requests for VS Code's REST Client extension (or
`curl`).

## Output

- `./test_data_cache/` &mdash; transient test data store; cleared on each
  run (`delete_on_start: true`, `delete_on_stop: true`).
- JSONL output for the `building-comfort` reaction is written under the
  test data cache by the `JsonlFile` output logger
  (`max_lines_per_file: 15000`).
- Per-query performance metrics are emitted by the `PerformanceMetrics`
  output logger.
