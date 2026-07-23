# Building Comfort &mdash; External Drasi Server (HTTP webhook)

End-to-end test that drives an **external Drasi Server** over HTTP webhooks
instead of gRPC.

## What this test does

1. The E2E test service generates change events for a `BuildingHierarchy`
   model (1 building × 3 floors × 4 rooms; rooms carry
   `temperature` / `humidity` / `co2`).
2. Events are POSTed as HTTP webhooks to Drasi Server's source
   `facilities-db` listening on **http://localhost:9000**.
3. Drasi Server runs two Cypher queries against the same source:
   - `building-comfort` &mdash; per-room raw values
     (`MATCH (r:Room) RETURN ...`) &mdash; results POSTed to
     **http://localhost:9001/reaction**.
   - `building-comfort-floor-agg` &mdash; one-hop traversal plus
     `avg` / `min` / `max` / `count` aggregations per floor
     (`MATCH (f:Floor)-[:FLOOR_ROOM]->(r:Room) RETURN ...`) &mdash;
     results POSTed to **http://localhost:9002/reaction**.
4. The test service stops each reaction after **100,000** events
   (`stop_triggers.RecordCount`).

```
test-service --HTTP source--> Drasi Server --HTTP reaction--> test-service
   :8080         :9000              :8080      :9001/reaction (per-room)
                                               :9002/reaction (floor-agg)
```

## Prerequisites

- One of:
  - a prebuilt `drasi-server` binary (see the [official download
    instructions](https://drasi.io/drasi-server/how-to-guides/installation/download-binary/)
    &mdash; downloads the binary into `./bin/drasi-server`); **or**
  - a checkout of the [drasi-server](https://github.com/drasi-project/drasi-server)
    repo with a working Rust toolchain.

  In either case the `source/http`, `reaction/http`, and `reaction/log`
  plugins are fetched automatically (`autoInstallPlugins: true`).
- This repository buildable via `cargo build --release`.

## 1. Start the external Drasi Server

Pick whichever option matches how you installed Drasi Server. In both
cases run from this folder so the relative `--config` path resolves.

### Option A: prebuilt binary

Replace `<path/to>` with the relative path from this folder to your
Drasi Server install (the binary lives at `<path/to>/bin/drasi-server`):

```bash
<path/to>/bin/drasi-server --config drasi_server_config.yaml
```

### Option B: cargo run from a drasi-server repo checkout

Replace `<path/to/drasi-server>` with the relative path from this folder
to your local drasi-server repo:

```bash
cargo run --release --manifest-path <path/to/drasi-server>/Cargo.toml -- --config drasi_server_config.yaml
```

Drasi Server will:

- Bind its admin API on `0.0.0.0:8080`.
- Start the HTTP webhook source `facilities-db` on `0.0.0.0:9000`.
- Auto-start the `building-comfort` query.
- Auto-start the `building-comfort-out` HTTP reaction, posting results to
  `http://localhost:9001/reaction`.

> Drasi Server's admin port (`8080`) clashes with the E2E test service's
> default API port. Edit `host`/`port` in `drasi_server_config.yaml` (or
> change the test service's API port) so they don't collide.

## 2. Run the E2E test

From this folder:

```bash
./run_test.sh
```

Use `./run_test_debug.sh` for verbose tracing.

### Optional: per-reaction SHA-256 determinism check

Set `SHA_CHECK=1` to attach a `DeterminismHash` output logger to each
reaction and a `Sha256Determinism` completion handler (with an empty
baseline and `missing_baseline: "Warn"`) to the test definition. The
run then prints the per-reaction SHA-256 in the framework log without
failing the test:

```bash
SHA_CHECK=1 ./run_test.sh
```

Requires `jq` on `PATH`. Two successive runs on the same machine should
produce identical SHAs; the CI variant
[`ci/drasi_server_http`](../../ci/drasi_server_http) maintains a locked
baseline. To enforce a baseline locally, paste the SHAs into a
`Sha256Determinism` block in `config.json` directly (see the CI variant's
`config.json` for the exact shape).

### Optional: query index backend (`DRASI_INDEX`)

By default the query runs against Drasi Server's **in-memory** index. Set
`DRASI_INDEX` to pick the backend the auto-launched drasi-server uses:

```bash
# In-memory indexes (default).
DRASI_INDEX=memory ./run_test.sh

# Persistent RocksDB indexes. The runner patches `persistIndex: true` into a
# temporary copy of drasi_server_config.yaml (the checked-in file is not
# modified) and clears ./data first for a clean cold start.
DRASI_INDEX=rocksdb ./run_test.sh
```

With `rocksdb`, drasi-server materializes its index under `./data/<id>/index`
in this folder (git-ignored). Query results are identical across backends, so
running both with `SHA_CHECK=1` and comparing the per-reaction SHAs verifies
that memory and rocksdb produce the same output.

> **Note:** `memory` and `rocksdb` are the only index backends compiled into
> the drasi-server binary. Redis-backed indexes exist only on the Kubernetes
> Drasi Platform path (`local/drasi_platform/query_container_redis/`), not for
> the binary, so they are out of scope here.

## Inspect / control while running

The test service exposes a REST API on `http://localhost:8080`. The
`web_api_source.http`, `web_api_query.http`, and `web_api_reaction.http`
files in this folder contain ready-to-run requests for VS Code's REST
Client extension (or `curl`).

## Default ports

| Component                                          | Port                    |
|----------------------------------------------------|-------------------------|
| Test service REST API                              | 8080                    |
| Drasi Server admin API                             | 8080 (override)         |
| Drasi Server HTTP source (`facilities-db`)         | 9000                    |
| Test service HTTP reaction handler (per-room)      | 9001 (path `/reaction`) |
| Test service HTTP reaction handler (floor-agg)     | 9002 (path `/reaction`) |

## Troubleshooting

- **Connection refused on 9000** &mdash; Drasi Server is not running or the
  HTTP source plugin failed to bind. Confirm the server logs show
  `http source listening on 0.0.0.0:9000`.
- **No reaction events received** &mdash; The reaction handler must be
  listening on port 9001 before Drasi Server starts pushing. The test
  service binds it as soon as the run starts; if the test service starts
  *after* Drasi Server, the first batch may be lost while the reaction
  retries.
- **`address already in use: 8080`** &mdash; Either the test service or
  Drasi Server is already bound. Stop the other process or change one of
  their ports.
