# Building Comfort &mdash; External Drasi Server (gRPC)

End-to-end test that drives an **external Drasi Server** with a synthetic
building-hierarchy data stream over gRPC.

## What this test does

1. The E2E test service generates change events for a `BuildingHierarchy` model
   (Building &rarr; Floor &rarr; Room with `temperature` / `humidity` / `co2`
   sensor properties).
2. Events are dispatched as `drasi.v1.SourceService` gRPC calls to a Drasi
   Server source named `facilities-db` listening on **localhost:50051**.
3. Drasi Server runs the `building-comfort` continuous Cypher query
   (`MATCH (r:Room) RETURN ...`) and streams results back as
   `drasi.v1.ReactionService` calls to the test service's gRPC reaction
   handler at **0.0.0.0:50052**.
4. The test service logs results and stops after **100,000** events
   (`stop_triggers.RecordCount`).

```
test-service  --gRPC source-->  Drasi Server  --gRPC reaction-->  test-service
   (ports 8080)    :50051          (port 8080)        :50052          (logs/JSONL)
```

## Prerequisites

- One of:
  - a prebuilt `drasi-server` binary (see the [official download
    instructions](https://drasi.io/drasi-server/how-to-guides/installation/download-binary/)
    &mdash; downloads the binary into `./bin/drasi-server`); **or**
  - a checkout of the [drasi-server](https://github.com/drasi-project/drasi-server)
    repo with a working Rust toolchain.

  In either case the `source/grpc`, `reaction/grpc`, and `reaction/log`
  plugins are fetched automatically (`autoInstallPlugins: true`).
- This repository's E2E test service buildable via `cargo build --release`.

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

- Bind its admin API on `0.0.0.0:8080` (the test service uses a different
  port, see below).
- Start the `facilities-db` gRPC source on `0.0.0.0:50051`.
- Auto-start the `building-comfort` query.
- Auto-start the `building-comfort-out` gRPC reaction, which connects to
  `grpc://localhost:50052`.

> Drasi Server's admin port (`8080`) clashes with the E2E test service's
> default API port. Edit `host`/`port` in `drasi_server_config.yaml` (or
> change the test service's API port) so they don't collide.

## 2. Run the E2E test

From this folder:

```bash
./run_test.sh
```

For verbose tracing use `./run_test_debug.sh`.

## Inspect / control while running

The test service exposes a REST API on `http://localhost:8080`. The
`web_api_source.http`, `web_api_query.http`, and `web_api_reaction.http`
files in this folder contain ready-to-run requests for VS Code's REST
Client extension (or `curl`).

## Default ports

| Component                                     | Port  |
|-----------------------------------------------|-------|
| Test service REST API                         | 8080  |
| Drasi Server admin API                        | 8080 (override needed) |
| Drasi Server gRPC source (`facilities-db`)    | 50051 |
| Test service gRPC reaction handler            | 50052 |

## Troubleshooting

- **Connection refused on 50051** &mdash; Drasi Server is not running or its
  source did not bind. Confirm the server logs show
  `gRPC source listening on 0.0.0.0:50051`.
- **No reaction events received** &mdash; Drasi Server's `reaction/grpc`
  plugin retries connecting to `grpc://localhost:50052`. The test service
  must be started so the gRPC reaction handler is listening before the
  reaction plugin gives up its retries (see `connectionRetryAttempts` in
  `drasi_server_config.yaml`).
- **`address already in use: 8080`** &mdash; Either the test service or
  Drasi Server is already bound. Stop the other process or change one of
  their ports.
