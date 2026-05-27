# Building Comfort &mdash; External Drasi Server (gRPC, adaptive batching)

Same end-to-end shape as `drasi_server_grpc/`, but the test service uses the
**adaptive gRPC dispatcher** to optimise throughput under load.

## What this test does

1. The E2E test service generates change events for a `BuildingHierarchy`
   model (Rooms with `temperature` / `humidity` / `co2`).
2. Events are dispatched over gRPC to the Drasi Server source `facilities-db`
   on **localhost:50051** with **adaptive batching** enabled
   (`batch_size: 1000`, `batch_timeout_ms: 50`, dynamic 1&ndash;2000 messages
   per batch).
3. Drasi Server runs the `building-comfort` Cypher query
   (`MATCH (r:Room) RETURN ...`) and streams results back to the test
   service's gRPC reaction handler at **0.0.0.0:50052**.
4. The test service stops after **100,000** events.

```
test-service  --adaptive gRPC--> Drasi Server  --gRPC reaction--> test-service
   :8080        batch up to 2000     :50051         :50052
```

## Prerequisites

- One of:
  - a prebuilt `drasi-server` binary (see the [official download
    instructions](https://drasi.io/drasi-server/how-to-guides/installation/download-binary/)); **or**
  - a checkout of the [drasi-server](https://github.com/drasi-project/drasi-server)
    repo with a working Rust toolchain.

  The `source/grpc`, `reaction/grpc`, and `reaction/log` plugins are
  auto-installed.
- This repository buildable via `cargo build --release`.

## 1. Start the external Drasi Server

Pick whichever option matches how you installed Drasi Server. Run from
this folder so the relative `--config` path resolves.

### Option A: prebuilt binary

```bash
<path/to>/bin/drasi-server --config drasi_server_config.yaml
```

### Option B: cargo run from a drasi-server repo checkout

```bash
cargo run --release --manifest-path <path/to/drasi-server>/Cargo.toml -- --config drasi_server_config.yaml
```

Drasi Server will start:

- gRPC source `facilities-db` on `0.0.0.0:50051`.
- Continuous query `building-comfort` (auto-started).
- gRPC reaction `building-comfort-out` connecting to `grpc://localhost:50052`
  with `batchSize: 100` to match the higher inbound rate.

## 2. Run the E2E test

From this folder:

```bash
./run_test.sh
```

Logs from the adaptive batcher are silenced by default; set
`RUST_LOG=info,test_run_host::utils::adaptive_batcher=debug` to inspect
batching decisions.

## Default ports

| Component                                     | Port  |
|-----------------------------------------------|-------|
| Test service REST API                         | 8080  |
| Drasi Server admin API                        | 8080 (override one of them) |
| Drasi Server gRPC source (`facilities-db`)    | 50051 |
| Test service gRPC reaction handler            | 50052 |

## Troubleshooting

- **Throughput plateaus** &mdash; Re-enable adaptive batcher logs to confirm
  batches grow toward the upper bound (~2000). If they stay small, the
  bottleneck is downstream of the dispatcher (Drasi Server or query plan).
- **Connection refused on 50051 / 50052** &mdash; See
  `drasi_server_grpc/README.md` for the same diagnostics.
