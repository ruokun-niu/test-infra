# Building Comfort &mdash; Embedded drasi-lib instance (CI)

End-to-end test that runs **drasi-lib in-process** inside the E2E test
service. No external Drasi Server is required. This is the CI variant of
[`local/drasi_lib`](../../local/drasi_lib) and mirrors the HTTP/gRPC
server-based variants (now the dynamic driver at
[`../../dynamic`](../../dynamic)); the difference
is that here drasi-lib runs in-process via an `application` source/reaction
instead of going over HTTP or gRPC.

## What this test does

1. The test service generates change events for a `BuildingHierarchy`
   model (1 building × 1 floor × 1 room; rooms carry
   `temperature` / `humidity` / `co2`).
2. Events are delivered via an **in-process channel** directly to a
   drasi-lib instance hosted by the test service
   (`source_change_dispatchers[].kind = "DrasiLibInstanceChannel"`).
3. The drasi-lib instance evaluates a Cypher query
   (`all-rooms`: `MATCH (r:Room) RETURN ...`) and pushes results back to
   the test service over the same in-process channel
   (`output_handler.kind = "DrasiLibInstanceChannel"`).
4. The test service stops each reaction once its `RecordCount` stop-trigger fires
   (see `config.json`).

```
test-service ── in-process channel ──> drasi-lib ── in-process channel ──> test-service
   (Model source data generator)       (queries + reactions)               (loggers)
```

## CI vs local

- **Per-query determinism check (CI-only baseline)**: `config.json`
  declares a `Sha256Determinism` completion handler. The baselines in
  `expected` are captured from the GitHub Actions runner
  (`ubuntu-latest`, x86_64). The drasi-lib variant compiles drasi-lib
  into the test-service binary, so the emitted byte stream depends on
  the build environment (OS, arch, toolchain) on top of the pinned
  `Cargo.lock`. **Running this variant locally on a different OS/arch
  (e.g. macOS arm64) will produce different SHAs and fail the check;
  that is expected.** The HTTP and gRPC variants run a pre-built
  drasi-server binary downloaded from a release, so they don't have
  this constraint and can be verified locally.
  - To re-baseline after an intentional dep bump or engine change:
    push the branch, read the actual SHAs from the failing CI run's
    `determinism_verdict.json` artifact, and paste them into
    `expected`.
- **No `delete_on_stop`**: the runner script patches `delete_on_start`
  and `delete_on_stop` to `false` so artifacts survive between phases.
- **No drasi-server binary**: drasi-lib runs in-process, so there's
  nothing to download and no admin-port patching to do.

## Run it from CI

Add a job to `.github/workflows/e2e-building-comfort.yml` that points
`WORKDIR` at this folder; copy the existing `building-comfort-drasi-server-*`
jobs and drop their drasi-server-specific steps if you want.

## Run it locally

```bash
cd e2e-test-framework/examples/building_comfort/ci/drasi_lib

export ARTIFACTS_DIR="$PWD/.local_run/artifacts"
export WORK_DIR="$PWD/.local_run/work"

./run_test_ci.sh
```

## Default ports

| Component                     | Port  |
|-------------------------------|-------|
| Test service REST API         | 63123 |

The drasi-lib host runs in-process, so it has no port of its own.

## Troubleshooting

- **`address already in use: 63123`** &mdash; another test service is
  bound. Override with `TEST_SERVICE_PORT`.
- **`Unsupported drasi-lib instance source kind`** &mdash; the embedded
  drasi-lib host only supports `kind: "application"` sources/reactions.
  Don't change those in `config.json`; if you need HTTP/gRPC transport,
  use the dynamic driver at `../../dynamic` (http_* / grpc_* variants).
- **`Determinism mismatch` on local runs** &mdash; expected. The
  baselines in `expected` are captured on the GitHub Actions runner
  (Linux x86_64); local runs on macOS arm64 (or any other host) will
  produce different SHAs because the embedded drasi-lib is compiled
  from source as part of the test-service binary. To verify the
  variant locally without failing the check, temporarily set
  `expected: {}` and `missing_baseline: "Warn"` in `config.json`
  (don't commit that change).
- **`Determinism mismatch` in CI** &mdash; means either the pinned
  `Cargo.lock` changed, drasi-lib changed behavior, or the workload
  shifted. Re-baseline by copying the actual SHAs from the failing
  run's `determinism_verdict.json` artifact into `expected`.
