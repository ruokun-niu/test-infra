# Building Comfort — Dynamic component-config driver

This is a **dynamic** alternative to static per-variant server folders (e.g.
`local/drasi_server_grpc`).
Instead of a fully-populated `drasi_server_config.yaml` per variant, it boots a
**bare Drasi Server** ([base/drasi_server.empty.yaml](base/drasi_server.empty.yaml))
and applies the scenario at runtime through the Drasi Server admin REST API,
composing it from reusable building blocks under [components/server/](components/server).

## Why

As we broaden coverage (issue #71) we want to test many component
configurations (source transport/batching, query buffers/middleware, reaction
handlers) and combinations of them. Maintaining a whole server YAML per
combination doesn't scale. Here a variant is just a small component JSON (or a
different `*_FILE` selection), applied via REST.

## What varies where

| Surface | How it's configured | Why |
| --- | --- | --- |
| Drasi Server source / queries / reactions | **Dynamic** — `POST /api/v1/{sources,queries,reactions}` from `components/server/*.json` | Drasi Server accepts full component configs over REST |
| Test-service (framework) source dispatcher + reaction handler | Static `config.json` | The framework keeps dispatcher/handler in the *test definition*; its REST API only references components by id, so they can't be injected |

## Layout

```
dynamic/
  base/
    drasi_server.empty.yaml         # standard profile: in-memory index, no state store
    drasi_server.persist_index.yaml # persist_index profile: built-in RocksDB (persistIndex: true)
    drasi_server.state_store.yaml   # state_store profile: redb plugin state store
  components/server/
    source_grpc.json                # gRPC source        (POST /api/v1/sources)
    source_http.json                # HTTP source, adaptiveEnabled: false
    source_http_adaptive.json       # HTTP source, adaptiveEnabled: true (+ tuned)
    queries.json                    # array; constant across variants
    reactions_grpc.json             # array; gRPC reactions (batchSize/flush tunable)
    reactions_http.json             # array; HTTP webhook reactions
  config.json                       # test-service config (gRPC dispatcher/handlers)
  config.http.json                  # test-service config (HTTP dispatcher/handlers)
  config.grpc_adaptive.json         # test-service config (gRPC adaptive dispatcher)
  run_dynamic.sh                    # the driver
```

## Variants

Selectable via `run_dynamic.sh` env vars (or the CI workflow's `variant` input):

| Variant | Source component | Reactions | Ingress port | Test-service config |
| --- | --- | --- | --- | --- |
| `grpc_standard` | `source_grpc.json` | `reactions_grpc.json` | 50051 | `config.json` |
| `grpc_adaptive` | `source_grpc.json` | `reactions_grpc.json` | 50051 | `config.grpc_adaptive.json` (adaptive dispatcher) |
| `http_standard` | `source_http.json` (`adaptiveEnabled:false`) | `reactions_http.json` | 9000 | `config.http.json` |
| `http_adaptive` | `source_http_adaptive.json` (`adaptiveEnabled:true`) | `reactions_http.json` | 9000 | `config.http.json` |

Note `http_standard` and `http_adaptive` differ by **one swapped source
component only** — adaptive batching lives inside the Drasi Server HTTP source,
so queries, reactions and the test-service config are identical and the
determinism baselines match (verified in CI: both hashes equal the standard
baseline).

The HTTP reactions use `recoveryPolicy: auto_skip_gap`. Each test observer stops
independently when it reaches its record-count target, so one callback endpoint
can close while the other query is still draining. Skipping that expected
post-completion delivery gap prevents the HTTP reaction from entering an error
state and blocking the remaining query.

For **gRPC**, the source has no `adaptiveEnabled` field, so `grpc_adaptive` puts
adaptive on the framework *dispatcher* instead (`config.grpc_adaptive.json`);
gRPC streaming delivers each event as a discrete message, so that is also
loss-free. (HTTP's `/events/batch` dispatcher path is **not** loss-free — it
collapses per-room updates — which is why HTTP adaptive is done server-side.)

## Configuration knobs (issue #71 coverage)

Beyond the transport variant, the driver applies several **orthogonal** config
axes, each exposed as a `run_dynamic.sh` env var **and** a workflow
`workflow_dispatch` input. All are **loss-free**: determinism SHAs must stay
equal to the baselines regardless of the setting — they change
performance/persistence, not results. A mismatch or hang is therefore a real
signal, not a config nuisance.

| Axis | Env var | Workflow input | Values (default) | Configures | Applies to |
| --- | --- | --- | --- | --- | --- |
| Source adaptive batching | `BATCHING_SPEED` | `batching_speed` | low / medium / high (**medium**) | adaptive source `adaptiveMax*` / gRPC adaptive dispatcher `batch_size`+`batch_timeout_ms` | adaptive variants only |
| Query capacity | `QUERY_TUNING` | `query_tuning` | low / medium / high (**medium**) | `priorityQueueCapacity`, `dispatchBufferCapacity`, `bootstrapBufferSize` on every query | all dynamic |
| Persistent index | `PERSIST_INDEX` | `persist_index` | true / false (**false**) | instance-level `persistIndex: true` (built-in RocksDB) + source WAL durability | all dynamic |
| State store | `STATE_STORE` | `state_store` | true / false (**false**) | instance-level redb `stateStore` | all dynamic |

`medium` (batching/query) and `false` (index/state store) equal the committed
defaults, so those settings are explicit no-ops; only the other values change
behavior. `persist_index` and `state_store` are **independent** — any of the
four on/off combinations is valid.

### Preset values

| Preset | `batching_speed` (batch_size / wait_ms) | `query_tuning` (priorityQueue / dispatchBuffer / bootstrapBuffer) |
| --- | --- | --- |
| low | 100 / 10 | 1000 / 100 / 1000 |
| medium | 1000 / 50 | 10000 / 1000 / 10000 |
| high | 5000 / 200 | 100000 / 10000 / 100000 |

### Server instance profiles (`persist_index` × `state_store`)

These two toggles are independent; each on/off combination selects a committed
base yaml under `base/`:

| `persist_index` | `state_store` | Base yaml | Effect |
| --- | --- | --- | --- |
| false | false | `drasi_server.empty.yaml` | In-memory index, no state store (baseline). |
| true | false | `drasi_server.persist_index.yaml` | `persistIndex: true` → built-in RocksDB index. A persistent query rejects non-replay sources, so the driver also enables **source WAL durability** (`durability.enabled`, `WAL_MAX_EVENTS` default 500000). Loss-free but slower (see limitations). |
| false | true | `drasi_server.state_store.yaml` | `stateStore: { kind: redb, path: ./data/state.redb }` for plugin state persistence. |
| true | true | `drasi_server.persist_index_state_store.yaml` | Both of the above. |

RocksDB index/WAL and the redb state store are written under `./data` relative
to the server's working directory; the driver runs the server from `WORK_DIR`
and pre-creates `./data` (and the RocksDB index dir) so nothing lands in the
source tree.



## Final materialized-state golden

Set `FINAL_STATE_CHECK=true` to switch the reaction `DeterminismHash` loggers
from their default order-sensitive stream mode to final-state mode:

```bash
FINAL_STATE_CHECK=true ./run_dynamic.sh
```

The final-state logger normalizes HTTP and gRPC add/update/delete payloads,
materializes the rows remaining at completion, and hashes the canonical rows in
stable order. The `Sha256Determinism` completion handler compares that state
against `goldens/final_state.json`.

If the golden does not exist, the runner uses `missing_baseline: Warn` and
writes `final_state_candidate.json` to the artifacts directory. Review that
candidate and copy it to `goldens/final_state.json`; subsequent runs require an
exact match and report bounded missing and unexpected row details in
`determinism_verdict.json`. Verification never updates the committed golden.

## Large-bootstrap presets (issue #78)

The driver can scale the building_comfort **initial dataset** (the graph that is
delivered as `op:"i"` inserts before steady-state begins) so that bootstrap load
time and throughput can be measured **separately** from steady-state throughput.

Select a preset with `BOOTSTRAP_SIZE` (env) or the workflow's `bootstrap_size`
input. The value is the target **room count** (the dominant element); the graph
is scaled `buildings × floors × rooms/floor` with `floors=10`, `rooms/floor=10`:

| `BOOTSTRAP_SIZE` | buildings | rooms (total) | floors (total) |
| --- | ---: | ---: | ---: |
| `10k` | 100   | 10,000    | 1,000   |
| `100k`| 1000  | 100,000   | 10,000  |
| `1m`  | 10000 | 1,000,000 | 100,000 |

Empty / `off` (the default, and on scheduled runs) keeps the committed small
scenario unchanged.

### How the split is measured

The initial graph is delivered as insert change events **before** the first
steady-state change (`send_initial_inserts` runs to completion, then the change
stream starts — a single ordered source stream). The reaction's
`PerformanceMetrics` logger is given a `bootstrap_record_count` (K) equal to the
number of records the query emits during bootstrap:

- `building-comfort` (`MATCH (r:Room)`) emits **one result per room**, so
  `K = rooms`.
- `building-comfort-floor-agg` (per-floor aggregate) re-emits its floor's
  aggregate once per `FLOOR_ROOM` relation added during bootstrap — i.e. once
  per room as it joins its floor — so its bootstrap output equals the room
  count, `K = rooms` (override with `BOOTSTRAP_K_AGG`).

The logger reports separate `bootstrap` and `steady_state` blocks
(duration + records/sec) in its metrics JSON, surfaced in the workflow's
**Throughput** summary table.

### Rooms-only (opt-in)

By default both queries run during bootstrap. The floor aggregate re-emits its
floor's aggregate once per `FLOOR_ROOM` relation, so its bootstrap output equals
the **room** count (not the floor count); the stop trigger is calibrated to
`K = rooms` so it stays above the bootstrap output and keeps draining throughout
bootstrap. (An earlier `K = floors` mis-calibration stopped it *mid-bootstrap*,
which backpressured the still-dispatching shared source and hung the run — that
was why rooms-only used to be the default.)

Set `BOOTSTRAP_ROOMS_ONLY=true` to run **only** the per-room `building-comfort`
query/reaction and drop the floor-aggregate — useful for isolating the per-room
path.

After bootstrap the driver runs the full steady-state workload
(`BOOTSTRAP_CHANGE_COUNT` **steady** changes, default **100000**). Note the
generator's `change_count` limit counts *every* dispatched event — including the
bootstrap inserts — so the driver sets the underlying `change_count` to
`bootstrap_events + BOOTSTRAP_CHANGE_COUNT` (else the source would "finish" the
instant the bootstrap exceeds the limit and the steady phase would never run).
Completion requires the source to finish **and** the reaction to reach its stop
count, so the stop is set to `K + steady_target` with the target safely below the
reaction's natural output: **95%** of the steady changes for `building-comfort`
(~1 result per change). The remaining ~5% tail is absorbed by the server's query
buffers after the reaction stops, so the source still finishes. Override with
`BOOTSTRAP_STEADY_MAIN` (and `BOOTSTRAP_STEADY_AGG` when the aggregate is
re-enabled).

### Determinism baseline

Because the bootstrap resultset differs from the committed small scenario, the
preset clears the inline `Sha256Determinism` baselines and sets
`missing_baseline: Warn` — the run computes and reports the hash (in the summary)
without failing. Pin a baseline by copying the reported SHA back into a config
once a green reference run exists.

### Run it locally

```bash
cd e2e-test-framework/examples/building_comfort/dynamic
BOOTSTRAP_SIZE=10k ./run_dynamic.sh         # gRPC, 10k-room bootstrap

# HTTP transport, 100k-room bootstrap
BOOTSTRAP_SIZE=100k \
SERVER_SOURCE_FILE=source_http.json SERVER_REACTIONS_FILE=reactions_http.json \
DRASI_SOURCE_PORT=9000 TEST_CFG_SRC="$PWD/config.http.json" ./run_dynamic.sh
```

> Note: `100k` and especially `1m` produce a large initial insert burst
> (`1m` ≈ 2.21M bootstrap events); allow extra time and, if combined with
> `PERSIST_INDEX=true`, expect it to be much slower (WAL fsync per event) — the
> driver auto-raises `WAL_MAX_EVENTS` to cover the bigger dataset.

## Apply order

The driver applies **source → queries → reactions** (a query needs its source to
exist; a reaction needs its query). Reactions `autoStart` and retry-connect to
the test-service's handler ports, so the test-service can start afterward.

## Run it locally (Apple Silicon)

```bash
cd e2e-test-framework/examples/building_comfort/dynamic

# Pre-download a server binary once (macOS arm64):
mkdir -p .local_run/bin
curl -fsSL -o .local_run/bin/drasi-server \
  https://github.com/drasi-project/drasi-server/releases/download/0.1.6/drasi-server-aarch64-apple-darwin
chmod +x .local_run/bin/drasi-server
xattr -d com.apple.quarantine .local_run/bin/drasi-server 2>/dev/null || true

export DRASI_SERVER_BIN="$PWD/.local_run/bin/drasi-server"
export ARTIFACTS_DIR="$PWD/.local_run/artifacts"
export WORK_DIR="$PWD/.local_run/work"

./run_dynamic.sh
```

On CI/Linux the driver downloads `drasi-server-x86_64-linux-gnu` automatically
(override with `DRASI_SERVER_VERSION` / `DRASI_TARGET`).

To build the server from source instead, set `DRASI_SERVER_REF` to a branch,
tag, or commit SHA. `DRASI_REPO` can target a fork:

```bash
DRASI_REPO=drasi-project/drasi-server DRASI_SERVER_REF=main ./run_dynamic.sh
```

When `DRASI_CORE_REF` is also set to a branch, the driver injects a temporary
`[patch.crates-io]` into its disposable drasi-server clone. This redirects all
drasi-core repository crates used by the server to `DRASI_CORE_REPO` at that
branch. It relaxes only the clone's direct core-family version constraints,
updates only those Cargo.lock entries, and verifies with `cargo metadata` that
no patched package silently remained on crates.io. The repository's real
`Cargo.toml` is not changed:

```bash
DRASI_SERVER_REF=main \
DRASI_CORE_REPO=drasi-project/drasi-core DRASI_CORE_REF=main \
./run_dynamic.sh
```

Set `DRASI_PLUGIN_TAG` to pin every untagged plugin reference in the generated
server config. The server resolver appends the current platform suffix, so
`DRASI_PLUGIN_TAG=drasi-nightly-test` resolves, for example, to
`source/grpc:drasi-nightly-test-linux-amd64`.

### Selecting a variant locally

```bash
# HTTP standard
SERVER_SOURCE_FILE=source_http.json SERVER_REACTIONS_FILE=reactions_http.json \
DRASI_SOURCE_PORT=9000 TEST_CFG_SRC="$PWD/config.http.json" ./run_dynamic.sh

# HTTP adaptive — same command, only the source component changes
SERVER_SOURCE_FILE=source_http_adaptive.json SERVER_REACTIONS_FILE=reactions_http.json \
DRASI_SOURCE_PORT=9000 TEST_CFG_SRC="$PWD/config.http.json" ./run_dynamic.sh
```

### Applying config axes locally

The config presets are independent env vars, combinable with any variant:

```bash
# gRPC standard with high query buffers and the RocksDB index
QUERY_TUNING=high PERSIST_INDEX=true \
SERVER_SOURCE_FILE=source_grpc.json SERVER_REACTIONS_FILE=reactions_grpc.json \
DRASI_SOURCE_PORT=50051 ./run_dynamic.sh
```

## Run it in CI

The dynamic variants live in the shared workflow
`.github/workflows/e2e-building-comfort.yml` (`workflow_dispatch` + scheduled).

**Variant selection** is one checkbox per variant (GitHub renders boolean inputs
as checkboxes); tick any of:

- `http_standard`
- `http_adaptive`
- `grpc_standard`
- `grpc_adaptive`

(plus `drasi_lib`, which uses the embedded engine via `ci/drasi_lib/run_test_ci.sh`.)

**Config axes** are separate dropdown/checkbox inputs applied to whichever
dynamic variants run: `batching_speed`, `query_tuning`, `persist_index`,
`state_store` (see the matrix above). On **scheduled** runs the inputs are
absent, so the driver falls back to each axis's default.

The workflow's *Resolve variant* step maps every non-`drasi_lib` variant onto the
`run_dynamic.sh` `*_FILE` / port / `TEST_CFG_SRC` env knobs; `drasi_lib` uses its
own `ci/drasi_lib/run_test_ci.sh`. The config-axis env vars (`BATCHING_SPEED`,
`QUERY_TUNING`, `PERSIST_INDEX`, `STATE_STORE`) are passed straight through on
the *Run test* step.

### Nightly full-stack performance test

`.github/workflows/nightly-perf.yml` runs daily at 22:00 UTC, allowing for both
GitHub scheduling delays and drasi-core's roughly 2.5-hour nightly plugin build.
It can also be started manually.

The workflow first queries the latest completed `nightly.yml` run in
`drasi-project/drasi-core`. It proceeds only when that run succeeded within the
last 24 hours; otherwise it skips neutrally because core's own nightly reports
its failures.

Each performance job:

1. builds drasi-server from `main`;
2. patches its drasi-core dependencies to drasi-core `main`;
3. pins server plugins to `drasi-nightly-test`;
4. runs the `http_standard` and `grpc_standard` building-comfort variants with
   the `10k` bootstrap preset; and
5. uploads metrics, determinism verdicts, and logs.

All cross-repository references remain in the dependency direction
test-infra → drasi-server → drasi-core. Neither lower-level repository needs a
branch containing a modified `Cargo.toml`, and drasi-core does not reference or
trigger test-infra.


## Adding a new variant

1. Drop a new component JSON under `components/server/` (e.g. a query set with
   different buffer/middleware config, or a reaction with different batching).
2. If the transport changes, add a matching test-service `config.*.json`.
3. Run with the `*_FILE` / `DRASI_SOURCE_PORT` / `TEST_CFG_SRC` overrides, or add
   an option to the workflow's `variant` input.

Queries stay constant (`queries.json`) across transports.

## Known limitations & invariants

- **Loss-free invariant.** Every config axis (`batching_speed`, `query_tuning`)
  and both instance toggles (`persist_index`, `state_store`) must yield
  determinism SHAs equal to the standard/memory baselines — they change
  performance or persistence, not results. A SHA mismatch or a hang (stop
  trigger never firing) is a real bug signal.
- **`persist_index` is much slower.** The RocksDB index plus the *mandatory*
  source WAL durability (a persistent query rejects non-replay sources) is ~10x
  slower here, dominated by the WAL doing one fsync'd redb transaction per event.
  This is a performance limitation, not a correctness issue — see
  [issue-drafts/wal-per-event-fsync-throughput.md](../../../../issue-drafts/wal-per-event-fsync-throughput.md).
- **drasi-lib config is out of scope** for this phase (Drasi Server only). The
  embedded drasi-lib instance runs with near-default config; its runtime /
  metrics / auth knobs are not currently configurable through the framework.
  embedded drasi-lib instance runs with near-default config; its runtime /
  metrics / auth knobs are not currently configurable through the framework.


## Plugin availability (important)

The bare server auto-installs the plugins listed in
[base/drasi_server.empty.yaml](base/drasi_server.empty.yaml) from the OCI
registry (`ghcr.io/drasi-project`) at startup. Install failures are **non-fatal**
(logged as warnings), so a missing plugin otherwise surfaces late as
`Unknown source kind: 'grpc'. Available: []`. The driver runs a **preflight
check** (`GET /api/v1/plugins/kinds`) and fails fast with an actionable message.

Known gap: on **darwin-arm64** the registry may not publish plugin builds for
the server's SDK version, so the plugins won't resolve locally. This affects the
static `local/drasi_server_*` configs identically — it is not specific to this
driver. To get a green run:

- run on a **Linux x86_64** host / CI (where the registry has the plugins), or
- point `pluginRegistry` at a local plugins directory built from `drasi-core`.
