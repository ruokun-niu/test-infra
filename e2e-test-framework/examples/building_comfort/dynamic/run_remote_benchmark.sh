#!/usr/bin/env bash
set -euo pipefail

if (( $# != 3 )); then
    echo "Usage: $0 <workspace> <environment-file> <artifacts-directory>" >&2
    exit 1
fi

WORKSPACE="$(cd "$1" && pwd)"
ENV_FILE="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"
ARTIFACTS_DIR="$3"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Benchmark environment file not found: $ENV_FILE" >&2
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

DRASI_SERVER_VERSION="${DRASI_SERVER_VERSION:-}"
DRASI_REPO="${DRASI_REPO:-drasi-project/drasi-server}"
: "${SUITE_WORK_DIR:?SUITE_WORK_DIR is required}"
: "${PERF_PROFILE_ID:?PERF_PROFILE_ID is required}"

sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    ca-certificates curl jq build-essential pkg-config libssl-dev libjq-dev libonig-dev \
    libprotobuf-dev protobuf-compiler cmake clang libclang-dev lsof

if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 --fail --show-error --silent \
        https://sh.rustup.rs |
        sh -s -- -y --profile minimal --default-toolchain 1.88.0
fi

source "$HOME/.cargo/env"
rustup toolchain install 1.88.0 --profile minimal

mkdir -p "$ARTIFACTS_DIR" "$(dirname "$DRASI_SERVER_BIN")" "$SUITE_WORK_DIR"

metadata_json="$ARTIFACTS_DIR/environment-metadata.json"
instance_metadata="$(curl -fsS \
    -H Metadata:true \
    'http://169.254.169.254/metadata/instance?api-version=2021-02-01' || echo '{}')"

jq -n \
    --arg profile_id "$PERF_PROFILE_ID" \
    --arg timestamp "$(date -u +%FT%TZ)" \
    --arg kernel "$(uname -srmo)" \
    --arg os_release "$(grep '^PRETTY_NAME=' /etc/os-release | cut -d= -f2- | tr -d '"')" \
    --arg cpu_model "$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo)" \
    --argjson cpu_count "$(nproc)" \
    --argjson mem_total_kb "$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)" \
    --argjson instance "$instance_metadata" \
    '{
        profile_id: $profile_id,
        timestamp: $timestamp,
        os: {
            description: $os_release,
            kernel: $kernel
        },
        hardware: {
            cpu_model: $cpu_model,
            cpu_count: $cpu_count,
            mem_total_kb: $mem_total_kb
        },
        azure: $instance
    }' > "$metadata_json"

tag="$DRASI_SERVER_VERSION"
if [[ -z "$tag" ]]; then
    tag="$(curl -fsSL "https://api.github.com/repos/${DRASI_REPO}/releases/latest" | jq -r '.tag_name')"
fi
[[ -n "$tag" && "$tag" != "null" ]] || {
    echo "Could not resolve the latest drasi-server release tag" >&2
    exit 1
}

asset_name="drasi-server-x86_64-linux-gnu"
curl --fail --show-error --silent --location \
    --retry 3 --retry-delay 5 --retry-all-errors \
    "https://github.com/${DRASI_REPO}/releases/download/${tag}/${asset_name}" \
    -o "$DRASI_SERVER_BIN"
chmod +x "$DRASI_SERVER_BIN"

export JQ_LIB_DIR
JQ_LIB_DIR="$(dirname "$(find /usr/lib -name 'libjq.so' -print -quit)")"

cd "$WORKSPACE/e2e-test-framework"
cargo build --release --locked --manifest-path test-service/Cargo.toml
export TEST_SERVICE_BIN="$WORKSPACE/e2e-test-framework/target/release/test-service"

cd "$WORKSPACE"
bash e2e-test-framework/examples/building_comfort/dynamic/run_benchmark_suite.sh
