#!/bin/bash

# Quick test of the Stock Market with internal drasi-lib instance

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo "Quick test of Stock Market with internal drasi-lib instance"
echo "======================================================"

cd "$ROOT_DIR"

# Run with cleaner output - suppress debug logs but show info and above
echo "Starting test-service with internal drasi-lib instance configuration..."
RUST_LOG='info,test_run_host::sources::source_change_dispatchers::drasi_server_channel_dispatcher=warn,test_run_host::reactions::reaction_handlers::drasi_server_channel_handler=warn' \
    cargo run -p test-service -- \
    --config "$SCRIPT_DIR/config.json" \
    --port 8080 2>&1 | grep -E "(INFO|WARN|ERROR)" | head -100

echo ""
echo "Test completed!"