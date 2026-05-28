#!/bin/bash

# Run Stock Market Trading Simulation locally

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "Starting Stock Market Trading Simulation..."
echo "=========================================="

# Check if Redis is running
if ! redis-cli ping > /dev/null 2>&1; then
    echo "Error: Redis is not running. Please start Redis first."
    echo "You can start Redis with: redis-server"
    exit 1
fi

# Run the test service with the stock market configuration
cd "$ROOT_DIR"

echo "Running test-service with stock market configuration..."
RUST_LOG=info cargo run -p test-service -- \
    --config "$SCRIPT_DIR/config.json" \
    --port 8080

echo ""
echo "Stock Market Trading Simulation completed!"