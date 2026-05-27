#!/bin/bash

# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GREEN="\033[32m"
YELLOW="\033[33m"
RESET="\033[0m"

echo -e "${GREEN}\nRunning E2E Test Service with Adaptive HTTP Batching...${RESET}"
echo -e "${YELLOW}Configuration:${RESET}"
echo -e "  - Adaptive batching: ENABLED"
echo -e "  - Dynamic batch sizes: 10-1000 messages"
echo -e "  - Adaptive wait times: 1ms-100ms"
echo -e "  - Throughput window: 5 seconds"
echo -e "  - Target events: 100,000"
echo ""

# Run with enhanced logging to see adaptive behavior
RUST_LOG='info,test_run_host::utils::adaptive_batcher=error,drasi_core::query::continuous_query=error,drasi_core::path_solver=error' \
    cargo run --release --manifest-path "$(dirname "$0")/../../../test-service/Cargo.toml" -- \
    --config "$(dirname "$0")/config.json"