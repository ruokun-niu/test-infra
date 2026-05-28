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

echo -e "${GREEN}\nRunning the Stock Market E2E Test with Adaptive HTTP Batching...${RESET}"
echo -e "${YELLOW}NOTE: This requires a Drasi Server with HTTP source running on port 9000${RESET}"
echo -e "${YELLOW}Using adaptive batching for high-throughput event delivery${RESET}\n"

# Run the test with appropriate logging for adaptive batching
RUST_LOG="info,test_run_host::sources::source_change_dispatchers::adaptive_http_dispatcher=debug,test_run_host::utils::adaptive_batcher=debug" \
	cargo run --release --manifest-path ./test-service/Cargo.toml -- --config examples/stock_market/drasi_server_http_adaptive/config.json \
	| egrep '^(TRACE|DEBUG|INFO|WARN|ERROR)'