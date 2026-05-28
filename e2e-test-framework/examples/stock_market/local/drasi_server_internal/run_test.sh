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

#!/bin/bash

GREEN="\033[32m"
RESET="\033[0m"

echo -e "${GREEN}\nRunning the Stock Market E2E Test with Internal drasi-lib instance...${RESET}"
# Set drasi_core modules to error level to suppress INFO logs from tracing instrumentation
# Also suppress debug logs from channel handlers to avoid "channel closed" messages after stop trigger
RUST_LOG="off,test_run_host=info,test_run_service=info,test_data_store=info,test_run_host::reactions::reaction_handlers::drasi_server_channel_handler=warn" \
	cargo run --release --manifest-path ./test-service/Cargo.toml -- --config examples/stock_market/drasi_server_internal/config.json \
	| egrep '^(TRACE|DEBUG|INFO|WARN|ERROR)'