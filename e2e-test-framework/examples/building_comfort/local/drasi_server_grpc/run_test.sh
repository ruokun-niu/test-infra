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

echo -e "${GREEN}\nRunning the E2E Test Service with gRPC as a local process...${RESET}"
# Set drasi_core modules to error level to suppress INFO logs from tracing instrumentation
RUST_LOG='info, drasi_core::query::continuous_query=error,drasi_core::path_solver=error' cargo run --release --manifest-path "$(dirname "$0")/../../../test-service/Cargo.toml" -- --config "$(dirname "$0")/config.json"

# RUST_LOG_STYLE=never \
# RUST_LOG="off,drasi_server=info, test_run_host=info, test_data_store=info" \
# 	cargo run --release --manifest-path "$(dirname "$0")/../../../test-service/Cargo.toml" -- --config "$(dirname "$0")/config.json" \
# 	| egrep '^(TRACE|DEBUG|INFO|WARN|ERROR)'