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

echo -e "${GREEN}\nRunning the E2E Test Service with Internal drasi-lib instance (DEBUG)...${RESET}"
# Set debug/trace levels but suppress drasi_core INFO logs from tracing instrumentation
RUST_LOG="off,test_run_host=debug, test_run_service=debug, test_data_store=debug" \
	cargo run --release --manifest-path "$(dirname "$0")/../../../test-service/Cargo.toml" -- --config "$(dirname "$0")/config.json" \
	| egrep '^(TRACE|DEBUG|INFO|WARN|ERROR)'