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

echo -e "${GREEN}\nRunning the Stock Market E2E Test with gRPC external Drasi Server (DEBUG MODE)...${RESET}"
# Enable debug logging for troubleshooting
RUST_LOG="debug,test_run_host=debug, test_run_service=debug, test_data_store=debug" \
	cargo run --release --manifest-path ./test-service/Cargo.toml -- --config examples/stock_market/drasi_server_grpc/config.json