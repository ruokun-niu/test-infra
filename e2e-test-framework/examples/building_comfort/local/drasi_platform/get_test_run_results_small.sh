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

NAMESPACE="drasi-system"
APP_NAME="drasi-test-service"
CONTAINER="drasi-test-service"

# Check if both parameters are provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <test-run-id>"
    echo "Example: $0 1"
    exit 1
fi

# Store the parameters
TEST_RUN_ID=$1
mkdir ${TEST_RUN_ID}

# Dynamically get the Pod name based on the app label
POD_NAME=$(kubectl get pod -n "$NAMESPACE" -l app="$APP_NAME" -o jsonpath="{.items[0].metadata.name}")

# Check if POD_NAME is empty (no matching Pod found)
if [ -z "$POD_NAME" ]; then
  echo "Error: No Pod found with label app=$APP_NAME in namespace $NAMESPACE"
  exit 1
fi

# Source
kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/github_dev_repo.building_comfort_small.test_run_001/sources/facilities-db/test_run_summary.json" "./${TEST_RUN_ID}/source_summary.json"

# Query Observer
kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/github_dev_repo.building_comfort_small.test_run_001/queries/room-comfort-level/test_run_summary.json" "./${TEST_RUN_ID}/query_summary.json"

# Query Result Profiler
kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/github_dev_repo.building_comfort_small.test_run_001/queries/room-comfort-level/result_stream_log/profiler/summary.json" "./${TEST_RUN_ID}/query_profiler_summary.json"

kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/github_dev_repo.building_comfort_small.test_run_001/queries/room-comfort-level/result_stream_log/profiler/change_all_abs.png" "./${TEST_RUN_ID}/query_profiler_viz_all_abs.png"
kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/github_dev_repo.building_comfort_small.test_run_001/queries/room-comfort-level/result_stream_log/profiler/change_all_rel.png" "./${TEST_RUN_ID}/query_profiler_viz_all_rel.png"
kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/github_dev_repo.building_comfort_small.test_run_001/queries/room-comfort-level/result_stream_log/profiler/change_drasi_only_abs.png" "./${TEST_RUN_ID}/query_profiler_viz_drasi_only_abs.png"
kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/github_dev_repo.building_comfort_small.test_run_001/queries/room-comfort-level/result_stream_log/profiler/change_drasi_only_rel.png" "./${TEST_RUN_ID}/query_profiler_viz_drasi_only_rel.png"

kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/github_dev_repo.building_comfort_small.test_run_001/queries/room-comfort-level/result_stream_log/profiler/change_rates.csv" "./${TEST_RUN_ID}/query_profiler_change_rates.csv"
kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/github_dev_repo.building_comfort_small.test_run_001/queries/room-comfort-level/result_stream_log/profiler/change_distributions.csv" "./${TEST_RUN_ID}/query_profiler_change_distributions.csv"

# kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/github_dev_repo.building_comfort_small.test_run_001/queries/room-comfort-level/result_stream_log/profiler/change_00000.jsonl" "./${TEST_RUN_ID}/query_profiler_change_log_sample.jsonl"
