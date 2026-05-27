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

drasi env kube
# echo -e "${GREEN}\n\nInstalling Drasi...${RESET}"
drasi init --local --version latest
# This is a workaround for the issue with the init command regularly failing.
drasi init --local --version latest 

# Deploy the Test Service and wait for it to be available
echo -e "${GREEN}\n\nDeploying Test Service...${RESET}"
kubectl apply -f examples/building_comfort/drasi_platform/test_service_deployment_dev.yaml
kubectl wait -n drasi-system --for=condition=available deployment/drasi-test-service_dev --timeout=300s

# Install the Test Source Provider and create the Test Source
echo -e "${GREEN}\n\nRegistering E2ETestService SourceProvider with Drasi...${RESET}"
drasi apply -f ./devops/drasi/e2e_test_source_provider.yaml

echo -e "${GREEN}\n\nCreating Test Source...${RESET}"
drasi apply -f examples/building_comfort/drasi_platform/source.yaml
drasi wait -f examples/building_comfort/drasi_platform/source.yaml -t 200

# Create the Continuous Queries
echo -e "${GREEN}\n\nCreating Drasi Continuous Queries...${RESET}"
drasi apply -f examples/building_comfort/drasi_platform/query_container_default/query_default_worker.yaml
drasi wait -f examples/building_comfort/drasi_platform/query_container_default/query_default_worker.yaml -t 200

# Forward the Test Service port and configure the Repository, Source, and Query
echo -e "${GREEN}\n\nPort forwarding to enable access the Test Service Web API...${RESET}"
kubectl port-forward -n drasi-system services/drasi-test-service 63123:63123 &

echo -e "${GREEN}\n\nDeployment Complete.${RESET}"