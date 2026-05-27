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

LOCAL_PORT=63123

PID=$(ps aux | grep "[k]ubectl port-forward.*$LOCAL_PORT" | awk '{print $2}')

if [ -n "$PID" ]; then
    echo "Found kubectl port-forward process with PID: $PID"
    # Kill the process
    kill -9 "$PID"
    if [ $? -eq 0 ]; then
        echo "Successfully stopped port forwarding on port $LOCAL_PORT"
    else
        echo "Failed to stop port forwarding"
    fi
else
    echo "No kubectl port-forward process found for port $LOCAL_PORT"
fi

# SHORTCUT FOR NOW
drasi uninstall -y
kubectl wait --for=delete namespace/drasi-system --timeout=5m
dapr uninstall -k -n dapr-system
exit


drasi delete query continent-country-population
drasi delete query country-city-population
drasi delete query city-population

drasi delete source geo-db

drasi delete sourceprovider E2ETestSource

# Delete Test Service
kubectl delete service drasi-test-service -n drasi-system
kubectl delete deployment drasi-test-service -n drasi-system

# Delete Redis
kubectl delete statefulset drasi-redis -n drasi-system
kubectl delete pvc data-drasi-redis-0 -n drasi-system

# Delete Mongo
kubectl delete statefulset drasi-mongo -n drasi-system
kubectl delete pvc data-drasi-mongo-0 -n drasi-system
kubectl delete configmap drasi-mongo-init -n drasi-system