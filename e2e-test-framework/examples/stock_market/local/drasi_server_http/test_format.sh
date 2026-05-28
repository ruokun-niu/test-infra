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
RED="\033[31m"
RESET="\033[0m"

echo -e "${GREEN}Testing HTTP Dispatcher Format Compliance${RESET}"
echo -e "${YELLOW}This test verifies the HTTP dispatcher sends events in the correct format${RESET}\n"

# Start a simple HTTP server to capture and display the request
echo -e "${GREEN}Starting test HTTP server on port 9000...${RESET}"

# Create a simple Python script to capture HTTP requests
cat > /tmp/test_http_server.py << 'EOF'
#!/usr/bin/env python3
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
import sys

class TestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # Get the path
        print(f"\n{'-'*60}")
        print(f"Received POST to: {self.path}")
        
        # Verify the path format
        if not self.path.startswith("/sources/") or not self.path.endswith("/events"):
            print(f"ERROR: Invalid path format. Expected /sources/{{source_id}}/events")
            self.send_response(404)
            self.end_headers()
            return
        
        # Extract source ID
        source_id = self.path.replace("/sources/", "").replace("/events", "")
        print(f"Source ID: {source_id}")
        
        # Read the request body
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        
        # Parse JSON
        try:
            data = json.loads(body)
            
            # Verify required fields
            required_fields = ["op", "payload"]
            payload_source_fields = ["db", "table", "ts_ns", "lsn"]
            
            if isinstance(data, list):
                print(f"Received batch of {len(data)} events")
                data = data[0] if data else {}
            
            missing = [f for f in required_fields if f not in data]
            if missing:
                print(f"ERROR: Missing required fields: {missing}")
            else:
                print("✓ All required top-level fields present")
            
            if "payload" in data and "source" in data["payload"]:
                source = data["payload"]["source"]
                missing_source = [f for f in payload_source_fields if f not in source]
                if missing_source:
                    print(f"ERROR: Missing source fields: {missing_source}")
                else:
                    print("✓ All required source fields present")
            
            # Display sample event structure
            print("\nSample Event Structure:")
            print(json.dumps(data, indent=2)[:500] + "...")
            
            # Check element format
            if "payload" in data:
                if "after" in data["payload"] and data["payload"]["after"]:
                    after = data["payload"]["after"]
                    if "id" in after and "labels" in after and "properties" in after:
                        print("✓ Element format correct (id, labels, properties)")
                    else:
                        print("ERROR: Invalid element format")
            
            print(f"{'-'*60}\n")
            
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON: {e}")
        
        # Send success response
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"status":"accepted","message":"Event processed successfully"}')
    
    def log_message(self, format, *args):
        # Suppress default logging
        pass

# Start server
server = HTTPServer(('localhost', 9000), TestHandler)
print("Test HTTP server listening on http://localhost:9000")
print("Waiting for events... (Press Ctrl+C to stop)\n")

try:
    server.serve_forever()
except KeyboardInterrupt:
    print("\nServer stopped")
    sys.exit(0)
EOF

# Start the Python server in background
python3 /tmp/test_http_server.py &
SERVER_PID=$!

# Give server time to start
sleep 2

echo -e "\n${GREEN}Running test with limited events...${RESET}"

# Create a minimal test config
cat > /tmp/test_http_format.json << 'EOF'
{
  "data_store": {
      "data_store_path": "/tmp/test_http_format_cache",
      "delete_on_start": true,
      "delete_on_stop": true,
      "test_repos": [
          {
              "id": "test_repo",
              "kind": "LocalStorage",
              "source_path": "examples/stock_market/drasi_server_http/dev_repo",
              "local_tests": [
                {
                    "test_id": "format_test",
                    "version": 1,
                    "description": "HTTP format test",
                    "test_folder": "stock_market",
                    "sources": [
                        {
                        "test_source_id": "test-source",
                        "kind": "Model",
                        "source_change_dispatchers": [ 
                            {
                            "kind": "Http",
                            "url": "http://localhost",
                            "port": 9000,
                            "timeout_seconds": 5,
                            "batch_events": false
                            }
                        ],
                        "model_data_generator": {
                            "kind": "StockTrade",
                            "stock_definitions": [
                                {"symbol": "TEST", "name": "Test Stock"}
                            ],
                            "change_count": 3,
                            "change_interval": [100000000, 100000000, 100000000, 100000000],
                            "send_initial_inserts": true
                        }
                        }
                    ],
                    "reactions": []
                }
              ]
          }
      ]
  },
  "test_run_host": {
      "test_runs": [
          {
              "test_id": "format_test",
              "test_repo_id": "test_repo",
              "test_run_id": "run_001",
              "sources": [
                  {
                      "test_source_id": "test-source",
                      "start_mode": "auto"
                  }
              ]
          }
      ]
  }
}
EOF

# Run the test briefly
timeout 5 cargo run --release --manifest-path ./test-service/Cargo.toml -- --config /tmp/test_http_format.json 2>&1 | grep -E "(INFO|ERROR)" &

# Wait for test to complete
sleep 6

# Kill the Python server
kill $SERVER_PID 2>/dev/null

# Cleanup
rm -f /tmp/test_http_server.py /tmp/test_http_format.json
rm -rf /tmp/test_http_format_cache

echo -e "\n${GREEN}Format test complete!${RESET}"
echo -e "${YELLOW}Check the output above to verify:${RESET}"
echo "1. Path format is /sources/{source_id}/events"
echo "2. Event structure includes op, payload fields"
echo "3. Source metadata has db, table, ts_ns, lsn"
echo "4. Elements have id, labels, properties format"