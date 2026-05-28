# Stock Market Example - gRPC Drasi Server Configuration

This configuration demonstrates the stock market trading simulation using an **external Drasi Server** with gRPC communication.

## Architecture

- **External Drasi Server**: Connects to a standalone Drasi Server instance
- **gRPC Communication**: Uses Drasi's v1 protocol for source updates and reaction handling
- **Distributed Setup**: Separates test service from Drasi Server for production-like testing

## Configuration Highlights

### gRPC Dispatcher
- **Source Dispatcher**: Sends stock updates to `localhost:50051`
- **Protocol**: Uses `drasi.v1.SourceService` for data ingestion
- **Batching**: Enabled for efficient transmission
- **Source ID**: `stock-exchange` registered with external server

### Reaction Handler
- **gRPC Server**: Listens on `0.0.0.0:50052` for query results
- **Protocol**: Implements `drasi.v1.ReactionService`
- **Query Subscription**: Monitors `stock-market-monitor` query
- **Correlation**: Uses `x-query-sequence` metadata for tracking

### Data Generator
Uses `StockTradeDataGenerator` with:
- 10 major stocks (MSFT, AAPL, GOOGL, AMZN, NVDA, TSLA, META, BRK.B, V, JPM)
- Realistic price momentum simulation
- Volume fluctuation modeling
- 100,000 total change events

## Prerequisites

### Start External Drasi Server
Before running this example, ensure a Drasi Server is running with appropriate configuration:

```bash
# Example: Start Drasi Server with required source and query
drasi-server --config server-config.yaml
```

The external server should have:
- Source `stock-exchange` configured to receive gRPC updates
- Query `stock-market-monitor` configured to process stock data
- Appropriate gRPC endpoints exposed (typically port 50051)

## Running the Example

### Basic Run
```bash
./run_test.sh
```

### Debug Mode
```bash
./run_test_debug.sh
```

## Testing Endpoints

While the test is running, you can interact with various endpoints:

### Source Control
```bash
# Get source state
curl http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/state

# Start data generation
curl -X POST http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/start

# Pause generation
curl -X POST http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/pause

# Step through events
curl -X POST "http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/step?steps=10"
```

### Reaction Monitoring
```bash
# Get reaction status
curl http://localhost:8080/api/runs/stock_market_run_001/reactions/stock-market-monitor

# Check received events
curl http://localhost:8080/api/runs/stock_market_run_001/reactions/stock-market-monitor/results
```

## Interactive Testing

Use the provided `.http` files with a REST client or VS Code REST Client extension:
- `web_api_source.http` - Source control
- `web_api_query.http` - Query testing (external server)
- `web_api_reaction.http` - Reaction monitoring

## Network Configuration

### Default Ports
- **Test Service API**: 8080
- **gRPC Source (to Drasi)**: 50051
- **gRPC Reaction (from Drasi)**: 50052

### Custom Configuration
Modify the `config.json` to adjust:
```json
{
  "kind": "Grpc",
  "host": "your-drasi-server",
  "port": 50051
}
```

## Expected Data Flow

1. **Test Service** generates stock market events
2. **gRPC Dispatcher** sends events to external Drasi Server (port 50051)
3. **Drasi Server** processes events through configured queries
4. **Query Results** sent back to Test Service reaction handler (port 50052)
5. **Test Service** logs and stores results

## Advantages

- **Production-Like**: Mimics real distributed architecture
- **Network Testing**: Validates gRPC communication and protocols
- **Scalability Testing**: Can test with multiple Drasi Server instances
- **Independent Components**: Services can be scaled and debugged separately

## Use Cases

This configuration is ideal for:
- Integration testing with production Drasi Servers
- Network resilience testing
- Performance benchmarking with distributed components
- Multi-instance Drasi deployments
- Production deployment validation

## Troubleshooting

### Connection Refused
Ensure the external Drasi Server is running and listening on the configured ports.

### Source Not Found
Verify the source ID (`stock-exchange`) is configured in the external Drasi Server.

### No Query Results
Check that the query (`stock-market-monitor`) exists and is subscribed to the source.

### gRPC Errors
Enable debug logging to see detailed gRPC communication:
```bash
RUST_LOG=debug ./run_test_debug.sh
```