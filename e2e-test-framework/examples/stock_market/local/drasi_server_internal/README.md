# Stock Market Example - Internal drasi-lib instance Configuration

This configuration demonstrates the stock market trading simulation using an **embedded drasi-lib instance** instance within the test service.

## Architecture

- **Embedded drasi-lib instance**: The test service hosts a drasi-lib instance internally using TestRunDrasiServer
- **Internal Communication**: Uses `DrasiLibInstanceChannel` dispatcher for direct in-memory communication
- **Self-Contained**: No external dependencies required

## Configuration Highlights

### drasi-lib instance Setup
The configuration embeds a complete drasi-lib instance with:
- **Source**: `stock-exchange` - Receives stock market updates via internal channels
- **Queries**:
  - `all-stocks`: Monitors all stock prices and volumes
  - `high-volume-trades`: Detects trades > 50M volume
  - `price-movements`: Tracks significant price changes (>5% from open)
- **Reaction**: `stock-market-alerts` - Processes query results internally

### Data Generator
Uses `StockTradeDataGenerator` with:
- 10 major stocks (MSFT, AAPL, GOOGL, AMZN, NVDA, TSLA, META, BRK.B, V, JPM)
- Realistic price momentum simulation
- Volume fluctuation modeling
- 100,000 total change events

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

### drasi-lib instance Management (Internal Only)
```bash
# Get instance status
curl http://localhost:8080/api/drasi_lib_instances/internal-stock-market-server

# Start all components
curl -X POST http://localhost:8080/api/drasi_lib_instances/internal-stock-market-server/start

# Stop all components
curl -X POST http://localhost:8080/api/drasi_lib_instances/internal-stock-market-server/stop
```

### Source Control
```bash
# Get source state
curl http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/state

# Start data generation
curl -X POST http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/start

# Pause generation
curl -X POST http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/pause
```

### Query Monitoring
```bash
# Get query status
curl http://localhost:8080/api/queries/all-stocks
curl http://localhost:8080/api/queries/high-volume-trades
curl http://localhost:8080/api/queries/price-movements
```

## Interactive Testing

Use the provided `.http` files with a REST client or VS Code REST Client extension:
- `web_api_drasi_lib_instance.http` - drasi-lib instance management
- `web_api_source.http` - Source control
- `web_api_query.http` - Query testing
- `web_api_reaction.http` - Reaction monitoring

## Expected Output

The system generates realistic stock market updates:
```json
{
  "op": "u",
  "payload": {
    "after": {
      "id": "MSFT",
      "labels": ["Stock"],
      "properties": {
        "symbol": "MSFT",
        "name": "Microsoft Corporation",
        "price": 425.50,
        "volume": 52000000,
        "daily_high": 428.00,
        "daily_low": 423.25,
        "daily_open": 424.00
      }
    }
  }
}
```

## Advantages

- **Simple Setup**: No external drasi-lib instance required
- **Fast Communication**: Direct in-memory channels
- **Easy Debugging**: All components in single process
- **Self-Contained Testing**: Complete test environment in one service

## Use Cases

This configuration is ideal for:
- Local development and testing
- CI/CD pipelines
- Quick prototyping
- Debugging Drasi queries and reactions
- Performance testing without network overhead