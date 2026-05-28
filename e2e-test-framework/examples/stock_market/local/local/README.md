# Stock Market Trading Simulation Example

This example demonstrates the StockTradeDataGenerator, which generates pseudo-random stock trades with realistic price movements and volume changes.

## Features

- Generates stock price updates for 10 major tech companies
- Uses momentum-based price movements for realistic market behavior
- Configurable volatility and trading volumes
- Deterministic random generation with seed support
- Supports various timing modes (Live, Recorded, Rebased)

## Configuration

The `config.json` file includes:

- **Stock Definitions**: 10 stocks including MSFT, AAPL, GOOGL, AMZN, NVDA, TSLA, META, BRK.B, V, JPM
- **Price Parameters**:
  - Initial price range: $150 ± $50
  - Price changes: $0.50 ± $2.00 per update
  - Price momentum: 5 steps with 30% reversal probability
  - Valid price range: $10 - $5,000
- **Volume Parameters**:
  - Initial volume: 10M ± 5M shares
  - Volume changes: 100K ± 50K shares per update
  - Volume momentum: 3 steps with 40% reversal probability
  - Valid volume range: 1K - 100M shares
- **Timing**:
  - 10,000 total changes
  - Change interval: 100ms ± 50ms (range: 50ms - 200ms)
  - Live timing mode for real-time updates

## Running the Example

### Prerequisites

1. Redis must be running:
   ```bash
   redis-server
   ```

2. Build the test framework:
   ```bash
   cd e2e-test-framework
   cargo build -p test-service
   ```

### Local Execution

Run the stock market simulation:
```bash
./run_local.sh
```

This will:
1. Start the test service on port 8080
2. Generate initial stock data for all configured stocks
3. Stream price and volume updates to the console
4. Continue for 10,000 updates or until stopped

### Monitoring

While the simulation is running, you can:

1. View the API documentation:
   ```
   http://localhost:8080/docs
   ```

2. Check the source status:
   ```bash
   curl http://localhost:8080/api/runs/stock_market_run_001/sources/stock_trades/state
   ```

3. Control the generator:
   ```bash
   # Pause the generator
   curl -X POST http://localhost:8080/api/runs/stock_market_run_001/sources/stock_trades/pause

   # Resume the generator
   curl -X POST http://localhost:8080/api/runs/stock_market_run_001/sources/stock_trades/start

   # Step through updates one at a time
   curl -X POST http://localhost:8080/api/runs/stock_market_run_001/sources/stock_trades/step?steps=1
   ```

## Output

The simulation outputs:
- Console logs showing each stock price update
- JSON Lines file with detailed trade data
- Redis stream with real-time updates (if queries are configured)

Example output:
```json
{
  "op": "u",
  "payload": {
    "source": {
      "db": "stock_trades",
      "lsn": 42,
      "table": "node",
      "ts_ns": 1627849200000000000
    },
    "before": {
      "id": "MSFT",
      "labels": ["Stock"],
      "properties": {
        "symbol": "MSFT",
        "name": "Microsoft Corporation",
        "price": 150.50,
        "volume": 10500000,
        "daily_high": 151.25,
        "daily_low": 149.75,
        "daily_open": 150.00
      }
    },
    "after": {
      "id": "MSFT",
      "labels": ["Stock"],
      "properties": {
        "symbol": "MSFT",
        "name": "Microsoft Corporation",
        "price": 151.00,
        "volume": 10600000,
        "daily_high": 151.25,
        "daily_low": 149.75,
        "daily_open": 150.00
      }
    }
  }
}
```

## Customization

You can modify `config.json` to:
- Add or remove stocks
- Adjust price volatility and ranges
- Change update frequency
- Modify volume patterns
- Switch timing modes
- Change the random seed for different patterns