# Stock Market Test with HTTP Dispatcher

This example demonstrates using the HTTP Source dispatcher to send stock trading events to an external Drasi Server via its HTTP source endpoint.

## Overview

The test uses the `StockTradeDataGenerator` to generate simulated stock market trading data and dispatches it to a Drasi Server running with an HTTP source configured.

## Prerequisites

1. A Drasi Server instance running with an HTTP source configured
2. The HTTP source should be listening on port 9000 (configurable)
3. An HTTP reaction handler endpoint listening on port 8081

## Drasi Server Configuration

Your Drasi Server should have an HTTP source configured like this:

```yaml
sources:
  - id: "stock-trades-db"
    source_type: "internal.http"
    auto_start: true
    properties:
      port: 9000
      host: "0.0.0.0"
```

## Running the Test

### 1. Start Your Drasi Server

Start your Drasi Server with the HTTP source configured:

```bash
# Example command (adjust based on your setup)
cargo run -- --config your-drasi-config.yaml
```

### 2. Start HTTP Reaction Handler (Optional)

If you want to receive reaction results, start an HTTP server on port 8081:

```bash
# Simple Python HTTP server for testing
python3 -m http.server 8081
```

### 3. Run the Test

```bash
./run_test.sh
```

## Configuration Details

### HTTP Dispatcher Settings

The HTTP dispatcher is configured to:
- Send events to `http://localhost:9000/sources/stock-trades-db/events`
- Use individual event mode (not batched) for real-time processing
- Timeout after 60 seconds

### Stock Generator Settings

- Generates data for 5 stocks (MSFT, AAPL, GOOGL, AMZN, NVDA)
- Sends 10,000 change events
- Uses momentum-based price and volume changes

### Event Format

Events are sent in the Drasi HTTP source format:

```json
{
  "op": "u",
  "reactivatorStart_ns": 1700000000000000000,
  "reactivatorEnd_ns": 1700000001000000000,
  "payload": {
    "source": {
      "db": "test",
      "table": "node",
      "ts_ns": 1700000000000000000,
      "lsn": 1001
    },
    "before": {
      "id": "MSFT",
      "labels": ["Stock"],
      "properties": {
        "symbol": "MSFT",
        "name": "Microsoft Corporation",
        "price": 350.00,
        "volume": 25000000
      }
    },
    "after": {
      "id": "MSFT",
      "labels": ["Stock"],
      "properties": {
        "symbol": "MSFT",
        "name": "Microsoft Corporation",
        "price": 351.25,
        "volume": 26500000
      }
    }
  }
}
```

## Monitoring

- Check the test service logs for dispatch status
- Monitor the Drasi Server logs to confirm event reception
- View reaction results in the HTTP handler logs

## Troubleshooting

1. **Connection Refused**: Ensure Drasi Server is running with HTTP source on port 9000
2. **404 Not Found**: Verify the source ID matches in both configurations
3. **Timeout Errors**: Check network connectivity and firewall settings

## Customization

You can modify the configuration to:
- Change the Drasi Server endpoint (url/port)
- Enable batch mode for higher throughput
- Add more stocks to the simulation
- Adjust event generation rate and count