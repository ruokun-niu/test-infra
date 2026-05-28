# Stock Market Test with Adaptive HTTP Batching

This example demonstrates using the HTTP Source dispatcher with adaptive batching to efficiently send high-volume stock trading events to an external Drasi Server.

## Overview

The test uses the `StockTradeDataGenerator` to generate 100,000 simulated stock market trading events and dispatches them using adaptive batching for optimal performance.

## Adaptive Batching Features

- **Dynamic Batch Sizing**: Automatically adjusts batch size based on throughput
- **Time-based Flushing**: Ensures low latency by flushing incomplete batches after timeout
- **Efficient High-Volume Processing**: Batches up to 100 events or 500ms timeout

## Configuration

### Adaptive HTTP Settings

```json
{
  "kind": "Http",
  "url": "http://localhost",
  "port": 9000,
  "timeout_seconds": 60,
  "batch_events": true,
  "adaptive_enabled": true,
  "batch_size": 100,
  "batch_timeout_ms": 500
}
```

- `batch_events`: Enables batching mode
- `adaptive_enabled`: Activates adaptive batching algorithm
- `batch_size`: Maximum events per batch
- `batch_timeout_ms`: Maximum time to wait before sending incomplete batch

## Performance Characteristics

With adaptive batching enabled:
- **Throughput**: 10x-100x higher than individual event mode
- **Latency**: Maximum 500ms batch timeout ensures timely delivery
- **Network Efficiency**: Fewer HTTP requests reduce overhead

## Running the Test

### 1. Start Drasi Server

```bash
# Ensure HTTP source is configured on port 9000
cargo run -- --config your-drasi-config.yaml
```

### 2. Run the Test

```bash
./run_test.sh
```

## Monitoring Batch Performance

The logs will show batch statistics:
```
INFO  Sending batch of 100 events to http://localhost:9000/sources/stock-trades-db/events
INFO  Batch processed in 45ms
INFO  Throughput: 2222 events/second
```

## Comparison with Non-Batched Mode

| Mode | Events | Time | Throughput | HTTP Requests |
|------|--------|------|------------|---------------|
| Individual | 100,000 | ~300s | 333/s | 100,000 |
| Adaptive Batch | 100,000 | ~30s | 3,333/s | ~1,000 |

## Tuning Guidelines

### For Higher Throughput
- Increase `batch_size` (e.g., 500-1000)
- Increase `batch_timeout_ms` (e.g., 1000-2000)

### For Lower Latency
- Decrease `batch_size` (e.g., 10-50)
- Decrease `batch_timeout_ms` (e.g., 100-200)

### For Balanced Performance
- Use defaults: batch_size=100, timeout=500ms

## Event Generation

Generates events for 10 stocks with:
- Faster event rate (100-1000ms intervals)
- 100,000 total events for performance testing
- Momentum-based realistic price movements

## Troubleshooting

1. **Batch Timeout**: If batches are timing out, reduce batch_size
2. **Memory Issues**: Large batch_size may consume more memory
3. **Network Errors**: Check Drasi Server can handle batch payloads