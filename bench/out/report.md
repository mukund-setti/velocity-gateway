## Velo benchmark — concurrency=16 duration=30s

| metric              | cold (cache miss only) | warm (batching + cache) | delta |
|---------------------|------------------------|-------------------------|-------|
| Requests/sec        | 21.6                  | 136.6                   | +532% |
| Tokens/sec          | 851.9                  | 5450.4                   | +540% |
| Latency p50         | 669.4 ms               | 97.6 ms                | -85% |
| Latency p95         | 869.7 ms               | 104.9 ms                | -88% |
| Latency p99         | 875.5 ms               | 856.6 ms                | -2% |
| TTFT p50            | 41.5 ms               | 1.1 ms                | -97% |
| Cache hit rate      | 0.0%                | 97.0%                 | +97.0pp |
