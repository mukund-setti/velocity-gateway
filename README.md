# Velo — an LLM inference gateway in Go

Velo is a high-throughput reverse proxy for OpenAI-compatible LLM endpoints.
It sits between client traffic and one or more model servers and adds:

- **Continuous micro-batching** — admission scheduler that groups compatible
  requests so backends with in-flight batching (vLLM, TGI, llama.cpp) get a
  tight burst instead of trickled-in singletons.
- **Semantic caching** — pgvector-backed nearest-neighbor cache with a
  pluggable embedder; serves cached responses on prompt paraphrases.
- **Distributed rate limiting** — atomic Redis token-bucket per API key,
  with graceful in-process fallback.
- **Multi-backend routing** — weighted power-of-two-choices load balancing
  across N backends, with health checks and transparent failover on error
  or timeout.
- **Full observability** — Prometheus metrics for tokens/sec, queue depth,
  batch size, latency percentiles, cache hit rate, backend health and
  simulated GPU utilization, plus a pre-built Grafana dashboard.

Everything runs out of the box with `docker compose up`. There's a mock
OpenAI-compatible backend baked in, so you can benchmark the full stack
without a real GPU or API key.

## Architecture

```
                    ┌──────────────────────────────────────────────┐
   client ─────────►│ Gateway (chi, SSE-aware)                     │
   (OpenAI SDK)     │   ├─ API-key auth                            │
                    │   ├─ Rate limiter   ──► Redis (token bucket) │
                    │   ├─ Semantic cache ──► Postgres + pgvector  │
                    │   └─ Scheduler                               │
                    │        └─ continuous micro-batcher           │
                    │             └─ Router                        │
                    │                  ├─ weighted P2C             │
                    │                  ├─ health-checked pool      │
                    │                  └─ retry / failover         │
                    └────────────┬─────────────────────┬───────────┘
                                 │                     │
                                 ▼                     ▼
                          ┌──────────────┐      ┌──────────────┐
                          │ mock-backend │      │ mock-backend │
                          │  (vLLM-like) │      │              │
                          └──────────────┘      └──────────────┘
                                 │                     │
                                 └──────► Prometheus ──┘
                                                │
                                                ▼
                                              Grafana
```

## The problem it solves

LLM workloads are uniquely hostile to traditional reverse proxies:

- **Long-lived streaming responses** mean connections stay open for seconds
  per call, so back-pressure has to be explicit (queue depth, not connection
  count).
- **Throughput scales with batching**, but only inside the model server, so
  the gateway has to deliver requests in tight temporal bursts to let the
  backend's iteration-level batcher group them on the GPU.
- **Prompt traffic is heavily redundant** in production — system prompts,
  RAG templates, few-shot exemplars — so semantic caching gives a massive
  free win compared to a key-by-hash cache.
- **Failures are common** (rate limits, OOMs, cold starts), so failover
  needs to be transparent at the request level, not just at the connection
  level.

Velo is one process that handles all four.

## Quickstart

```bash
# 1. Bring up the full stack.
make up

# 2. Send a streaming request.
curl -N http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer sk-velo-dev" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mock-llm",
    "stream": true,
    "messages": [{"role":"user","content":"Explain continuous batching"}]
  }'

# 3. Open Grafana → http://localhost:3000 → "Velo Gateway" dashboard.

# 4. Run the benchmark (cold vs warm comparison, markdown report).
make bench
cat bench/out/report.md
```

## Benchmark

`make bench` runs two passes against a fresh stack, 16 concurrent streaming
workers for 30 s each, against two mock backends (15 ms and 20 ms per-token
latency, 40 max tokens):

1. **Cold** — `Cache-Control: no-store` and unique prompts. Measures the
   cache-off floor: just batching + routing + streaming overhead.
2. **Warm** — cache enabled, ~60 % of requests reuse a recent prompt. The
   semantic cache catches paraphrases and the scheduler has steady traffic
   to micro-batch.

### Results (Windows 11 / Go 1.26 / 2× local mock backend)

| metric              | cold (cache off)       | warm (batching + cache) | delta    |
|---------------------|------------------------|-------------------------|----------|
| Requests/sec        | 21.6                   | 136.6                   | **+532 %** |
| Tokens/sec          | 851.9                  | 5,450.4                 | **+540 %** |
| Latency p50         | 669 ms                 | 98 ms                   | **−85 %**  |
| Latency p95         | 870 ms                 | 105 ms                  | **−88 %**  |
| Latency p99         | 876 ms                 | 857 ms                  | −2 %      |
| TTFT p50            | 41.5 ms                | 1.1 ms                  | **−97 %**  |
| Cache hit rate      | 0.0 %                  | 97.0 %                  | +97 pp   |

p99 stays around the cold value because a cache miss in the warm run still
pays the full mock-backend latency (40 tokens × 15 ms ≈ 600 ms + queueing).
That's the right behaviour — the median improves dramatically with cache
hits, but the tail is bounded by the slowest upstream path. With faster
real backends (or larger `MaxBatchSize`) the tail compresses too.

Re-run on your hardware with `make bench` and paste the new table here.

## Design notes

### Continuous batching at the gateway tier

True continuous batching ("iteration-level scheduling") happens inside the
model server. The gateway can't reach into the backend's KV cache, but it
can do the next-best thing: hold a request for a few milliseconds so
compatible peers arrive, then release them as a tightly-spaced burst. A
backend that supports in-flight batching will fuse them into a single GPU
forward pass; one that doesn't will still benefit from warm connections.

Two knobs drive the throughput/latency tradeoff:

- `MaxBatchSize` (default 16) — flush as soon as this many requests pile up.
- `MaxWait` (default 40ms) — flush every batch this often regardless of
  size, so a quiet system doesn't stall the lone request waiting.

The implementation lives in [`internal/scheduler/scheduler.go`](internal/scheduler/scheduler.go)
and is intentionally short — the trick is the bookkeeping, not the math.

### Semantic cache

Embeddings live behind a one-method interface. There are two implementations:

- `HashingEmbedder` — hashing-trick embedder with FNV + sign bits, no
  dependencies. Used in tests, benchmarks, and any deployment that doesn't
  want to pay for embeddings on every request.
- `OpenAIEmbedder` — real `text-embedding-3-small` calls.

The store sits behind another one-method interface. The production store is
pgvector with an IVFFlat cosine index; the test store is an in-process slice
with brute-force linear scan. We L2-normalize on write so the index can use
cosine distance directly and the in-memory store can use dot products.

Threshold of 0.92 was picked by hand to err strongly on the side of false
negatives — the cost of a false cache hit is a wrong answer; the cost of a
miss is a recomputation. Tune in `configs/velo.yaml`.

### Rate limiter

Token-bucket per API key, atomic via a Lua script on Redis so N gateway
replicas share one bucket. If Redis becomes unreachable we fall back to a
per-process bucket and log — we **never** fail-closed on Redis errors,
because a Redis outage shouldn't take down the gateway.

### Router

Weighted power-of-two-choices: pick two healthy backends weighted by their
configured weights, then pick the one with fewer in-flight requests. P2C is
the standard low-overhead load-balancer because it has tight tail-latency
bounds without per-decision global state.

Health is a Boolean — a backend that fails `FailureThreshold` consecutive
health probes flips to unhealthy; a single successful probe flips it back.
On a transient request error, the router retries on a different healthy
backend up to `RetryAttempts` times; failure to all backends propagates as
`502`.

### Observability

Everything important is on the Grafana dashboard:

- Requests/sec, tokens/sec, cache hit rate, queue depth (stats)
- Latency p50/p95/p99 and TTFT (time series)
- Scheduler batch size distribution and dispatch rate
- Per-backend health and latency
- Simulated GPU utilization (from the mock backend)
- Rate-limit and scheduler rejections

The mock backend exposes `mock_backend_gpu_utilization` that scales with
in-flight requests, so the dashboard looks realistic without a real GPU.

## Repo layout

```
cmd/
  velo/                  # main gateway binary  (`velo serve`)
  mock-backend/          # OpenAI-compatible mock LLM
internal/
  gateway/               # HTTP server, auth, SSE, handler
  scheduler/             # continuous micro-batcher  ← centerpiece
  cache/                 # semantic cache (embedder + store interfaces)
  ratelimit/             # token-bucket (redis + memory)
  router/                # health-checked weighted-P2C backend pool
  metrics/               # Prometheus registry
  config/                # YAML + env-var config
bench/
  cmd/load/              # streaming benchmark harness
  prompts.txt            # bench prompts
deploy/
  docker-compose.yml
  Dockerfile.velo, Dockerfile.mock-backend
  prometheus.yml
  grafana/               # provisioning + dashboard JSON
  postgres/init.sql
configs/
  velo.yaml              # reference config (all env vars documented inline)
```

## Configuration

Velo loads YAML and lets `VELO_*` env vars override any value (full list
in [`internal/config/config.go`](internal/config/config.go)). The defaults
in `configs/velo.yaml` are tuned for the docker compose dev stack.

Notable env vars:

| variable                    | purpose                                |
|-----------------------------|----------------------------------------|
| `VELO_SERVER_ADDR`          | gateway listen address                 |
| `VELO_API_KEYS`             | comma-separated allowed API keys       |
| `VELO_SCHEDULER_MAX_BATCH`  | scheduler max batch size               |
| `VELO_SCHEDULER_MAX_WAIT`   | scheduler max wait (e.g. `40ms`)       |
| `VELO_CACHE_ENABLED`        | toggle semantic cache                  |
| `VELO_CACHE_THRESHOLD`      | cosine similarity cutoff               |
| `VELO_POSTGRES_URL`         | pgvector DSN                           |
| `VELO_REDIS_ADDR`           | Redis host:port                        |
| `VELO_BACKENDS`             | `name1=url1,name2=url2`                |
| `VELO_OPENAI_API_KEY`       | only needed when `embedder: openai`    |

## Development

```bash
make test         # go test ./internal/...
make test-race    # with -race
make lint         # golangci-lint (falls back to go vet)
make build        # local binaries → bin/
```

CI runs the same `test`, `lint`, and `docker build` matrix on every push;
see [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## License

MIT — see [LICENSE](LICENSE).
