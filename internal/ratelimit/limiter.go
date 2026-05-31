// Package ratelimit provides a distributed token-bucket rate limiter.
//
// Two implementations live behind the same Limiter interface:
//
//   - RedisLimiter: a Redis-backed bucket that runs an atomic Lua script so
//     N gateway replicas share one bucket per API key. This is the default.
//   - MemoryLimiter: a per-process bucket. Used when Redis is unreachable
//     (graceful degradation) and in tests.
//
// Both use the same canonical bucket math: a bucket has capacity B and
// refills at R tokens/sec. Allow() deducts one token if available; if
// not, the call is rejected.
package ratelimit

import (
	"context"
	"errors"
	"time"
)

// Limiter is the interface used by the gateway.
type Limiter interface {
	Allow(ctx context.Context, key string) (bool, error)
	Close() error
}

// ErrNoKey is returned when a request arrived without an API key.
var ErrNoKey = errors.New("ratelimit: missing api key")

// Config is shared by all implementations.
type Config struct {
	RPS        int           // refill rate (tokens/sec)
	Burst      int           // bucket capacity
	WindowSize time.Duration // averaging window for the refill calculation
}

func (c Config) effective() (int, int) {
	rps, burst := c.RPS, c.Burst
	if rps <= 0 {
		rps = 10
	}
	if burst <= 0 {
		burst = rps * 2
	}
	return rps, burst
}
