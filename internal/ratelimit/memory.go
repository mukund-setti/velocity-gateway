package ratelimit

import (
	"context"
	"sync"
	"time"
)

// MemoryLimiter is a single-process token bucket. Maintains one bucket per
// key with lazy creation. Refill is computed on each Allow() call.
type MemoryLimiter struct {
	cfg     Config
	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func NewMemoryLimiter(cfg Config) *MemoryLimiter {
	return &MemoryLimiter{cfg: cfg, buckets: map[string]*bucket{}}
}

func (m *MemoryLimiter) Allow(_ context.Context, key string) (bool, error) {
	if key == "" {
		return false, ErrNoKey
	}
	rps, burst := m.cfg.effective()
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[key]
	now := time.Now()
	if !ok {
		b = &bucket{tokens: float64(burst), last: now}
		m.buckets[key] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * float64(rps)
	if b.tokens > float64(burst) {
		b.tokens = float64(burst)
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens -= 1
		return true, nil
	}
	return false, nil
}

func (m *MemoryLimiter) Close() error { return nil }
