package ratelimit

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// allowScript is an atomic token-bucket update.
// KEYS[1] = bucket key
// ARGV[1] = refill rate (tokens/sec)
// ARGV[2] = burst (capacity)
// ARGV[3] = now (unix ms)
//
// Returns 1 if allowed, 0 if rejected.
//
// Bucket state is stored as a hash with fields `t` (tokens, float) and `ts`
// (last refill timestamp, ms). A 1-hour TTL keeps idle keys from sticking
// around forever.
var allowScript = redis.NewScript(`
local key   = KEYS[1]
local rps   = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now   = tonumber(ARGV[3])

local data = redis.call('HMGET', key, 't', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end
local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(burst, tokens + elapsed * rps)

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HSET', key, 't', tokens, 'ts', now)
redis.call('PEXPIRE', key, 3600000)
return allowed
`)

// RedisLimiter is a Redis-backed token bucket. It uses an atomic Lua script
// so concurrent gateway replicas can share one bucket per API key.
//
// If Redis becomes unreachable, Allow() falls back to a per-process
// MemoryLimiter and logs the degradation. We *never* fail-closed on Redis
// errors — that would turn a Redis outage into a gateway outage.
type RedisLimiter struct {
	cfg      Config
	client   *redis.Client
	fallback *MemoryLimiter
}

func NewRedisLimiter(addr string, cfg Config) (*RedisLimiter, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &RedisLimiter{
		cfg:      cfg,
		client:   client,
		fallback: NewMemoryLimiter(cfg),
	}, nil
}

func (r *RedisLimiter) Allow(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, ErrNoKey
	}
	rps, burst := r.cfg.effective()
	now := time.Now().UnixMilli()
	v, err := allowScript.Run(ctx, r.client, []string{"velo:rl:" + key}, rps, burst, now).Int()
	if err != nil {
		// Degrade rather than fail-closed.
		log.Printf("ratelimit: redis error %v, falling back to in-memory", err)
		return r.fallback.Allow(ctx, key)
	}
	return v == 1, nil
}

func (r *RedisLimiter) Close() error {
	return r.client.Close()
}
