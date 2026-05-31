package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryLimiter_BurstThenRefill(t *testing.T) {
	lim := NewMemoryLimiter(Config{RPS: 10, Burst: 5})
	ctx := context.Background()

	// First 5 calls should pass (burst capacity).
	for i := 0; i < 5; i++ {
		ok, err := lim.Allow(ctx, "k1")
		require.NoError(t, err)
		require.True(t, ok, "burst token %d should be granted", i+1)
	}
	// 6th should be rejected.
	ok, err := lim.Allow(ctx, "k1")
	require.NoError(t, err)
	require.False(t, ok, "bucket should be empty after burst")

	// After ~150ms, ≥1 token should refill at 10 rps.
	time.Sleep(150 * time.Millisecond)
	ok, err = lim.Allow(ctx, "k1")
	require.NoError(t, err)
	require.True(t, ok, "expected at least one refilled token")
}

func TestMemoryLimiter_PerKeyIsolation(t *testing.T) {
	lim := NewMemoryLimiter(Config{RPS: 1, Burst: 2})
	ctx := context.Background()
	// Drain k1's bucket.
	for i := 0; i < 2; i++ {
		ok, _ := lim.Allow(ctx, "k1")
		require.True(t, ok)
	}
	ok, _ := lim.Allow(ctx, "k1")
	require.False(t, ok, "k1 should be drained")
	// k2 should still have its full burst.
	ok, _ = lim.Allow(ctx, "k2")
	require.True(t, ok, "k2 bucket must be independent")
}

func TestMemoryLimiter_RequiresKey(t *testing.T) {
	lim := NewMemoryLimiter(Config{RPS: 1, Burst: 1})
	_, err := lim.Allow(context.Background(), "")
	require.ErrorIs(t, err, ErrNoKey)
}

func TestConfigEffective_Defaults(t *testing.T) {
	c := Config{}
	rps, burst := c.effective()
	require.Greater(t, rps, 0)
	require.GreaterOrEqual(t, burst, rps)
}
