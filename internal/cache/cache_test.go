package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHashingEmbedder_Deterministic(t *testing.T) {
	e := NewHashingEmbedder(128)
	v1, err := e.Embed(context.Background(), "What is the CAP theorem?")
	require.NoError(t, err)
	v2, err := e.Embed(context.Background(), "What is the CAP theorem?")
	require.NoError(t, err)
	require.Equal(t, v1, v2)
}

func TestHashingEmbedder_SimilarPromptsAreNearer(t *testing.T) {
	e := NewHashingEmbedder(384)
	ctx := context.Background()
	a, _ := e.Embed(ctx, "Explain how transformers work in deep learning")
	b, _ := e.Embed(ctx, "Explain how transformers work in deep learning architectures")
	c, _ := e.Embed(ctx, "What is the capital of France")

	simAB := cosineDot(a, b)
	simAC := cosineDot(a, c)
	require.Greater(t, simAB, simAC, "near-paraphrase should be closer than unrelated prompt")
	require.Greater(t, simAB, 0.7, "near-paraphrase should be very similar")
}

func TestMemoryStore_HitMiss(t *testing.T) {
	ctx := context.Background()
	emb := NewHashingEmbedder(256)
	store := NewMemoryStore(100)
	c := New(emb, store, 0.85, 10*time.Minute)

	hit, err := c.Lookup(ctx, "anything")
	require.NoError(t, err)
	require.Nil(t, hit, "fresh cache should miss")

	prompt := "Describe how Raft achieves leader election"
	require.NoError(t, c.Store(ctx, prompt, &Entry{
		Model: "m", Content: "Raft elects leaders via randomized timeouts and vote requests.",
	}))

	// Exact match → hit.
	hit, err = c.Lookup(ctx, prompt)
	require.NoError(t, err)
	require.NotNil(t, hit)
	require.Contains(t, hit.Content, "Raft")

	// Near-paraphrase → hit (with similarity ≥ 0.85 from hashing).
	hit, err = c.Lookup(ctx, "Describe Raft leader election")
	require.NoError(t, err)
	require.NotNil(t, hit, "near-paraphrase should hit at threshold 0.85")

	// Completely unrelated → miss.
	hit, err = c.Lookup(ctx, "What is HNSW vector search")
	require.NoError(t, err)
	require.Nil(t, hit)
}

func TestMemoryStore_TTLExpiry(t *testing.T) {
	ctx := context.Background()
	emb := NewHashingEmbedder(128)
	store := NewMemoryStore(100)
	c := New(emb, store, 0.5, 50*time.Millisecond)

	prompt := "Explain backpressure"
	require.NoError(t, c.Store(ctx, prompt, &Entry{Content: "stuff"}))
	hit, _ := c.Lookup(ctx, prompt)
	require.NotNil(t, hit)

	time.Sleep(100 * time.Millisecond)
	hit, _ = c.Lookup(ctx, prompt)
	require.Nil(t, hit, "entry should be expired by TTL")
}

func TestMemoryStore_Eviction(t *testing.T) {
	ctx := context.Background()
	emb := NewHashingEmbedder(64)
	store := NewMemoryStore(3)
	c := New(emb, store, 0.5, time.Hour)

	for i := 0; i < 10; i++ {
		require.NoError(t, c.Store(ctx, time.Now().String()+":prompt"+string(rune('a'+i)), &Entry{Content: "x"}))
		time.Sleep(time.Millisecond)
	}
	n, _ := store.Size(ctx)
	require.LessOrEqual(t, n, 3, "memory store should cap at maxSize")
}
