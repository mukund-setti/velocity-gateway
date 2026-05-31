// Package cache implements Velo's semantic cache.
//
// On each incoming chat request, the gateway flattens the prompt, embeds it,
// and asks the configured Store for the nearest neighbor by cosine
// similarity. If the best match is above SimilarityThreshold and not expired,
// the stored completion is returned and the gateway short-circuits the
// upstream call. On miss, the gateway forwards to the backend and writes
// the result back into the cache.
//
// # Design decisions
//
//   - We index by *embedding*, not by an exact prompt hash, because the
//     value of semantic caching is matching paraphrases. A trigram or LSH
//     index would also work, but pgvector + cosine is the simplest thing
//     that holds up at production scale.
//   - The Embedder interface is swappable: a deterministic HashingEmbedder
//     for offline tests/benchmarks, the OpenAIEmbedder for production. This
//     means the rest of the system never has to mock an embeddings API.
//   - We L2-normalize embeddings at write time so cosine similarity reduces
//     to a vector dot product, which lets pgvector use its IVFFlat index
//     unchanged.
//   - The threshold defaults to 0.92 — conservative enough that "what is X"
//     and "explain X" don't collide unless the user wants them to.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/mukundu/velo/internal/metrics"
)

// Entry is a single cached completion.
type Entry struct {
	Model     string          `json:"model"`
	Content   string          `json:"content"`
	Raw       json.RawMessage `json:"raw,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// Store is the pluggable persistence layer (pgvector or in-memory).
type Store interface {
	Nearest(ctx context.Context, vec []float32, threshold float64, freshUntil time.Time) (*Entry, float64, error)
	Put(ctx context.Context, vec []float32, prompt string, entry *Entry) error
	Size(ctx context.Context) (int, error)
	Close() error
}

// Cache ties an Embedder and a Store together with a similarity threshold
// and a TTL policy.
type Cache struct {
	emb       Embedder
	store     Store
	threshold float64
	ttl       time.Duration
}

func New(emb Embedder, store Store, threshold float64, ttl time.Duration) *Cache {
	return &Cache{emb: emb, store: store, threshold: threshold, ttl: ttl}
}

// Lookup runs an embedding + nearest-neighbor search. Returns (nil, nil) on
// a clean miss; only returns an error for actual infrastructure failures.
func (c *Cache) Lookup(ctx context.Context, prompt string) (*Entry, error) {
	if c == nil || c.store == nil {
		return nil, nil
	}
	t0 := time.Now()
	defer func() { metrics.CacheLookupSeconds.Observe(time.Since(t0).Seconds()) }()

	vec, err := c.emb.Embed(ctx, prompt)
	if err != nil {
		return nil, err
	}
	freshUntil := time.Now().Add(-c.ttl)
	entry, _, err := c.store.Nearest(ctx, vec, c.threshold, freshUntil)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		metrics.CacheMisses.Inc()
		return nil, nil
	}
	metrics.CacheHits.Inc()
	return entry, nil
}

// Store writes a completion under the embedding of its prompt.
func (c *Cache) Store(ctx context.Context, prompt string, entry *Entry) error {
	if c == nil || c.store == nil {
		return nil
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	vec, err := c.emb.Embed(ctx, prompt)
	if err != nil {
		return err
	}
	if err := c.store.Put(ctx, vec, prompt, entry); err != nil {
		return err
	}
	if n, err := c.store.Size(ctx); err == nil {
		metrics.CacheStoreSize.Set(float64(n))
	}
	return nil
}

// Close releases store resources.
func (c *Cache) Close() error {
	if c == nil || c.store == nil {
		return nil
	}
	return c.store.Close()
}

// ErrNotConfigured is returned when callers try to use a cache that wasn't
// fully wired (no store, no embedder).
var ErrNotConfigured = errors.New("cache not configured")
