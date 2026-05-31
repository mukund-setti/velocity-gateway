package cache

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore is an in-process cache store useful for tests, benchmarks, and
// single-instance deployments that don't want a Postgres dependency.
//
// It does a brute-force linear scan on every Nearest() call. That's O(N×d)
// per lookup, but for the dimensions and entry counts the gateway sees in
// practice (a few thousand entries × 384 floats), it stays well under a
// millisecond — and we get exact nearest-neighbor without needing an index.
type MemoryStore struct {
	mu       sync.RWMutex
	entries  []memEntry
	maxSize  int
}

type memEntry struct {
	vec   []float32
	entry *Entry
}

func NewMemoryStore(maxSize int) *MemoryStore {
	if maxSize <= 0 {
		maxSize = 10_000
	}
	return &MemoryStore{maxSize: maxSize}
}

func (m *MemoryStore) Nearest(_ context.Context, vec []float32, threshold float64, freshUntil time.Time) (*Entry, float64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	bestSim := -1.0
	var best *Entry
	for _, e := range m.entries {
		if e.entry.CreatedAt.Before(freshUntil) {
			continue
		}
		sim := cosineDot(vec, e.vec)
		if sim > bestSim {
			bestSim = sim
			best = e.entry
		}
	}
	if best == nil || bestSim < threshold {
		return nil, bestSim, nil
	}
	return best, bestSim, nil
}

func (m *MemoryStore) Put(_ context.Context, vec []float32, _ string, entry *Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, memEntry{vec: vec, entry: entry})
	// Simple FIFO eviction once we exceed cap.
	if len(m.entries) > m.maxSize {
		// Keep the most recent maxSize entries.
		sort.SliceStable(m.entries, func(i, j int) bool {
			return m.entries[i].entry.CreatedAt.After(m.entries[j].entry.CreatedAt)
		})
		m.entries = m.entries[:m.maxSize]
	}
	return nil
}

func (m *MemoryStore) Size(_ context.Context) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries), nil
}

func (m *MemoryStore) Close() error { return nil }

// cosineDot assumes both vectors are L2-normalized. Under that assumption
// cosine similarity is just the dot product.
func cosineDot(a, b []float32) float64 {
	if len(a) != len(b) {
		return -1
	}
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}
