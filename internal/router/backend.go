package router

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Backend is a single upstream LLM server the router can forward to.
//
// The struct mixes config (Name, URL, Weight) with runtime telemetry
// (healthy, in-flight count, recent failure count). The telemetry fields
// are read on every Pick() so they're either atomics or guarded by mu.
type Backend struct {
	Name   string
	URL    string
	Weight int

	client *http.Client

	mu         sync.Mutex
	failures   int       // consecutive health-check failures
	lastFailAt time.Time // last upstream error (not health check)

	healthy  atomic.Bool
	inFlight atomic.Int64
}

func NewBackend(name, url string, weight int, timeout time.Duration) *Backend {
	if weight <= 0 {
		weight = 1
	}
	b := &Backend{
		Name:   name,
		URL:    url,
		Weight: weight,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
	b.healthy.Store(true)
	return b
}

func (b *Backend) IsHealthy() bool { return b.healthy.Load() }
func (b *Backend) InFlight() int64 { return b.inFlight.Load() }

func (b *Backend) markFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	b.lastFailAt = time.Now()
}

func (b *Backend) markSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
}
