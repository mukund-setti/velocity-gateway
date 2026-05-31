// Package router selects an upstream LLM backend and forwards requests to it.
//
// Selection strategy is "weighted power-of-two-choices" (weighted P2C):
// pick two healthy backends at random (with probability proportional to
// their configured weights), then pick the one with fewer in-flight
// requests. P2C is the gold-standard low-overhead load-balancing strategy —
// it's cheap, has tight tail-latency bounds in theory and practice, and
// degrades to plain weighted-random when N=1 or N=2.
//
// On a request error or timeout, the router transparently retries up to
// RetryAttempts times against *other* healthy backends (never the same one
// twice in the same request). This turns a transient single-backend
// failure into invisible jitter rather than a 5xx.
package router

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/mukundu/velo/internal/config"
	"github.com/mukundu/velo/internal/metrics"
)

// Router orchestrates a pool of backends.
type Router struct {
	cfg      config.Router
	backends []*Backend

	mu     sync.Mutex
	rrIdx  int

	stop chan struct{}
}

// New constructs a Router from config and starts its health-check loop.
func New(cfg config.Router) (*Router, error) {
	if len(cfg.Backends) == 0 {
		return nil, errors.New("router: no backends configured")
	}
	r := &Router{cfg: cfg, stop: make(chan struct{})}
	for _, b := range cfg.Backends {
		r.backends = append(r.backends, NewBackend(b.Name, b.URL, b.Weight, cfg.RequestTimeout))
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-r.stop
		cancel()
	}()
	go r.healthLoop(ctx)
	// Mark all backends initially up — the first health tick will correct any.
	for _, b := range r.backends {
		metrics.BackendUp.WithLabelValues(b.Name).Set(1)
	}
	return r, nil
}

// Close stops the health-check loop.
func (r *Router) Close() error {
	close(r.stop)
	return nil
}

// Do forwards a chat-completion body to one of the configured backends.
// On retriable errors it tries another backend, up to RetryAttempts times.
// It returns the upstream response *and* the backend name that served it.
func (r *Router) Do(ctx context.Context, body []byte, stream bool) (*http.Response, string, error) {
	attempts := r.cfg.RetryAttempts
	if attempts < 1 {
		attempts = 1
	}
	tried := map[string]bool{}
	var lastErr error
	for i := 0; i < attempts; i++ {
		b := r.pick(tried)
		if b == nil {
			break
		}
		tried[b.Name] = true
		resp, err := r.doOnce(ctx, b, body, stream)
		if err == nil {
			return resp, b.Name, nil
		}
		lastErr = err
		metrics.BackendErrors.WithLabelValues(b.Name, classify(err)).Inc()
		b.markFailure()
		if i+1 < attempts {
			metrics.BackendRetries.Inc()
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no healthy backends available")
	}
	return nil, "", lastErr
}

func (r *Router) doOnce(ctx context.Context, b *Backend, body []byte, stream bool) (*http.Response, error) {
	t0 := time.Now()
	b.inFlight.Add(1)

	url := b.URL + "/v1/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		b.inFlight.Add(-1)
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}

	resp, err := b.client.Do(req)
	if err != nil {
		b.inFlight.Add(-1)
		return nil, err
	}
	if resp.StatusCode/100 == 5 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		b.inFlight.Add(-1)
		return nil, fmt.Errorf("upstream %s: %d %s", b.Name, resp.StatusCode, string(buf))
	}
	// Wrap the body so we can decrement in-flight + record latency on close.
	wrapped := &countingBody{ReadCloser: resp.Body, onClose: func() {
		b.inFlight.Add(-1)
		metrics.BackendLatency.WithLabelValues(b.Name).Observe(time.Since(t0).Seconds())
		b.markSuccess()
	}}
	resp.Body = wrapped
	return resp, nil
}

// pick chooses a backend using weighted power-of-two-choices among the
// healthy backends not already in `exclude`. Falls back to any healthy
// backend not in `exclude` if only one such backend exists.
func (r *Router) pick(exclude map[string]bool) *Backend {
	candidates := make([]*Backend, 0, len(r.backends))
	for _, b := range r.backends {
		if exclude[b.Name] {
			continue
		}
		if !b.IsHealthy() {
			continue
		}
		candidates = append(candidates, b)
	}
	if len(candidates) == 0 {
		// Last resort: pick any non-excluded backend even if unhealthy —
		// better to try a degraded backend than to return 503.
		for _, b := range r.backends {
			if !exclude[b.Name] {
				return b
			}
		}
		return nil
	}
	if r.cfg.Strategy == "round_robin" {
		r.mu.Lock()
		b := candidates[r.rrIdx%len(candidates)]
		r.rrIdx++
		r.mu.Unlock()
		return b
	}
	// Default: weighted P2C.
	if len(candidates) == 1 {
		return candidates[0]
	}
	a := weightedPick(candidates)
	c := weightedPick(candidates)
	if a == c && len(candidates) > 1 {
		// Force two distinct picks.
		for i, x := range candidates {
			if x != a {
				c = candidates[i]
				break
			}
		}
	}
	if a.InFlight() <= c.InFlight() {
		return a
	}
	return c
}

func weightedPick(bs []*Backend) *Backend {
	total := 0
	for _, b := range bs {
		total += b.Weight
	}
	if total <= 0 {
		return bs[rand.Intn(len(bs))]
	}
	x := rand.Intn(total)
	for _, b := range bs {
		x -= b.Weight
		if x < 0 {
			return b
		}
	}
	return bs[len(bs)-1]
}

func classify(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "error"
}

type countingBody struct {
	io.ReadCloser
	onClose func()
	closed  bool
}

func (c *countingBody) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.onClose != nil {
		c.onClose()
	}
	return c.ReadCloser.Close()
}
