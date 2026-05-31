package router

import (
	"context"
	"net/http"
	"time"

	"github.com/mukundu/velo/internal/metrics"
)

// healthLoop polls /health on each backend at HealthInterval. A backend that
// fails FailureThreshold consecutive checks flips to unhealthy; a single
// successful check flips it back. This gives flapping backends a chance to
// recover quickly without ping-ponging on every blip.
func (r *Router) healthLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.HealthInterval)
	defer t.Stop()
	r.checkAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.checkAll(ctx)
		}
	}
}

func (r *Router) checkAll(ctx context.Context) {
	for _, b := range r.backends {
		b := b
		go r.checkOne(ctx, b)
	}
}

func (r *Router) checkOne(ctx context.Context, b *Backend) {
	cctx, cancel := context.WithTimeout(ctx, r.cfg.HealthTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, b.URL+r.cfg.HealthCheckPath, nil)
	if err != nil {
		r.recordHealth(b, false)
		return
	}
	resp, err := b.client.Do(req)
	if err != nil || resp.StatusCode/100 != 2 {
		r.recordHealth(b, false)
		if resp != nil {
			resp.Body.Close()
		}
		return
	}
	resp.Body.Close()
	r.recordHealth(b, true)
}

func (r *Router) recordHealth(b *Backend, ok bool) {
	if ok {
		b.mu.Lock()
		b.failures = 0
		b.mu.Unlock()
		if !b.healthy.Load() {
			b.healthy.Store(true)
		}
		metrics.BackendUp.WithLabelValues(b.Name).Set(1)
		return
	}
	b.mu.Lock()
	b.failures++
	threshold := r.cfg.FailureThreshold
	b.mu.Unlock()
	if b.failures >= threshold && b.healthy.Load() {
		b.healthy.Store(false)
		metrics.BackendUp.WithLabelValues(b.Name).Set(0)
		metrics.BackendErrors.WithLabelValues(b.Name, "unhealthy").Inc()
	}
}
