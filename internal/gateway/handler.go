package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/mukundu/velo/internal/cache"
	"github.com/mukundu/velo/internal/metrics"
	"github.com/mukundu/velo/internal/ratelimit"
	"github.com/mukundu/velo/internal/router"
	"github.com/mukundu/velo/internal/scheduler"
)

// Deps bundles the subsystem dependencies the gateway handler needs.
// Any of these may be nil — the handler degrades gracefully.
type Deps struct {
	Cache       *cache.Cache
	Limiter     ratelimit.Limiter
	Scheduler   *scheduler.Scheduler
	Router      *router.Router
}

// chatHandler implements POST /v1/chat/completions.
//
// Pipeline:
//   1. Parse request, capture raw bytes for verbatim forwarding.
//   2. Apply per-API-key rate limit.
//   3. Embed prompt and try semantic cache. On hit, replay and return.
//   4. Otherwise hand off to the scheduler, which batches compatible
//      requests and dispatches them via the router to an upstream backend.
//   5. Stream the upstream response back, and write the completion into
//      the semantic cache for future hits.
func (s *Server) chatHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := 200
	defer func() {
		metrics.RequestsTotal.WithLabelValues("/v1/chat/completions", r.Method, statusLabel(status)).Inc()
		metrics.RequestDuration.WithLabelValues("/v1/chat/completions", statusLabel(status)).Observe(time.Since(start).Seconds())
	}()

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		status = http.StatusBadRequest
		http.Error(w, `{"error":"body too large or unreadable"}`, status)
		return
	}
	var req ChatRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		status = http.StatusBadRequest
		http.Error(w, fmt.Sprintf(`{"error":"bad json: %s"}`, err), status)
		return
	}
	req.Raw = raw

	apiKey := APIKeyFromContext(r.Context())
	if s.deps.Limiter != nil {
		ok, err := s.deps.Limiter.Allow(r.Context(), apiKey)
		if err != nil {
			log.Printf("ratelimit error for key=%q: %v (allowing)", apiKey, err)
		} else if !ok {
			status = http.StatusTooManyRequests
			metrics.RateLimitRejections.WithLabelValues(apiKey).Inc()
			w.Header().Set("Retry-After", "1")
			http.Error(w, `{"error":"rate limit exceeded"}`, status)
			return
		}
	}

	prompt := flattenPrompt(req.Messages)

	// 1. Semantic cache lookup.
	if s.deps.Cache != nil {
		if hit, err := s.deps.Cache.Lookup(r.Context(), prompt); err != nil {
			log.Printf("cache lookup error: %v", err)
		} else if hit != nil {
			s.serveCacheHit(w, r, &req, hit)
			return
		}
	}

	// 2. Hand off to scheduler/router. The scheduler is the centerpiece —
	//    it batches compatible requests and dispatches them via the router.
	job := &scheduler.Job{
		Model:    req.Model,
		Stream:   req.Stream,
		Body:     raw,
		Submitted: time.Now(),
		Done:     make(chan struct{}),
	}

	if s.deps.Scheduler != nil {
		if err := s.deps.Scheduler.Submit(r.Context(), job); err != nil {
			if errors.Is(err, scheduler.ErrQueueFull) {
				status = http.StatusServiceUnavailable
				http.Error(w, `{"error":"server busy, try again"}`, status)
				return
			}
			status = http.StatusInternalServerError
			http.Error(w, fmt.Sprintf(`{"error":"submit: %s"}`, err), status)
			return
		}
	} else {
		// Scheduler disabled — dispatch directly.
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			dispatchDirect(ctx, s.deps.Router, job)
		}()
	}

	// Wait for the dispatcher to acquire a backend connection and start
	// streaming. The dispatcher pushes the upstream response body into
	// job.Response (or an error into job.Err).
	select {
	case <-r.Context().Done():
		status = 499 // client closed connection
		return
	case <-job.Done:
	}

	if job.Err != nil {
		status = http.StatusBadGateway
		http.Error(w, fmt.Sprintf(`{"error":"upstream: %s"}`, job.Err), status)
		return
	}
	defer job.Response.Body.Close()

	if req.Stream {
		content, _, err := streamProxy(r.Context(), w, job.Response.Body, job.Backend, req.Model)
		if err != nil {
			log.Printf("stream proxy error: %v", err)
		}
		// Store in cache after a successful full stream.
		if s.deps.Cache != nil && content != "" {
			if err := s.deps.Cache.Store(context.Background(), prompt, &cache.Entry{
				Model: req.Model, Content: content,
			}); err != nil {
				log.Printf("cache store error: %v", err)
			}
		}
		return
	}

	// Non-streaming: forward the JSON body straight through but also peek at
	// it to populate the cache.
	body, err := io.ReadAll(job.Response.Body)
	if err != nil {
		status = http.StatusBadGateway
		http.Error(w, fmt.Sprintf(`{"error":"read upstream: %s"}`, err), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Velo-Backend", job.Backend)
	_, _ = w.Write(body)

	if s.deps.Cache != nil {
		if content := extractAssistantContent(body); content != "" {
			if err := s.deps.Cache.Store(context.Background(), prompt, &cache.Entry{
				Model: req.Model, Content: content, Raw: body,
			}); err != nil {
				log.Printf("cache store error: %v", err)
			}
		}
	}
}

func (s *Server) serveCacheHit(w http.ResponseWriter, r *http.Request, req *ChatRequest, hit *cache.Entry) {
	if req.Stream {
		_ = replayCachedStream(r.Context(), w, req.Model, hit.Content)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Velo-Cache", "HIT")
	if len(hit.Raw) > 0 {
		_, _ = w.Write(hit.Raw)
		return
	}
	resp := map[string]any{
		"id": fmt.Sprintf("chatcmpl-cache-%d", time.Now().UnixNano()),
		"object": "chat.completion",
		"created": time.Now().Unix(),
		"model": req.Model,
		"choices": []map[string]any{{
			"index": 0,
			"message": map[string]string{"role": "assistant", "content": hit.Content},
			"finish_reason": "stop",
		}},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// dispatchDirect is used when the scheduler is disabled. It executes the
// upstream call and signals the job, mirroring what the scheduler does.
func dispatchDirect(ctx context.Context, rt *router.Router, job *scheduler.Job) {
	defer close(job.Done)
	if rt == nil {
		job.Err = errors.New("no router configured")
		return
	}
	resp, backend, err := rt.Do(ctx, job.Body, job.Stream)
	if err != nil {
		job.Err = err
		return
	}
	job.Response = resp
	job.Backend = backend
}

func flattenPrompt(msgs []ChatMessage) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(m.Role)
		b.WriteString(": ")
		b.WriteString(m.Content)
		b.WriteByte('\n')
	}
	return strings.TrimSpace(b.String())
}

func extractAssistantContent(body []byte) string {
	var resp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(body), &resp); err != nil {
		return ""
	}
	if len(resp.Choices) == 0 {
		return ""
	}
	return resp.Choices[0].Message.Content
}

func statusLabel(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}
