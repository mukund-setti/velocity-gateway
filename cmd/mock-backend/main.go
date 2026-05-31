// Command mock-backend simulates an OpenAI-compatible LLM endpoint.
//
// It implements POST /v1/chat/completions with both streaming (SSE) and
// non-streaming responses, configurable per-token latency, and a /health
// endpoint. A /metrics endpoint exposes a simulated GPU utilization gauge
// that rises with concurrent in-flight requests, so the gateway's dashboards
// look realistic without a real GPU.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
	MaxTokens int          `json:"max_tokens,omitempty"`
}

type chatChoice struct {
	Index        int          `json:"index"`
	Message      *chatMessage `json:"message,omitempty"`
	Delta        *chatMessage `json:"delta,omitempty"`
	FinishReason string       `json:"finish_reason,omitempty"`
}

type chatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type chatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
	Usage   *chatUsage   `json:"usage,omitempty"`
}

var (
	addr           = flag.String("addr", ":9000", "listen address")
	tokenLatency   = flag.Duration("token-latency", 15*time.Millisecond, "simulated per-token latency")
	defaultTokens  = flag.Int("default-tokens", 60, "default number of tokens to emit when max_tokens is unset")
	failureRate    = flag.Float64("failure-rate", 0.0, "probability [0,1) of returning 500 to simulate flakiness")
	backendName    = flag.String("name", "mock-1", "backend identifier used in IDs and logs")

	inFlight  atomic.Int64
	maxInFlight atomic.Int64
)

// canned token vocabulary — short words so the stream looks like text.
var vocab = []string{
	"The", "Velo", "gateway", "is", "an", "LLM", "inference", "proxy", "with",
	"continuous", "batching", "semantic", "cache", "and", "rate", "limiting", ".",
	"It", "supports", "multi-backend", "failover", "streaming", "SSE", "tokens",
	"and", "Prometheus", "metrics", "for", "observability", ".",
	"Throughput", "scales", "linearly", "with", "batch", "size", "until", "the", "backend",
	"saturates", ".",
}

func main() {
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", handleChat)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/metrics", handleMetrics)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("mock-backend %s listening on %s (token-latency=%s)", *backendName, *addr, *tokenLatency)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":    "ok",
		"backend":   *backendName,
		"in_flight": inFlight.Load(),
	})
}

// handleMetrics exposes a tiny Prometheus exposition: just a simulated GPU
// utilization that scales with concurrent requests. Good enough for the
// Grafana dashboard to look alive.
func handleMetrics(w http.ResponseWriter, r *http.Request) {
	cur := inFlight.Load()
	maxObs := maxInFlight.Load()
	if maxObs < 1 {
		maxObs = 1
	}
	util := float64(cur) / float64(maxObs+4) // saturates as in-flight grows
	if util > 1.0 {
		util = 1.0
	}
	fmt.Fprintf(w, "# HELP mock_backend_gpu_utilization Simulated GPU utilization in [0,1].\n")
	fmt.Fprintf(w, "# TYPE mock_backend_gpu_utilization gauge\n")
	fmt.Fprintf(w, "mock_backend_gpu_utilization{backend=%q} %f\n", *backendName, util)
	fmt.Fprintf(w, "# HELP mock_backend_in_flight Requests currently being processed.\n")
	fmt.Fprintf(w, "# TYPE mock_backend_in_flight gauge\n")
	fmt.Fprintf(w, "mock_backend_in_flight{backend=%q} %d\n", *backendName, cur)
}

func handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cur := inFlight.Add(1)
	defer inFlight.Add(-1)
	for {
		m := maxInFlight.Load()
		if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
			break
		}
	}

	if *failureRate > 0 && rand.Float64() < *failureRate {
		http.Error(w, `{"error":"simulated backend failure"}`, http.StatusInternalServerError)
		return
	}

	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"bad request: %s"}`, err), http.StatusBadRequest)
		return
	}
	if req.Model == "" {
		req.Model = "mock-llm"
	}
	nTokens := req.MaxTokens
	if nTokens <= 0 {
		nTokens = *defaultTokens
	}

	// Construct a deterministic-ish reply that echoes the last user message,
	// so the semantic cache has something repeatable to match on.
	tokens := buildTokens(req.Messages, nTokens)
	promptTokens := countPromptTokens(req.Messages)

	id := fmt.Sprintf("chatcmpl-%s-%d", *backendName, time.Now().UnixNano())

	if req.Stream {
		streamResponse(w, r, id, req.Model, tokens)
		return
	}
	nonStreamResponse(w, id, req.Model, tokens, promptTokens)
}

func buildTokens(msgs []chatMessage, n int) []string {
	// seed: echo the tail of the last user message, then pad from vocab.
	var lastUser string
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "user" {
			lastUser = msgs[i].Content
			break
		}
	}
	echo := strings.Fields(lastUser)
	if len(echo) > 6 {
		echo = echo[len(echo)-6:]
	}
	out := make([]string, 0, n)
	out = append(out, "Answer:")
	out = append(out, echo...)
	for len(out) < n {
		out = append(out, vocab[rand.Intn(len(vocab))])
	}
	return out[:n]
}

func countPromptTokens(msgs []chatMessage) int {
	n := 0
	for _, m := range msgs {
		n += len(strings.Fields(m.Content)) + 2
	}
	return n
}

func nonStreamResponse(w http.ResponseWriter, id, model string, tokens []string, promptTokens int) {
	// Sleep proportional to token count to simulate real generation time.
	time.Sleep(time.Duration(len(tokens)) * *tokenLatency)
	resp := chatResponse{
		ID:      id,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []chatChoice{{
			Index:        0,
			Message:      &chatMessage{Role: "assistant", Content: strings.Join(tokens, " ")},
			FinishReason: "stop",
		}},
		Usage: &chatUsage{
			PromptTokens:     promptTokens,
			CompletionTokens: len(tokens),
			TotalTokens:      promptTokens + len(tokens),
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func streamResponse(w http.ResponseWriter, r *http.Request, id, model string, tokens []string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	send := func(c chatResponse) {
		b, _ := json.Marshal(c)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
	}

	created := time.Now().Unix()
	// First chunk announces the role.
	send(chatResponse{
		ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
		Choices: []chatChoice{{Index: 0, Delta: &chatMessage{Role: "assistant"}}},
	})

	ctx := r.Context()
	for i, t := range tokens {
		select {
		case <-ctx.Done():
			return
		case <-time.After(*tokenLatency):
		}
		piece := t
		if i > 0 {
			piece = " " + t
		}
		send(chatResponse{
			ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
			Choices: []chatChoice{{Index: 0, Delta: &chatMessage{Content: piece}}},
		})
	}
	// Final chunk with finish_reason and [DONE] sentinel (OpenAI convention).
	send(chatResponse{
		ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
		Choices: []chatChoice{{Index: 0, Delta: &chatMessage{}, FinishReason: "stop"}},
	})
	_, _ = fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}
