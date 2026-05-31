// Package gateway implements the public HTTP surface of the Velo gateway:
// API-key auth, OpenAI-compatible /v1/chat/completions, SSE streaming, and
// orchestration of the cache → rate limiter → scheduler → router pipeline.
package gateway

import "encoding/json"

// ChatMessage matches the OpenAI message shape.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest mirrors the public OpenAI ChatCompletion request payload.
// We keep extra fields as RawMessage so unknown options pass through untouched.
type ChatRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
	MaxTokens int          `json:"max_tokens,omitempty"`
	Temperature float64    `json:"temperature,omitempty"`

	// Raw is the original request bytes — we forward these verbatim to the
	// backend rather than rebuilding the payload, so any provider-specific
	// extension fields (functions, tools, response_format, etc.) survive.
	Raw json.RawMessage `json:"-"`
}

// CachedCompletion is the JSON we store in the semantic cache. The full
// non-streaming response is preserved so we can replay it byte-for-byte or
// chunk it back out as SSE on a cache hit.
type CachedCompletion struct {
	Model   string      `json:"model"`
	Content string      `json:"content"`
	Raw     json.RawMessage `json:"raw,omitempty"`
}
