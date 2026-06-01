package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"strings"
	"time"
)

// Embedder turns a prompt into a fixed-size, L2-normalized vector. The interface
// is intentionally narrow so the cache layer doesn't care whether the vector
// came from a real model or a deterministic hash function.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
	Dim() int
}

// HashingEmbedder is a deterministic, dependency-free embedder used in tests
// and benchmarks. It applies the hashing trick (Weinberger 2009): each token
// is hashed into one of Dim buckets and a sign bit, the bucket is incremented
// by ±1, then the vector is L2-normalized. Cosine similarity between two
// HashingEmbedder vectors approximates Jaccard-like prompt overlap - good
// enough for cache-hit experiments without an embedding model.
type HashingEmbedder struct {
	dim int
}

func NewHashingEmbedder(dim int) *HashingEmbedder {
	if dim <= 0 {
		dim = 384
	}
	return &HashingEmbedder{dim: dim}
}

func (h *HashingEmbedder) Dim() int { return h.dim }

func (h *HashingEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	v := make([]float32, h.dim)
	for _, tok := range tokenize(text) {
		idx, sign := hashToken(tok, h.dim)
		if sign {
			v[idx] += 1
		} else {
			v[idx] -= 1
		}
	}
	// L2 normalize so cosine similarity reduces to a dot product.
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	norm := float32(math.Sqrt(sum))
	if norm > 0 {
		for i := range v {
			v[i] /= norm
		}
	}
	return v, nil
}

func tokenize(s string) []string {
	s = strings.ToLower(s)
	out := make([]string, 0, 16)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			if b.Len() > 0 {
				out = append(out, b.String())
				b.Reset()
			}
		}
	}
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out
}

func hashToken(tok string, dim int) (int, bool) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tok))
	v := h.Sum64()
	// Use the low bits for bucket and the next bit for sign - keeps the
	// distribution roughly uniform.
	bucket := int(v % uint64(dim))
	sign := (v>>32)&1 == 0
	return bucket, sign
}

// OpenAIEmbedder hits the OpenAI embeddings endpoint. It's behind the same
// Embedder interface as HashingEmbedder so the rest of the cache doesn't
// care which one is in use.
type OpenAIEmbedder struct {
	APIKey string
	Model  string // e.g. "text-embedding-3-small"
	dim    int
	client *http.Client
}

func NewOpenAIEmbedder(apiKey, model string, dim int) *OpenAIEmbedder {
	if model == "" {
		model = "text-embedding-3-small"
	}
	if dim <= 0 {
		dim = 1536
	}
	return &OpenAIEmbedder{
		APIKey: apiKey,
		Model:  model,
		dim:    dim,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (e *OpenAIEmbedder) Dim() int { return e.dim }

func (e *OpenAIEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	body, _ := json.Marshal(map[string]any{
		"model":      e.Model,
		"input":      text,
		"dimensions": e.dim,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://api.openai.com/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+e.APIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		buf, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("openai embeddings: %s: %s", resp.Status, buf)
	}
	var out struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.Data) == 0 {
		return nil, fmt.Errorf("openai embeddings: empty response")
	}
	v := out.Data[0].Embedding
	// L2 normalize so the store can rely on cosine == dot.
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	norm := float32(math.Sqrt(sum))
	if norm > 0 {
		for i := range v {
			v[i] /= norm
		}
	}
	return v, nil
}
