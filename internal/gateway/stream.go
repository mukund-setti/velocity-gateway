package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mukundu/velo/internal/metrics"
)

// streamProxy copies an SSE response from the upstream backend to the client,
// flushing on every chunk. It returns the assembled assistant content and the
// number of completion tokens (whitespace-split, good enough for metrics),
// so the gateway can populate the semantic cache after the stream completes.
func streamProxy(ctx context.Context, w http.ResponseWriter, body io.Reader, backend, model string) (string, int, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return "", 0, errors.New("response writer does not support flushing")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Velo-Backend", backend)

	br := bufio.NewReaderSize(body, 64*1024)
	var content strings.Builder
	tokens := 0
	firstByte := false
	start := time.Now()

	for {
		if ctx.Err() != nil {
			return content.String(), tokens, ctx.Err()
		}
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if !firstByte {
				metrics.TimeToFirstToken.Observe(time.Since(start).Seconds())
				firstByte = true
			}
			if _, werr := w.Write(line); werr != nil {
				return content.String(), tokens, werr
			}
			flusher.Flush()
			// Extract the delta content for caching/metrics. We tolerate any
			// non-JSON SSE noise — e.g. the [DONE] sentinel.
			if bytes.HasPrefix(line, []byte("data: ")) {
				payload := bytes.TrimSpace(line[len("data: "):])
				if !bytes.Equal(payload, []byte("[DONE]")) {
					var chunk struct {
						Choices []struct {
							Delta struct {
								Content string `json:"content"`
							} `json:"delta"`
						} `json:"choices"`
					}
					if json.Unmarshal(payload, &chunk) == nil && len(chunk.Choices) > 0 {
						content.WriteString(chunk.Choices[0].Delta.Content)
					}
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return content.String(), tokens, err
		}
	}
	tokens = len(strings.Fields(content.String()))
	metrics.TokensGenerated.WithLabelValues(backend, model).Add(float64(tokens))
	return content.String(), tokens, nil
}

// replayCachedStream emits a cached completion as SSE chunks so a streaming
// client receives the cached response the same way it would receive a live
// one. Each word becomes one delta chunk with a tiny inter-chunk delay so
// downstream UIs animate naturally.
func replayCachedStream(ctx context.Context, w http.ResponseWriter, model, content string) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return errors.New("response writer does not support flushing")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Velo-Cache", "HIT")

	id := fmt.Sprintf("chatcmpl-cache-%d", time.Now().UnixNano())
	created := time.Now().Unix()
	emit := func(delta map[string]any, finish string) error {
		obj := map[string]any{
			"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []map[string]any{{
				"index": 0,
				"delta": delta,
			}},
		}
		if finish != "" {
			obj["choices"].([]map[string]any)[0]["finish_reason"] = finish
		}
		b, _ := json.Marshal(obj)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if err := emit(map[string]any{"role": "assistant"}, ""); err != nil {
		return err
	}
	for i, tok := range strings.Fields(content) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		piece := tok
		if i > 0 {
			piece = " " + tok
		}
		if err := emit(map[string]any{"content": piece}, ""); err != nil {
			return err
		}
		// Tiny pause for nicer UX; far faster than a real backend.
		time.Sleep(2 * time.Millisecond)
	}
	if err := emit(map[string]any{}, "stop"); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
	return err
}
