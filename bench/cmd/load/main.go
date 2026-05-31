// Command load is Velo's benchmark harness.
//
// It spins up a worker pool of N goroutines, each one driving a streaming
// chat-completion against the gateway, and records per-request latency and
// time-to-first-token. It can drive the same workload twice with different
// configurations (e.g. batching+cache on vs off) and emit a markdown table
// comparing the two runs — ready to paste into a README or a resume.
//
// Usage:
//
//	load -url http://localhost:8080 -concurrency 16 -duration 30s -compare
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	url         = flag.String("url", "http://localhost:8080", "gateway base URL")
	apiKey      = flag.String("api-key", "sk-velo-dev", "API key to use")
	concurrency = flag.Int("concurrency", 16, "number of concurrent worker goroutines")
	duration    = flag.Duration("duration", 30*time.Second, "test duration")
	maxTokens   = flag.Int("max-tokens", 40, "max_tokens to request per call")
	prompts     = flag.String("prompts", "bench/prompts.txt", "path to prompts file (one per line)")
	repeatRate  = flag.Float64("repeat-rate", 0.6, "fraction of requests that reuse a recent prompt (drives cache hits)")
	model       = flag.String("model", "mock-llm", "model name to send")
	compare     = flag.Bool("compare", false, "run twice: warmup + measure, print before/after table")
	output      = flag.String("output", "", "write the markdown report to this file")
)

type result struct {
	ok        bool
	cacheHit  bool
	ttftMs    float64
	totalMs   float64
	tokens    int
}

func main() {
	flag.Parse()

	pps, err := loadPrompts(*prompts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load prompts: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("loaded %d prompts; running %s @ concurrency=%d duration=%s repeat=%.0f%%\n",
		len(pps), *url, *concurrency, *duration, *repeatRate*100)

	if *compare {
		// Run 1: cold — flush cache by sending unique prompts only.
		fmt.Println("\n== Run 1: cold (no cache hits expected) ==")
		cold := runWorkload(pps, true /*uniqueOnly*/)
		printSummary("cold", cold)

		fmt.Println("\n== Run 2: warm (batching + cache enabled, prompt reuse) ==")
		warm := runWorkload(pps, false)
		printSummary("warm", warm)

		md := renderMarkdown(cold, warm)
		fmt.Println("\n" + md)
		if *output != "" {
			if err := os.WriteFile(*output, []byte(md), 0644); err != nil {
				fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			} else {
				fmt.Printf("wrote report to %s\n", *output)
			}
		}
		return
	}

	res := runWorkload(pps, false)
	printSummary("run", res)
}

func loadPrompts(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		t := strings.TrimSpace(sc.Text())
		if t != "" && !strings.HasPrefix(t, "#") {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no prompts in %s", path)
	}
	return out, sc.Err()
}

type aggregate struct {
	results []result
	requests, ok, cacheHits int64
	totalTokens int64
	wallStart, wallEnd time.Time
}

func runWorkload(prompts []string, uniqueOnly bool) aggregate {
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		agg      aggregate
		requests atomic.Int64
		ok       atomic.Int64
		cacheH   atomic.Int64
		tokens   atomic.Int64
	)
	agg.wallStart = time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID*1_000_003) + time.Now().UnixNano()))
			recent := make([]string, 0, 8)
			client := &http.Client{Timeout: 60 * time.Second}
			for ctx.Err() == nil {
				var p string
				if uniqueOnly || len(recent) == 0 || rng.Float64() > *repeatRate {
					p = fmt.Sprintf("[w%d-%d] %s", workerID, rng.Int63(), prompts[rng.Intn(len(prompts))])
					if len(recent) < 8 {
						recent = append(recent, p)
					} else {
						recent[rng.Intn(len(recent))] = p
					}
				} else {
					p = recent[rng.Intn(len(recent))]
				}
				r := callStream(ctx, client, p)
				requests.Add(1)
				if r.ok {
					ok.Add(1)
				}
				if r.cacheHit {
					cacheH.Add(1)
				}
				tokens.Add(int64(r.tokens))
				mu.Lock()
				agg.results = append(agg.results, r)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	agg.wallEnd = time.Now()
	agg.requests = requests.Load()
	agg.ok = ok.Load()
	agg.cacheHits = cacheH.Load()
	agg.totalTokens = tokens.Load()
	return agg
}

func callStream(ctx context.Context, client *http.Client, prompt string) result {
	body, _ := json.Marshal(map[string]any{
		"model": *model,
		"stream": true,
		"max_tokens": *maxTokens,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *url+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return result{}
	}
	req.Header.Set("Authorization", "Bearer "+*apiKey)
	req.Header.Set("Content-Type", "application/json")

	t0 := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return result{}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return result{}
	}
	cacheHit := resp.Header.Get("X-Velo-Cache") == "HIT"

	br := bufio.NewReader(resp.Body)
	tokens := 0
	var ttft time.Duration
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if ttft == 0 {
				ttft = time.Since(t0)
			}
			if bytes.HasPrefix(line, []byte("data: ")) {
				payload := bytes.TrimSpace(line[len("data: "):])
				if bytes.Equal(payload, []byte("[DONE]")) {
					break
				}
				var chunk struct {
					Choices []struct {
						Delta struct{ Content string `json:"content"` } `json:"delta"`
					} `json:"choices"`
				}
				if json.Unmarshal(payload, &chunk) == nil && len(chunk.Choices) > 0 {
					tokens += len(strings.Fields(chunk.Choices[0].Delta.Content))
				}
			}
		}
		if err != nil {
			break
		}
	}
	return result{
		ok: true, cacheHit: cacheHit, tokens: tokens,
		ttftMs:  float64(ttft.Microseconds()) / 1000,
		totalMs: float64(time.Since(t0).Microseconds()) / 1000,
	}
}

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func printSummary(label string, a aggregate) {
	if a.requests == 0 {
		fmt.Printf("%s: no requests completed\n", label)
		return
	}
	totals := make([]float64, 0, len(a.results))
	ttfts := make([]float64, 0, len(a.results))
	for _, r := range a.results {
		if !r.ok {
			continue
		}
		totals = append(totals, r.totalMs)
		ttfts = append(ttfts, r.ttftMs)
	}
	wall := a.wallEnd.Sub(a.wallStart).Seconds()
	hitRate := 0.0
	if a.ok > 0 {
		hitRate = float64(a.cacheHits) / float64(a.ok)
	}
	fmt.Printf("[%s] requests=%d ok=%d errors=%d rps=%.1f tokens/s=%.1f cache-hit=%.1f%%\n",
		label, a.requests, a.ok, a.requests-a.ok,
		float64(a.ok)/wall, float64(a.totalTokens)/wall, hitRate*100)
	fmt.Printf("[%s] latency  p50=%.1fms p95=%.1fms p99=%.1fms\n",
		label, percentile(totals, 0.50), percentile(totals, 0.95), percentile(totals, 0.99))
	fmt.Printf("[%s] TTFT     p50=%.1fms p95=%.1fms p99=%.1fms\n",
		label, percentile(ttfts, 0.50), percentile(ttfts, 0.95), percentile(ttfts, 0.99))
}

func renderMarkdown(cold, warm aggregate) string {
	row := func(a aggregate) (rps, tps, hit, p50, p95, p99, t50 float64) {
		var totals, ttfts []float64
		for _, r := range a.results {
			if !r.ok {
				continue
			}
			totals = append(totals, r.totalMs)
			ttfts = append(ttfts, r.ttftMs)
		}
		wall := a.wallEnd.Sub(a.wallStart).Seconds()
		if a.ok > 0 {
			hit = float64(a.cacheHits) / float64(a.ok) * 100
		}
		if wall > 0 {
			rps = float64(a.ok) / wall
			tps = float64(a.totalTokens) / wall
		}
		p50 = percentile(totals, 0.50)
		p95 = percentile(totals, 0.95)
		p99 = percentile(totals, 0.99)
		t50 = percentile(ttfts, 0.50)
		return
	}
	rps1, tps1, h1, p501, p951, p991, t501 := row(cold)
	rps2, tps2, h2, p502, p952, p992, t502 := row(warm)
	b := &strings.Builder{}
	fmt.Fprintf(b, "## Velo benchmark — concurrency=%d duration=%s\n\n", *concurrency, *duration)
	fmt.Fprintln(b, "| metric              | cold (cache miss only) | warm (batching + cache) | delta |")
	fmt.Fprintln(b, "|---------------------|------------------------|-------------------------|-------|")
	fmt.Fprintf(b, "| Requests/sec        | %.1f                  | %.1f                   | %s |\n", rps1, rps2, pctDelta(rps1, rps2))
	fmt.Fprintf(b, "| Tokens/sec          | %.1f                  | %.1f                   | %s |\n", tps1, tps2, pctDelta(tps1, tps2))
	fmt.Fprintf(b, "| Latency p50         | %.1f ms               | %.1f ms                | %s |\n", p501, p502, pctDeltaInv(p501, p502))
	fmt.Fprintf(b, "| Latency p95         | %.1f ms               | %.1f ms                | %s |\n", p951, p952, pctDeltaInv(p951, p952))
	fmt.Fprintf(b, "| Latency p99         | %.1f ms               | %.1f ms                | %s |\n", p991, p992, pctDeltaInv(p991, p992))
	fmt.Fprintf(b, "| TTFT p50            | %.1f ms               | %.1f ms                | %s |\n", t501, t502, pctDeltaInv(t501, t502))
	fmt.Fprintf(b, "| Cache hit rate      | %.1f%%                | %.1f%%                 | +%.1fpp |\n", h1, h2, h2-h1)
	return b.String()
}

func pctDelta(a, b float64) string {
	if a == 0 {
		return "—"
	}
	return fmt.Sprintf("%+.0f%%", (b-a)/a*100)
}

// pctDeltaInv reports improvements where lower is better.
func pctDeltaInv(a, b float64) string {
	if a == 0 {
		return "—"
	}
	return fmt.Sprintf("%+.0f%%", (b-a)/a*100)
}
