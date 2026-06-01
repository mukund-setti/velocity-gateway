// Command velo is the Velo gateway CLI.
//
// Usage:
//   velo serve --config configs/velo.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mukundu/velo/internal/cache"
	"github.com/mukundu/velo/internal/config"
	"github.com/mukundu/velo/internal/gateway"
	"github.com/mukundu/velo/internal/metrics"
	"github.com/mukundu/velo/internal/ratelimit"
	"github.com/mukundu/velo/internal/router"
	"github.com/mukundu/velo/internal/scheduler"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		serveCmd(os.Args[2:])
	case "version":
		fmt.Println("velo dev")
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Velo - LLM inference gateway")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  velo serve --config configs/velo.yaml")
	fmt.Fprintln(os.Stderr, "  velo version")
}

func serveCmd(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	cfgPath := fs.String("config", "configs/velo.yaml", "path to YAML config")
	_ = fs.Parse(args)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Router first - scheduler needs it as a dispatcher.
	rt, err := router.New(cfg.Router)
	if err != nil {
		log.Fatalf("router: %v", err)
	}
	defer rt.Close()

	// Scheduler.
	var sch *scheduler.Scheduler
	if cfg.Scheduler.Enabled {
		sch = scheduler.New(scheduler.Config{
			MaxBatchSize:   cfg.Scheduler.MaxBatchSize,
			MaxWait:        cfg.Scheduler.MaxWait,
			QueueCapacity:  cfg.Scheduler.QueueCapacity,
			Workers:        cfg.Scheduler.Workers,
			RequestTimeout: cfg.Router.RequestTimeout,
		}, rt)
		sch.Start()
		defer sch.Stop()
		log.Printf("scheduler: enabled max_batch=%d max_wait=%s workers=%d",
			cfg.Scheduler.MaxBatchSize, cfg.Scheduler.MaxWait, cfg.Scheduler.Workers)
	} else {
		log.Printf("scheduler: disabled (direct dispatch)")
	}

	// Cache.
	c, err := buildCache(ctx, cfg.Cache)
	if err != nil {
		log.Printf("cache: disabled (%v)", err)
	} else if c != nil {
		log.Printf("cache: enabled embedder=%s store=%s threshold=%.2f",
			cfg.Cache.Embedder, cfg.Cache.Store, cfg.Cache.SimilarityThreshold)
		defer c.Close()
	}

	// Rate limiter.
	var lim ratelimit.Limiter
	if cfg.RateLimit.Enabled {
		rlCfg := ratelimit.Config{RPS: cfg.RateLimit.DefaultRPS, Burst: cfg.RateLimit.DefaultBurst}
		switch cfg.RateLimit.Store {
		case "redis":
			rl, err := ratelimit.NewRedisLimiter(cfg.RateLimit.RedisAddr, rlCfg)
			if err != nil {
				log.Printf("ratelimit: redis unreachable (%v), falling back to memory", err)
				lim = ratelimit.NewMemoryLimiter(rlCfg)
			} else {
				lim = rl
				defer rl.Close()
				log.Printf("ratelimit: redis rps=%d burst=%d", rlCfg.RPS, rlCfg.Burst)
			}
		default:
			lim = ratelimit.NewMemoryLimiter(rlCfg)
			log.Printf("ratelimit: memory rps=%d burst=%d", rlCfg.RPS, rlCfg.Burst)
		}
	}

	// Build gateway.
	gw := gateway.NewServer(cfg.Server, cfg.Auth, gateway.Deps{
		Cache: c, Limiter: lim, Scheduler: sch, Router: rt,
	})

	// Separate Prometheus listener on a different port so users don't have
	// to authenticate scrape requests with an API key.
	go runMetricsServer(ctx, cfg.Server.MetricsAddr)

	log.Printf("velo: listening on %s (metrics on %s)", cfg.Server.Addr, cfg.Server.MetricsAddr)
	if err := gw.ListenAndServe(ctx); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gateway: %v", err)
	}
}

func buildCache(ctx context.Context, cc config.Cache) (*cache.Cache, error) {
	if !cc.Enabled {
		return nil, nil
	}
	var emb cache.Embedder
	switch cc.Embedder {
	case "openai":
		if cc.OpenAIKey == "" {
			return nil, fmt.Errorf("openai embedder requires VELO_OPENAI_API_KEY")
		}
		emb = cache.NewOpenAIEmbedder(cc.OpenAIKey, cc.OpenAIModel, cc.EmbedderDim)
	default:
		emb = cache.NewHashingEmbedder(cc.EmbedderDim)
	}

	var store cache.Store
	switch cc.Store {
	case "pgvector":
		// Retry briefly so the gateway can start while Postgres warms up
		// inside docker compose.
		var err error
		for i := 0; i < 10; i++ {
			pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			store, err = cache.NewPgvectorStore(pctx, cc.PostgresURL, emb.Dim())
			cancel()
			if err == nil {
				break
			}
			log.Printf("cache: pgvector not ready (%v), retrying...", err)
			time.Sleep(2 * time.Second)
		}
		if err != nil {
			return nil, err
		}
	default:
		store = cache.NewMemoryStore(cc.MaxEntries)
	}
	return cache.New(emb, store, cc.SimilarityThreshold, cc.TTL), nil
}

func runMetricsServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("metrics server: %v", err)
	}
}
