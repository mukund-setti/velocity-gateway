// Package config loads Velo gateway configuration from a YAML file with
// environment-variable overrides. Env vars use the VELO_ prefix and underscore
// separators (e.g. VELO_SERVER_ADDR=":8080").
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server    Server    `yaml:"server"`
	Auth      Auth      `yaml:"auth"`
	Scheduler Scheduler `yaml:"scheduler"`
	Cache     Cache     `yaml:"cache"`
	RateLimit RateLimit `yaml:"ratelimit"`
	Router    Router    `yaml:"router"`
	Logging   Logging   `yaml:"logging"`
}

type Server struct {
	Addr         string        `yaml:"addr"`
	MetricsAddr  string        `yaml:"metrics_addr"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type Auth struct {
	APIKeys []string `yaml:"api_keys"`
}

type Scheduler struct {
	Enabled       bool          `yaml:"enabled"`
	MaxBatchSize  int           `yaml:"max_batch_size"`
	MaxWait       time.Duration `yaml:"max_wait"`
	QueueCapacity int           `yaml:"queue_capacity"`
	Workers       int           `yaml:"workers"`
}

type Cache struct {
	Enabled             bool          `yaml:"enabled"`
	Store               string        `yaml:"store"` // "pgvector" | "memory"
	PostgresURL         string        `yaml:"postgres_url"`
	Embedder            string        `yaml:"embedder"` // "hashing" | "openai"
	EmbedderDim         int           `yaml:"embedder_dim"`
	OpenAIKey           string        `yaml:"openai_key"`
	OpenAIModel         string        `yaml:"openai_model"`
	SimilarityThreshold float64       `yaml:"similarity_threshold"`
	TTL                 time.Duration `yaml:"ttl"`
	MaxEntries          int           `yaml:"max_entries"`
}

type RateLimit struct {
	Enabled     bool   `yaml:"enabled"`
	Store       string `yaml:"store"` // "redis" | "memory"
	RedisAddr   string `yaml:"redis_addr"`
	DefaultRPS  int    `yaml:"default_rps"`
	DefaultBurst int   `yaml:"default_burst"`
}

type Backend struct {
	Name   string `yaml:"name"`
	URL    string `yaml:"url"`
	Weight int    `yaml:"weight"`
}

type Router struct {
	Strategy          string        `yaml:"strategy"` // "weighted_p2c" | "round_robin"
	HealthCheckPath   string        `yaml:"health_check_path"`
	HealthInterval    time.Duration `yaml:"health_interval"`
	HealthTimeout     time.Duration `yaml:"health_timeout"`
	FailureThreshold  int           `yaml:"failure_threshold"`
	RetryAttempts     int           `yaml:"retry_attempts"`
	RequestTimeout    time.Duration `yaml:"request_timeout"`
	Backends          []Backend     `yaml:"backends"`
}

type Logging struct {
	Level string `yaml:"level"`
}

// Default returns a Config populated with safe defaults suitable for the
// docker-compose dev stack.
func Default() Config {
	return Config{
		Server: Server{
			Addr:         ":8080",
			MetricsAddr:  ":9100",
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 5 * time.Minute,
		},
		Auth: Auth{APIKeys: []string{"sk-velo-dev"}},
		Scheduler: Scheduler{
			Enabled:       true,
			MaxBatchSize:  16,
			MaxWait:       40 * time.Millisecond,
			QueueCapacity: 2048,
			Workers:       8,
		},
		Cache: Cache{
			Enabled:             true,
			Store:               "pgvector",
			PostgresURL:         "postgres://velo:velo@postgres:5432/velo?sslmode=disable",
			Embedder:            "hashing",
			EmbedderDim:         384,
			OpenAIModel:         "text-embedding-3-small",
			SimilarityThreshold: 0.92,
			TTL:                 1 * time.Hour,
			MaxEntries:          50_000,
		},
		RateLimit: RateLimit{
			Enabled:      true,
			Store:        "redis",
			RedisAddr:    "redis:6379",
			DefaultRPS:   20,
			DefaultBurst: 40,
		},
		Router: Router{
			Strategy:         "weighted_p2c",
			HealthCheckPath:  "/health",
			HealthInterval:   5 * time.Second,
			HealthTimeout:    2 * time.Second,
			FailureThreshold: 3,
			RetryAttempts:    2,
			RequestTimeout:   60 * time.Second,
			Backends: []Backend{
				{Name: "mock-1", URL: "http://mock-backend:9000", Weight: 1},
			},
		},
		Logging: Logging{Level: "info"},
	}
}

// Load reads a YAML config file and applies env-var overrides on top.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(b, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config: %w", err)
		}
	}
	applyEnv(&cfg)
	return cfg, nil
}

func applyEnv(c *Config) {
	if v := os.Getenv("VELO_SERVER_ADDR"); v != "" {
		c.Server.Addr = v
	}
	if v := os.Getenv("VELO_METRICS_ADDR"); v != "" {
		c.Server.MetricsAddr = v
	}
	if v := os.Getenv("VELO_API_KEYS"); v != "" {
		c.Auth.APIKeys = splitCSV(v)
	}
	if v := os.Getenv("VELO_SCHEDULER_ENABLED"); v != "" {
		c.Scheduler.Enabled = parseBool(v)
	}
	if v := os.Getenv("VELO_SCHEDULER_MAX_BATCH"); v != "" {
		c.Scheduler.MaxBatchSize = parseInt(v, c.Scheduler.MaxBatchSize)
	}
	if v := os.Getenv("VELO_SCHEDULER_MAX_WAIT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.Scheduler.MaxWait = d
		}
	}
	if v := os.Getenv("VELO_CACHE_ENABLED"); v != "" {
		c.Cache.Enabled = parseBool(v)
	}
	if v := os.Getenv("VELO_CACHE_THRESHOLD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			c.Cache.SimilarityThreshold = f
		}
	}
	if v := os.Getenv("VELO_POSTGRES_URL"); v != "" {
		c.Cache.PostgresURL = v
	}
	if v := os.Getenv("VELO_REDIS_ADDR"); v != "" {
		c.RateLimit.RedisAddr = v
	}
	if v := os.Getenv("VELO_RATELIMIT_ENABLED"); v != "" {
		c.RateLimit.Enabled = parseBool(v)
	}
	if v := os.Getenv("VELO_BACKENDS"); v != "" {
		// Format: name1=url1,name2=url2
		c.Router.Backends = parseBackendsCSV(v)
	}
	if v := os.Getenv("VELO_LOG_LEVEL"); v != "" {
		c.Logging.Level = v
	}
	if v := os.Getenv("VELO_OPENAI_API_KEY"); v != "" {
		c.Cache.OpenAIKey = v
	}
}

func splitCSV(s string) []string {
	out := []string{}
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

func parseBool(s string) bool {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false
	}
	return b
}

func parseInt(s string, fallback int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return fallback
	}
	return n
}

func parseBackendsCSV(s string) []Backend {
	var out []Backend
	for _, p := range splitCSV(s) {
		eq := -1
		for i := 0; i < len(p); i++ {
			if p[i] == '=' {
				eq = i
				break
			}
		}
		if eq < 0 {
			out = append(out, Backend{Name: p, URL: p, Weight: 1})
			continue
		}
		out = append(out, Backend{Name: p[:eq], URL: p[eq+1:], Weight: 1})
	}
	return out
}
