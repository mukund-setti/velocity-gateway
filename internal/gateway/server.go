package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/mukundu/velo/internal/config"
	"github.com/mukundu/velo/internal/metrics"
)

// Server is the public Velo HTTP server.
type Server struct {
	cfg  config.Server
	auth config.Auth
	deps Deps
	srv  *http.Server
}

// NewServer wires the gateway server with the given dependencies.
func NewServer(cfg config.Server, auth config.Auth, deps Deps) *Server {
	return &Server{cfg: cfg, auth: auth, deps: deps}
}

// Routes returns the configured chi router.
func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(APIKeyAuth(s.auth.APIKeys))

	r.Get("/health", s.healthHandler)
	r.Get("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics.Handler().ServeHTTP(w, r)
	})
	r.Post("/v1/chat/completions", s.chatHandler)
	return r
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ListenAndServe starts the gateway HTTP server. It blocks until the context
// is cancelled or the server errors out.
func (s *Server) ListenAndServe(ctx context.Context) error {
	s.srv = &http.Server{
		Addr:              s.cfg.Addr,
		Handler:           s.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       s.cfg.ReadTimeout,
		WriteTimeout:      s.cfg.WriteTimeout,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- s.srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.srv.Shutdown(shutCtx)
	case err := <-errCh:
		return err
	}
}
