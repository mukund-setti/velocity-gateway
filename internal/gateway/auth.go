package gateway

import (
	"context"
	"net/http"
	"strings"
)

type ctxKey int

const apiKeyCtxKey ctxKey = 1

// APIKeyAuth returns middleware that enforces a Bearer token from the
// Authorization header against the configured key set. The matched key is
// stored in request context for downstream rate-limiter lookups.
func APIKeyAuth(allowed []string) func(http.Handler) http.Handler {
	set := make(map[string]struct{}, len(allowed))
	for _, k := range allowed {
		set[k] = struct{}{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Allow unauthenticated access to health/metrics.
			if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}
			h := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if !strings.HasPrefix(h, prefix) {
				http.Error(w, `{"error":"missing bearer token"}`, http.StatusUnauthorized)
				return
			}
			key := strings.TrimSpace(h[len(prefix):])
			if _, ok := set[key]; !ok {
				http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), apiKeyCtxKey, key))
			next.ServeHTTP(w, r)
		})
	}
}

// APIKeyFromContext extracts the API key set by APIKeyAuth.
func APIKeyFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(apiKeyCtxKey).(string); ok {
		return v
	}
	return ""
}
