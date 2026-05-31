package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
)

// PgvectorStore persists cache entries in PostgreSQL with the pgvector
// extension. The schema (see deploy/postgres/init.sql) is:
//
//   CREATE TABLE velo_cache (
//     id          BIGSERIAL PRIMARY KEY,
//     prompt      TEXT NOT NULL,
//     embedding   VECTOR(N) NOT NULL,
//     model       TEXT NOT NULL,
//     content     TEXT NOT NULL,
//     raw         JSONB,
//     created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
//   );
//   CREATE INDEX velo_cache_emb_idx ON velo_cache
//     USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
//
// pgvector's `<=>` operator returns cosine *distance* (1 - cosine_similarity),
// so we convert in both directions.
type PgvectorStore struct {
	pool *pgxpool.Pool
	dim  int
}

// NewPgvectorStore connects, ensures the schema exists with the right
// dimensionality, and returns the store.
func NewPgvectorStore(ctx context.Context, dsn string, dim int) (*PgvectorStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	s := &PgvectorStore{pool: pool, dim: dim}
	if err := s.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *PgvectorStore) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS velo_cache (
			id          BIGSERIAL PRIMARY KEY,
			prompt      TEXT NOT NULL,
			embedding   VECTOR(%d) NOT NULL,
			model       TEXT NOT NULL,
			content     TEXT NOT NULL,
			raw         JSONB,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`, s.dim),
		`CREATE INDEX IF NOT EXISTS velo_cache_emb_idx
			ON velo_cache USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)`,
		`CREATE INDEX IF NOT EXISTS velo_cache_created_idx ON velo_cache (created_at DESC)`,
	}
	for _, q := range stmts {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("exec %q: %w", q, err)
		}
	}
	return nil
}

func (s *PgvectorStore) Nearest(ctx context.Context, vec []float32, threshold float64, freshUntil time.Time) (*Entry, float64, error) {
	// pgvector `<=>` returns cosine distance; cosine similarity = 1 - distance.
	// We filter freshness in SQL so the index can be used.
	q := `SELECT model, content, raw, created_at, 1 - (embedding <=> $1) AS sim
		  FROM velo_cache
		  WHERE created_at >= $2
		  ORDER BY embedding <=> $1
		  LIMIT 1`
	row := s.pool.QueryRow(ctx, q, pgvector.NewVector(vec), freshUntil)
	var (
		model     string
		content   string
		raw       []byte
		createdAt time.Time
		sim       float64
	)
	if err := row.Scan(&model, &content, &raw, &createdAt, &sim); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	if sim < threshold {
		return nil, sim, nil
	}
	e := &Entry{
		Model: model, Content: content, CreatedAt: createdAt,
	}
	if len(raw) > 0 {
		e.Raw = json.RawMessage(raw)
	}
	return e, sim, nil
}

func (s *PgvectorStore) Put(ctx context.Context, vec []float32, prompt string, entry *Entry) error {
	q := `INSERT INTO velo_cache (prompt, embedding, model, content, raw, created_at)
		  VALUES ($1, $2, $3, $4, $5, $6)`
	var raw any
	if len(entry.Raw) > 0 {
		raw = []byte(entry.Raw)
	}
	_, err := s.pool.Exec(ctx, q, prompt, pgvector.NewVector(vec), entry.Model, entry.Content, raw, entry.CreatedAt)
	return err
}

func (s *PgvectorStore) Size(ctx context.Context) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM velo_cache`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *PgvectorStore) Close() error {
	s.pool.Close()
	return nil
}
