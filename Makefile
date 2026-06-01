.PHONY: build up down logs test test-race lint fmt bench bench-quick clean tidy

# --- Local builds ---------------------------------------------------------

build:
	go build -o bin/velo ./cmd/velo
	go build -o bin/mock-backend ./cmd/mock-backend
	go build -o bin/load ./bench/cmd/load

# --- Stack ----------------------------------------------------------------

up:
	docker compose -f deploy/docker-compose.yml up -d --build
	@echo
	@echo "Velo gateway:   http://localhost:8080  (Authorization: Bearer sk-velo-dev)"
	@echo "Velo metrics:   http://localhost:9100/metrics"
	@echo "Prometheus:     http://localhost:9090"
	@echo "Grafana:        http://localhost:3000  (anonymous admin)"

down:
	docker compose -f deploy/docker-compose.yml down -v

logs:
	docker compose -f deploy/docker-compose.yml logs -f velo

# --- Tests / lint ---------------------------------------------------------

test:
	go test ./internal/... -count=1

test-race:
	go test -race ./internal/... -count=1

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not installed - falling back to go vet"; \
		go vet ./...; exit 0; }
	golangci-lint run ./...

fmt:
	gofmt -w .

tidy:
	go mod tidy

# --- Benchmarks -----------------------------------------------------------

# A short bench, useful for iterating locally.
bench-quick:
	go run ./bench/cmd/load -url http://localhost:8080 -concurrency 8 -duration 10s

# Full before/after comparison; produces bench/report.md.
bench:
	@mkdir -p bench/out
	go run ./bench/cmd/load \
		-url http://localhost:8080 \
		-concurrency 16 \
		-duration 30s \
		-compare \
		-output bench/out/report.md
	@echo "report written to bench/out/report.md"

clean:
	rm -rf bin bench/out
