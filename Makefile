.PHONY: build test vet check up down logs

# Compile both binaries into ./bin
build:
	go build -trimpath -o bin/ ./cmd/...

# Tests with the race detector, uncached, with coverage
test:
	go test -race -count=1 -cover ./...

vet:
	go vet ./...

# The same gate CI runs: build, vet, race tests
check:
	go build ./... && go vet ./... && go test -race -count=1 ./...

# Start the whole stack (Kafka, Redis, processor, gateway, Prometheus)
up:
	docker compose up --build -d

down:
	docker compose down

# Follow the application logs
logs:
	docker compose logs -f gateway processor

loadtest-up:
	docker compose -f docker-compose.yml -f docker-compose.loadtest.yml up -d --force-recreate gateway processor

loadgen:
	go run ./cmd/loadgen -rate 500 -duration 30s