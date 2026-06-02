.PHONY: all build build-worker start-temporal stop-temporal run clean

all: build

build: build-worker

build-worker:
	@echo "Building WASM worker for Temporal example using TinyGo..."
	tinygo build -o worker/worker.wasm -target=wasi worker/main.go

start-temporal:
	@echo "Starting Temporal Dev Server in Docker..."
	docker compose up -d

stop-temporal:
	@echo "Stopping Temporal Dev Server..."
	docker compose down

run: build-worker
	@echo "Running Temporal example host..."
	cd host && go run main.go

clean:
	@echo "Cleaning Temporal example artifacts..."
	rm -f worker/worker.wasm
