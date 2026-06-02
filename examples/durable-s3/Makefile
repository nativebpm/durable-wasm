.PHONY: all build build-worker run clean

all: build

build: build-worker

build-worker:
	@echo "Building WASM worker for durable-s3 example using TinyGo..."
	tinygo build -o worker/worker.wasm -target=wasi worker/main.go

run: build-worker
	@echo "Running durable-s3 example host..."
	cd host && go run main.go

run-docker: build-worker
	@echo "Creating local seaweedfs data directory..."
	mkdir -p seaweed_data
	@echo "Running durable-s3 example with Litestream and SeaweedFS..."
	docker compose up --build

clean:
	@echo "Cleaning durable-s3 example artifacts..."
	rm -f worker/worker.wasm
	rm -rf seaweed_data

