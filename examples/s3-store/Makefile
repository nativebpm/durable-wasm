.PHONY: all build build-worker run clean run-docker

all: build

build: build-worker

build-worker:
	@echo "Building WASM worker for s3-store example using TinyGo..."
	tinygo build -o worker/worker.wasm -target=wasi worker/main.go

run: build-worker
	@echo "Running s3-store example host..."
	cd host && go run main.go

run-docker: build-worker
	@echo "Running local MinIO and starting the s3-store demonstration..."
	docker compose up -d minio
	@echo "Waiting for MinIO to be ready..."
	sleep 5
	cd host && go run main.go
	docker compose down

clean:
	@echo "Cleaning s3-store example artifacts..."
	rm -f worker/worker.wasm
