.PHONY: all build build-worker run clean

all: build

build: build-worker

build-worker:
	@echo "Building WASM worker for Camunda example using TinyGo..."
	tinygo build -o worker/worker.wasm -target=wasi worker/main.go

run: build-worker
	@echo "Running Camunda example host..."
	cd host && go run main.go

clean:
	@echo "Cleaning Camunda example artifacts..."
	rm -f worker/worker.wasm
