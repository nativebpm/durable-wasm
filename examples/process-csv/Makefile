.PHONY: all build build-worker run clean

all: build

build: build-worker

build-worker:
	@echo "Building WASM worker for CSV processing example using TinyGo..."
	tinygo build -o worker/worker.wasm -target=wasi worker/main.go

run: build-worker
	@echo "Running CSV processing example host..."
	cd host && go run main.go

clean:
	@echo "Cleaning CSV example artifacts..."
	rm -f worker/worker.wasm
