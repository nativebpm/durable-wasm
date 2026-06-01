.PHONY: all build build-worker run clean

all: build

build: build-worker

build-worker:
	@echo "Building WASM worker for Gotenberg-Telegram example using TinyGo..."
	tinygo build -o worker/worker.wasm -target=wasi worker/main.go

run: build-worker
	@echo "Running Gotenberg-Telegram example host..."
	cd host && go run main.go

clean:
	@echo "Cleaning Gotenberg-Telegram example artifacts..."
	rm -f worker/worker.wasm
