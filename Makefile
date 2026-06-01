.PHONY: all build build-worker build-host run clean test docker-build

all: build

build: build-worker build-host

build-worker:
	@echo "Building WASM worker using TinyGo..."
	tinygo build -o worker/worker.wasm -target=wasi worker/main.go

build-host:
	@echo "Building Go host orchestrator..."
	cd host && go build -o host main.go

run: build-worker
	@echo "Running Go host (which will execute WASM)..."
	cd host && go run main.go

test: build-worker
	@echo "Running tests..."
	cd host && go test -v ./...

clean:
	@echo "Cleaning artifacts..."
	rm -f worker/worker.wasm host/host
	make -C examples/process-csv clean
	make -C examples/temporal clean
	make -C examples/camunda clean
	make -C examples/gotenberg-telegram clean

run-csv-example:
	make -C examples/process-csv run

run-temporal-example:
	make -C examples/temporal run

run-camunda-example:
	make -C examples/camunda run

run-gotenberg-telegram-example:
	make -C examples/gotenberg-telegram run

docker-build:
	@echo "Building scratch-based Docker image..."
	docker build -t wasm-durable-host -f host/Dockerfile .
