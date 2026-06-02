.PHONY: all build build-worker build-host run clean test docker-build

all: build

build: build-worker build-host

build-worker:
	@echo "Building WASM workers for all examples..."
	tinygo build -o examples/durable-s3/worker/worker.wasm -target=wasi examples/durable-s3/worker/main.go
	tinygo build -o examples/process-csv/worker/worker.wasm -target=wasi examples/process-csv/worker/main.go
	tinygo build -o examples/temporal/worker/worker.wasm -target=wasi examples/temporal/worker/main.go
	tinygo build -o examples/camunda/worker/worker.wasm -target=wasi examples/camunda/worker/main.go
	tinygo build -o examples/gotenberg-telegram/worker/worker.wasm -target=wasi examples/gotenberg-telegram/worker/main.go

build-host:
	@echo "Building Go host orchestrator..."
	cd examples/durable-s3/host && go build -o host main.go

run:
	make -C examples/durable-s3 run

test: build-worker
	@echo "Running tests..."
	go test -v ./...

clean:
	@echo "Cleaning artifacts..."
	rm -f examples/durable-s3/worker/worker.wasm examples/durable-s3/host/host
	rm -f examples/process-csv/worker/worker.wasm
	rm -f examples/temporal/worker/worker.wasm
	rm -f examples/camunda/worker/worker.wasm
	rm -f examples/gotenberg-telegram/worker/worker.wasm
	make -C examples/durable-s3 clean
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

run-durable-s3-docker:
	make -C examples/durable-s3 run-docker

docker-build:

	@echo "Building scratch-based Docker image..."
	docker build -t wasm-durable-host -f examples/durable-s3/host/Dockerfile .


# --- Declarative Atlas Migrations ---
.PHONY: atlas-inspect-sqlite atlas-apply-sqlite atlas-inspect-postgres atlas-apply-postgres

atlas-inspect-sqlite:
	atlas schema inspect --env sqlite --format '{{ sql . }}' | sed 's/CREATE TABLE/CREATE TABLE IF NOT EXISTS/g' > schema/sqlite.sql

atlas-apply-sqlite:
	atlas schema apply --env sqlite --auto-approve

atlas-inspect-postgres:
	atlas schema inspect --env postgres --format '{{ sql . }}' | sed 's/CREATE TABLE/CREATE TABLE IF NOT EXISTS/g' > schema/postgres.sql

atlas-apply-postgres:
	atlas schema apply --env postgres --auto-approve

