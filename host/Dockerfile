# Stage 1: Build the static Go host binary using Debian (glibc)
FROM golang:1.26-bookworm AS builder

# Install build tools and static glibc libraries
RUN apt-get update && apt-get install -y gcc g++ make git curl libc6-dev

WORKDIR /app/host

# Copy dependency files
COPY host/go.mod host/go.sum* ./
RUN go mod download

# Copy the entire host source code
COPY host/ ./

# Build static binary, linking glibc statically
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags '-extldflags "-static"' -o host main.go

# Stage 2: Deploy in a scratch container
FROM scratch

WORKDIR /app

# Copy the static binary and the WASM worker into the container
COPY --from=builder /app/host/host /app/host
COPY worker/worker.wasm /app/worker/worker.wasm

# Set execution command
ENTRYPOINT ["/app/host"]
