//go:build !wasm

package wasman

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

var (
	ErrWasmVersionMismatch = fmt.Errorf("wasm module hash mismatch")
	ErrConcurrentExecution = fmt.Errorf("concurrent execution detected (OCC fencing)")
)

// InstanceMeta holds execution metadata for safety checks and OCC.
type InstanceMeta struct {
	InstanceID string `json:"instance_id"`
	WasmHash   string `json:"wasm_hash"`
	Version    int    `json:"version"`
	ETag       string `json:"etag,omitempty"`
}

// OplogEntry represents a single external call log.
type OplogEntry struct {
	CallIndex       int    `json:"call_index"`
	ApiName         string `json:"api_name"`
	RequestPayload  []byte `json:"request_payload"`
	ResponsePayload []byte `json:"response_payload"`
}

// SnapshotStore abstracts the storage backend for linear memory snapshots, deltas, and oplog.
type SnapshotStore interface {
	Save(id string, snapshot []byte) error
	Load(id string) ([]byte, error)
	Delete(id string) error

	// Delta Snapshots
	SaveDeltas(id string, deltas map[int][]byte) error
	LoadDeltas(id string) (map[int][]byte, error)
	TruncateDeltas(id string) error

	// Oplog
	SaveOplog(id string, callIndex int, apiName string, request []byte, response []byte) error
	LoadOplog(id string) ([]OplogEntry, error)
	TruncateOplog(id string, beforeCallIndex int) error

	// Metadata & OCC
	SaveMetadata(meta *InstanceMeta) (bool, error)
	LoadMetadata(id string) (*InstanceMeta, error)

	// WASM Registry for Multi-Version Support
	SaveWasm(hash string, wasmBytes []byte) error
	LoadWasm(hash string) ([]byte, error)

	// Active Index for Cockpit visualization
	UpdateActiveIndex(id string, info []byte, completed bool) error
	LoadActiveIndex() ([]byte, error)
}

// Engine coordinates execution, compilation, and snapshotting of WASM modules.
type Engine struct {
	runtime    wazero.Runtime
	compiled   wazero.CompiledModule
	store      SnapshotStore
	httpClient *http.Client
	wasmHash   string
}

// Session tracks the dynamic execution state of a running WASM instance.
type Session struct {
	engine                  *Engine
	ctx                     context.Context
	mod                     api.Module
	memory                  api.Memory
	instanceID              string
	serverAddr              string
	shouldCrashOnCheckpoint bool
	crashed                 bool
	meta                    *InstanceMeta

	// Dirty-page snapshot hashes and Oplog progress
	pageHashes map[int]uint64
	callIndex  int

	// Upload Stream-first context
	uploadPipeW   *io.PipeWriter
	uploadErrChan chan error

	// Download Stream-first context
	downloadResp *http.Response
	downloadEOF  bool
}

var defaultHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	},
}

// EngineOption defines a configuration option for the Engine.
type EngineOption func(*Engine)

// WithHTTPClient allows configuring a custom HTTP client.
func WithHTTPClient(client *http.Client) EngineOption {
	return func(e *Engine) {
		e.httpClient = client
	}
}
