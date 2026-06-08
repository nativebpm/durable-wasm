//go:build !wasm

package wasman

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

var (
	ErrWasmVersionMismatch = fmt.Errorf("wasm module hash mismatch")
	ErrConcurrentExecution = fmt.Errorf("concurrent execution detected (OCC fencing)")
)

type apiHandlerCtxKey struct{}
type downloadHandlerCtxKey struct{}
type uploadHandlerCtxKey struct{}

// WithApiHandler binds an in-memory host_call_api handler to context.
func WithApiHandler(ctx context.Context, h func(apiName string, request []byte) ([]byte, error)) context.Context {
	return context.WithValue(ctx, apiHandlerCtxKey{}, h)
}

// WithDownloadHandler binds an in-memory stream download handler to context.
func WithDownloadHandler(ctx context.Context, h func() ([]byte, error)) context.Context {
	return context.WithValue(ctx, downloadHandlerCtxKey{}, h)
}

// WithUploadHandler binds an in-memory stream upload handler to context.
func WithUploadHandler(ctx context.Context, h func(payload []byte) error) context.Context {
	return context.WithValue(ctx, uploadHandlerCtxKey{}, h)
}

func getApiHandler(ctx context.Context) func(apiName string, request []byte) ([]byte, error) {
	if val := ctx.Value(apiHandlerCtxKey{}); val != nil {
		return val.(func(apiName string, request []byte) ([]byte, error))
	}
	return nil
}

func getDownloadHandler(ctx context.Context) func() ([]byte, error) {
	if val := ctx.Value(downloadHandlerCtxKey{}); val != nil {
		return val.(func() ([]byte, error))
	}
	return nil
}

func getUploadHandler(ctx context.Context) func(payload []byte) error {
	if val := ctx.Value(uploadHandlerCtxKey{}); val != nil {
		return val.(func(payload []byte) error)
	}
	return nil
}

// InstanceMeta holds execution metadata for safety checks and OCC.
type InstanceMeta struct {
	InstanceID     string `json:"instance_id"`
	WasmHash       string `json:"wasm_hash"`
	Version        int    `json:"version"`
	ETag           string `json:"etag,omitempty"`
	ProcessID      string `json:"process_id,omitempty"`
	DefinitionHash string `json:"definition_hash,omitempty"`
	BusinessKey    string `json:"business_key,omitempty"`
	BpmnState      []byte `json:"bpmn_state,omitempty"`
	Completed      bool   `json:"completed,omitempty"`
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

	// Active Index for Console visualization
	UpdateActiveIndex(id string, info []byte, completed bool) error
	LoadActiveIndex() ([]byte, error)
}

// Engine coordinates execution, compilation, and snapshotting of WASM modules.
type Engine struct {
	runtime          wazero.Runtime
	compiled         wazero.CompiledModule
	store            SnapshotStore
	wasmHash         string
	compiledCache    map[string]wazero.CompiledModule
	cacheMu          sync.RWMutex
	activeSessions   map[string]*Session
	activeSessionsMu sync.Mutex
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

	// Upload buffer context (Synchronous)
	uploadBuffer []byte

	// Download Stream-first context
	downloadEOF bool

	// In-memory handlers (bypassing loopback HTTP)
	ApiHandler      func(apiName string, request []byte) ([]byte, error)
	DownloadHandler func() ([]byte, error)
	UploadHandler   func(payload []byte) error

	downloadReader io.Reader
}

// EngineOption defines a configuration option for the Engine.
type EngineOption func(*Engine)
