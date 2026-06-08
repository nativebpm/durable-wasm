//go:build !wasm

package wasman

import (
	"context"
)

// Execution represents a fluent builder for configuring and running a WASM execution session.
type Execution struct {
	engine          *Engine
	instanceID      string
	entrypoint      string
	serverAddr      string
	shouldCrash     bool
	params          []uint64
	apiHandler      func(apiName string, request []byte) ([]byte, error)
	downloadHandler func() ([]byte, error)
	uploadHandler   func(payload []byte) error
}

// Session creates a new Execution builder for the specified WASM instance.
func (e *Engine) Session(instanceID string) *Execution {
	return &Execution{
		engine:     e,
		instanceID: instanceID,
		entrypoint: "run", // Default entrypoint function in WASM
	}
}

// WithEntrypoint configures the function name to call in WASM (default is "run").
func (ex *Execution) WithEntrypoint(entrypoint string) *Execution {
	ex.entrypoint = entrypoint
	return ex
}

// WithServer configures the server address for HTTP upload/download routing.
func (ex *Execution) WithServer(serverAddr string) *Execution {
	ex.serverAddr = serverAddr
	return ex
}

// WithCrash configures whether to simulate a host crash at the first checkpoint.
func (ex *Execution) WithCrash(shouldCrash bool) *Execution {
	ex.shouldCrash = shouldCrash
	return ex
}

// WithArgs configures the parameters to pass to the WASM function.
func (ex *Execution) WithArgs(params ...uint64) *Execution {
	ex.params = params
	return ex
}

// WithApiHandler configures an in-memory API handler to bypass loopback HTTP calls.
func (ex *Execution) WithApiHandler(h func(apiName string, request []byte) ([]byte, error)) *Execution {
	ex.apiHandler = h
	return ex
}

// WithDownloadHandler configures an in-memory stream download handler to bypass loopback HTTP calls.
func (ex *Execution) WithDownloadHandler(h func() ([]byte, error)) *Execution {
	ex.downloadHandler = h
	return ex
}

// WithUploadHandler configures an in-memory stream upload handler to bypass loopback HTTP calls.
func (ex *Execution) WithUploadHandler(h func(payload []byte) error) *Execution {
	ex.uploadHandler = h
	return ex
}

// Run executes the WASM instance with the configured session options.
func (ex *Execution) Run(ctx context.Context) (crashed bool, err error) {
	runCtx := ctx
	if ex.apiHandler != nil {
		runCtx = WithApiHandler(runCtx, ex.apiHandler)
	}
	if ex.downloadHandler != nil {
		runCtx = WithDownloadHandler(runCtx, ex.downloadHandler)
	}
	if ex.uploadHandler != nil {
		runCtx = WithUploadHandler(runCtx, ex.uploadHandler)
	}
	return ex.engine.ExecuteWithArgs(runCtx, ex.instanceID, ex.entrypoint, ex.serverAddr, ex.shouldCrash, ex.params...)
}

