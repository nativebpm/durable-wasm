//go:build !wasm

package runner

import (
	"errors"
	"io"
	"time"
)

// Stub error for non-WASM execution
var errOnlySupportedInWASM = errors.New("wasman guest runner is only supported when compiling for WebAssembly (wasm)")

// Checkpoint stub
func Checkpoint() {}

// GetTime stub
func GetTime() time.Time {
	return time.Now()
}

// CallAPI stub
func CallAPI(apiName string, request []byte) ([]byte, error) {
	return nil, errOnlySupportedInWASM
}

// StreamReader stub
type StreamReader struct{}

func (r StreamReader) Read(p []byte) (n int, err error) {
	return 0, errOnlySupportedInWASM
}

// StreamWriter stub
type StreamWriter struct{}

func (w StreamWriter) Write(p []byte) (n int, err error) {
	return 0, errOnlySupportedInWASM
}

func (w StreamWriter) Close() error {
	return nil
}

var (
	Reader io.Reader      = StreamReader{}
	Writer io.WriteCloser = StreamWriter{}
)

// Run stub
func Run(steps ...func() error) int32 {
	panic("Run is only supported when executing inside a WebAssembly environment")
}

// === Fluent API Stubs ===

// Workflow represents a stub fluent builder for workflow steps.
type Workflow struct {
	steps []func() error
}

// NewWorkflow stub
func NewWorkflow() *Workflow {
	return &Workflow{}
}

// Step stub
func (w *Workflow) Step(step func() error) *Workflow {
	w.steps = append(w.steps, step)
	return w
}

// Run stub
func (w *Workflow) Run() int32 {
	panic("Run is only supported when executing inside a WebAssembly environment")
}

// APICall represents a stub fluent builder for external host API calls.
type APICall struct {
	name    string
	payload []byte
}

// Call stub
func Call(apiName string) *APICall {
	return &APICall{name: apiName}
}

// WithPayload stub
func (c *APICall) WithPayload(payload []byte) *APICall {
	c.payload = payload
	return c
}

// Send stub
func (c *APICall) Send() ([]byte, error) {
	return nil, errOnlySupportedInWASM
}

// RunTask stub
func RunTask(handler func(vars map[string]interface{}) error) int32 {
	panic("RunTask is only supported when executing inside a WebAssembly environment")
}

