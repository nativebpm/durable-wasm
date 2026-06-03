//go:build wasm

package runner

import (
	"errors"
	"io"
	"time"
	"unsafe"
)

// Host function imports from the "env" namespace.
//
//go:wasmimport env checkpoint
func hostCheckpoint()

//go:wasmimport env host_get_time
func hostGetTime() int64

//go:wasmimport env host_call_api
func hostCallAPI(apiNamePtr uint32, apiNameLen uint32, reqPtr uint32, reqLen uint32, respPtr uint32, respMaxLen uint32) int32

//go:wasmimport env stream_data
func hostStreamData(direction int32, ptr uint32, length uint32) int32

// Checkpoint triggers a manual memory snapshot save on the host.
func Checkpoint() {
	hostCheckpoint()
}

// GetTime returns a deterministic time from the host (replayed from Oplog if restoring).
func GetTime() time.Time {
	return time.Unix(0, hostGetTime())
}

// CallAPI invokes a host-registered API call and returns the response payload.
// Automatically records to and replays from Oplog to ensure determinism.
func CallAPI(apiName string, request []byte) ([]byte, error) {
	apiBytes := []byte(apiName)
	var apiPtr uint32
	if len(apiBytes) > 0 {
		apiPtr = uint32(uintptr(unsafe.Pointer(&apiBytes[0])))
	}

	var reqPtr uint32
	if len(request) > 0 {
		reqPtr = uint32(uintptr(unsafe.Pointer(&request[0])))
	}

	// Allocate a 64KB buffer for the response.
	respBuf := make([]byte, 65536)
	respPtr := uint32(uintptr(unsafe.Pointer(&respBuf[0])))

	n := hostCallAPI(
		apiPtr, uint32(len(apiBytes)),
		reqPtr, uint32(len(request)),
		respPtr, uint32(len(respBuf)),
	)
	if n < 0 {
		if n == -2 {
			return nil, errors.New("response buffer too small")
		}
		return nil, errors.New("host api call failed")
	}

	res := make([]byte, n)
	copy(res, respBuf[:n])
	return res, nil
}

// StreamReader reads data chunks from the host network stream.
type StreamReader struct{}

func (r StreamReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	ptr := uint32(uintptr(unsafe.Pointer(&p[0])))
	res := hostStreamData(0, ptr, uint32(len(p)))
	if res < 0 {
		return 0, errors.New("stream read error")
	}
	if res == 0 {
		return 0, io.EOF
	}
	return int(res), nil
}

// StreamWriter writes data chunks back to the host network stream.
type StreamWriter struct{}

func (w StreamWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	ptr := uint32(uintptr(unsafe.Pointer(&p[0])))
	res := hostStreamData(1, ptr, uint32(len(p)))
	if res != int32(len(p)) {
		return int(res), errors.New("stream write error")
	}
	return len(p), nil
}

// Close signals EOF to the output network stream.
func (w StreamWriter) Close() error {
	hostStreamData(1, 0, 0)
	return nil
}

var (
	// Reader is a package-level io.Reader interface for reading host input.
	Reader io.Reader = StreamReader{}

	// Writer is a package-level io.WriteCloser interface for writing host output.
	Writer io.WriteCloser = StreamWriter{}
)

// Workflow runner internals
var currentStep int32 = 0

// Run executes the sequence of steps, checkpointing after each step.
// This function should be called inside the worker's exported run() function.
func Run(steps ...func() error) int32 {
	for int(currentStep) < len(steps) {
		stepFunc := steps[currentStep]
		if err := stepFunc(); err != nil {
			println("[WASMAN ERROR] step failed:", err.Error())
			return -1
		}
		currentStep++
		hostCheckpoint()
	}
	return 0
}

// === Fluent API ===

// Workflow represents a fluent builder for workflow steps.
type Workflow struct {
	steps []func() error
}

// NewWorkflow initializes a new fluent workflow builder.
func NewWorkflow() *Workflow {
	return &Workflow{}
}

// Step adds a single function/step to the workflow sequence.
func (w *Workflow) Step(step func() error) *Workflow {
	w.steps = append(w.steps, step)
	return w
}

// Run starts the workflow execution using the registered steps.
func (w *Workflow) Run() int32 {
	return Run(w.steps...)
}

// APICall represents a fluent builder for external host API calls.
type APICall struct {
	name    string
	payload []byte
}

// Call initializes a new fluent API call builder for the specified API name.
func Call(apiName string) *APICall {
	return &APICall{name: apiName}
}

// WithPayload attaches the request payload to the API call.
func (c *APICall) WithPayload(payload []byte) *APICall {
	c.payload = payload
	return c
}

// Send executes the API call deterministically and returns the response.
func (c *APICall) Send() ([]byte, error) {
	return CallAPI(c.name, c.payload)
}
