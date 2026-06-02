//go:build wasm

package main

import (
	"encoding/json"
	"io"
	"unsafe"
)

// Global state variables for business logic
var (
	step          int32   = 0
	activityID    string  = "ACT-TEMP-4455"
	baseRate      float64 = 0.0
	multiplier    float64 = 0.0
	calculatedVal float64 = 0.0
)

// Host function imports
//
//go:wasmimport env checkpoint
func checkpoint()

//go:wasmimport env stream_data
func stream_data(direction int32, ptr uint32, length uint32) int32

// StreamReader wraps host functions to implement standard io.Reader inside WASM
type StreamReader struct {
	direction int32
}

func (r *StreamReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	ptr := uint32(uintptr(unsafe.Pointer(&p[0])))
	bytesRead := stream_data(r.direction, ptr, uint32(len(p)))
	if bytesRead < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if bytesRead == 0 {
		return 0, io.EOF
	}
	return int(bytesRead), nil
}

// StreamWriter wraps host functions to implement standard io.Writer inside WASM
type StreamWriter struct {
	direction int32
}

func (w *StreamWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	ptr := uint32(uintptr(unsafe.Pointer(&p[0])))
	bytesWritten := stream_data(w.direction, ptr, uint32(len(p)))
	if bytesWritten < 0 {
		return 0, io.ErrClosedPipe
	}
	return int(bytesWritten), nil
}

// Structured entities
type ParamRequest struct {
	ActivityID string `json:"activity_id"`
}

type ParamResponse struct {
	BaseRate   float64 `json:"base_rate"`
	Multiplier float64 `json:"multiplier"`
}

type FinalCalculationResult struct {
	ActivityID string  `json:"activity_id"`
	BaseRate   float64 `json:"base_rate"`
	Multiplier float64 `json:"multiplier"`
	Result     float64 `json:"result_value"`
	Completed  bool    `json:"completed"`
}

//export run
func run() int32 {
	for {
		switch step {
		case 0:
			println("[TEMPORAL WORKER] Step 0: Starting Temporal durable activity...")
			println("[TEMPORAL WORKER] Activity initialized:", activityID)
			step = 1
			println("[TEMPORAL WORKER] Step 0 completed. Saving state to checkpoint.")
			checkpoint()

		case 1:
			println("[TEMPORAL WORKER] Step 1: Downloading calculation parameters from host...")

			writer := &StreamWriter{direction: 1}
			reader := &StreamReader{direction: 0}

			// Send param request
			req := ParamRequest{ActivityID: activityID}
			err := json.NewEncoder(writer).Encode(req)
			if err != nil {
				println("[TEMPORAL WORKER] Request parameters encode failed:", err.Error())
				return -1
			}

			// Flush / Signal EOF on upload
			var dummy [1]byte
			stream_data(1, uint32(uintptr(unsafe.Pointer(&dummy[0]))), 0)

			// Read response
			var resp ParamResponse
			err = json.NewDecoder(reader).Decode(&resp)
			if err != nil {
				println("[TEMPORAL WORKER] Response parameters decode failed:", err.Error())
				return -1
			}

			baseRate = resp.BaseRate
			multiplier = resp.Multiplier
			println("[TEMPORAL WORKER] Parameters received: BaseRate =", baseRate, ", Multiplier =", multiplier)

			step = 2
			println("[TEMPORAL WORKER] Step 1 completed. Saving state to checkpoint.")
			checkpoint()

		case 2:
			println("[TEMPORAL WORKER] Step 2: Performing calculation...")

			// Simulate complex calculation
			calculatedVal = baseRate * multiplier * 150.0
			println("[TEMPORAL WORKER] Calculated value:", calculatedVal)

			step = 3
			println("[TEMPORAL WORKER] Step 2 completed. Saving state to checkpoint.")
			checkpoint()

		case 3:
			println("[TEMPORAL WORKER] Step 3: Saving final calculation record to database...")

			writer := &StreamWriter{direction: 1}

			result := FinalCalculationResult{
				ActivityID: activityID,
				BaseRate:   baseRate,
				Multiplier: multiplier,
				Result:     calculatedVal,
				Completed:  true,
			}

			err := json.NewEncoder(writer).Encode(result)
			if err != nil {
				println("[TEMPORAL WORKER] Final result encode failed:", err.Error())
				return -1
			}

			// Flush upload
			var dummy [1]byte
			stream_data(1, uint32(uintptr(unsafe.Pointer(&dummy[0]))), 0)

			println("[TEMPORAL WORKER] Final record sent to database.")

			step = 4
			println("[TEMPORAL WORKER] Step 3 completed. Saving final checkpoint.")
			checkpoint()

		case 4:
			println("[TEMPORAL WORKER] Step 4: Temporal activity completed successfully.")
			return 1
		}
	}
}

func main() {}
