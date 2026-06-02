//go:build wasm

package main

import (
	"encoding/json"
	"io"
	"unsafe"
)

// Global state variables for business logic
var (
	step        int32  = 0
	orderID     string = "ORD-CAM-8899"
	inventoryOk bool   = false
	paymentOk   bool   = false
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
type InventoryRequest struct {
	OrderID string `json:"order_id"`
	ItemID  string `json:"item_id"`
	Qty     int32  `json:"qty"`
}

type InventoryResponse struct {
	Status string `json:"status"`
}

type PaymentRequest struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

type PaymentResponse struct {
	Status string `json:"status"`
}

type FinalOrderRecord struct {
	OrderID     string `json:"order_id"`
	InventoryOk bool   `json:"inventory_ok"`
	PaymentOk   bool   `json:"payment_ok"`
	Status      string `json:"status"`
}

//export run
func run() int32 {
	for {
		switch step {
		case 0:
			println("[CAMUNDA WORKER] Step 0: Initializing order process for Camunda...")
			println("[CAMUNDA WORKER] Order ID:", orderID)
			step = 1
			println("[CAMUNDA WORKER] Step 0 completed. Saving state to checkpoint.")
			checkpoint()

		case 1:
			println("[CAMUNDA WORKER] Step 1: Performing Inventory Check...")

			writer := &StreamWriter{direction: 1}
			reader := &StreamReader{direction: 0}

			// Send inventory request
			req := InventoryRequest{OrderID: orderID, ItemID: "item-7788", Qty: 3}
			err := json.NewEncoder(writer).Encode(req)
			if err != nil {
				println("[CAMUNDA WORKER] Inventory request encode failed:", err.Error())
				return -1
			}

			// Flush / Signal EOF on upload
			var dummy [1]byte
			stream_data(1, uint32(uintptr(unsafe.Pointer(&dummy[0]))), 0)

			// Read response
			var resp InventoryResponse
			err = json.NewDecoder(reader).Decode(&resp)
			if err != nil {
				println("[CAMUNDA WORKER] Inventory response decode failed:", err.Error())
				return -1
			}

			if resp.Status == "available" {
				inventoryOk = true
				println("[CAMUNDA WORKER] Inventory check status: AVAILABLE")
			} else {
				println("[CAMUNDA WORKER] Inventory check status: NOT AVAILABLE")
				return -2
			}

			step = 2
			println("[CAMUNDA WORKER] Step 1 completed. Saving state to checkpoint.")
			checkpoint()

		case 2:
			println("[CAMUNDA WORKER] Step 2: Capturing Payment...")

			writer := &StreamWriter{direction: 1}
			reader := &StreamReader{direction: 0}

			// Send payment request
			req := PaymentRequest{OrderID: orderID, Amount: 350.0}
			err := json.NewEncoder(writer).Encode(req)
			if err != nil {
				println("[CAMUNDA WORKER] Payment request encode failed:", err.Error())
				return -1
			}

			// Flush / Signal EOF on upload
			var dummy [1]byte
			stream_data(1, uint32(uintptr(unsafe.Pointer(&dummy[0]))), 0)

			// Read response
			var resp PaymentResponse
			err = json.NewDecoder(reader).Decode(&resp)
			if err != nil {
				println("[CAMUNDA WORKER] Payment response decode failed:", err.Error())
				return -1
			}

			if resp.Status == "success" {
				paymentOk = true
				println("[CAMUNDA WORKER] Payment captured successfully.")
			} else {
				println("[CAMUNDA WORKER] Payment capture failed.")
				return -3
			}

			step = 3
			println("[CAMUNDA WORKER] Step 2 completed. Saving state to checkpoint.")
			checkpoint()

		case 3:
			println("[CAMUNDA WORKER] Step 3: Saving final order record to DB...")

			writer := &StreamWriter{direction: 1}

			result := FinalOrderRecord{
				OrderID:     orderID,
				InventoryOk: inventoryOk,
				PaymentOk:   paymentOk,
				Status:      "processed",
			}

			err := json.NewEncoder(writer).Encode(result)
			if err != nil {
				println("[CAMUNDA WORKER] Final order record encode failed:", err.Error())
				return -1
			}

			// Flush upload
			var dummy [1]byte
			stream_data(1, uint32(uintptr(unsafe.Pointer(&dummy[0]))), 0)

			println("[CAMUNDA WORKER] Final order record sent to database.")

			step = 4
			println("[CAMUNDA WORKER] Step 3 completed. Saving final checkpoint.")
			checkpoint()

		case 4:
			println("[CAMUNDA WORKER] Step 4: Camunda process completed successfully.")
			return 1
		}
	}
}

func main() {}
