//go:build wasm

package main

import (
	"encoding/json"
	"fmt"

	"github.com/nativebpm/durable-wasm"
)

// State holds the workflow state.
// All fields are automatically preserved during checkpoints by memory snapshotting.
type State struct {
	OrderID     string
	InventoryOk bool
	PaymentOk   bool
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

var state = &State{
	OrderID:     "ORD-CAM-8899",
	InventoryOk: false,
	PaymentOk:   false,
}

//export run
func run() int32 {
	return durable.NewWorkflow(&state).
		Step((*State).initialize).
		Step((*State).checkInventory).
		Step((*State).capturePayment).
		Step((*State).saveOrderRecord).
		Step((*State).finalizeProcess).
		Run()
}

func main() {}

func (s *State) initialize() error {
	println("[CAMUNDA WORKER] Step 0: Initializing order process for Camunda...")
	fmt.Printf("[CAMUNDA WORKER] Order ID: %s\n", s.OrderID)
	return nil
}

func (s *State) checkInventory() error {
	println("[CAMUNDA WORKER] Step 1: Performing Inventory Check...")

	// Send inventory request to host stream
	req := InventoryRequest{OrderID: s.OrderID, ItemID: "item-7788", Qty: 3}
	err := json.NewEncoder(durable.Writer).Encode(req)
	if err != nil {
		return fmt.Errorf("inventory request encode failed: %w", err)
	}

	// Flush and signal EOF on the upload stream
	err = durable.Writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Read response from host stream
	var resp InventoryResponse
	err = json.NewDecoder(durable.Reader).Decode(&resp)
	if err != nil {
		return fmt.Errorf("inventory response decode failed: %w", err)
	}

	if resp.Status == "available" {
		s.InventoryOk = true
		println("[CAMUNDA WORKER] Inventory check status: AVAILABLE")
	} else {
		println("[CAMUNDA WORKER] Inventory check status: NOT AVAILABLE")
		return fmt.Errorf("inventory not available")
	}

	return nil
}

func (s *State) capturePayment() error {
	println("[CAMUNDA WORKER] Step 2: Capturing Payment...")

	// Send payment request
	req := PaymentRequest{OrderID: s.OrderID, Amount: 350.0}
	err := json.NewEncoder(durable.Writer).Encode(req)
	if err != nil {
		return fmt.Errorf("payment request encode failed: %w", err)
	}

	// Flush and signal EOF
	err = durable.Writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Read response
	var resp PaymentResponse
	err = json.NewDecoder(durable.Reader).Decode(&resp)
	if err != nil {
		return fmt.Errorf("payment response decode failed: %w", err)
	}

	if resp.Status == "success" {
		s.PaymentOk = true
		println("[CAMUNDA WORKER] Payment captured successfully.")
	} else {
		println("[CAMUNDA WORKER] Payment capture failed.")
		return fmt.Errorf("payment capture failed")
	}

	return nil
}

func (s *State) saveOrderRecord() error {
	println("[CAMUNDA WORKER] Step 3: Saving final order record to DB...")

	result := FinalOrderRecord{
		OrderID:     s.OrderID,
		InventoryOk: s.InventoryOk,
		PaymentOk:   s.PaymentOk,
		Status:      "processed",
	}

	err := json.NewEncoder(durable.Writer).Encode(result)
	if err != nil {
		return fmt.Errorf("final order record encode failed: %w", err)
	}

	// Flush upload stream
	err = durable.Writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	println("[CAMUNDA WORKER] Final order record sent to database.")
	return nil
}

func (s *State) finalizeProcess() error {
	println("[CAMUNDA WORKER] Step 4: Camunda process completed successfully.")
	return nil
}
