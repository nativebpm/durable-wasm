//go:build wasm

package main

import (
	"encoding/json"
	"fmt"

	"github.com/nativebpm/wasman/runner"
)

// State holds the workflow state.
// All fields are automatically preserved during checkpoints by memory snapshotting.
type State struct {
	ActivityID    string
	BaseRate      float64
	Multiplier    float64
	CalculatedVal float64
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

var state = &State{
	ActivityID:    "ACT-TEMP-4455",
	BaseRate:      0.0,
	Multiplier:    0.0,
	CalculatedVal: 0.0,
}

//export run
func run() int32 {
	return runner.NewWorkflow().
		Step(state.initialize).
		Step(state.downloadParams).
		Step(state.performCalculation).
		Step(state.saveFinalRecord).
		Step(state.finalizeActivity).
		Run()
}

func main() {}

func (s *State) initialize() error {
	println("[TEMPORAL WORKER] Step 0: Starting Temporal durable activity...")
	fmt.Printf("[TEMPORAL WORKER] Activity initialized: %s\n", s.ActivityID)
	return nil
}

func (s *State) downloadParams() error {
	println("[TEMPORAL WORKER] Step 1: Downloading calculation parameters from host...")

	// Send param request
	req := ParamRequest{ActivityID: s.ActivityID}
	err := json.NewEncoder(runner.Writer).Encode(req)
	if err != nil {
		return fmt.Errorf("request parameters encode failed: %w", err)
	}

	// Flush and signal EOF
	err = runner.Writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Read response
	var resp ParamResponse
	err = json.NewDecoder(runner.Reader).Decode(&resp)
	if err != nil {
		return fmt.Errorf("response parameters decode failed: %w", err)
	}

	s.BaseRate = resp.BaseRate
	s.Multiplier = resp.Multiplier
	fmt.Printf("[TEMPORAL WORKER] Parameters received: BaseRate = %f, Multiplier = %f\n", s.BaseRate, s.Multiplier)
	return nil
}

func (s *State) performCalculation() error {
	println("[TEMPORAL WORKER] Step 2: Performing calculation...")

	// Simulate complex calculation and store in state variable to pass to next step
	s.CalculatedVal = s.BaseRate * s.Multiplier * 150.0
	fmt.Printf("[TEMPORAL WORKER] Calculated value: %f\n", s.CalculatedVal)
	return nil
}

func (s *State) saveFinalRecord() error {
	println("[TEMPORAL WORKER] Step 3: Saving final calculation record to database...")

	result := FinalCalculationResult{
		ActivityID: s.ActivityID,
		BaseRate:   s.BaseRate,
		Multiplier: s.Multiplier,
		Result:     s.CalculatedVal,
		Completed:  true,
	}

	err := json.NewEncoder(runner.Writer).Encode(result)
	if err != nil {
		return fmt.Errorf("final result encode failed: %w", err)
	}

	// Flush upload stream
	err = runner.Writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	println("[TEMPORAL WORKER] Final record sent to database.")
	return nil
}

func (s *State) finalizeActivity() error {
	println("[TEMPORAL WORKER] Step 4: Temporal activity completed successfully.")
	return nil
}
