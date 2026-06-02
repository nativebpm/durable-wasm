//go:build wasm

package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/nativebpm/wasman"
)

// State holds the workflow state.
// All fields are automatically preserved during checkpoints by memory snapshotting.
type State struct {
	TotalAmount  float64
	ValidRecords int32
}

// Target JSON record format
type UserRecord struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Email  string  `json:"email"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

var state = &State{
	TotalAmount:  0.0,
	ValidRecords: 0,
}

//export run
func run() int32 {
	return wasman.NewWorkflow().
		Step(state.initialize).
		Step(state.processCSVStream).
		Step(state.finalizeProcess).
		Run()
}

func main() {}

func (s *State) initialize() error {
	println("[CSV WORKER] Step 0: Initializing CSV processor...")
	return nil
}

func (s *State) processCSVStream() error {
	println("[CSV WORKER] Step 1: Processing CSV stream and generating JSON output...")

	csvReader := csv.NewReader(wasman.Reader)
	// Read the header row
	header, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("error reading CSV header: %w", err)
	}

	// Verify header structure: id,name,email,amount
	if len(header) < 4 {
		return fmt.Errorf("CSV header requires at least 4 columns")
	}

	// We will write output as line-delimited JSON or JSON stream
	// To stream items one by one, we use json.NewEncoder
	jsonEncoder := json.NewEncoder(wasman.Writer)

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading CSV row: %w", err)
		}

		id := row[0]
		name := row[1]
		email := row[2]
		amountStr := row[3]

		status := "active"
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			status = "invalid_amount"
			amount = 0.0
		}

		if !strings.Contains(email, "@") {
			status = "invalid_email"
		}

		if status == "active" {
			s.TotalAmount += amount
			s.ValidRecords++
		}

		// Construct the target structured record
		record := UserRecord{
			ID:     id,
			Name:   name,
			Email:  email,
			Amount: amount,
			Status: status,
		}

		// Encode and write immediately to the host's upload pipe.
		// This achieves strict O(1) RAM streaming since we don't buffer the records list in memory.
		err = jsonEncoder.Encode(record)
		if err != nil {
			return fmt.Errorf("error encoding JSON record: %w", err)
		}
	}

	// Close the upload stream by sending EOF
	return wasman.Writer.Close()
}

func (s *State) finalizeProcess() error {
	println("[CSV WORKER] Step 2: Finalizing business validation...")
	fmt.Printf("[CSV WORKER] Total valid records processed: %d\n", s.ValidRecords)
	fmt.Printf("[CSV WORKER] Sum of active user amounts: %s\n", strconv.FormatFloat(s.TotalAmount, 'f', 2, 64))
	return nil
}
