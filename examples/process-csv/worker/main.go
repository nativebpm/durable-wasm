package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"unsafe"
)

// Global state variables
var (
	step          int32 = 0
	totalAmount   float64 = 0.0
	validRecords  int32 = 0
)

// Host imports
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

// Target JSON record format
type UserRecord struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Email  string  `json:"email"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

//export run
func run() int32 {
	for {
		switch step {
		case 0:
			println("[CSV WORKER] Step 0: Initializing CSV processor...")
			step = 1
			println("[CSV WORKER] Step 0 completed. Saving checkpoint.")
			checkpoint()

		case 1:
			println("[CSV WORKER] Step 1: Processing CSV stream and generating JSON output...")

			reader := &StreamReader{direction: 0}
			writer := &StreamWriter{direction: 1}

			csvReader := csv.NewReader(reader)
			// Read the header row
			header, err := csvReader.Read()
			if err != nil {
				println("[CSV WORKER] Error reading CSV header:", err.Error())
				return -1
			}

			// Verify header structure: id,name,email,amount
			if len(header) < 4 {
				println("[CSV WORKER] Error: CSV header requires at least 4 columns")
				return -1
			}

			// We will write output as line-delimited JSON or JSON stream
			// To stream items one by one, we use json.NewEncoder
			jsonEncoder := json.NewEncoder(writer)

			for {
				row, err := csvReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					println("[CSV WORKER] Error reading CSV row:", err.Error())
					return -1
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
					totalAmount += amount
					validRecords++
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
					println("[CSV WORKER] Error encoding JSON record:", err.Error())
					return -1
				}
			}

			// Close the upload stream by sending a zero-length write signal
			// Since writer is a StreamWriter wrapping direction 1:
			var dummy [1]byte
			stream_data(1, uint32(uintptr(unsafe.Pointer(&dummy[0]))), 0)

			step = 2
			println("[CSV WORKER] Step 1 completed. Saving checkpoint.")
			checkpoint()

		case 2:
			println("[CSV WORKER] Step 2: Finalizing business validation...")
			println("[CSV WORKER] Total valid records processed:", validRecords)
			println("[CSV WORKER] Sum of active user amounts:", strconv.FormatFloat(totalAmount, 'f', 2, 64))
			step = 3
			println("[CSV WORKER] Step 2 completed. Final checkpoint.")
			checkpoint()

		case 3:
			println("[CSV WORKER] Execution completed.")
			return 1
		}
	}
}

func main() {}
