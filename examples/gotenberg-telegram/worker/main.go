//go:build wasm

package main

import (
	"fmt"
	"io"

	"github.com/nativebpm/durable-wasm"
)

// State holds the workflow state.
// All fields are automatically preserved during checkpoints by memory snapshotting.
type State struct {
	ChatID    int64
	FileID    string
	DocxBytes []byte
	PdfBytes  []byte
}

var state = &State{
	ChatID: 77665544,
	FileID: "file_docx_invoice_102",
}

//export run
func run() int32 {
	return durable.NewWorkflow(&state).
		Step((*State).initialize).
		Step((*State).downloadDocx).
		Step((*State).convertToPdf).
		Step((*State).sendPdfToUser).
		Step((*State).finalizeWorkflow).
		Run()
}

func main() {}

func (s *State) initialize() error {
	println("[TELEGRAM-GOTENBERG WORKER] Step 0: Initializing workflow...")
	fmt.Printf("[TELEGRAM-GOTENBERG WORKER] Target user ChatID: %d\n", s.ChatID)
	fmt.Printf("[TELEGRAM-GOTENBERG WORKER] Target docx FileID: %s\n", s.FileID)
	return nil
}

func (s *State) downloadDocx() error {
	println("[TELEGRAM-GOTENBERG WORKER] Step 1: Downloading DOCX from Telegram API...")

	// Read the incoming DOCX file stream into memory slice.
	buf := make([]byte, 1024)
	for {
		n, err := durable.Reader.Read(buf)
		if n > 0 {
			s.DocxBytes = append(s.DocxBytes, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("download failed: %w", err)
		}
	}

	fmt.Printf("[TELEGRAM-GOTENBERG WORKER] DOCX successfully downloaded. Size: %d bytes\n", len(s.DocxBytes))
	return nil
}

func (s *State) convertToPdf() error {
	println("[TELEGRAM-GOTENBERG WORKER] Step 2: Converting DOCX to PDF via Gotenberg API...")

	// Stream the docx bytes from WASM memory to Gotenberg via host stream
	n, err := durable.Writer.Write(s.DocxBytes)
	if err != nil {
		return fmt.Errorf("gotenberg upload failed: %w", err)
	}
	if n != len(s.DocxBytes) {
		return fmt.Errorf("mismatch in bytes uploaded: expected %d, got %d", len(s.DocxBytes), n)
	}

	// Flush and signal upload EOF
	err = durable.Writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Read converted PDF bytes back into memory slice
	buf := make([]byte, 1024)
	for {
		n, err := durable.Reader.Read(buf)
		if n > 0 {
			s.PdfBytes = append(s.PdfBytes, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gotenberg download failed: %w", err)
		}
	}

	fmt.Printf("[TELEGRAM-GOTENBERG WORKER] PDF successfully generated. Size: %d bytes\n", len(s.PdfBytes))
	return nil
}

func (s *State) sendPdfToUser() error {
	println("[TELEGRAM-GOTENBERG WORKER] Step 3: Sending PDF back to Telegram User...")

	// Stream PDF bytes from WASM memory to Telegram sendDocument via host
	n, err := durable.Writer.Write(s.PdfBytes)
	if err != nil {
		return fmt.Errorf("telegram upload failed: %w", err)
	}
	if n != len(s.PdfBytes) {
		return fmt.Errorf("mismatch in bytes uploaded: expected %d, got %d", len(s.PdfBytes), n)
	}

	// Flush and signal upload EOF
	err = durable.Writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	println("[TELEGRAM-GOTENBERG WORKER] PDF document successfully sent to Telegram user.")
	return nil
}

func (s *State) finalizeWorkflow() error {
	println("[TELEGRAM-GOTENBERG WORKER] Step 4: Workflow complete.")
	return nil
}
