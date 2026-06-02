//go:build wasm

package main

import (
	"io"
	"unsafe"
)

// Global state variables.
// These variables reside in WebAssembly linear memory and will be persisted
// inside the snapshot, saving files across host crashes.
var (
	step      int32  = 0
	chatID    int64  = 77665544
	fileID    string = "file_docx_invoice_102"
	docxBytes []byte
	pdfBytes  []byte
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

//export run
func run() int32 {
	for {
		switch step {
		case 0:
			println("[TELEGRAM-GOTENBERG WORKER] Step 0: Initializing workflow...")
			println("[TELEGRAM-GOTENBERG WORKER] Target user ChatID:", chatID)
			println("[TELEGRAM-GOTENBERG WORKER] Target docx FileID:", fileID)
			step = 1
			println("[TELEGRAM-GOTENBERG WORKER] Step 0 completed. Saving checkpoint.")
			checkpoint()

		case 1:
			println("[TELEGRAM-GOTENBERG WORKER] Step 1: Downloading DOCX from Telegram API...")

			reader := &StreamReader{direction: 0}

			// Read the incoming DOCX file stream into global memory slice.
			// In production, we'd read in chunks.
			buf := make([]byte, 1024)
			for {
				n, err := reader.Read(buf)
				if n > 0 {
					docxBytes = append(docxBytes, buf[:n]...)
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					println("[TELEGRAM-GOTENBERG WORKER] Download failed:", err.Error())
					return -1
				}
			}

			println("[TELEGRAM-GOTENBERG WORKER] DOCX successfully downloaded. Size:", len(docxBytes), "bytes")

			step = 2
			println("[TELEGRAM-GOTENBERG WORKER] Step 1 completed. Saving checkpoint.")
			checkpoint()

		case 2:
			println("[TELEGRAM-GOTENBERG WORKER] Step 2: Converting DOCX to PDF via Gotenberg API...")

			writer := &StreamWriter{direction: 1}
			reader := &StreamReader{direction: 0}

			// Stream the docx bytes from WASM memory to Gotenberg via host stream_data
			n, err := writer.Write(docxBytes)
			if err != nil || n != len(docxBytes) {
				println("[TELEGRAM-GOTENBERG WORKER] Gotenberg upload failed:", err)
				return -1
			}

			// Flush / Signal upload EOF
			var dummy [1]byte
			stream_data(1, uint32(uintptr(unsafe.Pointer(&dummy[0]))), 0)

			// Read converted PDF bytes back into global memory slice
			buf := make([]byte, 1024)
			for {
				n, err := reader.Read(buf)
				if n > 0 {
					pdfBytes = append(pdfBytes, buf[:n]...)
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					println("[TELEGRAM-GOTENBERG WORKER] Gotenberg download failed:", err.Error())
					return -1
				}
			}

			println("[TELEGRAM-GOTENBERG WORKER] PDF successfully generated. Size:", len(pdfBytes), "bytes")

			step = 3
			println("[TELEGRAM-GOTENBERG WORKER] Step 2 completed. Saving checkpoint.")
			checkpoint()

		case 3:
			println("[TELEGRAM-GOTENBERG WORKER] Step 3: Sending PDF back to Telegram User...")

			writer := &StreamWriter{direction: 1}

			// Stream PDF bytes from WASM memory to Telegram sendDocument via host
			n, err := writer.Write(pdfBytes)
			if err != nil || n != len(pdfBytes) {
				println("[TELEGRAM-GOTENBERG WORKER] Telegram upload failed:", err)
				return -1
			}

			// Flush / Signal upload EOF
			var dummy [1]byte
			stream_data(1, uint32(uintptr(unsafe.Pointer(&dummy[0]))), 0)

			println("[TELEGRAM-GOTENBERG WORKER] PDF document successfully sent to Telegram user.")

			step = 4
			println("[TELEGRAM-GOTENBERG WORKER] Step 3 completed. Saving final checkpoint.")
			checkpoint()

		case 4:
			println("[TELEGRAM-GOTENBERG WORKER] Step 4: Workflow complete.")
			return 1
		}
	}
}

func main() {}
