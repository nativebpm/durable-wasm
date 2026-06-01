# Gotenberg & Telegram Document Conversion Pipeline

This example demonstrates a multi-stage document processing pipeline (Download DOCX from Telegram -> Convert to PDF via Gotenberg API -> Upload PDF to Telegram) executed inside a crash-resilient WASM engine.

## The Problem It Solves
Integrations handling media assets (like invoices, contracts, or reports) are often brittle:
1. **Network Failures**: Converting a file requires calling slow external APIs (Gotenberg for rendering, Telegram Bot API for file transfer). If the connection drops or the server restarts mid-flight, the process is lost.
2. **Redundant Network I/O**: If the host crashes *after* downloading a 50MB document but *before* sending it to the PDF converter, the restarted task must download the file from Telegram again, wasting bandwidth and hitting API rate limits.

## The Durable WASM Solution
In our architecture, the downloaded document bytes are kept directly inside the WASM worker's memory. When the worker checkpoints:
1. The entire downloaded DOCX payload and progress status are saved in the memory snapshot.
2. If the host crashes while sending the file to the Gotenberg PDF converter, the engine resumes with the **DOCX file already present in WASM memory**.
3. The worker skips the download step entirely, immediately goes to the PDF conversion step, and uploads the final document back to Telegram.

---

## How to Run

From this directory, run:
```bash
make run
```
This command will:
1. Build the TinyGo worker into `worker/worker.wasm`.
2. Start a mock server representing Telegram Bot API and Gotenberg PDF Engine.
3. Run the Go host which:
   - Starts execution, downloads the DOCX, saves the checkpoint, and **simulates a crash** (Run 1).
   - Reloads the checkpoint containing the DOCX bytes in memory, calls Gotenberg for PDF conversion, and uploads the PDF back to the user (Run 2).
