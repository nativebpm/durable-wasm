//go:build !wasm

package wasman

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"

	"github.com/nativebpm/httpstream"
)

func (s *Session) handleDownload(ptr int32, length int32) int32 {
	if s.downloadEOF {
		return 0
	}

	size := s.memory.Size()
	memoryBytes, ok := s.memory.Read(0, size)
	if !ok {
		slog.Error("[ENGINE] Failed to read memory in handleDownload")
		return -1
	}

	// Validate bounds before copy
	if ptr < 0 || length < 0 || int(ptr)+int(length) > len(memoryBytes) {
		slog.Error("[ENGINE] Memory access out of bounds in handleDownload", "ptr", ptr, "length", length, "mem_size", len(memoryBytes))
		return -1
	}

	if s.downloadResp == nil {
		url := fmt.Sprintf("http://%s/download", s.serverAddr)
		slog.Info("[ENGINE] GET Request (Stream-first)", "url", url)
		resp, err := httpstream.NewRequest(s.ctx, *s.engine.httpClient, "GET", url).Send()
		if err != nil {
			slog.Error("[ENGINE] GET failed", "error", err)
			return -1
		}
		s.downloadResp = resp
	}

	buf := make([]byte, length)
	n, err := s.downloadResp.Body.Read(buf)
	if n > 0 {
		s.memory.Write(uint32(ptr), buf[:n])
	}

	if err == io.EOF {
		slog.Info("[ENGINE] GET Stream EOF. Closing response")
		s.downloadResp.Body.Close()
		s.downloadResp = nil
		s.downloadEOF = true
		return int32(n)
	}

	if err != nil {
		slog.Error("[ENGINE] Read failed", "error", err)
		s.downloadResp.Body.Close()
		s.downloadResp = nil
		return -1
	}

	return int32(n)
}

func (s *Session) handleUpload(ptr int32, length int32) int32 {
	size := s.memory.Size()
	memoryBytes, ok := s.memory.Read(0, size)
	if !ok {
		slog.Error("[ENGINE] Failed to read memory in handleUpload")
		return -1
	}

	// Validate bounds before access
	if ptr < 0 || length < 0 || int(ptr)+int(length) > len(memoryBytes) {
		slog.Error("[ENGINE] Memory access out of bounds in handleUpload", "ptr", ptr, "length", length, "mem_size", len(memoryBytes))
		return -1
	}

	if length == 0 {
		if len(s.uploadBuffer) == 0 {
			return 0
		}

		url := fmt.Sprintf("http://%s/upload", s.serverAddr)
		slog.Info("[ENGINE] POST Request (Synchronous at EOF)", "url", url, "size", len(s.uploadBuffer))

		resp, err := httpstream.NewRequest(s.ctx, *s.engine.httpClient, "POST", url).
			Body(io.NopCloser(bytes.NewReader(s.uploadBuffer)), "application/octet-stream").
			Send()

		s.uploadBuffer = nil

		// Reset download stream state to allow next download requests
		s.downloadResp = nil
		s.downloadEOF = false

		if err != nil {
			slog.Error("[ENGINE] Synchronous POST failed", "error", err)
			return -1
		}
		defer resp.Body.Close()

		_, _ = io.Copy(io.Discard, resp.Body)
		slog.Info("[ENGINE] Synchronous POST completed successfully")
		return 0
	}

	dataToWrite := memoryBytes[ptr : ptr+length]
	s.uploadBuffer = append(s.uploadBuffer, dataToWrite...)

	return int32(length)
}
