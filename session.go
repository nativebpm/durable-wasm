//go:build !wasm

package wasman

import (
	"bytes"
	"io"
	"log/slog"
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

	if s.downloadReader == nil {
		if s.DownloadHandler == nil {
			slog.Error("[ENGINE] No in-memory download handler configured")
			return -1
		}
		data, err := s.DownloadHandler()
		if err != nil {
			slog.Error("[ENGINE] In-memory download handler failed", "error", err)
			return -1
		}
		s.downloadReader = bytes.NewReader(data)
	}

	buf := make([]byte, length)
	n, err := s.downloadReader.Read(buf)
	if n > 0 {
		s.memory.Write(uint32(ptr), buf[:n])
	}

	if err == io.EOF {
		slog.Info("[ENGINE] In-memory GET Stream EOF")
		s.downloadReader = nil
		s.downloadEOF = true
		return int32(n)
	}

	if err != nil {
		slog.Error("[ENGINE] Read failed", "error", err)
		s.downloadReader = nil
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

		if s.UploadHandler == nil {
			slog.Error("[ENGINE] No in-memory upload handler configured")
			return -1
		}

		err := s.UploadHandler(s.uploadBuffer)
		s.uploadBuffer = nil
		s.downloadReader = nil
		s.downloadEOF = false
		if err != nil {
			slog.Error("[ENGINE] In-memory upload handler failed", "error", err)
			return -1
		}
		slog.Info("[ENGINE] In-memory upload completed successfully")
		return 0
	}

	dataToWrite := memoryBytes[ptr : ptr+length]
	s.uploadBuffer = append(s.uploadBuffer, dataToWrite...)

	return int32(length)
}
