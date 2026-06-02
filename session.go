//go:build !wasm

package wasman

import (
	"fmt"
	"io"
	"log/slog"
	"unsafe"

	"github.com/nativebpm/httpstream"
)

func (s *Session) handleDownload(ptr int32, length int32) int32 {
	if s.downloadEOF {
		return 0
	}

	mPtr := s.memory.Data(s.store)
	mSize := s.memory.DataSize(s.store)
	memoryBytes := unsafe.Slice((*byte)(mPtr), mSize)

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
		copy(memoryBytes[ptr:ptr+int32(n)], buf[:n])
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
	mPtr := s.memory.Data(s.store)
	mSize := s.memory.DataSize(s.store)
	memoryBytes := unsafe.Slice((*byte)(mPtr), mSize)

	// Validate bounds before access
	if ptr < 0 || length < 0 || int(ptr)+int(length) > len(memoryBytes) {
		slog.Error("[ENGINE] Memory access out of bounds in handleUpload", "ptr", ptr, "length", length, "mem_size", len(memoryBytes))
		return -1
	}

	if s.uploadPipeW == nil {
		url := fmt.Sprintf("http://%s/upload", s.serverAddr)
		slog.Info("[ENGINE] POST Request (Stream-first via io.Pipe)", "url", url)

		pipeReader, pipeWriter := io.Pipe()
		s.uploadPipeW = pipeWriter
		s.uploadErrChan = make(chan error, 1)

		go func() {
			resp, err := httpstream.NewRequest(s.ctx, *s.engine.httpClient, "POST", url).
				Body(pipeReader, "application/octet-stream").
				Send()
			if err != nil {
				pipeReader.CloseWithError(err)
				s.uploadErrChan <- err
				return
			}
			defer resp.Body.Close()

			_, _ = io.Copy(io.Discard, resp.Body)
			s.uploadErrChan <- nil
		}()
	}

	if length == 0 {
		slog.Info("[ENGINE] Closing upload stream (EOF). Waiting for response")
		s.uploadPipeW.Close()
		err := <-s.uploadErrChan
		s.uploadPipeW = nil

		// Reset download stream state to allow next download requests
		s.downloadResp = nil
		s.downloadEOF = false

		if err != nil {
			slog.Error("[ENGINE] POST failed", "error", err)
			return -1
		}
		slog.Info("[ENGINE] POST completed successfully")
		return 0
	}

	dataToWrite := memoryBytes[ptr : ptr+length]
	n, err := s.uploadPipeW.Write(dataToWrite)
	if err != nil {
		slog.Error("[ENGINE] Write to pipe failed", "error", err)
		return -1
	}

	return int32(n)
}
