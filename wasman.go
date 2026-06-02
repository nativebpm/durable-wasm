//go:build !wasm

package wasman

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/bytecodealliance/wasmtime-go/v20"
)

// NewEngine creates a new reusable WASM Durable Execution Engine.
func NewEngine(wasmPath string, store SnapshotStore, opts ...EngineOption) (*Engine, error) {
	// Read WASM bytes to calculate SHA256 hash (WASM Module Versioning)
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read WASM module file: %w", err)
	}
	hash := sha256.Sum256(wasmBytes)
	wasmHash := hex.EncodeToString(hash[:])

	// Save WASM module in registry for future multi-version execution
	err = store.SaveWasm(wasmHash, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to save WASM module in registry: %w", err)
	}

	// Configure Wasmtime with strict float determinism (NaN Canonicalization)
	config := wasmtime.NewConfig()

	wasmEngine := wasmtime.NewEngineWithConfig(config)
	module, err := wasmtime.NewModule(wasmEngine, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to compile WASM module: %w", err)
	}

	engine := &Engine{
		wasmEngine: wasmEngine,
		module:     module,
		store:      store,
		wasmHash:   wasmHash,
		httpClient: defaultHTTPClient,
	}

	for _, opt := range opts {
		opt(engine)
	}

	return engine, nil
}

// nolint:gocyclo
// Execute runs the WASM instance with a given entrypoint and session context.
// If it finds a saved snapshot, it automatically restores the linear memory.
func (e *Engine) Execute(ctx context.Context, instanceID string, entrypoint string, serverAddr string, shouldCrash bool) (bool, error) {
	// Load or initialize metadata (WASM Module Versioning & OCC)
	meta, err := e.store.LoadMetadata(instanceID)
	if err != nil {
		return false, fmt.Errorf("failed to load metadata: %w", err)
	}

	var runModule *wasmtime.Module
	if meta != nil {
		if meta.WasmHash != e.wasmHash {
			slog.Info("[ENGINE] Instance requires a different WASM module version", "instance_id", instanceID, "required_hash", meta.WasmHash, "current_hash", e.wasmHash)
			loadedBytes, err := e.store.LoadWasm(meta.WasmHash)
			if err != nil {
				return false, fmt.Errorf("failed to load required WASM version %s from registry: %w: %w", meta.WasmHash, err, ErrWasmVersionMismatch)
			}
			runModule, err = wasmtime.NewModule(e.wasmEngine, loadedBytes)
			if err != nil {
				return false, fmt.Errorf("failed to compile historical WASM module %s: %w", meta.WasmHash, err)
			}
		} else {
			runModule = e.module
		}
	} else {
		meta = &InstanceMeta{
			InstanceID: instanceID,
			WasmHash:   e.wasmHash,
			Version:    0,
		}
		runModule = e.module
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	session := &Session{
		engine:                  e,
		ctx:                     ctx,
		instanceID:              instanceID,
		serverAddr:              serverAddr,
		shouldCrashOnCheckpoint: shouldCrash,
		meta:                    meta,
	}

	// Guarantee cleanup of HTTP connections and pipes on return
	defer func() {
		if session.downloadResp != nil {
			session.downloadResp.Body.Close()
		}
		if session.uploadPipeW != nil {
			session.uploadPipeW.Close()
		}
	}()

	store := wasmtime.NewStore(e.wasmEngine)
	defer store.Close() // Explicitly release C-memory!
	session.store = store

	// Configure WASI
	wasiConfig := wasmtime.NewWasiConfig()
	wasiConfig.InheritStdout()
	wasiConfig.InheritStderr()
	store.SetWasi(wasiConfig)

	// Create Linker and define WASI imports
	linker := wasmtime.NewLinker(e.wasmEngine)
	err = linker.DefineWasi()
	if err != nil {
		return false, fmt.Errorf("failed to link WASI: %w", err)
	}

	// Register Host Function: checkpoint (using local closure)
	err = linker.DefineFunc(store, "env", "checkpoint", func(caller *wasmtime.Caller) *wasmtime.Trap {
		slog.Info("[ENGINE] 'checkpoint' invoked", "instance_id", session.instanceID)

		// 1. Атомарный Compare-And-Swap версии метаданных (OCC)
		session.meta.WasmHash = e.wasmHash
		ok, err := e.store.SaveMetadata(session.meta)
		if err != nil {
			slog.Error("[ENGINE] Failed to save metadata", "error", err)
			return wasmtime.NewTrap("failed to save metadata")
		}
		if !ok {
			slog.Warn("[ENGINE] OCC conflict detected. Aborting execution.")
			return wasmtime.NewTrap("concurrent_execution_detected")
		}

		ext := caller.GetExport("memory")
		if ext == nil {
			return wasmtime.NewTrap("memory export not found")
		}
		mem := ext.Memory()
		session.memory = mem

		// Read and snapshot the linear memory safely using unsafe.Slice
		ptr := mem.Data(store)
		size := mem.DataSize(store)
		if size == 0 {
			return wasmtime.NewTrap("memory data size is zero")
		}
		memoryBytes := unsafe.Slice((*byte)(ptr), size)

		// 2. Snapshotting strategy (Full vs Deltas) & Truncation
		// Делаем полный снапшот при первом чекпоинте (Version = 1, так как SaveMetadata только что инкрементировал её с 0 до 1)
		// или каждые 5 чекпоинтов.
		isFullSnapshot := session.meta.Version == 1 || (session.meta.Version > 1 && session.meta.Version%5 == 0)

		if isFullSnapshot {
			slog.Info("[ENGINE] Writing Full Memory Snapshot", "version", session.meta.Version)
			snapshotCopy := make([]byte, len(memoryBytes))
			copy(snapshotCopy, memoryBytes)
			err := e.store.Save(session.instanceID, snapshotCopy)
			if err != nil {
				slog.Error("[ENGINE] Failed to save full snapshot", "error", err)
				return wasmtime.NewTrap("failed to write snapshot")
			}

			// Truncate Oplog & memory_deltas only for periodic full snapshots (Version > 1)
			if session.meta.Version > 1 {
				slog.Info("[ENGINE] Truncating Oplog and memory deltas", "before_call_index", session.callIndex)
				_ = e.store.TruncateOplog(session.instanceID, session.callIndex)
				_ = e.store.TruncateDeltas(session.instanceID)
			}
		} else {
			// Инкрементальный чекпоинт (Dirty-Page deltas)
			blockSize := 4096
			numBlocks := len(memoryBytes) / blockSize
			deltas := make(map[int][]byte)
			for i := 0; i < numBlocks; i++ {
				start := i * blockSize
				end := start + blockSize
				if end > len(memoryBytes) {
					end = len(memoryBytes)
				}
				blockData := memoryBytes[start:end]
				h := fnv.New64a()
				h.Write(blockData)
				hashVal := h.Sum64()

				prevHash, exists := session.pageHashes[i]
				if !exists || prevHash != hashVal {
					blockCopy := make([]byte, len(blockData))
					copy(blockCopy, blockData)
					deltas[i] = blockCopy
					session.pageHashes[i] = hashVal
				}
			}

			if len(deltas) > 0 {
				err = e.store.SaveDeltas(session.instanceID, deltas)
				if err != nil {
					slog.Error("[ENGINE] Failed to save memory deltas", "error", err)
					return wasmtime.NewTrap("failed to write memory deltas")
				}
				slog.Info("[ENGINE] Memory deltas successfully saved", "dirty_blocks", len(deltas))
			}
		}

		if session.shouldCrashOnCheckpoint {
			session.crashed = true
			slog.Warn("[ENGINE] Simulating host crash. Aborting WASM execution.")
			return wasmtime.NewTrap("simulated_host_crash")
		}

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("failed to register 'checkpoint': %w", err)
	}

	// Register Host Function: host_get_time
	err = linker.DefineFunc(store, "env", "host_get_time", func(caller *wasmtime.Caller) int64 {
		session.callIndex++
		callIdx := session.callIndex

		// Check Oplog Replay for host_get_time
		oplog, err := e.store.LoadOplog(session.instanceID)
		if err == nil {
			for _, entry := range oplog {
				if entry.CallIndex == callIdx && entry.ApiName == "host_get_time" {
					slog.Info("[ENGINE] Oplog Replay (Time)", "call_index", callIdx)
					val, err := strconv.ParseInt(string(entry.ResponsePayload), 10, 64)
					if err == nil {
						return val
					}
					slog.Error("[ENGINE] Failed to parse time from oplog", "error", err)
				}
			}
		}

		// Live execution
		nowNano := time.Now().UnixNano()
		slog.Info("[ENGINE] Oplog Execution: host_get_time", "call_index", callIdx, "time", nowNano)

		payload := []byte(strconv.FormatInt(nowNano, 10))
		err = e.store.SaveOplog(session.instanceID, callIdx, "host_get_time", nil, payload)
		if err != nil {
			slog.Error("[ENGINE] Failed to save Oplog for host_get_time", "error", err)
		}
		return nowNano
	})
	if err != nil {
		return false, fmt.Errorf("failed to register 'host_get_time': %w", err)
	}

	// Register Host Function: host_call_api
	err = linker.DefineFunc(store, "env", "host_call_api", func(
		caller *wasmtime.Caller,
		apiNamePtr int32, apiNameLen int32,
		reqPtr int32, reqLen int32,
		respPtr int32, respMaxLen int32,
	) int32 {

		ext := caller.GetExport("memory")
		if ext == nil {
			slog.Error("[ENGINE] host_call_api: memory export not found")
			return -1
		}
		mem := ext.Memory()
		session.memory = mem

		mPtr := mem.Data(store)
		mSize := mem.DataSize(store)
		memoryBytes := unsafe.Slice((*byte)(mPtr), mSize)

		if apiNamePtr < 0 || apiNameLen < 0 || int(apiNamePtr)+int(apiNameLen) > len(memoryBytes) {
			slog.Error("[ENGINE] host_call_api: memory access out of bounds for api name")
			return -1
		}
		apiName := string(memoryBytes[apiNamePtr : apiNamePtr+apiNameLen])

		if reqPtr < 0 || reqLen < 0 || int(reqPtr)+int(reqLen) > len(memoryBytes) {
			slog.Error("[ENGINE] host_call_api: memory access out of bounds for request")
			return -1
		}
		request := memoryBytes[reqPtr : reqPtr+reqLen]

		session.callIndex++
		callIdx := session.callIndex

		// Check Oplog Replay
		oplog, err := e.store.LoadOplog(session.instanceID)
		if err == nil {
			for _, entry := range oplog {
				if entry.CallIndex == callIdx && entry.ApiName == apiName {
					slog.Info("[ENGINE] Oplog Replay", "api", apiName, "call_index", callIdx)
					if int(respMaxLen) < len(entry.ResponsePayload) {
						slog.Error("[ENGINE] Oplog Replay: response buffer too small")
						return -2
					}
					copy(memoryBytes[respPtr:respPtr+int32(len(entry.ResponsePayload))], entry.ResponsePayload)
					return int32(len(entry.ResponsePayload))
				}
			}
		}

		// Perform real operation
		slog.Info("[ENGINE] Oplog Execution: invoking real API call", "api", apiName, "call_index", callIdx)
		var response []byte

		if apiName == "test_api" {
			// For testing / simulation purposes
			time.Sleep(10 * time.Millisecond)
			response = []byte(fmt.Sprintf("resp_for_%s_call_%d", string(request), callIdx))
		} else {
			// Real external REST API call if endpoint matches serverAddr
			url := fmt.Sprintf("http://%s/%s", session.serverAddr, apiName)
			req, err := http.NewRequestWithContext(session.ctx, "POST", url, strings.NewReader(string(request)))
			if err != nil {
				slog.Error("[ENGINE] Failed to create HTTP request", "url", url, "error", err)
				return -1
			}
			req.Header.Set("Content-Type", "application/octet-stream")
			resp, err := e.httpClient.Do(req)
			if err != nil {
				slog.Error("[ENGINE] Real API call failed", "url", url, "error", err)
				return -1
			}
			defer resp.Body.Close()
			response, _ = io.ReadAll(resp.Body)
		}

		// Save response to Oplog
		err = e.store.SaveOplog(session.instanceID, callIdx, apiName, request, response)
		if err != nil {
			slog.Error("[ENGINE] Failed to save Oplog", "error", err)
			return -1
		}

		if int(respMaxLen) < len(response) {
			slog.Error("[ENGINE] Response buffer too small for live response")
			return -2
		}
		copy(memoryBytes[respPtr:respPtr+int32(len(response))], response)
		return int32(len(response))
	})
	if err != nil {
		return false, fmt.Errorf("failed to register 'host_call_api': %w", err)
	}

	// Register Host Function: stream_data (using local closure)
	err = linker.DefineFunc(store, "env", "stream_data", func(caller *wasmtime.Caller, direction int32, ptr int32, length int32) int32 {
		ext := caller.GetExport("memory")
		if ext == nil {
			slog.Error("[ENGINE] stream_data: memory export not found")
			return -1
		}
		mem := ext.Memory()
		session.memory = mem

		switch direction {
		case 0:
			return session.handleDownload(ptr, length)
		case 1:
			return session.handleUpload(ptr, length)
		}

		slog.Error("[ENGINE] stream_data: invalid direction", "direction", direction)
		return -1
	})
	if err != nil {
		return false, fmt.Errorf("failed to register 'stream_data': %w", err)
	}

	// Instantiate the WASM module
	instance, err := linker.Instantiate(store, runModule)
	if err != nil {
		return false, fmt.Errorf("failed to instantiate WASM: %w", err)
	}

	// Fetch memory export from the new instance
	ext := instance.GetExport(store, "memory")
	if ext == nil {
		return false, fmt.Errorf("failed to find memory export on instantiation")
	}
	session.memory = ext.Memory()

	// RESTORE: Check if there is an existing snapshot to restore
	session.pageHashes = make(map[int]uint64)

	// 1. Load full snapshot if exists
	snapshot, err := e.store.Load(instanceID)
	if err == nil && len(snapshot) > 0 {
		slog.Info("[ENGINE] Found saved full snapshot. Restoring memory...", "instance_id", instanceID)
		currentPages := session.memory.Size(store)
		neededPages := (uint64(len(snapshot)) + 65535) / 65536
		if neededPages > currentPages {
			growPages := neededPages - currentPages
			slog.Info("[ENGINE] Growing memory", "pages", growPages)
			_, err = session.memory.Grow(store, growPages)
			if err != nil {
				return false, fmt.Errorf("failed to grow memory for snapshot: %w", err)
			}
		}
		ptr := session.memory.Data(store)
		size := session.memory.DataSize(store)
		memoryBytes := unsafe.Slice((*byte)(ptr), size)
		copy(memoryBytes, snapshot)

		// Populate base hashes from full snapshot
		blockSize := 4096
		numBlocks := len(memoryBytes) / blockSize
		for i := 0; i < numBlocks; i++ {
			start := i * blockSize
			end := start + blockSize
			h := fnv.New64a()
			h.Write(memoryBytes[start:end])
			session.pageHashes[i] = h.Sum64()
		}
		slog.Info("[ENGINE] Memory successfully restored from full snapshot")
	}

	// 2. Load memory deltas if exists, and overlay them
	deltas, err := e.store.LoadDeltas(instanceID)
	if err == nil && len(deltas) > 0 {
		slog.Info("[ENGINE] Found saved memory deltas. Applying to memory...", "instance_id", instanceID)
		maxPageIndex := 0
		for idx := range deltas {
			if idx > maxPageIndex {
				maxPageIndex = idx
			}
		}
		neededSize := (maxPageIndex + 1) * 4096
		neededPages := (uint64(neededSize) + 65535) / 65536
		currentPages := session.memory.Size(store)
		if neededPages > currentPages {
			growPages := neededPages - currentPages
			slog.Info("[ENGINE] Growing memory for deltas", "pages", growPages)
			_, err = session.memory.Grow(store, growPages)
			if err != nil {
				return false, fmt.Errorf("failed to grow memory for deltas: %w", err)
			}
		}
		ptr := session.memory.Data(store)
		size := session.memory.DataSize(store)
		memoryBytes := unsafe.Slice((*byte)(ptr), size)

		for idx, data := range deltas {
			start := idx * 4096
			copy(memoryBytes[start:start+len(data)], data)
			h := fnv.New64a()
			h.Write(data)
			session.pageHashes[idx] = h.Sum64()
		}
		slog.Info("[ENGINE] Memory deltas successfully applied", "restored_pages", len(deltas))
	}

	// Locate entrypoint
	runFunc := instance.GetFunc(store, entrypoint)
	if runFunc == nil {
		return false, fmt.Errorf("entrypoint function '%s' not found", entrypoint)
	}

	slog.Info("[ENGINE] Invoking entrypoint", "entrypoint", entrypoint)
	result, err := runFunc.Call(store)
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if err != nil {
		if session.crashed {
			return true, err // True indicates a simulated crash occurred
		}
		if strings.Contains(err.Error(), "concurrent_execution_detected") {
			return false, ErrConcurrentExecution
		}
		return false, err
	}

	if result != nil {
		slog.Info("[ENGINE] Execution completed", "result", result)
	} else {
		slog.Info("[ENGINE] Execution completed successfully with no return value")
	}

	return false, nil
}
