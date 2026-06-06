//go:build !wasm

package wasman

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
)

type contextKey struct{}

func WithSession(ctx context.Context, s *Session) context.Context {
	return context.WithValue(ctx, contextKey{}, s)
}

func GetSession(ctx context.Context) *Session {
	if val := ctx.Value(contextKey{}); val != nil {
		return val.(*Session)
	}
	return nil
}

// NewEngine creates a new reusable WASM Durable Execution Engine from a WASM module file path.
func NewEngine(wasmPath string, store SnapshotStore, opts ...EngineOption) (*Engine, error) {
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read WASM module file: %w", err)
	}
	return NewEngineWithBytes(wasmBytes, store, opts...)
}

// NewEngineWithBytes creates a new reusable WASM Durable Execution Engine directly from in-memory WASM bytes.
func NewEngineWithBytes(wasmBytes []byte, store SnapshotStore, opts ...EngineOption) (*Engine, error) {
	hash := sha256.Sum256(wasmBytes)
	wasmHash := hex.EncodeToString(hash[:])

	// Save WASM module in registry for future multi-version execution
	err := store.SaveWasm(wasmHash, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to save WASM module in registry: %w", err)
	}

	ctx := context.Background()
	runtime := wazero.NewRuntime(ctx)

	compiled, err := runtime.CompileModule(ctx, wasmBytes)
	if err != nil {
		runtime.Close(ctx)
		return nil, fmt.Errorf("failed to compile WASM module: %w", err)
	}

	// Instantiate WASI imports.
	// We override proc_exit to NOT call CloseWithExitCode, because the default
	// implementation sets Sys=nil on the module, making it unusable for subsequent
	// exported function calls. TinyGo-compiled modules call proc_exit(0) at the end
	// of _start, but we still need the module alive to call 'run' or 'run_test' afterwards.
	wasiBuilder := runtime.NewHostModuleBuilder("wasi_snapshot_preview1")
	wasi_snapshot_preview1.NewFunctionExporter().ExportFunctions(wasiBuilder)
	wasiBuilder.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, mod api.Module, exitCode uint32) {
			// Only interrupt execution, do NOT close the module.
			// This preserves mod.Sys (file descriptors, stdout/stderr) for subsequent calls.
			panic(sys.NewExitError(exitCode))
		}).
		Export("proc_exit")
	_, err = wasiBuilder.Instantiate(ctx)
	if err != nil {
		runtime.Close(ctx)
		return nil, fmt.Errorf("failed to instantiate WASI module: %w", err)
	}

	// Register Host Module: env
	_, err = runtime.NewHostModuleBuilder("env").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module) {
			s := GetSession(ctx)
			if s == nil {
				panic("no session in context")
			}
			slog.Info("[ENGINE] 'checkpoint' invoked", "instance_id", s.instanceID)

			// 1. Compare-And-Swap version metadata (OCC)
			s.meta.WasmHash = s.engine.wasmHash
			ok, err := s.engine.store.SaveMetadata(s.meta)
			if err != nil {
				slog.Error("[ENGINE] Failed to save metadata", "error", err)
				panic("failed to save metadata")
			}
			if !ok {
				slog.Warn("[ENGINE] OCC conflict detected. Aborting execution.")
				panic("concurrent_execution_detected")
			}

			mem := m.Memory()
			s.memory = mem

			size := mem.Size()
			if size == 0 {
				panic("memory data size is zero")
			}
			memoryBytes, ok := mem.Read(0, size)
			if !ok {
				panic("failed to read memory")
			}

			// 2. Snapshotting strategy (Full vs Deltas) & Truncation
			isFullSnapshot := s.meta.Version == 1 || (s.meta.Version > 1 && s.meta.Version%5 == 0)

			if isFullSnapshot {
				slog.Info("[ENGINE] Writing Full Memory Snapshot", "version", s.meta.Version)
				snapshotCopy := make([]byte, len(memoryBytes))
				copy(snapshotCopy, memoryBytes)
				err := s.engine.store.Save(s.instanceID, snapshotCopy)
				if err != nil {
					slog.Error("[ENGINE] Failed to save full snapshot", "error", err)
					panic("failed to write snapshot")
				}

				if s.meta.Version > 1 {
					slog.Info("[ENGINE] Truncating Oplog and memory deltas", "before_call_index", s.callIndex)
					_ = s.engine.store.TruncateOplog(s.instanceID, s.callIndex)
					_ = s.engine.store.TruncateDeltas(s.instanceID)
				}
			} else {
				// Incremental checkpoint (Dirty-Page deltas)
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

					prevHash, exists := s.pageHashes[i]
					if !exists || prevHash != hashVal {
						blockCopy := make([]byte, len(blockData))
						copy(blockCopy, blockData)
						deltas[i] = blockCopy
						s.pageHashes[i] = hashVal
					}
				}

				if len(deltas) > 0 {
					err = s.engine.store.SaveDeltas(s.instanceID, deltas)
					if err != nil {
						slog.Error("[ENGINE] Failed to save memory deltas", "error", err)
						panic("failed to write memory deltas")
					}
					slog.Info("[ENGINE] Memory deltas successfully saved", "dirty_blocks", len(deltas))
				}
			}

			if s.shouldCrashOnCheckpoint {
				s.crashed = true
				slog.Warn("[ENGINE] Simulating host crash. Aborting WASM execution.")
				panic("simulated_host_crash")
			}
		}).
		Export("checkpoint").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module) int64 {
			s := GetSession(ctx)
			if s == nil {
				panic("no session in context")
			}
			s.callIndex++
			callIdx := s.callIndex

			// Check Oplog Replay
			oplog, err := s.engine.store.LoadOplog(s.instanceID)
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
			err = s.engine.store.SaveOplog(s.instanceID, callIdx, "host_get_time", nil, payload)
			if err != nil {
				slog.Error("[ENGINE] Failed to save Oplog for host_get_time", "error", err)
			}
			return nowNano
		}).
		Export("host_get_time").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, apiNamePtr, apiNameLen, reqPtr, reqLen, respPtr, respMaxLen int32) int32 {
			s := GetSession(ctx)
			if s == nil {
				panic("no session in context")
			}
			mem := m.Memory()
			s.memory = mem

			memoryBytes, ok := mem.Read(0, mem.Size())
			if !ok {
				slog.Error("[ENGINE] host_call_api: failed to read memory")
				return -1
			}

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

			s.callIndex++
			callIdx := s.callIndex

			// Check Oplog Replay
			oplog, err := s.engine.store.LoadOplog(s.instanceID)
			if err == nil {
				for _, entry := range oplog {
					if entry.CallIndex == callIdx && entry.ApiName == apiName {
						slog.Info("[ENGINE] Oplog Replay", "api", apiName, "call_index", callIdx)
						if int(respMaxLen) < len(entry.ResponsePayload) {
							slog.Error("[ENGINE] Oplog Replay: response buffer too small")
							return -2
						}
						mem.Write(uint32(respPtr), entry.ResponsePayload)
						return int32(len(entry.ResponsePayload))
					}
				}
			}

			// Perform real operation
			slog.Info("[ENGINE] Oplog Execution: invoking real API call", "api", apiName, "call_index", callIdx)
			var response []byte

			if apiName == "test_api" {
				time.Sleep(10 * time.Millisecond)
				response = []byte(fmt.Sprintf("resp_for_%s_call_%d", string(request), callIdx))
			} else {
				url := fmt.Sprintf("http://%s/%s", s.serverAddr, apiName)
				req, err := http.NewRequestWithContext(s.ctx, "POST", url, strings.NewReader(string(request)))
				if err != nil {
					slog.Error("[ENGINE] Failed to create HTTP request", "url", url, "error", err)
					return -1
				}
				req.Header.Set("Content-Type", "application/octet-stream")
				resp, err := s.engine.httpClient.Do(req)
				if err != nil {
					slog.Error("[ENGINE] Real API call failed", "url", url, "error", err)
					return -1
				}
				defer resp.Body.Close()
				response, _ = io.ReadAll(resp.Body)
			}

			// Save response to Oplog
			err = s.engine.store.SaveOplog(s.instanceID, callIdx, apiName, request, response)
			if err != nil {
				slog.Error("[ENGINE] Failed to save Oplog", "error", err)
				return -1
			}

			if int(respMaxLen) < len(response) {
				slog.Error("[ENGINE] Response buffer too small for live response")
				return -2
			}
			mem.Write(uint32(respPtr), response)
			return int32(len(response))
		}).
		Export("host_call_api").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, direction, ptr, length int32) int32 {
			s := GetSession(ctx)
			if s == nil {
				panic("no session in context")
			}
			s.memory = m.Memory()

			switch direction {
			case 0:
				return s.handleDownload(ptr, length)
			case 1:
				return s.handleUpload(ptr, length)
			}

			slog.Error("[ENGINE] stream_data: invalid direction", "direction", direction)
			return -1
		}).
		Export("stream_data").
		Instantiate(ctx)

	if err != nil {
		runtime.Close(ctx)
		return nil, fmt.Errorf("failed to instantiate host module 'env': %w", err)
	}

	engine := &Engine{
		runtime:       runtime,
		compiled:      compiled,
		store:         store,
		wasmHash:      wasmHash,
		httpClient:    defaultHTTPClient,
		compiledCache: make(map[string]wazero.CompiledModule),
	}
	engine.compiledCache[wasmHash] = compiled

	for _, opt := range opts {
		opt(engine)
	}

	return engine, nil
}

// Store returns the SnapshotStore associated with the Engine.
func (e *Engine) Store() SnapshotStore {
	return e.store
}

// Execute runs the WASM instance with a given entrypoint and session context.
// If it finds a saved snapshot, it automatically restores the linear memory.
func (e *Engine) Execute(ctx context.Context, instanceID string, entrypoint string, serverAddr string, shouldCrash bool) (bool, error) {
	// Load or initialize metadata (WASM Module Versioning & OCC)
	meta, err := e.store.LoadMetadata(instanceID)
	if err != nil {
		return false, fmt.Errorf("failed to load metadata: %w", err)
	}

	var runModule wazero.CompiledModule
	var errCompile error

	if meta != nil {
		if meta.WasmHash != e.wasmHash {
			e.cacheMu.RLock()
			cachedModule, ok := e.compiledCache[meta.WasmHash]
			e.cacheMu.RUnlock()
			if ok {
				runModule = cachedModule
			} else {
				slog.Info("[ENGINE] Instance requires a different WASM module version", "instance_id", instanceID, "required_hash", meta.WasmHash, "current_hash", e.wasmHash)
				loadedBytes, err := e.store.LoadWasm(meta.WasmHash)
				if err != nil {
					return false, fmt.Errorf("failed to load required WASM version %s from registry: %w: %w", meta.WasmHash, err, ErrWasmVersionMismatch)
				}

				e.cacheMu.Lock()
				if cachedModule, ok = e.compiledCache[meta.WasmHash]; ok {
					runModule = cachedModule
				} else {
					runModule, errCompile = e.runtime.CompileModule(ctx, loadedBytes)
					if errCompile != nil {
						e.cacheMu.Unlock()
						return false, fmt.Errorf("failed to compile historical WASM module %s: %w", meta.WasmHash, errCompile)
					}
					e.compiledCache[meta.WasmHash] = runModule
				}
				e.cacheMu.Unlock()
			}
		} else {
			runModule = e.compiled
		}
	} else {
		meta = &InstanceMeta{
			InstanceID: instanceID,
			WasmHash:   e.wasmHash,
			Version:    0,
		}
		runModule = e.compiled
	}

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
	}()

	// Bind session to context
	executeCtx := WithSession(ctx, session)

	// Instantiate the module config with a unique name to allow concurrent executions.
	// We disable automatic _start to prevent wazero from closing the module when
	// TinyGo's _start calls proc_exit(0). This keeps mod.Sys alive for subsequent calls.
	config := wazero.NewModuleConfig().
		WithName(fmt.Sprintf("main-%s", instanceID)).
		WithStdout(os.Stdout).
		WithStderr(os.Stderr).
		WithStartFunctions() // Empty: disable automatic _start call

	// Instantiate the compiled WASM module (without calling _start)
	mod, err := e.runtime.InstantiateModule(executeCtx, runModule, config)
	if err != nil {
		slog.Error("[ENGINE] InstantiateModule failed", "error", err)
		return false, fmt.Errorf("failed to instantiate WASM: %w", err)
	}
	defer mod.Close(executeCtx)

	// Manually call _start to initialize the TinyGo runtime.
	// TinyGo's _start runs package init() + main(), then calls proc_exit(0).
	// Our custom proc_exit only panics without closing the module, so Sys stays alive.
	if startFunc := mod.ExportedFunction("_start"); startFunc != nil {
		if _, err := startFunc.Call(executeCtx); err != nil {
			var exitErr *sys.ExitError
			if errors.As(err, &exitErr) && exitErr.ExitCode() == 0 {
				// Expected: TinyGo _start exits with code 0 after initialization.
				// Module is still alive because our proc_exit doesn't close it.
			} else {
				return false, fmt.Errorf("_start failed: %w", err)
			}
		}
	}

	session.mod = mod
	session.memory = mod.Memory()

	// RESTORE: Check if there is an existing snapshot to restore
	session.pageHashes = make(map[int]uint64)

	// 1. Load full snapshot if exists
	snapshot, err := e.store.Load(instanceID)
	if err == nil && len(snapshot) > 0 {
		slog.Info("[ENGINE] Found saved full snapshot. Restoring memory...", "instance_id", instanceID)
		currentPages, _ := session.memory.Grow(0)
		neededPages := (uint64(len(snapshot)) + 65535) / 65536
		if uint32(neededPages) > currentPages {
			growPages := uint32(neededPages) - currentPages
			slog.Info("[ENGINE] Growing memory", "pages", growPages)
			_, ok := session.memory.Grow(growPages)
			if !ok {
				return false, fmt.Errorf("failed to grow memory for snapshot")
			}
		}

		size := session.memory.Size()
		memoryBytes, ok := session.memory.Read(0, size)
		if !ok {
			return false, fmt.Errorf("failed to read memory for snapshot restoration")
		}
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
		currentPages, _ := session.memory.Grow(0)
		if uint32(neededPages) > currentPages {
			growPages := uint32(neededPages) - currentPages
			slog.Info("[ENGINE] Growing memory for deltas", "pages", growPages)
			_, ok := session.memory.Grow(growPages)
			if !ok {
				return false, fmt.Errorf("failed to grow memory for deltas")
			}
		}

		size := session.memory.Size()
		memoryBytes, ok := session.memory.Read(0, size)
		if !ok {
			return false, fmt.Errorf("failed to read memory for deltas restoration")
		}

		for idx, data := range deltas {
			start := idx * 4096
			copy(memoryBytes[start:start+len(data)], data)
			h := fnv.New64a()
			h.Write(data)
			session.pageHashes[idx] = h.Sum64()
		}
		slog.Info("[ENGINE] Memory deltas successfully applied", "restored_pages", len(deltas))
	}

	runFunc := mod.ExportedFunction(entrypoint)
	if runFunc == nil {
		// Fallback to _start if the requested entrypoint is "run" but it is not exported (typical of standard Go main tasks)
		if entrypoint == "run" {
			runFunc = mod.ExportedFunction("_start")
			if runFunc != nil {
				entrypoint = "_start"
			}
		}
		if runFunc == nil {
			return false, fmt.Errorf("entrypoint function '%s' not found", entrypoint)
		}
	}

	if entrypoint == "_start" {
		slog.Info("[ENGINE] Entrypoint is '_start' which was executed during instantiation. Skipping redundant Call.")
		return false, nil
	}

	slog.Info("[ENGINE] Invoking entrypoint", "entrypoint", entrypoint)
	result, err := runFunc.Call(executeCtx)
	if executeCtx.Err() != nil {
		return false, executeCtx.Err()
	}
	if err != nil {
		var exitErr *sys.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 0 {
			return false, nil
		}
		if session.crashed {
			return true, err // True indicates a simulated crash occurred
		}
		if strings.Contains(err.Error(), "concurrent_execution_detected") {
			return false, ErrConcurrentExecution
		}
		slog.Error("[ENGINE] runFunc.Call failed", "error", err, "crashed", session.crashed)
		return false, err
	}

	if len(result) > 0 {
		slog.Info("[ENGINE] Execution completed", "result", result)
	} else {
		slog.Info("[ENGINE] Execution completed successfully with no return value")
	}

	return false, nil
}
