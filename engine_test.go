package durable

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bytecodealliance/wasmtime-go/v20"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDurableExecutionLifecycle(t *testing.T) {
	instanceID := "test-worker-instance"
	serverAddr := "localhost:18081"

	// 2. Start mock HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello from durable test stream!"))
	})

	var receivedBytes []byte
	var uploadErr error
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		receivedBytes, uploadErr = io.ReadAll(r.Body)
		if uploadErr != nil {
			http.Error(w, uploadErr.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", serverAddr)
	require.NoError(t, err)

	go func() {
		_ = server.Serve(ln)
	}()
	defer server.Shutdown(context.Background())

	// Give the server a small moment to start
	time.Sleep(50 * time.Millisecond)

	// 3. Initialize engine
	wasmPath := filepath.Join("examples", "durable-s3", "worker", "worker.wasm")

	// Use an in-memory SQLite store for maximum speed and zero disk cleanup
	store, err := NewSqliteSnapshotStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err, "Failed to compile WASM module. Make sure worker.wasm is built.")

	// 4. RUN 1: Execute with simulated crash
	crashed, err := engine.Execute(instanceID, "run", serverAddr, true)
	require.Error(t, err)
	assert.True(t, crashed, "Expected run 1 to crash at checkpoint")

	// Verify snapshot exists in SQLite database
	snapshot, err := store.Load(instanceID)
	require.NoError(t, err, "Snapshot should exist in SQLite database")
	assert.NotEmpty(t, snapshot, "Snapshot data should not be empty")

	// 5. RUN 2: Restore from checkpoint and run to completion
	crashed, err = engine.Execute(instanceID, "run", serverAddr, false)
	require.NoError(t, err, "Run 2 should complete without errors")
	assert.False(t, crashed, "Run 2 should not crash")

	// 6. Verify processed output
	expectedOutput := "HELLO FROM DURABLE TEST STREAM!"
	assert.Equal(t, expectedOutput, string(receivedBytes), "Data processed by WASM worker should be converted to uppercase")
}

func TestDirtyPageAndOplog(t *testing.T) {
	instanceID := "test-dirty-oplog-instance"

	// Write simple WebAssembly Text (WAT) module to simulate Oplog and Dirty pages
	wat := `
	(module
	  (import "env" "checkpoint" (func $checkpoint))
	  (import "env" "host_call_api" (func $host_call_api (param i32 i32 i32 i32 i32 i32) (result i32)))
	  (memory (export "memory") 2)
	  (data (i32.const 0) "test_api")
	  (data (i32.const 16) "hello")
	  (data (i32.const 100) "world")
	  (func (export "run_test")
	    ;; Call test_api with payload "hello" -> outputs to offset 32
	    (call $host_call_api
	      (i32.const 0)   ;; apiNamePtr
	      (i32.const 8)   ;; apiNameLen
	      (i32.const 16)  ;; reqPtr
	      (i32.const 5)   ;; reqLen
	      (i32.const 32)  ;; respPtr
	      (i32.const 64)  ;; respMaxLen
	    )
	    drop

	    ;; First checkpoint (Crash point 1)
	    (call $checkpoint)

	    ;; Modify memory in the 2nd page (offset 70000) to trigger dirty-page tracking
	    (i32.store (i32.const 70000) (i32.const 42))

	    ;; Call test_api with payload "world" -> outputs to offset 200
	    (call $host_call_api
	      (i32.const 0)   ;; apiNamePtr
	      (i32.const 8)   ;; apiNameLen
	      (i32.const 100) ;; reqPtr
	      (i32.const 5)   ;; reqLen
	      (i32.const 200) ;; respPtr
	      (i32.const 64)  ;; respMaxLen
	    )
	    drop

	    ;; Second checkpoint
	    (call $checkpoint)
	  )
	)
	`
	wasmBytes, err := wasmtime.Wat2Wasm(wat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store, err := NewSqliteSnapshotStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// RUN 1: Run and crash on first checkpoint
	crashed, err := engine.Execute(instanceID, "run_test", "localhost:0", true)
	require.Error(t, err)
	assert.True(t, crashed)

	// Verify full snapshot and oplog saved on first checkpoint (Version = 1)
	snapshot, err := store.Load(instanceID)
	require.NoError(t, err)
	assert.NotEmpty(t, snapshot, "Full snapshot should be saved on version 1")

	oplog, err := store.LoadOplog(instanceID)
	require.NoError(t, err)
	require.Len(t, oplog, 1, "Should have exactly 1 oplog entry")
	assert.Equal(t, "test_api", oplog[0].ApiName)
	assert.Equal(t, "hello", string(oplog[0].RequestPayload))
	assert.Equal(t, "resp_for_hello_call_1", string(oplog[0].ResponsePayload))

	// RUN 2: Resume, should replay first api call without crash, modify page 2, and complete second checkpoint without crash
	crashed, err = engine.Execute(instanceID, "run_test", "localhost:0", false)
	require.NoError(t, err)
	assert.False(t, crashed)

	// Verify memory delta has dirty pages saved (Version = 2 check, since only deltas are written)
	deltas2, err := store.LoadDeltas(instanceID)
	require.NoError(t, err)
	// Block size is 4KB, offset 70000 lies in page index 70000/4096 = 17
	assert.Contains(t, deltas2, 17, "Delta snapshot must contain dirty page index 17 (offset 70000)")

	// Verify oplog contains 2 calls
	oplog2, err := store.LoadOplog(instanceID)
	require.NoError(t, err)
	assert.Len(t, oplog2, 2, "Oplog must contain 2 entries after complete run")
	assert.Equal(t, "world", string(oplog2[1].RequestPayload))
}

func TestPostgresSnapshotStore(t *testing.T) {
	// Try to connect to a local PostgreSQL instance (default credentials or env)
	connStr := os.Getenv("POSTGRES_CONN")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	// Ping PG to see if it is available
	db, err := net.DialTimeout("tcp", "localhost:5432", 1*time.Second)
	if err != nil {
		t.Skip("PostgreSQL is not running on localhost:5432. Skipping Postgres integration test.")
		return
	}
	db.Close()

	store, err := NewPostgresSnapshotStore(connStr)
	if err != nil {
		t.Skipf("PostgreSQL connection failed (credentials or DB might not be configured): %v. Skipping Postgres integration test.", err)
		return
	}
	defer store.Close()

	instanceID := "postgres-test-instance"
	defer store.Delete(instanceID)

	// Test basic save/load
	err = store.Save(instanceID, []byte("postgres-full-snapshot"))
	require.NoError(t, err)

	snapshot, err := store.Load(instanceID)
	require.NoError(t, err)
	assert.Equal(t, "postgres-full-snapshot", string(snapshot))

	// Test deltas
	deltas := map[int][]byte{
		0: []byte("page-0-data"),
		5: []byte("page-5-data"),
	}
	err = store.SaveDeltas(instanceID, deltas)
	require.NoError(t, err)

	loadedDeltas, err := store.LoadDeltas(instanceID)
	require.NoError(t, err)
	assert.Len(t, loadedDeltas, 2)
	assert.Equal(t, "page-0-data", string(loadedDeltas[0]))
	assert.Equal(t, "page-5-data", string(loadedDeltas[5]))

	// Test oplog
	err = store.SaveOplog(instanceID, 1, "test_call", []byte("req"), []byte("resp"))
	require.NoError(t, err)

	oplog, err := store.LoadOplog(instanceID)
	require.NoError(t, err)
	require.Len(t, oplog, 1)
	assert.Equal(t, 1, oplog[0].CallIndex)
	assert.Equal(t, "test_call", oplog[0].ApiName)
	assert.Equal(t, "req", string(oplog[0].RequestPayload))
	assert.Equal(t, "resp", string(oplog[0].ResponsePayload))
}

func TestHostGetTime(t *testing.T) {
	instanceID := "test-time-instance"

	wat := `
	(module
	  (import "env" "host_get_time" (func $host_get_time (result i64)))
	  (import "env" "checkpoint" (func $checkpoint))
	  (memory (export "memory") 1)
	  (func (export "run_test")
	    ;; Call time 1
	    (i64.store (i32.const 0) (call $host_get_time))

	    ;; First checkpoint
	    (call $checkpoint)

	    ;; Call time 2
	    (i64.store (i32.const 8) (call $host_get_time))

	    ;; Second checkpoint
	    (call $checkpoint)
	  )
	)
	`
	wasmBytes, err := wasmtime.Wat2Wasm(wat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store, err := NewSqliteSnapshotStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// RUN 1: Run and crash on first checkpoint
	crashed, err := engine.Execute(instanceID, "run_test", "localhost:0", true)
	require.Error(t, err)
	assert.True(t, crashed)

	// Recover oplog
	oplog1, err := store.LoadOplog(instanceID)
	require.NoError(t, err)
	require.Len(t, oplog1, 1)
	assert.Equal(t, "host_get_time", oplog1[0].ApiName)

	timeVal1, err := strconv.ParseInt(string(oplog1[0].ResponsePayload), 10, 64)
	require.NoError(t, err)
	assert.True(t, timeVal1 > 0)

	// Give a small delay to make sure system time actually advances
	time.Sleep(10 * time.Millisecond)

	// RUN 2: Resume, it should replay time 1 from Oplog (same value) and record time 2 (new value)
	crashed, err = engine.Execute(instanceID, "run_test", "localhost:0", false)
	require.NoError(t, err)
	assert.False(t, crashed)

	oplog2, err := store.LoadOplog(instanceID)
	require.NoError(t, err)
	require.Len(t, oplog2, 2)

	timeValReplayed, err := strconv.ParseInt(string(oplog2[0].ResponsePayload), 10, 64)
	require.NoError(t, err)
	assert.Equal(t, timeVal1, timeValReplayed, "Replayed time must be identical to the original time")

	timeVal2, err := strconv.ParseInt(string(oplog2[1].ResponsePayload), 10, 64)
	require.NoError(t, err)
	assert.True(t, timeVal2 > timeVal1, "New time call must be greater than the replayed time")
}

func TestMultiCheckpointRecovery(t *testing.T) {
	instanceID := "test-multi-checkpoint-instance"

	wat := `
	(module
	  (import "env" "checkpoint" (func $checkpoint))
	  (memory (export "memory") 1)
	  (func (export "run_test")
	    (local $val i32)
	    ;; Read value from offset 0
	    (local.set $val (i32.load (i32.const 0)))

	    ;; If val == 0 (First execution)
	    (if (i32.eq (local.get $val) (i32.const 0))
	      (then
	        (i32.store (i32.const 0) (i32.const 10))
	        (call $checkpoint)
	      )
	    )

	    ;; If val == 10
	    (if (i32.eq (local.get $val) (i32.const 10))
	      (then
	        (i32.store (i32.const 0) (i32.const 20))
	        (call $checkpoint)
	      )
	    )

	    ;; If val == 20
	    (if (i32.eq (local.get $val) (i32.const 20))
	      (then
	        (i32.store (i32.const 0) (i32.const 30))
	        (call $checkpoint)
	      )
	    )

	    ;; If val == 30
	    (if (i32.eq (local.get $val) (i32.const 30))
	      (then
	        (i32.store (i32.const 0) (i32.const 40))
	        (call $checkpoint)
	      )
	    )

	    ;; If val == 40
	    (if (i32.eq (local.get $val) (i32.const 40))
	      (then
	        (i32.store (i32.const 0) (i32.const 50))
	        (call $checkpoint)
	      )
	    )
	  )
	)
	`
	wasmBytes, err := wasmtime.Wat2Wasm(wat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store, err := NewSqliteSnapshotStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// We will run and crash on checkpoints 1, 2, 3, 4 sequentially, verifying version increment
	for expectedVal := 10; expectedVal <= 50; expectedVal += 10 {
		shouldCrash := expectedVal < 50
		crashed, err := engine.Execute(instanceID, "run_test", "localhost:0", shouldCrash)
		if shouldCrash {
			require.Error(t, err)
			assert.True(t, crashed)
		} else {
			require.NoError(t, err)
			assert.False(t, crashed)
		}

		// Verify state of memory (should be expectedVal)
		meta, err := store.LoadMetadata(instanceID)
		require.NoError(t, err)
		assert.Equal(t, expectedVal/10, meta.Version)

		deltas, err := store.LoadDeltas(instanceID)
		require.NoError(t, err)

		snapshot, err := store.Load(instanceID)
		
		val := int32(0)
		if len(deltas) > 0 && len(deltas[0]) >= 4 {
			val = int32(deltas[0][0]) | int32(deltas[0][1])<<8 | int32(deltas[0][2])<<16 | int32(deltas[0][3])<<24
		} else if len(snapshot) >= 4 {
			val = int32(snapshot[0]) | int32(snapshot[1])<<8 | int32(snapshot[2])<<16 | int32(snapshot[3])<<24
		}
		assert.Equal(t, int32(expectedVal), val, "Memory at offset 0 should contain expected progress value")
	}
}

func TestWasmModuleHashMismatch(t *testing.T) {
	instanceID := "test-hash-mismatch-instance"

	wat1 := `
	(module
	  (import "env" "checkpoint" (func $checkpoint))
	  (memory (export "memory") 1)
	  (func (export "run_test")
	    (i32.store (i32.const 0) (i32.const 100))
	    (call $checkpoint)
	  )
	)
	`
	wat2 := `
	(module
	  (import "env" "checkpoint" (func $checkpoint))
	  (memory (export "memory") 1)
	  (func (export "run_test")
	    (i32.store (i32.const 0) (i32.const 200))
	    (call $checkpoint)
	  )
	)
	`

	tempDir := t.TempDir()
	wasmPath1 := filepath.Join(tempDir, "test1.wasm")
	wasmPath2 := filepath.Join(tempDir, "test2.wasm")

	wasmBytes1, err := wasmtime.Wat2Wasm(wat1)
	require.NoError(t, err)
	err = os.WriteFile(wasmPath1, wasmBytes1, 0644)
	require.NoError(t, err)

	wasmBytes2, err := wasmtime.Wat2Wasm(wat2)
	require.NoError(t, err)
	err = os.WriteFile(wasmPath2, wasmBytes2, 0644)
	require.NoError(t, err)

	store, err := NewSqliteSnapshotStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	// 1. Run with module 1
	engine1, err := NewEngine(wasmPath1, store)
	require.NoError(t, err)

	crashed, err := engine1.Execute(instanceID, "run_test", "localhost:0", true)
	require.Error(t, err)
	assert.True(t, crashed)

	// 2. Manually alter the saved metadata in SQLite to point to a non-existent WASM hash
	meta, err := store.LoadMetadata(instanceID)
	require.NoError(t, err)
	meta.WasmHash = "non-existent-wasm-hash"
	
	// Bypass OCC for test setup by updating metadata directly in DB
	_, queryErr := store.db.Exec("UPDATE instance_meta SET wasm_hash = ? WHERE instance_id = ?;", meta.WasmHash, instanceID)
	require.NoError(t, queryErr)

	// 3. Try to run with module 2 -> should return ErrWasmVersionMismatch because the required hash is not in the registry
	engine2, err := NewEngine(wasmPath2, store)
	require.NoError(t, err)

	_, err = engine2.Execute(instanceID, "run_test", "localhost:0", false)
	assert.ErrorIs(t, err, ErrWasmVersionMismatch)
}

func TestConcurrentExecution(t *testing.T) {
	instanceID := "test-concurrent-instance"
	serverAddr := "localhost:18084"

	wat := `
	(module
	  (import "env" "host_call_api" (func $host_call_api (param i32 i32 i32 i32 i32 i32) (result i32)))
	  (import "env" "checkpoint" (func $checkpoint))
	  (memory (export "memory") 1)
	  (data (i32.const 0) "trigger_race")
	  (func (export "run_test")
	    (call $checkpoint) ;; Checkpoint 1

	    ;; Call trigger_race API to increment version in DB behind our back
	    (call $host_call_api (i32.const 0) (i32.const 12) (i32.const 0) (i32.const 0) (i32.const 100) (i32.const 10))
	    drop

	    (call $checkpoint) ;; Checkpoint 2 (Should fail due to OCC)
	  )
	)
	`
	wasmBytes, err := wasmtime.Wat2Wasm(wat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store, err := NewSqliteSnapshotStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// Set up local server for API calls
	mux := http.NewServeMux()
	mux.HandleFunc("/trigger_race", func(w http.ResponseWriter, r *http.Request) {
		// Increment version in DB to simulate another process taking over (split-brain)
		// We execute a direct SQL query to bypass CAS checks
		_, queryErr := store.db.Exec("UPDATE instance_meta SET version = 10 WHERE instance_id = ?;", instanceID)
		require.NoError(t, queryErr)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", serverAddr)
	require.NoError(t, err)

	go func() {
		_ = server.Serve(ln)
	}()
	defer server.Shutdown(context.Background())

	time.Sleep(50 * time.Millisecond)

	// 1. First run, crash at 1st checkpoint (version becomes 1 in db)
	crashed, err := engine.Execute(instanceID, "run_test", serverAddr, true)
	require.Error(t, err)
	assert.True(t, crashed)

	// 2. Try to resume. It will restore memory, call trigger_race (which pushes version to 11 in DB),
	// and then attempt checkpoint 2. Local version is still 1, but DB is 11, so it must abort with ErrConcurrentExecution.
	_, err = engine.Execute(instanceID, "run_test", serverAddr, false)
	assert.ErrorIs(t, err, ErrConcurrentExecution)
}

func TestOplogTruncation(t *testing.T) {
	instanceID := "test-truncation-instance"

	wat := `
	(module
	  (import "env" "host_call_api" (func $host_call_api (param i32 i32 i32 i32 i32 i32) (result i32)))
	  (import "env" "checkpoint" (func $checkpoint))
	  (memory (export "memory") 1)
	  (data (i32.const 0) "test_api")
	  (data (i32.const 16) "hello")
	  (func (export "run_test")
	    (local $val i32)
	    (local.set $val (i32.load (i32.const 200)))

	    ;; Call API 1
	    (call $host_call_api (i32.const 0) (i32.const 8) (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 64))
	    drop
	    ;; If val == 0
	    (if (i32.eq (local.get $val) (i32.const 0))
	      (then
	        (i32.store (i32.const 200) (i32.const 10))
	        (call $checkpoint)
	      )
	    )

	    ;; Call API 2
	    (call $host_call_api (i32.const 0) (i32.const 8) (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 64))
	    drop
	    ;; If val == 10
	    (if (i32.eq (local.get $val) (i32.const 10))
	      (then
	        (i32.store (i32.const 200) (i32.const 20))
	        (call $checkpoint)
	      )
	    )

	    ;; Call API 3
	    (call $host_call_api (i32.const 0) (i32.const 8) (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 64))
	    drop
	    ;; If val == 20
	    (if (i32.eq (local.get $val) (i32.const 20))
	      (then
	        (i32.store (i32.const 200) (i32.const 30))
	        (call $checkpoint)
	      )
	    )

	    ;; Call API 4
	    (call $host_call_api (i32.const 0) (i32.const 8) (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 64))
	    drop
	    ;; If val == 30
	    (if (i32.eq (local.get $val) (i32.const 30))
	      (then
	        (i32.store (i32.const 200) (i32.const 40))
	        (call $checkpoint)
	      )
	    )

	    ;; Call API 5
	    (call $host_call_api (i32.const 0) (i32.const 8) (i32.const 16) (i32.const 5) (i32.const 32) (i32.const 64))
	    drop
	    ;; If val == 40
	    (if (i32.eq (local.get $val) (i32.const 40))
	      (then
	        (i32.store (i32.const 200) (i32.const 50))
	        (call $checkpoint)
	      )
	    )
	  )
	)
	`
	wasmBytes, err := wasmtime.Wat2Wasm(wat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store, err := NewSqliteSnapshotStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// Run up to checkpoint 4 (version 4)
	var crashed bool
	for i := 0; i < 4; i++ {
		var execErr error
		crashed, execErr = engine.Execute(instanceID, "run_test", "localhost:0", true)
		require.Error(t, execErr)
		assert.True(t, crashed)
	}

	// Verify oplog has 4 entries
	oplog, err := store.LoadOplog(instanceID)
	require.NoError(t, err)
	assert.Len(t, oplog, 4)

	// Run again and crash on checkpoint 5 (version 5, which triggers full snapshot and truncation, then crashes)
	crashed, err = engine.Execute(instanceID, "run_test", "localhost:0", true)
	require.Error(t, err)
	assert.True(t, crashed)

	// Verify oplog was truncated! It should have 0 entries now (since all 5 calls occurred before or at checkpoint 5)
	oplogTruncated, err := store.LoadOplog(instanceID)
	require.NoError(t, err)
	assert.Empty(t, oplogTruncated, "Oplog must be empty after truncation at checkpoint 5")

	// Verify deltas table is empty
	deltas, err := store.LoadDeltas(instanceID)
	require.NoError(t, err)
	assert.Empty(t, deltas, "Deltas must be empty after truncation")

	// Verify full snapshot still exists in db
	snapshot, err := store.Load(instanceID)
	require.NoError(t, err)
	assert.NotEmpty(t, snapshot, "Full snapshot must exist")
}

func TestMultiVersionWasmExecution(t *testing.T) {
	instanceID := "test-multi-version-instance"

	wat1 := `
	(module
	  (import "env" "checkpoint" (func $checkpoint))
	  (memory (export "memory") 1)
	  (func (export "run_test")
	    (i32.store (i32.const 0) (i32.const 777))
	    (call $checkpoint)
	    (i32.store (i32.const 0) (i32.const 888))
	    (call $checkpoint)
	  )
	)
	`
	wat2 := `
	(module
	  (import "env" "checkpoint" (func $checkpoint))
	  (memory (export "memory") 1)
	  (func (export "run_test")
	    (i32.store (i32.const 0) (i32.const 111))
	    (call $checkpoint)
	    (i32.store (i32.const 0) (i32.const 222))
	    (call $checkpoint)
	  )
	)
	`

	tempDir := t.TempDir()
	wasmPath1 := filepath.Join(tempDir, "test1.wasm")
	wasmPath2 := filepath.Join(tempDir, "test2.wasm")

	wasmBytes1, err := wasmtime.Wat2Wasm(wat1)
	require.NoError(t, err)
	err = os.WriteFile(wasmPath1, wasmBytes1, 0644)
	require.NoError(t, err)

	wasmBytes2, err := wasmtime.Wat2Wasm(wat2)
	require.NoError(t, err)
	err = os.WriteFile(wasmPath2, wasmBytes2, 0644)
	require.NoError(t, err)

	store, err := NewSqliteSnapshotStore(":memory:")
	require.NoError(t, err)
	defer store.Close()

	// 1. Initialize engine 1 (wat1) and crash
	engine1, err := NewEngine(wasmPath1, store)
	require.NoError(t, err)

	crashed, err := engine1.Execute(instanceID, "run_test", "localhost:0", true)
	require.Error(t, err)
	assert.True(t, crashed)

	// 2. Initialize engine 2 (wat2, new code version) and resume.
	// It should transparently compile and run wat1 module, loading it from store.
	engine2, err := NewEngine(wasmPath2, store)
	require.NoError(t, err)

	crashed, err = engine2.Execute(instanceID, "run_test", "localhost:0", false)
	require.NoError(t, err)
	assert.False(t, crashed)

	// 3. Verify that the memory actually reflects wat1's execution (val == 888, not 222)
	// We check deltas because checkpoint 2 (Version=2) was executed and recorded delta for page 0
	deltas, err := store.LoadDeltas(instanceID)
	require.NoError(t, err)

	val := int32(0)
	if len(deltas) > 0 && len(deltas[0]) >= 4 {
		val = int32(deltas[0][0]) | int32(deltas[0][1])<<8 | int32(deltas[0][2])<<16 | int32(deltas[0][3])<<24
	}
	assert.Equal(t, int32(888), val, "Should execute wat1 code and write 888")
}
