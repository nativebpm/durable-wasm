package durable

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bytecodealliance/wasmtime-go/v20"
	"github.com/google/uuid"
	"github.com/nativebpm/durable-wasm/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type inMemorySnapshotStore struct {
	mu        sync.RWMutex
	snapshots map[string][]byte
	deltas    map[string]map[int][]byte
	oplogs    map[string][]OplogEntry
	meta      map[string]*InstanceMeta
	wasm      map[string][]byte
}

func newInMemorySnapshotStore() *inMemorySnapshotStore {
	return &inMemorySnapshotStore{
		snapshots: make(map[string][]byte),
		deltas:    make(map[string]map[int][]byte),
		oplogs:    make(map[string][]OplogEntry),
		meta:      make(map[string]*InstanceMeta),
		wasm:      make(map[string][]byte),
	}
}

func (s *inMemorySnapshotStore) Save(id string, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]byte, len(snapshot))
	copy(copied, snapshot)
	s.snapshots[id] = copied
	return nil
}

func (s *inMemorySnapshotStore) Load(id string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap, ok := s.snapshots[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return snap, nil
}

func (s *inMemorySnapshotStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.snapshots, id)
	delete(s.deltas, id)
	delete(s.oplogs, id)
	delete(s.meta, id)
	return nil
}

func (s *inMemorySnapshotStore) SaveDeltas(id string, deltas map[int][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.deltas[id]
	if !ok {
		current = make(map[int][]byte)
		s.deltas[id] = current
	}
	for k, v := range deltas {
		copiedVal := make([]byte, len(v))
		copy(copiedVal, v)
		current[k] = copiedVal
	}
	return nil
}

func (s *inMemorySnapshotStore) LoadDeltas(id string) (map[int][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	current, ok := s.deltas[id]
	if !ok {
		return nil, nil
	}
	copied := make(map[int][]byte)
	for k, v := range current {
		copied[k] = v
	}
	return copied, nil
}

func (s *inMemorySnapshotStore) TruncateDeltas(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.deltas, id)
	return nil
}

func (s *inMemorySnapshotStore) SaveOplog(id string, callIndex int, apiName string, request []byte, response []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	reqCopied := make([]byte, len(request))
	copy(reqCopied, request)
	respCopied := make([]byte, len(response))
	copy(respCopied, response)

	s.oplogs[id] = append(s.oplogs[id], OplogEntry{
		CallIndex:       callIndex,
		ApiName:         apiName,
		RequestPayload:  reqCopied,
		ResponsePayload: respCopied,
	})
	return nil
}

func (s *inMemorySnapshotStore) LoadOplog(id string) ([]OplogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, ok := s.oplogs[id]
	if !ok {
		return nil, nil
	}
	return list, nil
}

func (s *inMemorySnapshotStore) TruncateOplog(id string, beforeCallIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.oplogs[id]
	var filtered []OplogEntry
	for _, entry := range list {
		if entry.CallIndex > beforeCallIndex {
			filtered = append(filtered, entry)
		}
	}
	s.oplogs[id] = filtered
	return nil
}

func (s *inMemorySnapshotStore) SaveMetadata(meta *InstanceMeta) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.meta[meta.InstanceID]
	if ok {
		if meta.Version == 0 {
			return false, nil
		}
		if existing.Version != meta.Version {
			return false, nil
		}
	} else if meta.Version > 0 {
		return false, nil
	}

	meta.Version++
	copied := *meta
	s.meta[meta.InstanceID] = &copied
	return true, nil
}

func (s *inMemorySnapshotStore) LoadMetadata(id string) (*InstanceMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.meta[id]
	if !ok {
		return nil, nil
	}
	copied := *meta
	return &copied, nil
}

func (s *inMemorySnapshotStore) SaveWasm(hash string, wasmBytes []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]byte, len(wasmBytes))
	copy(copied, wasmBytes)
	s.wasm[hash] = copied
	return nil
}

func (s *inMemorySnapshotStore) LoadWasm(hash string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.wasm[hash]
	if !ok {
		return nil, errors.New("not found")
	}
	return w, nil
}

var _ SnapshotStore = (*inMemorySnapshotStore)(nil)

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

	store := newInMemorySnapshotStore()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err, "Failed to compile WASM module. Make sure worker.wasm is built.")

	// 4. RUN 1: Execute with simulated crash
	crashed, err := engine.Execute(context.Background(), instanceID, "run", serverAddr, true)
	require.Error(t, err)
	assert.True(t, crashed, "Expected run 1 to crash at checkpoint")

	// Verify snapshot exists in in-memory database
	snapshot, err := store.Load(instanceID)
	require.NoError(t, err, "Snapshot should exist in SQLite database")
	assert.NotEmpty(t, snapshot, "Snapshot data should not be empty")

	// 5. RUN 2: Restore from checkpoint and run to completion
	crashed, err = engine.Execute(context.Background(), instanceID, "run", serverAddr, false)
	require.NoError(t, err, "Run 2 should complete without errors")
	assert.False(t, crashed, "Run 2 should not crash")

	// 6. Verify processed output
	expectedOutput := "HELLO FROM DURABLE TEST STREAM!"
	assert.Equal(t, expectedOutput, string(receivedBytes), "Data processed by WASM worker should be converted to uppercase")
}

func TestDirtyPageAndOplog(t *testing.T) {
	instanceID := "test-dirty-oplog-instance"

	wasmBytes, err := wasmtime.Wat2Wasm(testdata.DirtyPageOplogWat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store := newInMemorySnapshotStore()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// RUN 1: Run and crash on first checkpoint
	crashed, err := engine.Execute(context.Background(), instanceID, "run_test", "localhost:0", true)
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
	crashed, err = engine.Execute(context.Background(), instanceID, "run_test", "localhost:0", false)
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


func TestS3SnapshotStore(t *testing.T) {
	// Try to connect to a local MinIO/S3 using environment variables
	bucket := os.Getenv("S3_BUCKET")
	endpoint := os.Getenv("S3_ENDPOINT")
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if bucket == "" || endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("S3 environment variables not fully set (S3_BUCKET, S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY). Skipping S3 integration test.")
		return
	}

	ctx := context.Background()

	// Initialize S3 store with credentials and endpoint config
	store, err := NewS3SnapshotStore(ctx, bucket, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.Region = "us-east-1"
		o.UsePathStyle = true
	})
	require.NoError(t, err)

	// Create bucket if it doesn't exist
	_, _ = store.Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})

	instanceID := "s3-test-instance-" + uuid.New().String()
	defer store.Delete(instanceID)

	// Test Save & Load Snapshot
	err = store.Save(instanceID, []byte("s3-full-snapshot-data"))
	require.NoError(t, err)

	snapshot, err := store.Load(instanceID)
	require.NoError(t, err)
	assert.Equal(t, "s3-full-snapshot-data", string(snapshot))

	// Test Save & Load Deltas
	deltas := map[int][]byte{
		0: []byte("s3-page-0-data"),
		9: []byte("s3-page-9-data"),
	}
	err = store.SaveDeltas(instanceID, deltas)
	require.NoError(t, err)

	loadedDeltas, err := store.LoadDeltas(instanceID)
	require.NoError(t, err)
	assert.Len(t, loadedDeltas, 2)
	assert.Equal(t, "s3-page-0-data", string(loadedDeltas[0]))
	assert.Equal(t, "s3-page-9-data", string(loadedDeltas[9]))

	// Test Save & Load Oplog
	err = store.SaveOplog(instanceID, 1, "test_s3_call", []byte("s3-req"), []byte("s3-resp"))
	require.NoError(t, err)

	oplog, err := store.LoadOplog(instanceID)
	require.NoError(t, err)
	require.Len(t, oplog, 1)
	assert.Equal(t, 1, oplog[0].CallIndex)
	assert.Equal(t, "test_s3_call", oplog[0].ApiName)
	assert.Equal(t, "s3-req", string(oplog[0].RequestPayload))
	assert.Equal(t, "s3-resp", string(oplog[0].ResponsePayload))

	// Test Metadata OCC (Optimistic Concurrency Control)
	meta := &InstanceMeta{
		InstanceID: instanceID,
		WasmHash:   "wasm-hash-val",
		Version:    0,
	}

	// First insert
	ok, err := store.SaveMetadata(meta)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 1, meta.Version)
	assert.NotEmpty(t, meta.ETag)

	// Try to insert again with Version=0 (should fail OCC)
	metaDup := &InstanceMeta{
		InstanceID: instanceID,
		WasmHash:   "wasm-hash-val-dup",
		Version:    0,
	}
	ok, err = store.SaveMetadata(metaDup)
	require.NoError(t, err)
	assert.False(t, ok)

	// Load metadata and check values
	loadedMeta, err := store.LoadMetadata(instanceID)
	require.NoError(t, err)
	require.NotNil(t, loadedMeta)
	assert.Equal(t, 1, loadedMeta.Version)
	assert.Equal(t, "wasm-hash-val", loadedMeta.WasmHash)
	assert.Equal(t, meta.ETag, loadedMeta.ETag)

	// Normal update
	loadedMeta.WasmHash = "wasm-hash-val-updated"
	ok, err = store.SaveMetadata(loadedMeta)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 2, loadedMeta.Version)

	// Stale update (using old meta ETag)
	meta.WasmHash = "wasm-hash-stale"
	ok, err = store.SaveMetadata(meta) // meta still has version 1 and old ETag
	require.NoError(t, err)
	assert.False(t, ok) // should fail OCC since ETag on S3 is already updated by loadedMeta

	// Verify final metadata state
	finalMeta, err := store.LoadMetadata(instanceID)
	require.NoError(t, err)
	assert.Equal(t, 2, finalMeta.Version)
	assert.Equal(t, "wasm-hash-val-updated", finalMeta.WasmHash)

	// Test WASM Registry
	wasmHash := "wasm-sha256-hash-s3"
	wasmBytes := []byte("wasm-dummy-binary-bytes")
	err = store.SaveWasm(wasmHash, wasmBytes)
	require.NoError(t, err)

	loadedWasm, err := store.LoadWasm(wasmHash)
	require.NoError(t, err)
	assert.Equal(t, wasmBytes, loadedWasm)

	// Clean up WASM registry file too (since it is outside instanceID path)
	_, err = store.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String("wasm/" + wasmHash + ".wasm"),
	})
	require.NoError(t, err)
}

func TestHostGetTime(t *testing.T) {
	instanceID := "test-time-instance"

	wasmBytes, err := wasmtime.Wat2Wasm(testdata.HostGetTimeWat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store := newInMemorySnapshotStore()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// RUN 1: Run and crash on first checkpoint
	crashed, err := engine.Execute(context.Background(), instanceID, "run_test", "localhost:0", true)
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
	crashed, err = engine.Execute(context.Background(), instanceID, "run_test", "localhost:0", false)
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

	wasmBytes, err := wasmtime.Wat2Wasm(testdata.MultiCheckpointWat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store := newInMemorySnapshotStore()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// We will run and crash on checkpoints 1, 2, 3, 4 sequentially, verifying version increment
	for expectedVal := 10; expectedVal <= 50; expectedVal += 10 {
		shouldCrash := expectedVal < 50
		crashed, err := engine.Execute(context.Background(), instanceID, "run_test", "localhost:0", shouldCrash)
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

	tempDir := t.TempDir()
	wasmPath1 := filepath.Join(tempDir, "test1.wasm")
	wasmPath2 := filepath.Join(tempDir, "test2.wasm")

	wasmBytes1, err := wasmtime.Wat2Wasm(testdata.HashMismatchWat1)
	require.NoError(t, err)
	err = os.WriteFile(wasmPath1, wasmBytes1, 0644)
	require.NoError(t, err)

	wasmBytes2, err := wasmtime.Wat2Wasm(testdata.HashMismatchWat2)
	require.NoError(t, err)
	err = os.WriteFile(wasmPath2, wasmBytes2, 0644)
	require.NoError(t, err)

	store := newInMemorySnapshotStore()

	// 1. Run with module 1
	engine1, err := NewEngine(wasmPath1, store)
	require.NoError(t, err)

	crashed, err := engine1.Execute(context.Background(), instanceID, "run_test", "localhost:0", true)
	require.Error(t, err)
	assert.True(t, crashed)

	// 2. Manually alter the saved metadata in SQLite to point to a non-existent WASM hash
	meta, err := store.LoadMetadata(instanceID)
	require.NoError(t, err)
	meta.WasmHash = "non-existent-wasm-hash"
	
	// Bypass OCC for test setup by updating metadata directly in store
	store.mu.Lock()
	store.meta[instanceID].WasmHash = meta.WasmHash
	store.mu.Unlock()

	// 3. Try to run with module 2 -> should return ErrWasmVersionMismatch because the required hash is not in the registry
	engine2, err := NewEngine(wasmPath2, store)
	require.NoError(t, err)

	_, err = engine2.Execute(context.Background(), instanceID, "run_test", "localhost:0", false)
	assert.ErrorIs(t, err, ErrWasmVersionMismatch)
}

func TestConcurrentExecution(t *testing.T) {
	instanceID := "test-concurrent-instance"
	serverAddr := "localhost:18084"

	wasmBytes, err := wasmtime.Wat2Wasm(testdata.ConcurrentExecutionWat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store := newInMemorySnapshotStore()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// Set up local server for API calls
	mux := http.NewServeMux()
	mux.HandleFunc("/trigger_race", func(w http.ResponseWriter, r *http.Request) {
		// Increment version in DB to simulate another process taking over (split-brain)
		store.mu.Lock()
		store.meta[instanceID].Version = 10
		store.mu.Unlock()

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
	crashed, err := engine.Execute(context.Background(), instanceID, "run_test", serverAddr, true)
	require.Error(t, err)
	assert.True(t, crashed)

	// 2. Try to resume. It will restore memory, call trigger_race (which pushes version to 11 in DB),
	// and then attempt checkpoint 2. Local version is still 1, but DB is 11, so it must abort with ErrConcurrentExecution.
	_, err = engine.Execute(context.Background(), instanceID, "run_test", serverAddr, false)
	assert.ErrorIs(t, err, ErrConcurrentExecution)
}

func TestOplogTruncation(t *testing.T) {
	instanceID := "test-truncation-instance"

	wasmBytes, err := wasmtime.Wat2Wasm(testdata.OplogTruncationWat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store := newInMemorySnapshotStore()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// Run up to checkpoint 4 (version 4)
	var crashed bool
	for i := 0; i < 4; i++ {
		var execErr error
		crashed, execErr = engine.Execute(context.Background(), instanceID, "run_test", "localhost:0", true)
		require.Error(t, execErr)
		assert.True(t, crashed)
	}

	// Verify oplog has 4 entries
	oplog, err := store.LoadOplog(instanceID)
	require.NoError(t, err)
	assert.Len(t, oplog, 4)

	// Run again and crash on checkpoint 5 (version 5, which triggers full snapshot and truncation, then crashes)
	crashed, err = engine.Execute(context.Background(), instanceID, "run_test", "localhost:0", true)
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

	tempDir := t.TempDir()
	wasmPath1 := filepath.Join(tempDir, "test1.wasm")
	wasmPath2 := filepath.Join(tempDir, "test2.wasm")

	wasmBytes1, err := wasmtime.Wat2Wasm(testdata.MultiVersionWat1)
	require.NoError(t, err)
	err = os.WriteFile(wasmPath1, wasmBytes1, 0644)
	require.NoError(t, err)

	wasmBytes2, err := wasmtime.Wat2Wasm(testdata.MultiVersionWat2)
	require.NoError(t, err)
	err = os.WriteFile(wasmPath2, wasmBytes2, 0644)
	require.NoError(t, err)

	store := newInMemorySnapshotStore()

	// 1. Initialize engine 1 (wat1) and crash
	engine1, err := NewEngine(wasmPath1, store)
	require.NoError(t, err)

	crashed, err := engine1.Execute(context.Background(), instanceID, "run_test", "localhost:0", true)
	require.Error(t, err)
	assert.True(t, crashed)

	// 2. Initialize engine 2 (wat2, new code version) and resume.
	// It should transparently compile and run wat1 module, loading it from store.
	engine2, err := NewEngine(wasmPath2, store)
	require.NoError(t, err)

	crashed, err = engine2.Execute(context.Background(), instanceID, "run_test", "localhost:0", false)
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

func TestExecuteCancellation(t *testing.T) {
	instanceID := "test-cancel-instance"
	serverAddr := "localhost:18085"

	wasmBytes, err := wasmtime.Wat2Wasm(testdata.ExecuteCancellationWat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store := newInMemorySnapshotStore()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// Local HTTP server that blocks for a while
	mux := http.NewServeMux()
	mux.HandleFunc("/long_call", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			// Request was canceled
		case <-time.After(1 * time.Second):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}
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

	ctx, cancel := context.WithCancel(context.Background())

	errChan := make(chan error, 1)
	go func() {
		_, err := engine.Execute(ctx, instanceID, "run_test", serverAddr, false)
		errChan <- err
	}()

	// Cancel context after 100ms (much faster than HTTP server 1s delay)
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case execErr := <-errChan:
		require.Error(t, execErr)
		assert.Contains(t, execErr.Error(), "context canceled")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for Execute to return after cancellation")
	}
}

type ErrorInjectingStore struct {
	SnapshotStore
	injectSaveErr bool
	injectMetaErr bool
}

func (e *ErrorInjectingStore) Save(id string, snapshot []byte) error {
	if e.injectSaveErr {
		return errors.New("injected storage save error")
	}
	return e.SnapshotStore.Save(id, snapshot)
}

func (e *ErrorInjectingStore) SaveMetadata(meta *InstanceMeta) (bool, error) {
	if e.injectMetaErr {
		return false, errors.New("injected metadata save error")
	}
	return e.SnapshotStore.SaveMetadata(meta)
}

func TestStorageErrorInjection(t *testing.T) {
	instanceID := "test-error-injection-instance"

	wasmBytes, err := wasmtime.Wat2Wasm(testdata.StorageErrorInjectionWat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	sqliteStore := newInMemorySnapshotStore()

	store := &ErrorInjectingStore{
		SnapshotStore: sqliteStore,
	}

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// Case 1: Injected metadata error during checkpoint
	store.injectMetaErr = true
	_, err = engine.Execute(context.Background(), instanceID, "run_test", "localhost:0", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save metadata")

	// Case 2: Injected snapshot error during checkpoint
	store.injectMetaErr = false
	store.injectSaveErr = true
	_, err = engine.Execute(context.Background(), instanceID, "run_test", "localhost:0", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write snapshot")
}

func TestSoakStressTesting(t *testing.T) {
	wasmBytes, err := wasmtime.Wat2Wasm(testdata.SoakStressWat)
	require.NoError(t, err)

	tempDir := t.TempDir()
	wasmPath := filepath.Join(tempDir, "test.wasm")
	err = os.WriteFile(wasmPath, wasmBytes, 0644)
	require.NoError(t, err)

	store := newInMemorySnapshotStore()

	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	const concurrency = 20
	const iterations = 10 // 200 total runs

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				instanceID := "stress-instance-" + strconv.Itoa(workerID) + "-" + strconv.Itoa(j)
				_, err := engine.Execute(context.Background(), instanceID, "run_test", "localhost:0", false)
				if err != nil {
					t.Errorf("Stress run failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
}
