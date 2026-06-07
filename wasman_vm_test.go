package wasman

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tetratelabs/wazero"
)

type GraphDefinition struct {
	ID          string               `json:"id"`
	Name        string               `json:"name"`
	Nodes       map[string]GraphNode `json:"nodes"`
	Connections []Connection         `json:"connections"`
	StartNodeID string               `json:"start_node_id"`
}

type GraphNode struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Name string `json:"name"`
}

type Connection struct {
	ID        string `json:"id"`
	SourceRef string `json:"source_ref"`
	TargetRef string `json:"target_ref"`
	Condition string `json:"condition,omitempty"`
}

type ProcessInstance struct {
	ID                       string                 `json:"id"`
	ProcessID                string                 `json:"process_id"`
	ActiveActivityInstances  []string               `json:"active_activity_instances"`
	WaitingActivityInstances []string               `json:"waiting_activity_instances"`
	CompletedNodes           []string               `json:"completed_nodes"`
	Variables                map[string]interface{} `json:"variables"`
	Completed                bool                   `json:"completed"`
}

func TestWasmVMDurableExecution(t *testing.T) {
	wasmPath := filepath.Join("testdata", "bpmn_vm.wasm")
	if _, err := os.Stat(wasmPath); os.IsNotExist(err) {
		t.Skip("bpmn_vm.wasm not compiled yet. Run tinygo build first.")
		return
	}

	ctx := context.Background()
	store := newInMemorySnapshotStore()

	// Load VM engine
	engine, err := NewEngine(wasmPath, store)
	require.NoError(t, err)

	// Define a simple BPMN definition: StartEvent -> UserTask (Wait State) -> EndEvent
	graph := GraphDefinition{
		ID:   "simple_wait_process",
		Name: "Simple Wait Process",
		Nodes: map[string]GraphNode{
			"start": {ID: "start", Type: "StartEvent", Name: "Start"},
			"wait":  {ID: "wait",  Type: "UserTask",   Name: "User Wait Task"},
			"end":   {ID: "end",   Type: "EndEvent",    Name: "End"},
		},
		Connections: []Connection{
			{ID: "flow1", SourceRef: "start", TargetRef: "wait"},
			{ID: "flow2", SourceRef: "wait",  TargetRef: "end"},
		},
		StartNodeID: "start",
	}

	graphBytes, err := json.Marshal(graph)
	require.NoError(t, err)

	variables := map[string]interface{}{
		"init_val": "hello_wasm",
	}
	variablesBytes, err := json.Marshal(variables)
	require.NoError(t, err)

	instanceID := "process-instance-123"

	session := &Session{
		engine:     engine,
		ctx:        ctx,
		instanceID: instanceID,
		meta: &InstanceMeta{
			InstanceID: instanceID,
			WasmHash:   engine.wasmHash,
			Version:    0,
		},
		pageHashes: make(map[int]uint64),
	}

	modConfig := engine.compiled
	require.NotNil(t, modConfig)

	// We instantiate the module config.
	execCtx := WithSession(ctx, session)
	m, err := engine.runtime.InstantiateModule(execCtx, modConfig, NewModuleConfig(instanceID))
	require.NoError(t, err)
	defer m.Close(execCtx)

	session.mod = m
	session.memory = m.Memory()

	// Run _start to init runtime
	if startFunc := m.ExportedFunction("_start"); startFunc != nil {
		_, _ = startFunc.Call(execCtx)
	}

	getBufPtrFunc := m.ExportedFunction("get_exchange_buffer_pointer")
	require.NotNil(t, getBufPtrFunc)
	res, err := getBufPtrFunc.Call(execCtx)
	require.NoError(t, err)
	bufPtr := uint32(res[0])

	// Write graph and variables to the WASM memory buffer
	mem := m.Memory()
	ok := mem.Write(bufPtr, graphBytes)
	require.True(t, ok)
	ok = mem.Write(bufPtr+uint32(len(graphBytes)), variablesBytes)
	require.True(t, ok)

	// Call execute(graphLen, variablesLen)
	executeFunc := m.ExportedFunction("execute")
	require.NotNil(t, executeFunc)
	callRes, err := executeFunc.Call(execCtx, uint64(len(graphBytes)), uint64(len(variablesBytes)))
	require.NoError(t, err)
	resultLen := uint32(callRes[0])

	// Read response bytes from exchange buffer
	respBytes, ok := mem.Read(bufPtr, resultLen)
	require.True(t, ok)

	var instanceState ProcessInstance
	err = json.Unmarshal(respBytes, &instanceState)
	require.NoError(t, err)

	// Assert execution reached the wait state (UserTask)
	assert.Equal(t, "inst_simple_wait_process", instanceState.ID)
	assert.Contains(t, instanceState.CompletedNodes, "start")
	assert.Contains(t, instanceState.WaitingActivityInstances, "wait")
	assert.Empty(t, instanceState.ActiveActivityInstances)
	assert.False(t, instanceState.Completed)

	// Close module to avoid instantiation name collision
	m.Close(execCtx)

	// 2. SIMULATE CRASH & RESTORE STATE
	// The snapshot is saved in `store` because checkpoint() was invoked inside the WASM core.
	snapBytes, err := store.Load(instanceID)
	require.NoError(t, err)
	assert.NotEmpty(t, snapBytes)

	meta2, err := store.LoadMetadata(instanceID)
	require.NoError(t, err)
	require.NotNil(t, meta2)

	// Instantiate a fresh session/module representing recovery
	session2 := &Session{
		engine:     engine,
		ctx:        ctx,
		instanceID: instanceID,
		meta:       meta2,
		pageHashes: make(map[int]uint64),
	}
	execCtx2 := WithSession(ctx, session2)

	m2, err := engine.runtime.InstantiateModule(execCtx2, modConfig, NewModuleConfig(instanceID))
	require.NoError(t, err)
	defer m2.Close(execCtx2)

	session2.mod = m2
	session2.memory = m2.Memory()

	// Run _start
	if startFunc := m2.ExportedFunction("_start"); startFunc != nil {
		_, _ = startFunc.Call(execCtx2)
	}

	// Restore memory manually to simulate automatic engine recovery
	mem2 := m2.Memory()
	currentPages, _ := mem2.Grow(0)
	neededPages := (uint64(len(snapBytes)) + 65535) / 65536
	if uint32(neededPages) > currentPages {
		_, ok := mem2.Grow(uint32(neededPages) - currentPages)
		require.True(t, ok)
	}
	mem2Bytes, ok := mem2.Read(0, mem2.Size())
	require.True(t, ok)
	copy(mem2Bytes, snapBytes)

	// Write graph and current instanceState bytes into the recovered exchange buffer
	getBufPtrFunc2 := m2.ExportedFunction("get_exchange_buffer_pointer")
	res2, err := getBufPtrFunc2.Call(execCtx2)
	require.NoError(t, err)
	bufPtr2 := uint32(res2[0])

	ok = mem2.Write(bufPtr2, graphBytes)
	require.True(t, ok)

	instanceStateBytes, err := json.Marshal(instanceState)
	require.NoError(t, err)
	ok = mem2.Write(bufPtr2+uint32(len(graphBytes)), instanceStateBytes)
	require.True(t, ok)

	// Write completed task ID "wait" at the end of variables
	completedTaskID := "wait"
	taskIDOffset := uint32(len(graphBytes) + len(instanceStateBytes))
	ok = mem2.Write(bufPtr2+taskIDOffset, []byte(completedTaskID))
	
	// Call resume(graphLen, instanceLen, taskIDPtr, taskIDLen)
	resumeFunc := m2.ExportedFunction("resume")
	require.NotNil(t, resumeFunc)
	resumeCallRes, err := resumeFunc.Call(execCtx2,
		uint64(len(graphBytes)),
		uint64(len(instanceStateBytes)),
		uint64(taskIDOffset),
		uint64(len(completedTaskID)),
	)
	require.NoError(t, err)
	resultLen2 := uint32(resumeCallRes[0])

	// Read final response state
	finalRespBytes, ok := mem2.Read(bufPtr2, resultLen2)
	require.True(t, ok)

	var finalState ProcessInstance
	err = json.Unmarshal(finalRespBytes, &finalState)
	require.NoError(t, err)

	// Assert process ran to completion successfully
	assert.True(t, finalState.Completed)
	assert.Contains(t, finalState.CompletedNodes, "start")
	assert.Contains(t, finalState.CompletedNodes, "wait")
	assert.Contains(t, finalState.CompletedNodes, "end")
	assert.Empty(t, finalState.ActiveActivityInstances)
	assert.Empty(t, finalState.WaitingActivityInstances)
}

func NewModuleConfig(instanceID string) wazero.ModuleConfig {
	return wazero.NewModuleConfig().
		WithName("test-module-" + instanceID).
		WithStdout(os.Stdout).
		WithStderr(os.Stderr).
		WithStartFunctions()
}
