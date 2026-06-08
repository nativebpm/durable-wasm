package wasman

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func BenchmarkSnapshotStore_SaveFull(b *testing.B) {
	store := newInMemorySnapshotStore()

	// Simulate 4 MB WASM linear memory
	memorySize := 4 * 1024 * 1024
	data := make([]byte, memorySize)
	_, _ = rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := store.Save("bench-instance", data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSnapshotStore_SaveDeltas(b *testing.B) {
	store := newInMemorySnapshotStore()

	// Simulate changes in 2 pages (each 4KB) -> total 8KB
	deltas := map[int][]byte{
		12: make([]byte, 4096),
		85: make([]byte, 4096),
	}
	_, _ = rand.Read(deltas[12])
	_, _ = rand.Read(deltas[85])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := store.SaveDeltas("bench-instance", deltas)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWasmVM_Execute(b *testing.B) {
	wasmPath := "testdata/bpmn_vm.wasm"
	if _, err := os.Stat(wasmPath); os.IsNotExist(err) {
		b.Skip("bpmn_vm.wasm not compiled yet")
		return
	}

	ctx := context.Background()
	store := newInMemorySnapshotStore()
	engine, err := NewEngine(wasmPath, store)
	if err != nil {
		b.Fatal(err)
	}

	graph := GraphDefinition{
		ID:   "simple_process",
		Name: "Simple Process",
		Nodes: map[string]GraphNode{
			"start": {ID: "start", Type: "StartEvent", Name: "Start"},
			"wait":  {ID: "wait", Type: "UserTask", Name: "User Wait Task"},
			"end":   {ID: "end", Type: "EndEvent", Name: "End"},
		},
		Connections: []Connection{
			{ID: "flow1", SourceRef: "start", TargetRef: "wait"},
			{ID: "flow2", SourceRef: "wait", TargetRef: "end"},
		},
		StartNodeID: "start",
	}

	graphBytes, _ := json.Marshal(graph)
	variables := map[string]interface{}{"val": "hello"}
	variablesBytes, _ := json.Marshal(variables)

	// Context with handlers
	apiHandler := func(apiName string, request []byte) ([]byte, error) {
		return nil, nil
	}
	downloadHandler := func() ([]byte, error) {
		return variablesBytes, nil
	}
	uploadHandler := func(payload []byte) error {
		return nil
	}

	runCtx := WithApiHandler(ctx, apiHandler)
	runCtx = WithDownloadHandler(runCtx, downloadHandler)
	runCtx = WithUploadHandler(runCtx, uploadHandler)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instanceID := fmt.Sprintf("bench-inst-%d", i)
		_, _, err := engine.RunBPMN(runCtx, instanceID, "execute", graphBytes, variablesBytes, "", "")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWasmVM_ExecuteWarm(b *testing.B) {
	wasmPath := "testdata/bpmn_vm.wasm"
	if _, err := os.Stat(wasmPath); os.IsNotExist(err) {
		b.Skip("bpmn_vm.wasm not compiled yet")
		return
	}

	ctx := context.Background()
	store := newInMemorySnapshotStore()
	engine, err := NewEngine(wasmPath, store)
	if err != nil {
		b.Fatal(err)
	}

	graph := GraphDefinition{
		ID:   "simple_process",
		Name: "Simple Process",
		Nodes: map[string]GraphNode{
			"start": {ID: "start", Type: "StartEvent", Name: "Start"},
			"wait":  {ID: "wait", Type: "UserTask", Name: "User Wait Task"},
			"end":   {ID: "end", Type: "EndEvent", Name: "End"},
		},
		Connections: []Connection{
			{ID: "flow1", SourceRef: "start", TargetRef: "wait"},
			{ID: "flow2", SourceRef: "wait", TargetRef: "end"},
		},
		StartNodeID: "start",
	}

	graphBytes, _ := json.Marshal(graph)
	variables := map[string]interface{}{"val": "hello"}
	variablesBytes, _ := json.Marshal(variables)

	apiHandler := func(apiName string, request []byte) ([]byte, error) {
		return nil, nil
	}
	downloadHandler := func() ([]byte, error) {
		return variablesBytes, nil
	}
	uploadHandler := func(payload []byte) error {
		return nil
	}

	runCtx := WithApiHandler(ctx, apiHandler)
	runCtx = WithDownloadHandler(runCtx, downloadHandler)
	runCtx = WithUploadHandler(runCtx, uploadHandler)
	runCtx = WithKeepAlive(runCtx) // Enable keep-alive

	// Run once to instantiate
	instanceID := "warm-bench-instance"
	_, _, err = engine.RunBPMN(runCtx, instanceID, "execute", graphBytes, variablesBytes, "", "")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := engine.RunBPMN(runCtx, instanceID, "resume", graphBytes, variablesBytes, "wait", "")
		if err != nil {
			b.Fatal(err)
		}
	}

	// Cleanup
	_ = engine.CloseInstance(ctx, instanceID)
}
