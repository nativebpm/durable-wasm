package main

import (
	"context"
	"fmt"
	"os"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: wasm-inspector <path-to-wasm-file>")
		os.Exit(1)
	}
	ctx := context.Background()
	r := wazero.NewRuntime(ctx)
	defer r.Close(ctx)

	wasmBytes, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Printf("failed to read file: %v\n", err)
		os.Exit(1)
	}

	// Register Host Module: env
	_, err = r.NewHostModuleBuilder("env").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module) {
			fmt.Println("[HOST STUB] checkpoint called")
		}).
		Export("checkpoint").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module) int64 {
			fmt.Println("[HOST STUB] host_get_time called")
			return 0
		}).
		Export("host_get_time").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, apiNamePtr, apiNameLen, reqPtr, reqLen, respPtr, respMaxLen int32) int32 {
			fmt.Println("[HOST STUB] host_call_api called")
			return 0
		}).
		Export("host_call_api").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, direction, ptr, length int32) int32 {
			fmt.Println("[HOST STUB] stream_data called")
			return 0
		}).
		Export("stream_data").
		Instantiate(ctx)

	if err != nil {
		fmt.Printf("failed to instantiate host env: %v\n", err)
		os.Exit(1)
	}

	// Customize WASI in wazero runtime:
	wasiBuilder := r.NewHostModuleBuilder("wasi_snapshot_preview1")
	wasi_snapshot_preview1.NewFunctionExporter().ExportFunctions(wasiBuilder)

	// Override proc_exit to prevent it from closing module if exit code is 0
	wasiBuilder.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, mod api.Module, exitCode uint32) {
			fmt.Printf("[WASI OVERRIDE] proc_exit called with code %d\n", exitCode)
			if exitCode != 0 {
				_ = mod.CloseWithExitCode(ctx, exitCode)
			}
		}).
		Export("proc_exit")

	_, err = wasiBuilder.Instantiate(ctx)
	if err != nil {
		fmt.Printf("failed to instantiate customized wasi: %v\n", err)
		os.Exit(1)
	}

	compiled, err := r.CompileModule(ctx, wasmBytes)
	if err != nil {
		fmt.Printf("failed to compile: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Instantiating module...")
	config := wazero.NewModuleConfig().
		WithStdout(os.Stdout).
		WithStderr(os.Stderr)

	mod, err := r.InstantiateModule(ctx, compiled, config)
	if err != nil {
		fmt.Printf("failed to instantiate guest module: %v\n", err)
		os.Exit(1)
	}
	defer mod.Close(ctx)

	runFunc := mod.ExportedFunction("run")
	if runFunc == nil {
		fmt.Println("run function not found")
		os.Exit(1)
	}

	fmt.Println("Running 'run' entrypoint...")
	_, err = runFunc.Call(ctx)
	if err != nil {
		fmt.Printf("run failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Execution completed successfully.")
}
