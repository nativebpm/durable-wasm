package main

import (
	"errors"
	"fmt"

	"github.com/nativebpm/wasman/runner"
)

func main() {
	// Using the high-level, safe RunTask API inside main().
	// Standard Go (wasip1) does not support exports, so main() is the correct entrypoint.
	// The developer only writes business logic modifying map variables.
	// JSON serialization, stream I/O, and writer cleanup are handled automatically.
	// There are no local networking or HTTP sockets used, ensuring 100% sandboxed execution safety.
	runner.RunTask(func(vars map[string]interface{}) error {
		println("[SAFE TASK] Execution started...")

		// Read input variables
		item, ok := vars["item"]
		if !ok {
			return errors.New("missing variable 'item'")
		}
		
		fmt.Printf("[SAFE TASK] Checking stock availability for: %v\n", item)

		// Simple business logic: out_of_stock_item is unavailable
		if item == "out_of_stock_item" {
			vars["in_stock"] = false
		} else {
			vars["in_stock"] = true
		}

		println("[SAFE TASK] Execution completed successfully")
		return nil
	})
}
