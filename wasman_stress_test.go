package wasman

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStress_1MillionRuns(t *testing.T) {
	wasmPath := "testdata/bpmn_vm.wasm"
	if _, err := os.Stat(wasmPath); os.IsNotExist(err) {
		t.Skip("bpmn_vm.wasm not compiled yet")
		return
	}

	numCPUs := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPUs)

	fmt.Printf("\n=== Инициализация стресс-теста (1,000,000 запусков) ===\n")
	fmt.Printf("Окружение: Macbook Air M5\n")
	fmt.Printf("Количество ядер CPU (доступно): %d\n", numCPUs)

	ctx := context.Background()
	store := newInMemorySnapshotStore()
	engine, err := NewEngine(wasmPath, store)
	if err != nil {
		t.Fatal(err)
	}

	// Инициализация BPMN процесса (Simple Process)
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

	// Контекст с in-memory обработчиками
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
	runCtx = WithKeepAlive(runCtx)

	totalRuns := int64(1000000)
	var completedRuns int64
	var wg sync.WaitGroup

	startTime := time.Now()

	// Запускаем параллельных воркеров
	workers := numCPUs
	runsPerWorker := totalRuns / int64(workers)

	fmt.Printf("Запуск %d воркеров, каждый выполнит %d горячих JIT-запусков...\n", workers, runsPerWorker)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			instanceID := fmt.Sprintf("stress-instance-%d", workerID)

			// Шаг 1: Первичный запуск (execute) для создания снапшота и прогрева JIT
			_, _, err := engine.RunBPMN(runCtx, instanceID, "execute", graphBytes, variablesBytes, "", "")
			if err != nil {
				panic(err)
			}

			// Шаг 2: Последовательное выполнение воркером (resume) горячих запусков
			for i := int64(0); i < runsPerWorker; i++ {
				_, _, err := engine.RunBPMN(runCtx, instanceID, "resume", graphBytes, variablesBytes, "wait", "")
				if err != nil {
					panic(err)
				}
				atomic.AddInt64(&completedRuns, 1)
			}

			// Шаг 3: Закрытие инстанса
			_ = engine.CloseInstance(ctx, instanceID)
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	// Вычисление метрик
	elapsedSeconds := elapsed.Seconds()
	runsPerSecond := float64(completedRuns) / elapsedSeconds
	avgLatencyMicro := (elapsed.Seconds() / float64(completedRuns)) * 1000000.0 * float64(workers)

	// Экстраполяция на 1 миллиард
	billionSeconds := float64(1000000000) / runsPerSecond
	billionDuration := time.Duration(billionSeconds * float64(time.Second))

	fmt.Printf("\n=== Результаты Стресс-Теста ===\n")
	fmt.Printf("Всего выполнено шагов (Warm JIT): %d\n", completedRuns)
	fmt.Printf("Общее затраченное время (Wall Time): %v\n", elapsed)
	fmt.Printf("Фактическая производительность кластера: %.2f runs/sec (RPS)\n", runsPerSecond)
	fmt.Printf("Средняя задержка выполнения (1 ядро): %.2f мкс (µs) / op\n", avgLatencyMicro)
	fmt.Printf("\n=== Экстраполяция на 1 Миллиард Выполнений ===\n")
	fmt.Printf("Необходимое время на этом Macbook Air (10 ядер): %v\n", billionDuration)
	fmt.Printf("Скорость обработки 1 млрд в часах: %.2f часов\n", billionDuration.Hours())
	fmt.Printf("Или в минутах: %.2f минут\n", billionDuration.Minutes())
	fmt.Printf("===============================\n\n")
}
