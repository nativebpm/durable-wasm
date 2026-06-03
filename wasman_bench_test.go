package wasman

import (
	"crypto/rand"
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
