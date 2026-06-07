package wasman

import (
	"encoding/json"
	"errors"
	"sync"
)

// MemorySnapshotStore implements SnapshotStore in memory.
type MemorySnapshotStore struct {
	mu          sync.RWMutex
	snapshots   map[string][]byte
	deltas      map[string]map[int][]byte
	oplogs      map[string][]OplogEntry
	meta        map[string]*InstanceMeta
	wasm        map[string][]byte
	activeIndex []byte
}

var _ SnapshotStore = (*MemorySnapshotStore)(nil)

// NewMemorySnapshotStore creates a new MemorySnapshotStore.
func NewMemorySnapshotStore() *MemorySnapshotStore {
	return &MemorySnapshotStore{
		snapshots:   make(map[string][]byte),
		deltas:      make(map[string]map[int][]byte),
		oplogs:      make(map[string][]OplogEntry),
		meta:        make(map[string]*InstanceMeta),
		wasm:        make(map[string][]byte),
		activeIndex: []byte("[]"),
	}
}

func (s *MemorySnapshotStore) Save(id string, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]byte, len(snapshot))
	copy(copied, snapshot)
	s.snapshots[id] = copied
	return nil
}

func (s *MemorySnapshotStore) Load(id string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap, ok := s.snapshots[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return snap, nil
}

func (s *MemorySnapshotStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.snapshots, id)
	delete(s.deltas, id)
	delete(s.oplogs, id)
	delete(s.meta, id)
	return nil
}

func (s *MemorySnapshotStore) SaveDeltas(id string, deltas map[int][]byte) error {
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

func (s *MemorySnapshotStore) LoadDeltas(id string) (map[int][]byte, error) {
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

func (s *MemorySnapshotStore) TruncateDeltas(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.deltas, id)
	return nil
}

func (s *MemorySnapshotStore) SaveOplog(id string, callIndex int, apiName string, request []byte, response []byte) error {
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

func (s *MemorySnapshotStore) LoadOplog(id string) ([]OplogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, ok := s.oplogs[id]
	if !ok {
		return nil, nil
	}
	return list, nil
}

func (s *MemorySnapshotStore) TruncateOplog(id string, beforeCallIndex int) error {
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

func (s *MemorySnapshotStore) SaveMetadata(meta *InstanceMeta) (bool, error) {
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
	if meta.BpmnState != nil {
		copied.BpmnState = make([]byte, len(meta.BpmnState))
		copy(copied.BpmnState, meta.BpmnState)
	}
	s.meta[meta.InstanceID] = &copied
	return true, nil
}

func (s *MemorySnapshotStore) LoadMetadata(id string) (*InstanceMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.meta[id]
	if !ok {
		return nil, nil
	}
	copied := *meta
	if meta.BpmnState != nil {
		copied.BpmnState = make([]byte, len(meta.BpmnState))
		copy(copied.BpmnState, meta.BpmnState)
	}
	return &copied, nil
}

func (s *MemorySnapshotStore) SaveWasm(hash string, wasmBytes []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]byte, len(wasmBytes))
	copy(copied, wasmBytes)
	s.wasm[hash] = copied
	return nil
}

func (s *MemorySnapshotStore) LoadWasm(hash string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.wasm[hash]
	if !ok {
		return nil, errors.New("not found")
	}
	return w, nil
}

func (s *MemorySnapshotStore) UpdateActiveIndex(id string, info []byte, completed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var index []map[string]interface{}
	if len(s.activeIndex) > 0 {
		_ = json.Unmarshal(s.activeIndex, &index)
	}

	var newInfo map[string]interface{}
	if err := json.Unmarshal(info, &newInfo); err != nil {
		return err
	}

	updated := false
	var nextIndex []map[string]interface{}
	for _, entry := range index {
		if entry["instance_id"] == id {
			if !completed {
				nextIndex = append(nextIndex, newInfo)
				updated = true
			}
		} else {
			nextIndex = append(nextIndex, entry)
		}
	}
	if !updated && !completed {
		nextIndex = append(nextIndex, newInfo)
	}

	newData, err := json.Marshal(nextIndex)
	if err != nil {
		return err
	}
	s.activeIndex = newData
	return nil
}

func (s *MemorySnapshotStore) LoadActiveIndex() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.activeIndex) == 0 {
		return []byte("[]"), nil
	}
	return s.activeIndex, nil
}
