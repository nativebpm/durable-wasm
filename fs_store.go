//go:build !wasm

package durable

import (
	"encoding/json"
	"fmt"
	"os"
)

// FileSnapshotStore implements SnapshotStore using the local file system.
type FileSnapshotStore struct {
	Dir string
}

var _ SnapshotStore = (*FileSnapshotStore)(nil)

// Save writes a full memory snapshot to a file.
func (f *FileSnapshotStore) Save(id string, snapshot []byte) error {
	path := fmt.Sprintf("%s.bin", id)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s.bin", f.Dir, id)
	}
	return os.WriteFile(path, snapshot, 0644)
}

// Load reads a full memory snapshot from a file.
func (f *FileSnapshotStore) Load(id string) ([]byte, error) {
	path := fmt.Sprintf("%s.bin", id)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s.bin", f.Dir, id)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (f *FileSnapshotStore) SaveDeltas(id string, deltas map[int][]byte) error {
	path := fmt.Sprintf("%s_deltas.json", id)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s_deltas.json", f.Dir, id)
	}
	current := make(map[int][]byte)
	data, err := os.ReadFile(path)
	if err == nil {
		_ = json.Unmarshal(data, &current)
	}
	for k, v := range deltas {
		current[k] = v
	}
	newData, err := json.Marshal(current)
	if err != nil {
		return err
	}
	return os.WriteFile(path, newData, 0644)
}

func (f *FileSnapshotStore) LoadDeltas(id string) (map[int][]byte, error) {
	path := fmt.Sprintf("%s_deltas.json", id)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s_deltas.json", f.Dir, id)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var deltas map[int][]byte
	err = json.Unmarshal(data, &deltas)
	return deltas, err
}

func (f *FileSnapshotStore) TruncateDeltas(id string) error {
	path := fmt.Sprintf("%s_deltas.json", id)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s_deltas.json", f.Dir, id)
	}
	_ = os.Remove(path)
	return nil
}

func (f *FileSnapshotStore) SaveOplog(id string, callIndex int, apiName string, request []byte, response []byte) error {
	path := fmt.Sprintf("%s_oplog.json", id)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s_oplog.json", f.Dir, id)
	}
	var list []OplogEntry
	data, err := os.ReadFile(path)
	if err == nil {
		_ = json.Unmarshal(data, &list)
	}
	list = append(list, OplogEntry{
		CallIndex:       callIndex,
		ApiName:         apiName,
		RequestPayload:  request,
		ResponsePayload: response,
	})
	newData, err := json.Marshal(list)
	if err != nil {
		return err
	}
	return os.WriteFile(path, newData, 0644)
}

func (f *FileSnapshotStore) LoadOplog(id string) ([]OplogEntry, error) {
	path := fmt.Sprintf("%s_oplog.json", id)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s_oplog.json", f.Dir, id)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var list []OplogEntry
	err = json.Unmarshal(data, &list)
	return list, err
}

func (f *FileSnapshotStore) TruncateOplog(id string, beforeCallIndex int) error {
	path := fmt.Sprintf("%s_oplog.json", id)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s_oplog.json", f.Dir, id)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var list []OplogEntry
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	var filtered []OplogEntry
	for _, entry := range list {
		if entry.CallIndex > beforeCallIndex {
			filtered = append(filtered, entry)
		}
	}
	newData, err := json.Marshal(filtered)
	if err != nil {
		return err
	}
	return os.WriteFile(path, newData, 0644)
}

func (f *FileSnapshotStore) SaveMetadata(meta *InstanceMeta) (bool, error) {
	path := fmt.Sprintf("%s_meta.json", meta.InstanceID)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s_meta.json", f.Dir, meta.InstanceID)
	}
	var existing InstanceMeta
	data, err := os.ReadFile(path)
	if err == nil {
		_ = json.Unmarshal(data, &existing)
		if meta.Version == 0 {
			return false, nil
		}
		if existing.Version != meta.Version {
			return false, nil
		}
	} else if !os.IsNotExist(err) {
		return false, err
	} else if meta.Version > 0 {
		return false, nil
	}

	meta.Version++
	newData, err := json.Marshal(meta)
	if err != nil {
		return false, err
	}
	err = os.WriteFile(path, newData, 0644)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (f *FileSnapshotStore) LoadMetadata(id string) (*InstanceMeta, error) {
	path := fmt.Sprintf("%s_meta.json", id)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s_meta.json", f.Dir, id)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var meta InstanceMeta
	err = json.Unmarshal(data, &meta)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (f *FileSnapshotStore) Delete(id string) error {
	path := fmt.Sprintf("%s.bin", id)
	pathDeltas := fmt.Sprintf("%s_deltas.json", id)
	pathOplog := fmt.Sprintf("%s_oplog.json", id)
	pathMeta := fmt.Sprintf("%s_meta.json", id)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/%s.bin", f.Dir, id)
		pathDeltas = fmt.Sprintf("%s/%s_deltas.json", f.Dir, id)
		pathOplog = fmt.Sprintf("%s/%s_oplog.json", f.Dir, id)
		pathMeta = fmt.Sprintf("%s/%s_meta.json", f.Dir, id)
	}
	_ = os.Remove(path)
	_ = os.Remove(pathDeltas)
	_ = os.Remove(pathOplog)
	_ = os.Remove(pathMeta)
	return nil
}

func (f *FileSnapshotStore) SaveWasm(hash string, wasmBytes []byte) error {
	path := fmt.Sprintf("wasm_%s.wasm", hash)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/wasm_%s.wasm", f.Dir, hash)
	}
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	return os.WriteFile(path, wasmBytes, 0644)
}

func (f *FileSnapshotStore) LoadWasm(hash string) ([]byte, error) {
	path := fmt.Sprintf("wasm_%s.wasm", hash)
	if f.Dir != "" {
		path = fmt.Sprintf("%s/wasm_%s.wasm", f.Dir, hash)
	}
	return os.ReadFile(path)
}
