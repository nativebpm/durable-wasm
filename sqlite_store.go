package durable

import (
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/nativebpm/durable-wasm/queries/sqlite"
	_ "modernc.org/sqlite"
)

//go:embed schema/sqlite.sql
var sqliteSchema string

// SqliteSnapshotStore implements SnapshotStore using a local SQLite database.
type SqliteSnapshotStore struct {
	db *sql.DB
}

var _ SnapshotStore = (*SqliteSnapshotStore)(nil)

// NewSqliteSnapshotStore initializes a new SQLite snapshot store and creates all required tables.
func NewSqliteSnapshotStore(dbPath string) (*SqliteSnapshotStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Enable WAL (Write-Ahead Logging) mode which is required by Litestream
	_, err = db.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	// Limit connection pool to a single connection to prevent "database is locked" errors
	// during concurrent read/write operations, which is a standard Go SQLite practice.
	db.SetMaxOpenConns(1)

	// Optimize performance parameters for concurrent reads and writes
	_, err = db.Exec("PRAGMA busy_timeout=5000;")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	_, err = db.Exec("PRAGMA synchronous=NORMAL;")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to configure sqlite synchronous pragma: %w", err)
	}

	_, err = db.Exec(sqliteSchema)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to execute sqlite schema: %w", err)
	}

	return &SqliteSnapshotStore{db: db}, nil
}

// Save inserts or updates a linear memory snapshot inside the database.
func (s *SqliteSnapshotStore) Save(id string, snapshot []byte) error {
	_, err := s.db.Exec(sqlite.SaveSnapshot, id, snapshot)
	if err != nil {
		return fmt.Errorf("failed to save snapshot for '%s': %w", id, err)
	}
	return nil
}

// Load retrieves a linear memory snapshot from the database.
func (s *SqliteSnapshotStore) Load(id string) ([]byte, error) {
	var snapshot []byte
	err := s.db.QueryRow(sqlite.LoadSnapshot, id).Scan(&snapshot)
	if err == sql.ErrNoRows {
		return nil, sql.ErrNoRows
	} else if err != nil {
		return nil, fmt.Errorf("failed to load snapshot for '%s': %w", id, err)
	}
	return snapshot, nil
}

// SaveDeltas saves dirty pages in transaction
func (s *SqliteSnapshotStore) SaveDeltas(id string, deltas map[int][]byte) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for SaveDeltas: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(sqlite.SaveDeltas)
	if err != nil {
		return fmt.Errorf("failed to prepare SaveDeltas query: %w", err)
	}
	defer stmt.Close()

	for pageIdx, data := range deltas {
		_, err := stmt.Exec(id, pageIdx, data)
		if err != nil {
			return fmt.Errorf("failed to save memory delta page %d: %w", pageIdx, err)
		}
	}

	return tx.Commit()
}

// LoadDeltas retrieves delta pages from the database
func (s *SqliteSnapshotStore) LoadDeltas(id string) (map[int][]byte, error) {
	rows, err := s.db.Query(sqlite.LoadDeltas, id)
	if err != nil {
		return nil, fmt.Errorf("failed to query memory deltas: %w", err)
	}
	defer rows.Close()

	deltas := make(map[int][]byte)
	for rows.Next() {
		var pageIdx int
		var data []byte
		if err := rows.Scan(&pageIdx, &data); err != nil {
			return nil, fmt.Errorf("failed to scan memory delta row: %w", err)
		}
		deltas[pageIdx] = data
	}
	return deltas, nil
}

// SaveOplog records API call in database
func (s *SqliteSnapshotStore) SaveOplog(id string, callIndex int, apiName string, request []byte, response []byte) error {
	_, err := s.db.Exec(sqlite.SaveOplog, id, callIndex, apiName, request, response)
	if err != nil {
		return fmt.Errorf("failed to save oplog: %w", err)
	}
	return nil
}

// LoadOplog retrieves the execution log
func (s *SqliteSnapshotStore) LoadOplog(id string) ([]OplogEntry, error) {
	rows, err := s.db.Query(sqlite.LoadOplog, id)
	if err != nil {
		return nil, fmt.Errorf("failed to query oplog: %w", err)
	}
	defer rows.Close()

	var list []OplogEntry
	for rows.Next() {
		var entry OplogEntry
		if err := rows.Scan(&entry.CallIndex, &entry.ApiName, &entry.RequestPayload, &entry.ResponsePayload); err != nil {
			return nil, fmt.Errorf("failed to scan oplog row: %w", err)
		}
		list = append(list, entry)
	}
	return list, nil
}

// Delete removes all data associated with the instance from all tables.
func (s *SqliteSnapshotStore) Delete(id string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	_, _ = tx.Exec(sqlite.DeleteSnapshots, id)
	_, _ = tx.Exec(sqlite.DeleteDeltas, id)
	_, _ = tx.Exec(sqlite.DeleteOplog, id)
	_, _ = tx.Exec(sqlite.DeleteMeta, id)

	return tx.Commit()
}

// TruncateDeltas deletes all memory deltas for the instance.
func (s *SqliteSnapshotStore) TruncateDeltas(id string) error {
	_, err := s.db.Exec(sqlite.TruncateDeltas, id)
	if err != nil {
		return fmt.Errorf("failed to truncate deltas: %w", err)
	}
	return nil
}

// TruncateOplog deletes all oplog entries for the instance at or below the given call index.
func (s *SqliteSnapshotStore) TruncateOplog(id string, beforeCallIndex int) error {
	_, err := s.db.Exec(sqlite.TruncateOplog, id, beforeCallIndex)
	if err != nil {
		return fmt.Errorf("failed to truncate oplog: %w", err)
	}
	return nil
}

// LoadMetadata retrieves the instance metadata from SQLite.
func (s *SqliteSnapshotStore) LoadMetadata(id string) (*InstanceMeta, error) {
	var meta InstanceMeta
	err := s.db.QueryRow(sqlite.LoadMetadata, id).Scan(&meta.InstanceID, &meta.WasmHash, &meta.Version)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}
	return &meta, nil
}

// SaveMetadata saves metadata or atomically updates version via CAS.
func (s *SqliteSnapshotStore) SaveMetadata(meta *InstanceMeta) (bool, error) {
	if meta.Version == 0 {
		_, err := s.db.Exec(sqlite.SaveMetadataInsert, meta.InstanceID, meta.WasmHash)
		if err != nil {
			return false, nil
		}
		meta.Version = 1
		return true, nil
	}

	res, err := s.db.Exec(sqlite.SaveMetadataUpdate, meta.Version+1, meta.WasmHash, meta.InstanceID, meta.Version)
	if err != nil {
		return false, fmt.Errorf("failed to update metadata: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows == 0 {
		return false, nil
	}
	meta.Version++
	return true, nil
}

// SaveWasm saves a WASM module binary by its SHA256 hash.
func (s *SqliteSnapshotStore) SaveWasm(hash string, wasmBytes []byte) error {
	_, err := s.db.Exec(sqlite.SaveWasm, hash, wasmBytes)
	if err != nil {
		return fmt.Errorf("failed to save WASM module %s: %w", hash, err)
	}
	return nil
}

// LoadWasm loads a WASM module binary by its SHA256 hash.
func (s *SqliteSnapshotStore) LoadWasm(hash string) ([]byte, error) {
	var wasmBytes []byte
	err := s.db.QueryRow(sqlite.LoadWasm, hash).Scan(&wasmBytes)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("wasm module not found: %s", hash)
	} else if err != nil {
		return nil, fmt.Errorf("failed to load WASM module %s: %w", hash, err)
	}
	return wasmBytes, nil
}

// Close gracefully closes the SQLite database connection.
func (s *SqliteSnapshotStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
