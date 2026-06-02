package durable

import (
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/nativebpm/durable-wasm/queries/postgres"
	_ "github.com/lib/pq"
)

//go:embed schema/postgres.sql
var postgresSchema string

// PostgresSnapshotStore implements SnapshotStore using a PostgreSQL database.
type PostgresSnapshotStore struct {
	db *sql.DB
}

var _ SnapshotStore = (*PostgresSnapshotStore)(nil)

// NewPostgresSnapshotStore initializes a new Postgres snapshot store and creates all required tables.
func NewPostgresSnapshotStore(connStr string) (*PostgresSnapshotStore, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}

	_, err = db.Exec(postgresSchema)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to execute postgres schema: %w", err)
	}

	return &PostgresSnapshotStore{db: db}, nil
}

// Save inserts or updates a linear memory snapshot inside the database.
func (s *PostgresSnapshotStore) Save(id string, snapshot []byte) error {
	_, err := s.db.Exec(postgres.SaveSnapshot, id, snapshot)
	if err != nil {
		return fmt.Errorf("failed to save snapshot for '%s': %w", id, err)
	}
	return nil
}

// Load retrieves a linear memory snapshot from the database.
func (s *PostgresSnapshotStore) Load(id string) ([]byte, error) {
	var snapshot []byte
	err := s.db.QueryRow(postgres.LoadSnapshot, id).Scan(&snapshot)
	if err == sql.ErrNoRows {
		return nil, sql.ErrNoRows
	} else if err != nil {
		return nil, fmt.Errorf("failed to load snapshot for '%s': %w", id, err)
	}
	return snapshot, nil
}

// SaveDeltas saves dirty pages in transaction
func (s *PostgresSnapshotStore) SaveDeltas(id string, deltas map[int][]byte) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for SaveDeltas: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(postgres.SaveDeltas)
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
func (s *PostgresSnapshotStore) LoadDeltas(id string) (map[int][]byte, error) {
	rows, err := s.db.Query(postgres.LoadDeltas, id)
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
func (s *PostgresSnapshotStore) SaveOplog(id string, callIndex int, apiName string, request []byte, response []byte) error {
	_, err := s.db.Exec(postgres.SaveOplog, id, callIndex, apiName, request, response)
	if err != nil {
		return fmt.Errorf("failed to save oplog: %w", err)
	}
	return nil
}

// LoadOplog retrieves the execution log
func (s *PostgresSnapshotStore) LoadOplog(id string) ([]OplogEntry, error) {
	rows, err := s.db.Query(postgres.LoadOplog, id)
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
func (s *PostgresSnapshotStore) Delete(id string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	_, _ = tx.Exec(postgres.DeleteSnapshots, id)
	_, _ = tx.Exec(postgres.DeleteDeltas, id)
	_, _ = tx.Exec(postgres.DeleteOplog, id)
	_, _ = tx.Exec(postgres.DeleteMeta, id)

	return tx.Commit()
}

// TruncateDeltas deletes all memory deltas for the instance.
func (s *PostgresSnapshotStore) TruncateDeltas(id string) error {
	_, err := s.db.Exec(postgres.TruncateDeltas, id)
	if err != nil {
		return fmt.Errorf("failed to truncate deltas in postgres: %w", err)
	}
	return nil
}

// TruncateOplog deletes all oplog entries for the instance at or below the given call index.
func (s *PostgresSnapshotStore) TruncateOplog(id string, beforeCallIndex int) error {
	_, err := s.db.Exec(postgres.TruncateOplog, id, beforeCallIndex)
	if err != nil {
		return fmt.Errorf("failed to truncate oplog in postgres: %w", err)
	}
	return nil
}

// LoadMetadata retrieves the instance metadata from Postgres.
func (s *PostgresSnapshotStore) LoadMetadata(id string) (*InstanceMeta, error) {
	var meta InstanceMeta
	err := s.db.QueryRow(postgres.LoadMetadata, id).Scan(&meta.InstanceID, &meta.WasmHash, &meta.Version)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to load metadata from postgres: %w", err)
	}
	return &meta, nil
}

// SaveMetadata saves metadata or atomically updates version via CAS in Postgres.
func (s *PostgresSnapshotStore) SaveMetadata(meta *InstanceMeta) (bool, error) {
	if meta.Version == 0 {
		_, err := s.db.Exec(postgres.SaveMetadataInsert, meta.InstanceID, meta.WasmHash)
		if err != nil {
			return false, nil
		}
		meta.Version = 1
		return true, nil
	}

	res, err := s.db.Exec(postgres.SaveMetadataUpdate, meta.Version+1, meta.WasmHash, meta.InstanceID, meta.Version)
	if err != nil {
		return false, fmt.Errorf("failed to update metadata in postgres: %w", err)
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

// SaveWasm saves a WASM module binary by its SHA256 hash in Postgres.
func (s *PostgresSnapshotStore) SaveWasm(hash string, wasmBytes []byte) error {
	_, err := s.db.Exec(postgres.SaveWasm, hash, wasmBytes)
	if err != nil {
		return fmt.Errorf("failed to save WASM module to postgres %s: %w", hash, err)
	}
	return nil
}

// LoadWasm loads a WASM module binary by its SHA256 hash from Postgres.
func (s *PostgresSnapshotStore) LoadWasm(hash string) ([]byte, error) {
	var wasmBytes []byte
	err := s.db.QueryRow(postgres.LoadWasm, hash).Scan(&wasmBytes)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("wasm module not found in postgres: %s", hash)
	} else if err != nil {
		return nil, fmt.Errorf("failed to load WASM module from postgres %s: %w", hash, err)
	}
	return wasmBytes, nil
}

// Close gracefully closes the Postgres connection.
func (s *PostgresSnapshotStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
