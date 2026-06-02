INSERT INTO snapshots (id, snapshot, updated_at)
VALUES ($1, $2, CURRENT_TIMESTAMP)
ON CONFLICT(id) DO UPDATE SET
    snapshot = excluded.snapshot,
    updated_at = CURRENT_TIMESTAMP;
