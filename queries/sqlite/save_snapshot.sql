INSERT INTO snapshots (id, snapshot, updated_at)
VALUES (?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(id) DO UPDATE SET
    snapshot = excluded.snapshot,
    updated_at = CURRENT_TIMESTAMP;
