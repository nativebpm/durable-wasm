INSERT INTO memory_deltas (instance_id, page_index, data, updated_at)
VALUES (?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(instance_id, page_index) DO UPDATE SET
    data = excluded.data,
    updated_at = CURRENT_TIMESTAMP;
