INSERT INTO memory_deltas (instance_id, page_index, data, updated_at)
VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
ON CONFLICT(instance_id, page_index) DO UPDATE SET
    data = excluded.data,
    updated_at = CURRENT_TIMESTAMP;
