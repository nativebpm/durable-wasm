INSERT INTO oplog (instance_id, call_index, api_name, request_payload, response_payload, created_at)
VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(instance_id, call_index) DO UPDATE SET
    api_name = excluded.api_name,
    request_payload = excluded.request_payload,
    response_payload = excluded.response_payload;
