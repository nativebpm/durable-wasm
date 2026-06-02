SELECT call_index, api_name, request_payload, response_payload FROM oplog WHERE instance_id = ? ORDER BY call_index ASC;
