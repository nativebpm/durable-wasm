UPDATE instance_meta SET version = ?, wasm_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE instance_id = ? AND version = ?;
