UPDATE instance_meta SET version = $1, wasm_hash = $2, updated_at = CURRENT_TIMESTAMP WHERE instance_id = $3 AND version = $4;
