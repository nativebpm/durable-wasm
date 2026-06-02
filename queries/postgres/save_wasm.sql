INSERT INTO wasm_modules (hash, wasm_bytes) VALUES ($1, $2) ON CONFLICT(hash) DO NOTHING;
