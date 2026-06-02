-- Create "snapshots" table
CREATE TABLE IF NOT EXISTS `snapshots` (
  `id` text NULL,
  `snapshot` blob NOT NULL,
  `updated_at` timestamp NULL DEFAULT (CURRENT_TIMESTAMP),
  PRIMARY KEY (`id`)
);
-- Create "memory_deltas" table
CREATE TABLE IF NOT EXISTS `memory_deltas` (
  `instance_id` text NULL,
  `page_index` integer NULL,
  `data` blob NOT NULL,
  `updated_at` timestamp NULL DEFAULT (CURRENT_TIMESTAMP),
  PRIMARY KEY (`instance_id`, `page_index`)
);
-- Create "oplog" table
CREATE TABLE IF NOT EXISTS `oplog` (
  `instance_id` text NULL,
  `call_index` integer NULL,
  `api_name` text NOT NULL,
  `request_payload` blob NULL,
  `response_payload` blob NULL,
  `created_at` timestamp NULL DEFAULT (CURRENT_TIMESTAMP),
  PRIMARY KEY (`instance_id`, `call_index`)
);
-- Create "instance_meta" table
CREATE TABLE IF NOT EXISTS `instance_meta` (
  `instance_id` text NULL,
  `wasm_hash` text NOT NULL,
  `version` integer NOT NULL,
  `updated_at` timestamp NULL DEFAULT (CURRENT_TIMESTAMP),
  PRIMARY KEY (`instance_id`)
);
-- Create "wasm_modules" table
CREATE TABLE IF NOT EXISTS `wasm_modules` (
  `hash` text NULL,
  `wasm_bytes` blob NOT NULL,
  `created_at` timestamp NULL DEFAULT (CURRENT_TIMESTAMP),
  PRIMARY KEY (`hash`)
);
