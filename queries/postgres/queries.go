package postgres

import _ "embed"

//go:embed save_snapshot.sql
var SaveSnapshot string

//go:embed load_snapshot.sql
var LoadSnapshot string

//go:embed save_deltas.sql
var SaveDeltas string

//go:embed load_deltas.sql
var LoadDeltas string

//go:embed truncate_deltas.sql
var TruncateDeltas string

//go:embed save_oplog.sql
var SaveOplog string

//go:embed load_oplog.sql
var LoadOplog string

//go:embed truncate_oplog.sql
var TruncateOplog string

//go:embed load_metadata.sql
var LoadMetadata string

//go:embed save_metadata_insert.sql
var SaveMetadataInsert string

//go:embed save_metadata_update.sql
var SaveMetadataUpdate string

//go:embed save_wasm.sql
var SaveWasm string

//go:embed load_wasm.sql
var LoadWasm string

//go:embed delete_snapshots.sql
var DeleteSnapshots string

//go:embed delete_deltas.sql
var DeleteDeltas string

//go:embed delete_oplog.sql
var DeleteOplog string

//go:embed delete_meta.sql
var DeleteMeta string
