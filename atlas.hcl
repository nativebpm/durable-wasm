env "sqlite" {
  src = "file://schema/sqlite.sql"
  url = "sqlite://durable_wasm.db"
  dev = "sqlite://dev.db"
}

env "postgres" {
  src = "file://schema/postgres.sql"
  url = "postgres://postgres:postgres@localhost:5432/durable_wasm?sslmode=disable"
  dev = "docker://postgres/15/dev"
}
