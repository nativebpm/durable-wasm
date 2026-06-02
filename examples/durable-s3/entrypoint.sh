#!/bin/sh
set -e

DB_PATH="/app/snapshots.db"
CONFIG_PATH="/etc/litestream.yml"

# 1. Wait for SeaweedFS Filer to be ready
echo "[ENTRYPOINT] Waiting for SeaweedFS Filer (port 8888) to be ready..."
until curl -s -o /dev/null http://seaweedfs:8888/; do
    sleep 0.5
done
echo "[ENTRYPOINT] SeaweedFS Filer is ready."

# 2. Wait for SeaweedFS S3 API to be ready
echo "[ENTRYPOINT] Waiting for SeaweedFS S3 API (port 8333) to be ready..."
until curl -s -o /dev/null http://seaweedfs:8333/; do
    sleep 0.5
done
echo "[ENTRYPOINT] SeaweedFS S3 API is ready."

# 3. Create the bucket in SeaweedFS Filer.
# Creating a directory under /buckets/ in SeaweedFS Filer automatically exposes it as an S3 bucket.
echo "[ENTRYPOINT] Creating snapshots-bucket in SeaweedFS..."
# We don't use -f because if the bucket already exists, it returns 409, which is fine.
curl -s -X POST http://seaweedfs:8888/buckets/snapshots-bucket/ > /dev/null
echo "[ENTRYPOINT] snapshots-bucket created or already exists."

# 1. Attempt to restore the SQLite database from S3 (seaweedfs) if it doesn't exist locally
if [ ! -f "$DB_PATH" ]; then
    echo "[ENTRYPOINT] Local database not found. Attempting to restore from S3..."
    # We use -if-replica-exists so it doesn't fail on the very first run when the bucket is empty
    litestream restore -config "$CONFIG_PATH" -if-replica-exists "$DB_PATH"
fi

# 2. Start a background helper to immediately create a database snapshot in S3 if none exists.
# This ensures we don't have to wait 24 hours for the first base snapshot.
(
    echo "[ENTRYPOINT-HELPER] Waiting for database file to be initialized..."
    until [ -f "$DB_PATH" ]; do
        sleep 0.5
    done
    # Give the host application a brief moment to initialize tables and write first checkpoints
    sleep 2
    
    echo "[ENTRYPOINT-HELPER] Checking for existing snapshots in S3..."
    if [ -z "$(litestream snapshots -config "$CONFIG_PATH" "$DB_PATH" 2>/dev/null)" ]; then
        echo "[ENTRYPOINT-HELPER] No database snapshot found in S3. Creating initial snapshot immediately..."
        litestream snapshot -config "$CONFIG_PATH" "$DB_PATH"
        echo "[ENTRYPOINT-HELPER] Initial database snapshot uploaded successfully."
    else
        echo "[ENTRYPOINT-HELPER] Existing database snapshots found in S3. Skipping initial snapshot creation."
    fi
) &

# 3. Run the host application wrapped in litestream replicate.
# This replicates WAL frames to S3 in real-time and gracefully exits when the host finishes.
echo "[ENTRYPOINT] Starting application with Litestream replication..."
exec litestream replicate -config "$CONFIG_PATH" -exec "/app/host"
