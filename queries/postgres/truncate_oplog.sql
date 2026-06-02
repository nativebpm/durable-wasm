DELETE FROM oplog WHERE instance_id = $1 AND call_index <= $2;
