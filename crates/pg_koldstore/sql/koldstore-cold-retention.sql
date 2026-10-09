-- Cold-object retention (docs/backup-and-operations.md).
--
-- With koldstore.cold_object_retention_seconds > 0, DROP TABLE and unmanage_table(drop_cold)
-- do not delete the table's cold objects at commit. They record each object key here, in the
-- dropping transaction (so an abort discards the rows, exactly like the catalog changes), and
-- leave the objects in place. koldstore.purge_deferred_cold_objects() deletes them once the
-- window has passed, which keeps a backup taken before the DROP restorable for that long.

CREATE TABLE IF NOT EXISTS koldstore.deferred_cold_deletes (
  id bigserial PRIMARY KEY,
  storage_id text NOT NULL,            -- koldstore.storage.id the object lives in
  table_oid oid NOT NULL,              -- the dropped / unmanaged table
  object_key text NOT NULL,
  staged_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (storage_id, object_key)
);
CREATE INDEX IF NOT EXISTS deferred_cold_deletes_staged_idx
  ON koldstore.deferred_cold_deletes (staged_at, id);
REVOKE ALL ON koldstore.deferred_cold_deletes FROM PUBLIC;
