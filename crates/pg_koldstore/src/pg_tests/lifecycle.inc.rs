#[pg_test]
fn extension_reports_non_empty_version() {
    let version = spi_get_text("SELECT koldstore_version()");
    assert!(!version.is_empty(), "koldstore_version must be non-empty");
}

#[pg_test]
fn snowflake_id_is_monotonic() {
    let first = spi_get_i64("SELECT snowflake_id()");
    let second = spi_get_i64("SELECT snowflake_id()");
    assert!(second > first, "snowflake_id must advance: {first} then {second}");
}

#[pg_test]
fn koldstore_user_id_guc_roundtrips() {
    // Validate the registered GUC (and the SQL helper that reads it).
    Spi::run("SET koldstore.user_id = '42'").expect("set user_id");
    let value = spi_get_text("SHOW koldstore.user_id");
    assert_eq!(value, "42");
    Spi::run("RESET koldstore.user_id").expect("reset user_id");
}

/// Queues one object for deferred deletion, `age_seconds` ago.
fn queue_deferred_cold_object(key: &str, age_seconds: i64) {
    Spi::run(&format!(
        "INSERT INTO koldstore.deferred_cold_deletes (storage_id, table_oid, object_key, staged_at) \
         VALUES ('no-such-storage', 1, '{key}', now() - make_interval(secs => {age_seconds}))"
    ))
    .expect("queue a deferred cold-object deletion");
}

fn deferred_cold_object_count() -> i64 {
    spi_get_i64("SELECT count(*) FROM koldstore.deferred_cold_deletes")
}

#[pg_test]
fn auto_purge_does_nothing_while_retention_is_off() {
    Spi::run("SET koldstore.cold_object_retention_seconds = 0").unwrap();
    queue_deferred_cold_object("a/b/old.parquet", 3600);
    assert!(crate::sql::backup::auto_purge_tick().unwrap().is_none());
    assert_eq!(deferred_cold_object_count(), 1, "nothing may be touched with retention off");
}

#[pg_test]
fn auto_purge_respects_its_switch() {
    Spi::run("SET koldstore.cold_object_retention_seconds = 60").unwrap();
    Spi::run("SET koldstore.cold_object_auto_purge = off").unwrap();
    queue_deferred_cold_object("a/b/old.parquet", 3600);
    assert!(crate::sql::backup::auto_purge_tick().unwrap().is_none());
    assert_eq!(deferred_cold_object_count(), 1);
}

#[pg_test]
fn auto_purge_waits_for_the_retention_window() {
    Spi::run("SET koldstore.cold_object_retention_seconds = 3600").unwrap();
    queue_deferred_cold_object("a/b/recent.parquet", 10);
    assert!(
        crate::sql::backup::auto_purge_tick().unwrap().is_none(),
        "an object queued 10s ago is inside a 1h window"
    );
    assert_eq!(deferred_cold_object_count(), 1);
}

#[pg_test]
fn auto_purge_handles_a_due_batch_and_keeps_what_it_cannot_delete() {
    Spi::run("SET koldstore.cold_object_retention_seconds = 60").unwrap();
    queue_deferred_cold_object("a/b/due.parquet", 3600);
    queue_deferred_cold_object("a/b/not-yet.parquet", 5);
    let report = crate::sql::backup::auto_purge_tick()
        .unwrap()
        .expect("a due object means a purge pass runs");
    assert_eq!(report["considered"], 1, "only the object past the window is considered: {report}");
    // The storage id is unknown here, so the delete cannot happen: the row must stay queued for a
    // later pass rather than being dropped, and the not-yet-due one is untouched.
    assert_eq!(report["failed"], 1, "{report}");
    assert_eq!(deferred_cold_object_count(), 2);
}
