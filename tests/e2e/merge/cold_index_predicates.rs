//! Predicates that hot-only indexes would serve must still see cold rows.
//!
//! GIN (jsonb, array, expression tsvector), BRIN, expression, partial and `text_pattern_ops`
//! indexes exist only on the hot heap. A query that the planner would answer through one of
//! them must return exactly what a plain-heap reference table returns, whichever scan types the
//! planner is allowed to use, both with everything flushed and after hydrating updates that
//! leave stale cold versions behind.

use anyhow::{ensure, Context, Result};

use crate::common;

const ROWS: i64 = 6000;

/// `(label, WHERE/ORDER clause over the table alias `t`)`; every query returns ids only.
const PREDICATES: &[(&str, &str)] = &[
    ("jsonb @>", "WHERE doc @> '{\"k\": 3}'"),
    ("jsonb ? key", "WHERE doc ? 'flag'"),
    ("array &&", "WHERE tags && ARRAY['5', '7']"),
    ("array @>", "WHERE tags @> ARRAY['3']"),
    (
        "brin range",
        "WHERE ts >= timestamptz '2026-01-10' AND ts < timestamptz '2026-01-12'",
    ),
    ("expression lower()", "WHERE lower(name) = 'name-000042'"),
    ("partial index", "WHERE qty > 95"),
    (
        "fts expression gin",
        "WHERE to_tsvector('simple', note) @@ to_tsquery('simple', 'alpha')",
    ),
    ("pattern_ops prefix", "WHERE name LIKE 'name-0004%'"),
    ("is null", "WHERE doc IS NULL"),
];

/// Ordered, limited reads served by an index in hot-only land.
const ORDERED: &[(&str, &str)] = &[
    ("order by ts desc", "ORDER BY ts DESC, id LIMIT 7"),
    ("order by name", "ORDER BY name, id LIMIT 11"),
    ("order by qty with offset", "ORDER BY qty DESC, id OFFSET 5 LIMIT 9"),
];

/// Planner settings that steer plan shape: default, forced index, bitmap only, no index.
const SETTINGS: &[(&str, &str)] = &[
    ("default", ""),
    ("seqscan off", "SET enable_seqscan = off"),
    (
        "index only",
        "SET enable_seqscan = off; SET enable_bitmapscan = off",
    ),
    ("no index", "SET enable_indexscan = off; SET enable_bitmapscan = off"),
];

const RESET: &str = "RESET enable_seqscan; RESET enable_bitmapscan; RESET enable_indexscan";

async fn answer(
    client: &tokio_postgres::Client,
    table: &str,
    clause: &str,
    ordered: bool,
) -> Result<String> {
    let sql = if ordered {
        format!(
            "SELECT coalesce(string_agg(id::text, ',' ORDER BY n), '<none>') \
             FROM (SELECT id, row_number() OVER () AS n FROM \
                   (SELECT id FROM {table} t {clause}) q) s"
        )
    } else {
        format!(
            "SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '<none>') \
             FROM {table} t {clause}"
        )
    };
    let row = client
        .query_one(&sql, &[])
        .await
        .with_context(|| format!("query failed: {sql}"))?;
    Ok(row.get(0))
}

async fn compare_all(
    client: &tokio_postgres::Client,
    managed: &str,
    reference: &str,
    phase: &str,
    failures: &mut Vec<String>,
) -> Result<()> {
    for (settings_label, settings) in SETTINGS {
        client.batch_execute(RESET).await?;
        if !settings.is_empty() {
            client.batch_execute(settings).await?;
        }
        for (ordered, list) in [(false, PREDICATES), (true, ORDERED)] {
            for (label, clause) in list {
                let want = answer(client, reference, clause, ordered).await?;
                match answer(client, managed, clause, ordered).await {
                    Ok(got) if got == want => {}
                    Ok(got) => failures.push(format!(
                        "[{phase} / {settings_label}] {label}: managed={} ids vs reference={} ids \
                         (first diff managed `{}` vs reference `{}`)",
                        got.matches(',').count() + 1,
                        want.matches(',').count() + 1,
                        got.chars().take(60).collect::<String>(),
                        want.chars().take(60).collect::<String>(),
                    )),
                    Err(error) => failures.push(format!(
                        "[{phase} / {settings_label}] {label}: error {error:#}"
                    )),
                }
            }
        }
    }
    client.batch_execute(RESET).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_only_index_predicates_return_cold_rows() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "cold_idx_preds").await?;
    let managed = db.relation("idx_audit");
    let reference = db.relation("idx_audit_ref");
    db.client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {managed} (
              id bigint PRIMARY KEY,
              doc jsonb,
              tags text[] NOT NULL,
              name text NOT NULL,
              ts timestamptz NOT NULL,
              qty integer NOT NULL,
              note text NOT NULL
            );
            CREATE INDEX idx_audit_doc ON {managed} USING gin (doc);
            CREATE INDEX idx_audit_tags ON {managed} USING gin (tags);
            CREATE INDEX idx_audit_ts ON {managed} USING brin (ts);
            CREATE INDEX idx_audit_lower ON {managed} (lower(name));
            CREATE INDEX idx_audit_partial ON {managed} (qty) WHERE qty > 90;
            CREATE INDEX idx_audit_fts ON {managed} USING gin (to_tsvector('simple', note));
            CREATE INDEX idx_audit_prefix ON {managed} (name text_pattern_ops);
            CREATE INDEX idx_audit_name ON {managed} (name, id);
            INSERT INTO {managed}
            SELECT g,
                   CASE WHEN g % 11 = 0 THEN NULL
                        WHEN g % 7 = 0 THEN jsonb_build_object('k', g % 5, 'flag', true)
                        ELSE jsonb_build_object('k', g % 5) END,
                   ARRAY[(g % 9)::text, (g % 13)::text, (g % 17)::text],
                   'name-' || lpad(g::text, 6, '0'),
                   timestamptz '2026-01-01' + (g || ' minutes')::interval,
                   g % 100,
                   CASE WHEN g % 3 = 0 THEN 'alpha beta' ELSE 'gamma delta' END
            FROM generate_series(1, {ROWS}) g;
            INSERT INTO {managed} VALUES
              ({ROWS} + 1, '{{}}', ARRAY['a"b', 'c\\d', NULL, '', '{{x}}', ' sp ', 'NULL'], 'name-tricky-1',
               timestamptz '2026-02-01', 1, 'gamma'),
              ({ROWS} + 2, NULL, '{{}}', 'name-tricky-2', timestamptz '2026-02-02', 2, 'gamma');
            CREATE TABLE {reference} AS SELECT * FROM {managed};
            ALTER TABLE {reference} ADD PRIMARY KEY (id);
            ANALYZE {managed};
            ANALYZE {reference};
            "#
        ))
        .await?;
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass, storage => $2, hot_row_limit => 2,
              min_flush_rows => 1, max_rows_per_file => 1000, migration_order_by => 'id',
              auto_flush => false
            )
            "#,
            &[&managed, &db.storage_name],
        )
        .await?;
    common::fence_async_mirror(&db.client).await?;
    db.flush_table(&managed).await?;
    common::fence_async_mirror(&db.client).await?;

    let cold: i64 = db
        .client
        .query_one(
            "SELECT count(*) FROM koldstore.cold_segments \
             WHERE table_oid = $1::text::regclass::oid AND status = 'active'",
            &[&managed],
        )
        .await?
        .get(0);
    ensure!(cold > 0, "expected cold segments after the flush");

    let mut failures = Vec::new();
    compare_all(&db.client, &managed, &reference, "all cold", &mut failures).await?;

    // Hydrating updates move rows hot and leave their old cold versions behind: the old values
    // must stop matching, the new ones must start matching, in both tables identically.
    let dml = |table: &str| {
        format!(
            "UPDATE {table} SET doc = jsonb_build_object('k', 3, 'flag', true), \
               tags = ARRAY['5', '7'], qty = 99, note = 'alpha omega', \
               name = 'name-' || lpad((id + 100000)::text, 6, '0') WHERE id % 40 = 0"
        )
    };
    let delete = |table: &str| format!("DELETE FROM {table} WHERE id % 97 = 0");
    db.client
        .batch_execute("SET koldstore.hydrate_on_write = on")
        .await?;
    db.client.batch_execute(&dml(&managed)).await?;
    db.client.batch_execute(&delete(&managed)).await?;
    db.client
        .batch_execute("RESET koldstore.hydrate_on_write")
        .await?;
    db.client.batch_execute(&dml(&reference)).await?;
    db.client.batch_execute(&delete(&reference)).await?;
    common::fence_async_mirror(&db.client).await?;
    compare_all(&db.client, &managed, &reference, "hot+cold", &mut failures).await?;

    db.flush_table(&managed).await?;
    common::fence_async_mirror(&db.client).await?;
    compare_all(&db.client, &managed, &reference, "reflushed", &mut failures).await?;

    let diff: i64 = db
        .client
        .query_one(
            &format!(
                "SELECT (SELECT count(*) FROM (SELECT * FROM {managed} EXCEPT SELECT * FROM {reference}) a) \
                      + (SELECT count(*) FROM (SELECT * FROM {reference} EXCEPT SELECT * FROM {managed}) b)"
            ),
            &[],
        )
        .await?
        .get(0);
    if diff != 0 {
        failures.push(format!("full-row comparison differs in {diff} rows"));
    }

    ensure!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}
