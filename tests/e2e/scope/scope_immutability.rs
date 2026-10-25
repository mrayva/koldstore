//! A key has exactly one scope: cold segments, delete markers and scope-pruned reads all assume
//! the scope column never moves, so an `UPDATE` that changes it is refused.

use anyhow::{ensure, Result};

use crate::common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn updating_the_scope_column_is_refused() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix().into_iter().next().expect("PostgreSQL target");
    let db = common::TestDb::start(target, "scope_immutable").await?;
    let table = db.create_user_notes_table("scope_immutable_notes").await?;
    let relation = table.relation.clone();
    db.manage_user_scoped(&relation, "user_id").await?;
    let moved = db
        .client
        .batch_execute(&format!("UPDATE {relation} SET user_id = 'user-b' WHERE id = 1"))
        .await;
    let error = moved.expect_err("moving a row to another scope must fail");
    ensure!(
        error.code().map(|code| code.code()) == Some("0A000"),
        "unexpected error: {error:?}"
    );
    ensure!(
        error.to_string().contains("scope column") || format!("{error:?}").contains("scope column"),
        "message does not name the scope column: {error:?}"
    );

    // Other columns, and rewriting the scope to its current value, stay allowed.
    db.client
        .batch_execute(&format!("UPDATE {relation} SET title = 'edited' WHERE id = 1"))
        .await?;
    db.client
        .batch_execute(&format!("UPDATE {relation} SET user_id = user_id WHERE id = 1"))
        .await?;
    let scope: String = db
        .client
        .query_one(&format!("SELECT user_id FROM {relation} WHERE id = 1"), &[])
        .await?
        .get(0);
    ensure!(scope == "user-a", "scope changed to {scope}");
    Ok(())
}
