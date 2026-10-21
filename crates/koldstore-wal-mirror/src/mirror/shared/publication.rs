//! Publication DDL helpers for async-mirror column lists (PostgreSQL-free).

use koldstore_common::{quote_ident, PrimaryKeyShape};

/// Quoted PK (+ optional segment-order) column list for `ALTER PUBLICATION … SET TABLE`.
#[must_use]
pub fn published_column_list(primary_key: &PrimaryKeyShape, order_column: Option<&str>) -> String {
    published_column_list_with_scope(primary_key, order_column, None)
}

/// [`published_column_list`] plus the scope column of a user-scoped table, so capture can record each
/// row's scope in the mirror (ADR-010).
#[must_use]
pub fn published_column_list_with_scope(
    primary_key: &PrimaryKeyShape,
    order_column: Option<&str>,
    scope_column: Option<&str>,
) -> String {
    let mut published = primary_key
        .columns()
        .iter()
        .map(|column| quote_ident(column.column().as_str()))
        .collect::<Vec<_>>();
    for extra in [order_column, scope_column].into_iter().flatten() {
        let quoted = quote_ident(extra);
        if !published.iter().any(|column| column == &quoted) {
            published.push(quoted);
        }
    }
    published.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use koldstore_common::{
        ColumnId, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
        PrimaryKeyShape,
    };

    fn shape(columns: &[&str]) -> PrimaryKeyShape {
        let cols = columns
            .iter()
            .enumerate()
            .map(|(i, name)| {
                PrimaryKeyColumnShape::new(
                    ColumnId::from_attnum(i as i16 + 1),
                    PkColumn::new(*name).unwrap(),
                    PkOrdinal::new(i as u16 + 1).unwrap(),
                    PgTypeOid::new(23).unwrap(),
                    PgTypeName::new("int4").unwrap(),
                    PgTypmod::new(-1),
                    None,
                    None,
                    true,
                )
            })
            .collect();
        PrimaryKeyShape::new(cols).unwrap()
    }

    #[test]
    fn includes_the_scope_column_once() {
        let pk = shape(&["id"]);
        assert_eq!(
            published_column_list_with_scope(&pk, Some("n"), Some("user_id")),
            format!("{}, {}, {}", quote_ident("id"), quote_ident("n"), quote_ident("user_id"))
        );
        assert_eq!(published_column_list_with_scope(&pk, None, Some("id")), quote_ident("id"));
    }

    #[test]
    fn includes_order_column_once() {
        let pk = shape(&["id"]);
        assert_eq!(published_column_list(&pk, Some("id")), quote_ident("id"));
        assert_eq!(
            published_column_list(&pk, Some("seg_order")),
            format!("{}, {}", quote_ident("id"), quote_ident("seg_order"))
        );
    }
}
