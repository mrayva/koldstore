//! Feeds a column of Arrow batches into a [`ValueSummaryBuilder`] (catalog point-lookup pruning).
//!
//! Values are hashed through the same Sort Key V1 encoding that produces the catalog `min_value` /
//! `max_value` bounds, so the planner's probe bytes and the stored summary agree by construction.
//! Only the dominant primary-key types are supported; any other type simply gets no summary, which
//! is always safe (a segment without a summary is never pruned).

use arrow_array::{Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray};
use koldstore_sortkey::{
    SortKeyType, SortKeyValue, ValueSummaryBuilder, encode_sort_key, encode_sort_key_pg_text,
};

/// True when summaries can be built for columns of this Sort Key type.
#[must_use]
pub const fn supports_value_summary(ty: SortKeyType) -> bool {
    matches!(
        ty,
        SortKeyType::Int2 | SortKeyType::Int4 | SortKeyType::Int8 | SortKeyType::Uuid
    )
}

/// Adds every non-null value of `column` in `batch` to `builder`.
///
/// # Errors
///
/// Returns an error when the column is missing, has an unexpected Arrow type, or a value cannot be
/// encoded. The caller must then discard the summary for the whole segment: a partially filled
/// summary would produce false negatives.
pub fn add_column_to_value_summary(
    builder: &mut ValueSummaryBuilder,
    batch: &RecordBatch,
    column: &str,
    ty: SortKeyType,
) -> Result<(), String> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| format!("value summary column `{column}` is missing from the batch"))?;
    let mismatch =
        || format!("value summary column `{column}` has an unexpected Arrow type for {ty:?}");
    let encode = |value: SortKeyValue| encode_sort_key(&value).map_err(|error| error.to_string());
    match ty {
        SortKeyType::Int2 => {
            let values = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(mismatch)?;
            for index in (0..values.len()).filter(|i| values.is_valid(*i)) {
                builder.insert(&encode(SortKeyValue::Int2(values.value(index)))?);
            }
        }
        SortKeyType::Int4 => {
            let values = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(mismatch)?;
            for index in (0..values.len()).filter(|i| values.is_valid(*i)) {
                builder.insert(&encode(SortKeyValue::Int4(values.value(index)))?);
            }
        }
        SortKeyType::Int8 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(mismatch)?;
            for index in (0..values.len()).filter(|i| values.is_valid(*i)) {
                builder.insert(&encode(SortKeyValue::Int8(values.value(index)))?);
            }
        }
        SortKeyType::Uuid => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(mismatch)?;
            for index in (0..values.len()).filter(|i| values.is_valid(*i)) {
                let bytes = encode_sort_key_pg_text(SortKeyType::Uuid, values.value(index))
                    .map_err(|error| error.to_string())?;
                builder.insert(&bytes);
            }
        }
        other => return Err(format!("value summaries are not supported for {other:?}")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use koldstore_sortkey::{SortKeyValue, encode_sort_key, summary_may_contain};

    use super::*;

    fn batch(ids: &[Option<i64>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let column: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        RecordBatch::try_new(schema, vec![column]).unwrap()
    }

    #[test]
    fn int8_values_round_trip_through_the_planner_encoding() {
        let mut builder = ValueSummaryBuilder::new();
        add_column_to_value_summary(
            &mut builder,
            &batch(&[Some(7), Some(-3), None, Some(1_000_000_007)]),
            "id",
            SortKeyType::Int8,
        )
        .unwrap();
        assert_eq!(builder.len(), 3, "nulls are skipped");
        let summary = builder.finish().unwrap();
        for id in [7_i64, -3, 1_000_000_007] {
            // The probe bytes are exactly what the planner derives from a `WHERE id = $1` bound.
            let probe = encode_sort_key(&SortKeyValue::Int8(id)).unwrap();
            assert!(summary_may_contain(&summary, &probe), "id {id}");
        }
    }

    #[test]
    fn unsupported_types_and_wrong_arrow_types_error_instead_of_guessing() {
        let mut builder = ValueSummaryBuilder::new();
        assert!(
            add_column_to_value_summary(&mut builder, &batch(&[Some(1)]), "id", SortKeyType::Date)
                .is_err()
        );
        assert!(
            add_column_to_value_summary(&mut builder, &batch(&[Some(1)]), "id", SortKeyType::Int4)
                .is_err()
        );
        assert!(
            add_column_to_value_summary(
                &mut builder,
                &batch(&[Some(1)]),
                "nope",
                SortKeyType::Int8
            )
            .is_err()
        );
        assert!(!supports_value_summary(SortKeyType::Timestamptz));
        assert!(supports_value_summary(SortKeyType::Uuid));
    }
}
