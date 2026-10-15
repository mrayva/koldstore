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

/// A summary rebuilt from an existing Parquet object.
#[derive(Debug, PartialEq, Eq)]
pub struct RebuiltSummary {
    /// The bitmap, or `None` when the segment is too large to carry one (see `SUMMARY_MAX_ROWS`).
    pub summary: Option<Vec<u8>>,
    /// Rows the object holds (the Parquet footer's row count).
    pub rows: usize,
}

/// Rebuilds the value summary of `column` from a whole Parquet object, reading only that column.
///
/// This is what the flush writer computes while it writes a segment, recomputed for segments written
/// before summaries existed. The summary covers every row in the object (delete markers included), so
/// the result is trusted only when every row contributed a value.
///
/// # Errors
///
/// Returns an error when the object cannot be read, the column is missing or has an unexpected Arrow
/// type, or a row has no value (a partially filled summary would produce false negatives).
pub fn rebuild_value_summary_from_parquet(
    data: impl Into<bytes::Bytes>,
    column: &str,
    ty: SortKeyType,
) -> Result<RebuiltSummary, String> {
    use parquet::arrow::ProjectionMask;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let builder = ParquetRecordBatchReaderBuilder::try_new(data.into())
        .map_err(|error| format!("cannot read parquet object: {error}"))?;
    let root = builder
        .schema()
        .index_of(column)
        .map_err(|_| format!("column `{column}` is not in the object"))?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), [root]);
    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|error| format!("cannot read parquet object: {error}"))?;

    let mut summary = ValueSummaryBuilder::new();
    let mut rows = 0_usize;
    for batch in reader {
        let batch = batch.map_err(|error| format!("cannot read parquet object: {error}"))?;
        rows += batch.num_rows();
        add_column_to_value_summary(&mut summary, &batch, column, ty)?;
    }
    // An oversized segment stops collecting (len 0) and gets no summary; otherwise every row must
    // have contributed a value.
    if summary.len() != 0 && summary.len() != rows {
        return Err(format!(
            "{} of {rows} rows have no value in `{column}`; refusing a partial summary",
            rows - summary.len()
        ));
    }
    Ok(RebuiltSummary {
        summary: summary.finish(),
        rows,
    })
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

    #[test]
    fn a_parquet_object_rebuilds_the_same_summary_the_writer_would() {
        use parquet::arrow::ArrowWriter;

        let ids: Vec<Option<i64>> = (0..500).map(|n| Some(n * 31)).collect();
        let batch = batch(&ids);
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let rebuilt = rebuild_value_summary_from_parquet(
            bytes::Bytes::from(buffer.clone()),
            "id",
            SortKeyType::Int8,
        )
        .unwrap();
        assert_eq!(rebuilt.rows, 500);
        let summary = rebuilt.summary.expect("fits under the cap");
        for id in ids.iter().flatten() {
            let probe = encode_sort_key(&SortKeyValue::Int8(*id)).unwrap();
            assert!(summary_may_contain(&summary, &probe), "id {id}");
        }

        // Identical to feeding the same batch to the builder directly (what flush does).
        let mut direct = ValueSummaryBuilder::new();
        add_column_to_value_summary(&mut direct, &batch, "id", SortKeyType::Int8).unwrap();
        assert_eq!(Some(summary), direct.finish());

        let missing = rebuild_value_summary_from_parquet(
            bytes::Bytes::from(buffer),
            "nope",
            SortKeyType::Int8,
        );
        assert!(missing.unwrap_err().contains("not in the object"));
    }

    #[test]
    fn a_null_key_makes_the_rebuild_refuse_rather_than_guess() {
        use parquet::arrow::ArrowWriter;

        let batch = batch(&[Some(1), None, Some(3)]);
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let error = rebuild_value_summary_from_parquet(
            bytes::Bytes::from(buffer),
            "id",
            SortKeyType::Int8,
        )
        .unwrap_err();
        assert!(error.contains("partial summary"), "{error}");
    }
}
