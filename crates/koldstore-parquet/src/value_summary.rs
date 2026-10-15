//! Feeds a column of Arrow batches into a [`ValueSummaryBuilder`] (catalog point-lookup pruning).
//!
//! Values are hashed through the same Sort Key V1 encoding that produces the catalog `min_value` /
//! `max_value` bounds, so the planner's probe bytes and the stored summary agree by construction.
//! Only the dominant primary-key types are supported; any other type simply gets no summary, which
//! is always safe (a segment without a summary is never pruned).

use arrow_array::{
    Array, Date32Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use koldstore_sortkey::{
    SortKeyType, SortKeyValue, ValueSummaryBuilder, encode_sort_key, encode_sort_key_pg_text,
    unix_days_to_pg, unix_micros_to_pg,
};

/// True when summaries can be built for columns of this Sort Key type.
///
/// A key made of several columns qualifies when each of them does.
#[must_use]
pub const fn supports_value_summary(ty: SortKeyType) -> bool {
    matches!(
        ty,
        SortKeyType::Int2
            | SortKeyType::Int4
            | SortKeyType::Int8
            | SortKeyType::Uuid
            | SortKeyType::Date
            | SortKeyType::Timestamp
            | SortKeyType::Timestamptz
    )
}

/// The Sort Key V1 bytes of row `index` of `array`, or `None` for a null.
fn encode_row(
    array: &dyn Array,
    ty: SortKeyType,
    index: usize,
    column: &str,
) -> Result<Option<Vec<u8>>, String> {
    if array.is_null(index) {
        return Ok(None);
    }
    let mismatch = || format!("value summary column `{column}` has an unexpected Arrow type for {ty:?}");
    let encode = |value: SortKeyValue| encode_sort_key(&value).map_err(|error| error.to_string());
    let bytes = match ty {
        SortKeyType::Int2 => encode(SortKeyValue::Int2(
            array.as_any().downcast_ref::<Int16Array>().ok_or_else(mismatch)?.value(index),
        ))?,
        SortKeyType::Int4 => encode(SortKeyValue::Int4(
            array.as_any().downcast_ref::<Int32Array>().ok_or_else(mismatch)?.value(index),
        ))?,
        SortKeyType::Int8 => encode(SortKeyValue::Int8(
            array.as_any().downcast_ref::<Int64Array>().ok_or_else(mismatch)?.value(index),
        ))?,
        SortKeyType::Uuid => {
            let text = array.as_any().downcast_ref::<StringArray>().ok_or_else(mismatch)?.value(index);
            encode_sort_key_pg_text(SortKeyType::Uuid, text).map_err(|error| error.to_string())?
        }
        // Arrow holds Unix-epoch values; the planner's probe is PostgreSQL-epoch (infinity-safe).
        SortKeyType::Date => encode(SortKeyValue::Date(unix_days_to_pg(
            array.as_any().downcast_ref::<Date32Array>().ok_or_else(mismatch)?.value(index),
        )))?,
        SortKeyType::Timestamp => encode(SortKeyValue::Timestamp(unix_micros_to_pg(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(mismatch)?
                .value(index),
        )))?,
        SortKeyType::Timestamptz => encode(SortKeyValue::Timestamptz(unix_micros_to_pg(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(mismatch)?
                .value(index),
        )))?,
        other => return Err(format!("value summaries are not supported for {other:?}")),
    };
    Ok(Some(bytes))
}

/// Adds the key of every row of `batch` to `builder`, where a row's key is the Sort Key V1 bytes of
/// its `columns`, in order, back to back.
///
/// Every supported type encodes to a fixed width, so the concatenation is unambiguous, and for a
/// single column it is exactly that column's encoding (what summaries of single-column keys have
/// always hashed). A row with a null in any of the columns contributes nothing; the caller notices
/// the shortfall and drops the summary.
///
/// # Errors
///
/// Returns an error when a column is missing, has an unexpected Arrow type, or a value cannot be
/// encoded. The caller must then discard the summary for the whole segment: a partially filled
/// summary would produce false negatives.
pub fn add_key_columns_to_value_summary(
    builder: &mut ValueSummaryBuilder,
    batch: &RecordBatch,
    columns: &[(&str, SortKeyType)],
) -> Result<(), String> {
    if columns.is_empty() {
        return Err("a value summary needs at least one key column".to_string());
    }
    let arrays = columns
        .iter()
        .map(|(name, _)| {
            batch
                .column_by_name(name)
                .ok_or_else(|| format!("value summary column `{name}` is missing from the batch"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    'rows: for index in 0..batch.num_rows() {
        let mut key = Vec::new();
        for ((name, ty), array) in columns.iter().zip(&arrays) {
            match encode_row(array.as_ref(), *ty, index, name)? {
                Some(bytes) => key.extend_from_slice(&bytes),
                None => continue 'rows,
            }
        }
        builder.insert(&key);
    }
    Ok(())
}

/// Adds every non-null value of `column` in `batch` to `builder` (a single-column key).
///
/// # Errors
///
/// As [`add_key_columns_to_value_summary`].
pub fn add_column_to_value_summary(
    builder: &mut ValueSummaryBuilder,
    batch: &RecordBatch,
    column: &str,
    ty: SortKeyType,
) -> Result<(), String> {
    add_key_columns_to_value_summary(builder, batch, &[(column, ty)])
}

/// A summary rebuilt from an existing Parquet object.
#[derive(Debug, PartialEq, Eq)]
pub struct RebuiltSummary {
    /// The bitmap, or `None` when the segment is too large to carry one (see `SUMMARY_MAX_ROWS`).
    pub summary: Option<Vec<u8>>,
    /// Rows the object holds (the Parquet footer's row count).
    pub rows: usize,
}

/// Rebuilds the value summary of a key from a whole Parquet object, reading only the key columns.
///
/// This is what the flush writer computes while it writes a segment, recomputed for segments written
/// before summaries existed. The summary covers every row in the object (delete markers included), so
/// the result is trusted only when every row contributed a key.
///
/// # Errors
///
/// Returns an error when the object cannot be read, a column is missing or has an unexpected Arrow
/// type, or a row has no key (a partially filled summary would produce false negatives).
pub fn rebuild_value_summary_from_parquet(
    data: impl Into<bytes::Bytes>,
    columns: &[(&str, SortKeyType)],
) -> Result<RebuiltSummary, String> {
    use parquet::arrow::ProjectionMask;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let builder = ParquetRecordBatchReaderBuilder::try_new(data.into())
        .map_err(|error| format!("cannot read parquet object: {error}"))?;
    let roots = columns
        .iter()
        .map(|(name, _)| {
            builder
                .schema()
                .index_of(name)
                .map_err(|_| format!("column `{name}` is not in the object"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|error| format!("cannot read parquet object: {error}"))?;

    let mut summary = ValueSummaryBuilder::new();
    let mut rows = 0_usize;
    for batch in reader {
        let batch = batch.map_err(|error| format!("cannot read parquet object: {error}"))?;
        rows += batch.num_rows();
        add_key_columns_to_value_summary(&mut summary, &batch, columns)?;
    }
    // An oversized segment stops collecting (len 0) and gets no summary; otherwise every row must
    // have contributed a key.
    if summary.len() != 0 && summary.len() != rows {
        return Err(format!(
            "{} of {rows} rows have a null key column; refusing a partial summary",
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
        assert!(supports_value_summary(SortKeyType::Timestamptz));
        assert!(supports_value_summary(SortKeyType::Uuid));
        assert!(!supports_value_summary(SortKeyType::Bool));
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
            &[("id", SortKeyType::Int8)],
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
            &[("nope", SortKeyType::Int8)],
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
            &[("id", SortKeyType::Int8)],
        )
        .unwrap_err();
        assert!(error.contains("partial summary"), "{error}");
    }

    #[test]
    fn a_single_column_key_hashes_exactly_its_own_encoding() {
        // Existing single-column summaries must stay valid: the key is that column's bytes, nothing more.
        let batch = batch(&[Some(7)]);
        let mut builder = ValueSummaryBuilder::new();
        add_key_columns_to_value_summary(&mut builder, &batch, &[("id", SortKeyType::Int8)]).unwrap();
        let summary = builder.finish().unwrap();
        assert!(summary_may_contain(&summary, &encode_sort_key(&SortKeyValue::Int8(7)).unwrap()));
    }

    #[test]
    fn a_composite_key_is_its_columns_back_to_back_and_order_matters() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30]));
        let batch = RecordBatch::try_new(schema, vec![a, b]).unwrap();
        let mut builder = ValueSummaryBuilder::new();
        add_key_columns_to_value_summary(
            &mut builder,
            &batch,
            &[("a", SortKeyType::Int4), ("b", SortKeyType::Int8)],
        )
        .unwrap();
        assert_eq!(builder.len(), 3);
        let summary = builder.finish().unwrap();
        let key = |a: i32, b: i64| {
            let mut bytes = encode_sort_key(&SortKeyValue::Int4(a)).unwrap();
            bytes.extend(encode_sort_key(&SortKeyValue::Int8(b)).unwrap());
            bytes
        };
        for (a, b) in [(1, 10), (2, 20), (3, 30)] {
            assert!(summary_may_contain(&summary, &key(a, b)), "({a},{b})");
        }
        // Pairs that exist only half-way (right first column, wrong second) are not members.
        let absent = [(1, 20), (2, 30), (3, 10), (9, 9)];
        assert!(
            absent.iter().filter(|(a, b)| summary_may_contain(&summary, &key(*a, *b))).count() < absent.len(),
            "a 64-bit-minimum bitmap over three keys should reject at least one absent pair"
        );
    }

    #[test]
    fn dates_and_timestamps_are_summarised_in_postgres_epoch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, false),
            Field::new("ts", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), false),
        ]));
        // 2020-01-05 is 18_266 days after 1970-01-01 and 7_309 after 2000-01-01; infinity is the extreme.
        let d: ArrayRef = Arc::new(Date32Array::from(vec![18_266, i32::MAX]));
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1_578_182_400_000_000_i64, i64::MAX]));
        let batch = RecordBatch::try_new(schema, vec![d, ts]).unwrap();

        let mut dates = ValueSummaryBuilder::new();
        add_key_columns_to_value_summary(&mut dates, &batch, &[("d", SortKeyType::Date)]).unwrap();
        let dates = dates.finish().unwrap();
        for value in [7_309, i32::MAX] {
            assert!(summary_may_contain(&dates, &encode_sort_key(&SortKeyValue::Date(value)).unwrap()), "{value}");
        }

        let mut stamps = ValueSummaryBuilder::new();
        add_key_columns_to_value_summary(&mut stamps, &batch, &[("ts", SortKeyType::Timestamptz)]).unwrap();
        let stamps = stamps.finish().unwrap();
        // 7_309 days after 2000-01-01, in microseconds.
        for value in [7_309_i64 * 86_400_000_000, i64::MAX] {
            assert!(
                summary_may_contain(&stamps, &encode_sort_key(&SortKeyValue::Timestamptz(value)).unwrap()),
                "{value}"
            );
        }
    }
}
