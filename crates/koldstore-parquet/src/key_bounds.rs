//! Tuple-level segment bounds of a composite primary key.
//!
//! Per-column bounds only support a box test ("two segments can share a key if their intervals
//! intersect on every key column"), which stays pessimistic after a sorted rewrite whenever a leading
//! value straddles two segments. The smallest and largest key *tuple* of a segment, compared
//! lexicographically, is exact: segments sorted by that minimum are key-disjoint iff each maximum is
//! below the next minimum.
//!
//! A tuple is encoded by framing each column's Sort Key V1 bytes (`0x00` becomes `0x00 0xFF`, then a
//! `0x00 0x01` terminator) and concatenating, so byte order of the result is the lexicographic order
//! of the tuple (a shorter component sorts before any extension of it) whatever the component widths.
//! Text uses raw UTF-8 and numeric the decimal order encoding, so the order is internally consistent
//! rather than collation order -- which is all a disjointness test needs.

use arrow_array::{Array, RecordBatch, StringArray};
use koldstore_sortkey::{SortKeyType, encode_numeric_key};

use crate::value_summary::encode_row;

/// True when a column of this type can be part of a tuple-bounded key.
#[must_use]
pub const fn supports_key_bounds(ty: SortKeyType) -> bool {
    !matches!(ty, SortKeyType::Bool)
}

/// Appends one column's encoding to a tuple key, escaped and terminated so concatenation preserves
/// lexicographic order.
pub fn frame_component(out: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        out.push(*byte);
        if *byte == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x01]);
}

fn encode_component(
    array: &dyn Array,
    ty: SortKeyType,
    index: usize,
    column: &str,
) -> Result<Option<Vec<u8>>, String> {
    match ty {
        SortKeyType::Text | SortKeyType::Numeric => {
            if array.is_null(index) {
                return Ok(None);
            }
            let text = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("key bounds column `{column}` has an unexpected Arrow type for {ty:?}"))?
                .value(index);
            if ty == SortKeyType::Numeric {
                encode_numeric_key(text).map(Some).map_err(|error| error.to_string())
            } else {
                Ok(Some(text.as_bytes().to_vec()))
            }
        }
        other => encode_row(array, other, index, column),
    }
}

/// Running minimum and maximum of the tuple keys of one segment.
#[derive(Debug, Default)]
pub struct KeyBoundsBuilder {
    min: Option<Vec<u8>>,
    max: Option<Vec<u8>>,
    rows: usize,
}

impl KeyBoundsBuilder {
    /// An empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds every row of `batch`, whose key is `columns` in key order.
    ///
    /// # Errors
    ///
    /// Returns an error when a column is missing, has an unexpected Arrow type, or a row has a null
    /// key column. The caller then discards the bounds for the whole segment: partial bounds would
    /// claim a narrower range than the segment holds.
    pub fn add_batch(&mut self, batch: &RecordBatch, columns: &[(&str, SortKeyType)]) -> Result<(), String> {
        if columns.is_empty() {
            return Err("key bounds need at least one key column".to_string());
        }
        let arrays = columns
            .iter()
            .map(|(name, _)| {
                batch
                    .column_by_name(name)
                    .ok_or_else(|| format!("key bounds column `{name}` is missing from the batch"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for index in 0..batch.num_rows() {
            let mut key = Vec::new();
            for ((name, ty), array) in columns.iter().zip(&arrays) {
                let Some(bytes) = encode_component(array.as_ref(), *ty, index, name)? else {
                    return Err(format!("null in key column `{name}`"));
                };
                frame_component(&mut key, &bytes);
            }
            if self.min.as_ref().is_none_or(|min| key < *min) {
                self.min = Some(key.clone());
            }
            if self.max.as_ref().is_none_or(|max| key > *max) {
                self.max = Some(key);
            }
            self.rows += 1;
        }
        Ok(())
    }

    /// Rows added so far.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// The bounds, trusted only when exactly `row_count` keys contributed.
    #[must_use]
    pub fn finish(self, row_count: usize) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.rows != row_count {
            return None;
        }
        self.min.zip(self.max)
    }
}

/// Recomputes a segment's tuple bounds from its Parquet object, reading only the key columns (for
/// segments written before the bounds existed).
///
/// # Errors
///
/// Returns an error when the object cannot be read, a column is missing or has an unexpected Arrow
/// type, or a row has a null key column.
pub fn rebuild_key_bounds_from_parquet(
    data: impl Into<bytes::Bytes>,
    columns: &[(&str, SortKeyType)],
) -> Result<Option<(Vec<u8>, Vec<u8>)>, String> {
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
    let mut bounds = KeyBoundsBuilder::new();
    let mut rows = 0_usize;
    for batch in reader {
        let batch = batch.map_err(|error| format!("cannot read parquet object: {error}"))?;
        rows += batch.num_rows();
        bounds.add_batch(&batch, columns)?;
    }
    Ok(bounds.finish(rows))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn batch(tenants: &[i32], ids: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Int32, false),
            Field::new("id", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(tenants.to_vec())), Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap()
    }

    const COLUMNS: &[(&str, SortKeyType)] = &[("tenant", SortKeyType::Int4), ("id", SortKeyType::Int8)];

    #[test]
    fn bounds_are_the_lexicographic_extremes_of_the_tuples() {
        let mut builder = KeyBoundsBuilder::new();
        builder.add_batch(&batch(&[2, 1, 3, 1], &[10, 99, 1, 5]), COLUMNS).unwrap();
        let (min, max) = builder.finish(4).unwrap();
        let mut expected_min = Vec::new();
        let mut expected_max = Vec::new();
        for (tenant, id) in [(1_i32, 5_i64), (3, 1)] {
            let (target, _) = if tenant == 1 { (&mut expected_min, ()) } else { (&mut expected_max, ()) };
            frame_component(target, &koldstore_sortkey::encode_sort_key(&koldstore_sortkey::SortKeyValue::Int4(tenant)).unwrap());
            frame_component(target, &koldstore_sortkey::encode_sort_key(&koldstore_sortkey::SortKeyValue::Int8(id)).unwrap());
        }
        assert_eq!(min, expected_min);
        assert_eq!(max, expected_max);
        assert!(min < max);
    }

    #[test]
    fn a_shared_leading_value_is_ordered_by_the_next_column() {
        // (3, 50..100) and (3, 1..49): the per-column boxes would overlap, the tuples do not
        let mut first = KeyBoundsBuilder::new();
        first.add_batch(&batch(&[3, 3], &[1, 49]), COLUMNS).unwrap();
        let mut second = KeyBoundsBuilder::new();
        second.add_batch(&batch(&[3, 3], &[50, 100]), COLUMNS).unwrap();
        let (_, first_max) = first.finish(2).unwrap();
        let (second_min, _) = second.finish(2).unwrap();
        assert!(first_max < second_min);
    }

    #[test]
    fn framing_keeps_a_shorter_component_below_its_extensions() {
        let mut a = Vec::new();
        frame_component(&mut a, b"a");
        let mut ab = Vec::new();
        frame_component(&mut ab, b"ab");
        let mut a_nul = Vec::new();
        frame_component(&mut a_nul, b"a\0");
        assert!(a < ab);
        assert!(a < a_nul);
        assert!(a_nul < ab);
    }

    #[test]
    fn an_incomplete_count_yields_no_bounds() {
        let mut builder = KeyBoundsBuilder::new();
        builder.add_batch(&batch(&[1], &[1]), COLUMNS).unwrap();
        assert!(builder.finish(2).is_none());
    }
}
