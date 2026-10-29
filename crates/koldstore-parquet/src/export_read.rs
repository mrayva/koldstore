//! Reads the plain Parquet files written by `koldstore.export_table`.
//!
//! An export file holds only the table's own columns (no sequence, operation or schema-version
//! bookkeeping), so the clean cold-segment reader does not apply. Rows are decoded with the same
//! per-type JSON codec the cold read path uses.

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::pg_type_codec::json_from_arrow_cell;
use crate::schema::PgColumn;
use koldstore_schema::PgType;

/// Decodes `bytes` batch by batch and hands each batch's rows (JSON objects keyed by column name)
/// to `sink`. Every column in `columns` must be present in the file.
///
/// # Errors
///
/// Returns an error when the bytes are not Parquet, a column is missing, a cell cannot be decoded,
/// or `sink` fails.
pub fn read_export_file_rows(
    bytes: Vec<u8>,
    columns: &[PgColumn],
    mut sink: impl FnMut(Vec<serde_json::Value>) -> Result<(), String>,
) -> Result<u64, String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .map_err(|error| error.to_string())?
        .build()
        .map_err(|error| error.to_string())?;
    let mut total = 0_u64;
    for batch in reader {
        let batch = batch.map_err(|error| error.to_string())?;
        let mut arrays = Vec::with_capacity(columns.len());
        for column in columns {
            arrays.push(
                batch
                    .column_by_name(&column.name)
                    .ok_or_else(|| format!("export file is missing column `{}`", column.name))?,
            );
        }
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row_index in 0..batch.num_rows() {
            let mut object = serde_json::Map::with_capacity(columns.len());
            for (column, array) in columns.iter().zip(&arrays) {
                let mut value =
                    json_from_arrow_cell(column.pg_type, &column.name, array.as_ref(), row_index)?;
                // json/jsonb cells are stored as JSON text; embed the document itself, or the
                // target column would receive a JSON string holding the text.
                if matches!(column.pg_type, PgType::Jsonb | PgType::Json) {
                    if let serde_json::Value::String(text) = &value {
                        value = serde_json::from_str(text).map_err(|error| {
                            format!("column `{}`: invalid JSON cell: {error}", column.name)
                        })?;
                    }
                }
                object.insert(column.name.clone(), value);
            }
            rows.push(serde_json::Value::Object(object));
        }
        total += rows.len() as u64;
        sink(rows)?;
    }
    Ok(total)
}
