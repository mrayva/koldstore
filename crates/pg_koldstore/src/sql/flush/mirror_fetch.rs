//! SPI mirror-row fetch and typed tuple decode for flush.

use koldstore_common::{CellValue, SqlParamType, SqlStatement};
use koldstore_flush::MirrorFlushPageCursor;
use koldstore_migrate::order::CatalogColumn;
use koldstore_parquet::{jsonb_cell_to_utf8, pg_bytea_hex, FlushMirrorRow};
use koldstore_schema::PgType;
use pgrx::datum::DatumWithOid;

/// Maps a catalog column type onto a flush keyset [`SqlParamType`].
///
/// # Errors
///
/// Returns an error when the type cannot be bound for ordered flush keyset paging.
pub(super) fn flush_keyset_param_type(pg_type: PgType) -> Result<SqlParamType, String> {
    match pg_type {
        PgType::Bool => Ok(SqlParamType::Boolean),
        PgType::Int2 | PgType::Int4 => Ok(SqlParamType::Integer),
        PgType::Int8 => Ok(SqlParamType::BigInt),
        PgType::Numeric => Ok(SqlParamType::Numeric),
        PgType::Text
        | PgType::Jsonb
        | PgType::Bytea
        | PgType::TextArray
        | PgType::Vector
        | PgType::Tsvector
        | PgType::Json
        | PgType::Inet
        | PgType::Cidr
        | PgType::Macaddr
        | PgType::Bpchar
        | PgType::Time
        | PgType::Timetz
        | PgType::Interval
        | PgType::Int2Array
        | PgType::Int4Array
        | PgType::Int8Array
        | PgType::Float4Array
        | PgType::Float8Array
        | PgType::NumericArray
        | PgType::UuidArray
        | PgType::BoolArray
        | PgType::DateArray
        | PgType::TimestampArray
        | PgType::TimestamptzArray
        | PgType::IntervalArray
        | PgType::Bit
        | PgType::Varbit
        | PgType::Macaddr8
        | PgType::Name
        | PgType::Oid | PgType::Xml | PgType::Int4Range | PgType::Int8Range | PgType::NumRange | PgType::TsRange | PgType::TstzRange | PgType::DateRange | PgType::Point | PgType::Lseg | PgType::Path | PgType::Box | PgType::Polygon | PgType::Line | PgType::Circle | PgType::Enum | PgType::Composite | PgType::Money => {
            Ok(SqlParamType::Text)
        }
        PgType::Uuid => Ok(SqlParamType::Uuid),
        PgType::Date => Ok(SqlParamType::Date),
        PgType::Timestamp => Ok(SqlParamType::Timestamp),
        PgType::Timestamptz => Ok(SqlParamType::TimestampWithTimeZone),
        PgType::Float4 => Ok(SqlParamType::Float4),
        PgType::Float8 => Ok(SqlParamType::Float8),
    }
}

/// Owned SPI bind values so [`DatumWithOid`] can borrow without temporary drop.
enum OwnedBind {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Text(String),
    Bytes(Vec<u8>),
    Float4(f32),
    Float8(f64),
    Uuid(pgrx::Uuid),
    /// `date` as days since the PostgreSQL epoch.
    Date(i32),
    /// `timestamp` as microseconds since the PostgreSQL epoch.
    Timestamp(i64),
    /// `timestamptz` as microseconds since the PostgreSQL epoch.
    Timestamptz(i64),
}

/// Fetches one keyset page of mirror rows selected for flush.
///
/// `fetch_limit` is the SPI `LIMIT` (typically
/// [`koldstore_flush::flush_mirror_fetch_limit`]) so callers can keep peak
/// decode memory near one Parquet segment.
///
/// # Errors
///
/// Returns an error when SPI preparation or execution fails.
pub(super) fn fetch_mirror_batch(
    columns: &[CatalogColumn],
    primary_key_columns: &[String],
    statement: &SqlStatement,
    max_seq: i64,
    cursor: &MirrorFlushPageCursor,
    fetch_limit: i64,
    include_order_key: bool,
    include_scope: bool,
) -> Result<Vec<FlushMirrorRow>, String> {
    let limit = fetch_limit.max(1);
    let spi_statement = crate::spi::SpiStatement::read_with_params(
        statement.operation.as_str(),
        &statement.sql,
        statement.param_types.clone(),
    )
    .map_err(|error| error.to_string())?;

    let owned = build_owned_binds(columns, primary_key_columns, max_seq, cursor, limit)?;
    let args = owned_binds_to_datums(&owned);
    crate::spi::execute_prepared(&spi_statement, &args, |tuples| {
        decode_mirror_batch(tuples, columns, include_order_key, include_scope)
    })
    .map_err(|error| error.to_string())
}

fn build_owned_binds(
    columns: &[CatalogColumn],
    primary_key_columns: &[String],
    max_seq: i64,
    cursor: &MirrorFlushPageCursor,
    limit: i64,
) -> Result<Vec<OwnedBind>, String> {
    match cursor {
        MirrorFlushPageCursor::AfterSeq { after_seq } => Ok(vec![
            OwnedBind::Int64(max_seq),
            OwnedBind::Int64(*after_seq),
            OwnedBind::Int64(limit),
        ]),
        MirrorFlushPageCursor::AfterOrderKey {
            after_order_key,
            after_pk_values,
            after_seq,
        } => {
            let first_page = after_order_key.is_none();
            if !first_page && after_pk_values.len() != primary_key_columns.len() {
                return Err(format!(
                    "ordered flush cursor has {} pk values but table has {}",
                    after_pk_values.len(),
                    primary_key_columns.len()
                ));
            }
            let mut binds = Vec::with_capacity(4 + primary_key_columns.len());
            binds.push(OwnedBind::Int64(max_seq));
            binds.push(OwnedBind::Bool(first_page));
            binds.push(OwnedBind::Bytes(
                after_order_key.clone().unwrap_or_default(),
            ));
            for (index, pk_name) in primary_key_columns.iter().enumerate() {
                let column = columns
                    .iter()
                    .find(|column| column.name == *pk_name)
                    .ok_or_else(|| {
                        format!("primary-key column `{pk_name}` missing from catalog")
                    })?;
                let value = if first_page {
                    default_cell_value(column.pg_type)
                } else {
                    after_pk_values
                        .get(index)
                        .cloned()
                        .unwrap_or(CellValue::Null)
                };
                binds.push(cell_value_to_owned_bind(&value, column.pg_type)?);
            }
            binds.push(OwnedBind::Int64(*after_seq));
            binds.push(OwnedBind::Int64(limit));
            Ok(binds)
        }
    }
}

fn owned_binds_to_datums(binds: &[OwnedBind]) -> Vec<DatumWithOid<'_>> {
    binds
        .iter()
        .map(|bind| match bind {
            OwnedBind::Bool(flag) => DatumWithOid::from(*flag),
            OwnedBind::Int32(n) => DatumWithOid::from(*n),
            OwnedBind::Int64(n) => DatumWithOid::from(*n),
            OwnedBind::Text(text) => DatumWithOid::from(text.as_str()),
            OwnedBind::Bytes(bytes) => DatumWithOid::from(bytes.as_slice()),
            OwnedBind::Float4(value) => DatumWithOid::from(*value),
            OwnedBind::Float8(value) => DatumWithOid::from(*value),
            OwnedBind::Uuid(uuid) => DatumWithOid::from(*uuid),
            // `saturating_from_raw` keeps PostgreSQL's `infinity` sentinels as infinities.
            OwnedBind::Date(days) => DatumWithOid::from(pgrx::datum::Date::saturating_from_raw(*days)),
            OwnedBind::Timestamp(micros) => {
                DatumWithOid::from(pgrx::datum::Timestamp::saturating_from_raw(*micros))
            }
            OwnedBind::Timestamptz(micros) => DatumWithOid::from(
                pgrx::datum::TimestampWithTimeZone::try_from(*micros).unwrap_or_else(|_| {
                    if *micros < 0 {
                        pgrx::datum::TimestampWithTimeZone::negative_infinity()
                    } else {
                        pgrx::datum::TimestampWithTimeZone::positive_infinity()
                    }
                }),
            ),
        })
        .collect()
}

fn default_cell_value(pg_type: PgType) -> CellValue {
    match pg_type {
        PgType::Bool => CellValue::Bool(false),
        PgType::Int2 => CellValue::Int16(0),
        PgType::Int4 => CellValue::Int32(0),
        PgType::Int8 | PgType::Money => CellValue::Int64(0),
        PgType::Timestamptz => CellValue::TimestamptzMicros(0),
        PgType::Timestamp => CellValue::TimestampMicros(0),
        PgType::Date => CellValue::DateDays(0),
        PgType::Float4 => CellValue::Float32(0.0),
        PgType::Float8 => CellValue::Float64(0.0),
        // The first-page placeholder is bound but never compared (the `$2::boolean OR ...` guard
        // short-circuits), yet it must still parse as the column's type: an empty string is not a
        // valid uuid, which made every ordered flush of a uuid-keyed table fail on its first page.
        PgType::Uuid => CellValue::Utf8(NIL_UUID_PLACEHOLDER.to_string()),
        // Bound but never compared on the first page, yet it must parse as `numeric`.
        PgType::Numeric => CellValue::Utf8("0".to_string()),
        // Placeholders that still parse as the column's own type.
        PgType::Inet => CellValue::Utf8("0.0.0.0".to_string()),
        PgType::Cidr => CellValue::Utf8("0.0.0.0/0".to_string()),
        PgType::Macaddr => CellValue::Utf8("00:00:00:00:00:00".to_string()),
        PgType::Time | PgType::Timetz => CellValue::Utf8("00:00:00".to_string()),
        PgType::Interval => CellValue::Utf8("0".to_string()),
        PgType::Int2Array => CellValue::Utf8("{}".to_string()),
        PgType::Int4Array => CellValue::Utf8("{}".to_string()),
        PgType::Int8Array => CellValue::Utf8("{}".to_string()),
        PgType::Float4Array => CellValue::Utf8("{}".to_string()),
        PgType::Float8Array => CellValue::Utf8("{}".to_string()),
        PgType::NumericArray => CellValue::Utf8("{}".to_string()),
        PgType::UuidArray => CellValue::Utf8("{}".to_string()),
        PgType::BoolArray
        | PgType::DateArray
        | PgType::TimestampArray
        | PgType::TimestamptzArray
        | PgType::IntervalArray
        | PgType::Bit
        | PgType::Varbit
        | PgType::Macaddr8
        | PgType::Name
        | PgType::Oid | PgType::Xml | PgType::Int4Range | PgType::Int8Range | PgType::NumRange | PgType::TsRange | PgType::TstzRange | PgType::DateRange | PgType::Point | PgType::Lseg | PgType::Path | PgType::Box | PgType::Polygon | PgType::Line | PgType::Circle | PgType::Enum | PgType::Composite => CellValue::Utf8("{}".to_string()),
        PgType::Json => CellValue::Utf8("null".to_string()),
        PgType::Text
        | PgType::Jsonb
        | PgType::Bytea
        | PgType::TextArray
        | PgType::Vector
        | PgType::Tsvector
        | PgType::Bpchar => {
            CellValue::Utf8(String::new())
        }
    }
}

/// First-page keyset placeholder for a `uuid` primary key (see [`default_cell_value`]).
const NIL_UUID_PLACEHOLDER: &str = "00000000-0000-0000-0000-000000000000";

fn cell_value_to_owned_bind(value: &CellValue, pg_type: PgType) -> Result<OwnedBind, String> {
    match (pg_type, value) {
        (PgType::Bool, CellValue::Bool(flag)) => Ok(OwnedBind::Bool(*flag)),
        (PgType::Int2, CellValue::Int16(n)) => Ok(OwnedBind::Int32(i32::from(*n))),
        (PgType::Int2, CellValue::Int32(n)) => Ok(OwnedBind::Int32(*n)),
        (PgType::Int2, CellValue::Int64(n)) => {
            Ok(OwnedBind::Int32(i32::try_from(*n).map_err(|_| {
                "int2 keyset value out of range".to_string()
            })?))
        }
        (PgType::Int4, CellValue::Int32(n)) => Ok(OwnedBind::Int32(*n)),
        (PgType::Int4, CellValue::Int16(n)) => Ok(OwnedBind::Int32(i32::from(*n))),
        (PgType::Int4, CellValue::Int64(n)) => {
            Ok(OwnedBind::Int32(i32::try_from(*n).map_err(|_| {
                "int4 keyset value out of range".to_string()
            })?))
        }
        (PgType::Int8, CellValue::Int64(n)) => Ok(OwnedBind::Int64(*n)),
        (PgType::Int8, CellValue::Int32(n)) => Ok(OwnedBind::Int64(i64::from(*n))),
        (PgType::Int8, CellValue::Int16(n)) => Ok(OwnedBind::Int64(i64::from(*n))),
        (
            PgType::Text
            | PgType::Numeric
            | PgType::Jsonb
            | PgType::TextArray
            | PgType::Bytea
            | PgType::Vector
            | PgType::Tsvector
            | PgType::Json
            | PgType::Inet
            | PgType::Cidr
            | PgType::Macaddr
            | PgType::Bpchar
            | PgType::Time
            | PgType::Timetz
            | PgType::Interval
            | PgType::Int2Array
            | PgType::Int4Array
            | PgType::Int8Array
            | PgType::Float4Array
            | PgType::Float8Array
            | PgType::NumericArray
            | PgType::UuidArray
            | PgType::BoolArray
            | PgType::DateArray
            | PgType::TimestampArray
            | PgType::TimestamptzArray
            | PgType::IntervalArray
            | PgType::Bit
            | PgType::Varbit
            | PgType::Macaddr8
            | PgType::Name
            | PgType::Oid | PgType::Xml | PgType::Int4Range | PgType::Int8Range | PgType::NumRange | PgType::TsRange | PgType::TstzRange | PgType::DateRange | PgType::Point | PgType::Lseg | PgType::Path | PgType::Box | PgType::Polygon | PgType::Line | PgType::Circle | PgType::Enum | PgType::Composite,
            CellValue::Utf8(text),
        ) => Ok(OwnedBind::Text(text.clone())),
        (PgType::Uuid, CellValue::Utf8(text)) => {
            let uuid = uuid::Uuid::parse_str(text)
                .map_err(|error| format!("invalid uuid keyset value: {error}"))?;
            Ok(OwnedBind::Uuid(crate::spi::uuid_to_pgrx(uuid)))
        }
        (PgType::Float4, CellValue::Float32(value)) => Ok(OwnedBind::Float4(*value)),
        (PgType::Float8, CellValue::Float64(value)) => Ok(OwnedBind::Float8(*value)),
        (PgType::Float8, CellValue::Float32(value)) => Ok(OwnedBind::Float8(f64::from(*value))),
        (PgType::Float4, CellValue::Float64(value)) => Ok(OwnedBind::Float4(*value as f32)),
        // A key that arrived as JSON text (`"NaN"`, `"Infinity"`) rather than a number.
        (PgType::Float4 | PgType::Float8, CellValue::Utf8(text)) => {
            let value: f64 = match text.to_ascii_lowercase().as_str() {
                "nan" => f64::NAN,
                "infinity" | "inf" => f64::INFINITY,
                "-infinity" | "-inf" => f64::NEG_INFINITY,
                other => other
                    .parse()
                    .map_err(|_| format!("invalid float keyset value `{text}`"))?,
            };
            Ok(if pg_type == PgType::Float4 {
                OwnedBind::Float4(value as f32)
            } else {
                OwnedBind::Float8(value)
            })
        }
        (PgType::Date, CellValue::DateDays(days)) => Ok(OwnedBind::Date(*days)),
        (PgType::Timestamp, CellValue::TimestampMicros(micros)) => Ok(OwnedBind::Timestamp(*micros)),
        (PgType::Timestamptz, CellValue::TimestamptzMicros(micros)) => {
            Ok(OwnedBind::Timestamptz(*micros))
        }
        (_, CellValue::Null) => {
            Err("ordered flush keyset primary key must not be null".to_string())
        }
        (pg_type, value) => Err(format!(
            "unsupported ordered flush keyset bind for {pg_type:?} value {value:?}"
        )),
    }
}

fn decode_mirror_batch(
    tuples: pgrx::spi::SpiTupleTable<'_>,
    columns: &[CatalogColumn],
    include_order_key: bool,
    include_scope: bool,
) -> pgrx::spi::Result<Vec<FlushMirrorRow>> {
    // Column layout from plan_mirror_flush_selection_batch:
    //   1..=N  application columns (catalog order)
    //   N+1    seq
    //   N+2    op
    //   N+3    order_key (optional)
    //   next   scope_id (optional, user-scoped tables)
    let seq_ordinal = columns.len() + 1;
    let op_ordinal = columns.len() + 2;
    let mut rows = Vec::with_capacity(tuples.len());
    for tuple in tuples {
        rows.push(decode_mirror_row(
            &tuple,
            columns,
            seq_ordinal,
            op_ordinal,
            include_order_key,
            include_scope,
        )?);
    }
    Ok(rows)
}

fn decode_mirror_row(
    tuple: &pgrx::spi::SpiHeapTupleData<'_>,
    columns: &[CatalogColumn],
    seq_ordinal: usize,
    op_ordinal: usize,
    include_order_key: bool,
    include_scope: bool,
) -> pgrx::spi::Result<FlushMirrorRow> {
    // PERFORMANCE: Ordinal access avoids per-column name lookups (SPI_fnumber).
    let seq = tuple
        .get::<i64>(seq_ordinal)?
        .ok_or_else(|| crate::spi::missing_attribute("seq"))?;
    let op = tuple
        .get::<i16>(op_ordinal)?
        .ok_or_else(|| crate::spi::missing_attribute("op"))?;
    let mut values = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        values.push(read_column(tuple, column, index + 1)?);
    }
    let order_key = include_order_key
        .then(|| tuple.get::<Vec<u8>>(columns.len() + 3))
        .transpose()?
        .flatten();
    let scope = include_scope
        .then(|| tuple.get::<String>(columns.len() + 3 + usize::from(include_order_key)))
        .transpose()?
        .flatten();
    Ok(FlushMirrorRow {
        seq,
        op,
        values,
        order_key,
        scope,
    })
}

/// Renders a one-dimensional `text[]` as its array literal (`{"a","b",NULL}`).
///
/// A lower bound other than 1 is kept as an explicit `[lo:hi]=` prefix so the bounds survive the
/// round trip through cold storage.
fn text_array_literal(items: &[Option<String>], lower_bound: i32) -> String {
    let mut out = String::new();
    if lower_bound != 1 && !items.is_empty() {
        let upper = i64::from(lower_bound) + items.len() as i64 - 1;
        out.push_str(&format!("[{lower_bound}:{upper}]="));
    }
    out.push('{');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        match item {
            None => out.push_str("NULL"),
            Some(text) => {
                out.push('"');
                for ch in text.chars() {
                    if ch == '"' || ch == '\\' {
                        out.push('\\');
                    }
                    out.push(ch);
                }
                out.push('"');
            }
        }
    }
    out.push('}');
    out
}

pub(super) fn read_column(
    tuple: &pgrx::spi::SpiHeapTupleData<'_>,
    column: &CatalogColumn,
    ordinal: usize,
) -> pgrx::spi::Result<CellValue> {
    let value = match column.pg_type {
        PgType::Bool => tuple
            .get::<bool>(ordinal)?
            .map(CellValue::Bool)
            .unwrap_or(CellValue::Null),
        PgType::Int2 => tuple
            .get::<i16>(ordinal)?
            .map(CellValue::Int16)
            .unwrap_or(CellValue::Null),
        PgType::Int4 => tuple
            .get::<i32>(ordinal)?
            .map(CellValue::Int32)
            .unwrap_or(CellValue::Null),
        // `money` is a by-value int64 (cents or the locale's smallest unit); keep it raw so the cold
        // copy does not depend on `lc_monetary`.
        PgType::Money => match tuple.get::<pgrx::AnyElement>(ordinal)? {
            Some(element) => CellValue::Int64(element.datum().value() as i64),
            None => CellValue::Null,
        },
        PgType::Int8 => tuple
            .get::<i64>(ordinal)?
            .map(CellValue::Int64)
            .unwrap_or(CellValue::Null),
        PgType::Float4 => tuple
            .get::<f32>(ordinal)?
            .map(CellValue::Float32)
            .unwrap_or(CellValue::Null),
        PgType::Float8 => tuple
            .get::<f64>(ordinal)?
            .map(CellValue::Float64)
            .unwrap_or(CellValue::Null),
        PgType::Text => tuple
            .get::<String>(ordinal)?
            .map(CellValue::Utf8)
            .unwrap_or(CellValue::Null),
        PgType::Uuid => tuple
            .get::<pgrx::Uuid>(ordinal)?
            .map(|uuid| CellValue::Utf8(uuid.to_string()))
            .unwrap_or(CellValue::Null),
        PgType::Jsonb => tuple
            .get::<pgrx::JsonB>(ordinal)?
            .map(|json| CellValue::Utf8(jsonb_cell_to_utf8(&json.0)))
            .unwrap_or(CellValue::Null),
        PgType::Bytea => tuple
            .get::<Vec<u8>>(ordinal)?
            .map(|bytes| CellValue::Utf8(pg_bytea_hex(&bytes)))
            .unwrap_or(CellValue::Null),
        // A `numeric` datum is not a text datum: read it as a number and keep its exact text form.
        PgType::Numeric => tuple
            .get::<pgrx::AnyNumeric>(ordinal)?
            .map(|number| CellValue::Utf8(number.to_string()))
            .unwrap_or(CellValue::Null),
        // An array datum is not a text datum: rebuild its literal from the elements.
        PgType::TextArray => match tuple.get::<pgrx::Array<'_, String>>(ordinal)? {
            Some(array) => {
                let items: Vec<Option<String>> = array.iter().collect();
                // SAFETY: the array pointer comes from the `Array` just read from this tuple.
                let header = array.into_array_type();
                let ndim = unsafe { (*header).ndim };
                if ndim > 1 {
                    // A multi-dimensional array keeps its shape (and any bounds) in `array_out`'s text.
                    // SAFETY: `header` is the detoasted `text[]` datum read from this tuple.
                    let text = unsafe {
                        crate::merge_scan::pg::literals::datum_output_text(
                            pgrx::pg_sys::Datum::from(header),
                            pgrx::pg_sys::TEXTARRAYOID,
                        )
                    };
                    return Ok(text.map_or(CellValue::Null, CellValue::Utf8));
                }
                // SAFETY: as above; a one-dimensional array stores its lower bound right after its
                // dimension length, both `int`s following the 16-byte `ArrayType` header.
                let lower_bound = unsafe {
                    if ndim == 1 {
                        header.cast::<u8>().add(16 + 4).cast::<i32>().read_unaligned()
                    } else {
                        1
                    }
                };
                CellValue::Utf8(text_array_literal(&items, lower_bound))
            }
            None => CellValue::Null,
        },
        // pgvector's datum is not a text datum: render it with the type's own output function.
        PgType::Vector => match tuple.get::<pgrx::AnyElement>(ordinal)? {
            Some(element) => {
                let datum = element.datum();
                // SAFETY: `datum` is a non-null `vector` value read from this tuple.
                let text = unsafe { crate::merge_scan::pg::literals::datum_output_text(
                    datum,
                    crate::merge_scan::pg::literals::vector_type_oid(),
                ) };
                CellValue::Utf8(text.unwrap_or_else(|| {
                    pgrx::error!("could not render a vector column for cold storage")
                }))
            }
            None => CellValue::Null,
        },
        // These datums are not text datums either: render each with its type's own output function.
        PgType::Tsvector
        | PgType::Json
        | PgType::Inet
        | PgType::Cidr
        | PgType::Macaddr
        | PgType::Bpchar
        | PgType::Time
        | PgType::Timetz
        | PgType::Interval
        | PgType::Int2Array
        | PgType::Int4Array
        | PgType::Int8Array
        | PgType::Float4Array
        | PgType::Float8Array
        | PgType::NumericArray
        | PgType::UuidArray
        | PgType::BoolArray
        | PgType::DateArray
        | PgType::TimestampArray
        | PgType::TimestamptzArray
        | PgType::IntervalArray
        | PgType::Bit
        | PgType::Varbit
        | PgType::Macaddr8
        | PgType::Name
        | PgType::Oid | PgType::Xml | PgType::Int4Range | PgType::Int8Range | PgType::NumRange | PgType::TsRange | PgType::TstzRange | PgType::DateRange | PgType::Point | PgType::Lseg | PgType::Path | PgType::Box | PgType::Polygon | PgType::Line | PgType::Circle
        | PgType::Enum | PgType::Composite => match tuple.get::<pgrx::AnyElement>(ordinal)? {
            Some(element) => {
                let datum = element.datum();
                // Enums carry their own per-column type OID; the others have a fixed one.
                let type_oid = if matches!(column.pg_type, PgType::Enum | PgType::Composite) {
                    element.oid()
                } else {
                    pgrx::pg_sys::Oid::from(column.pg_type.type_oid())
                };
                // SAFETY: `datum` is a non-null value of this column's type read from this tuple.
                let text = unsafe {
                    crate::merge_scan::pg::literals::datum_output_text(
                        datum,
                        type_oid,
                    )
                };
                CellValue::Utf8(text.unwrap_or_else(|| {
                    pgrx::error!("could not render a {} column for cold storage", column.pg_type.catalog_type_name())
                }))
            }
            None => CellValue::Null,
        },
        PgType::Date => match tuple.get::<pgrx::datum::Date>(ordinal)? {
            // Raw `DateADT` days since 2000-01-01; `infinity` is `i32::MAX` / `i32::MIN`.
            Some(date) => CellValue::DateDays(pgrx::pg_sys::DateADT::from(date)),
            None => CellValue::Null,
        },
        PgType::Timestamp => match tuple.get::<pgrx::datum::Timestamp>(ordinal)? {
            Some(timestamp) => CellValue::TimestampMicros(pgrx::pg_sys::Timestamp::from(timestamp)),
            None => CellValue::Null,
        },
        PgType::Timestamptz => {
            // Keep CellValue in PostgreSQL-epoch micros (same as Datum / hot).
            // Arrow TimestampMicrosecond is Unix-epoch; convert here only.
            match tuple.get::<pgrx::datum::TimestampWithTimeZone>(ordinal)? {
                Some(timestamp) => CellValue::TimestamptzMicros(timestamp.into_inner()),
                None => CellValue::Null,
            }
        }
    };
    Ok(value)
}
