//! Typed cell and row-image values for hot/cold merge payloads.
//!
//! Prefer these over `serde_json::Value` on scan, flush, and resolve paths so
//! backends avoid per-row JSON allocate/parse. JSON conversion stays at SQL /
//! admin boundaries (`JsonB` operators, change-feed contracts).

use std::collections::HashMap;
use std::ops::Index;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Number, Value};

use crate::{KoldstoreError, Result};

/// One typed column value in a hot or cold row image.
///
/// Matches the flush/Parquet cell vocabulary so decode, merge, and encode share
/// one representation without JSON round-trips.
#[derive(Debug, Clone, PartialEq)]
pub enum CellValue {
    /// SQL NULL.
    Null,
    /// Boolean column.
    Bool(bool),
    /// `int2`.
    Int16(i16),
    /// `int4`.
    Int32(i32),
    /// `int8`.
    Int64(i64),
    /// `float4`.
    Float32(f32),
    /// `float8`.
    Float64(f64),
    /// Text-like columns (`text`, `jsonb`, `uuid`, `bytea`, `numeric`, `text[]`).
    Utf8(String),
    /// `timestamptz` stored as PostgreSQL-epoch UTC microseconds
    /// (`TimestampTzADT`). Arrow physical storage remains Unix-epoch micros;
    /// convert at the Arrow boundary only.
    TimestamptzMicros(i64),
    /// `timestamp` (without time zone) stored as PostgreSQL-epoch microseconds
    /// (`Timestamp`). Arrow physical storage remains Unix-epoch micros.
    TimestampMicros(i64),
    /// `date` stored as days since the PostgreSQL epoch (2000-01-01) (`DateADT`).
    /// Arrow physical storage remains Unix-epoch days (`Date32`).
    DateDays(i32),
}

impl CellValue {
    /// Returns true for SQL NULL.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns a boolean when this cell is [`Self::Bool`].
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    /// Returns an integer when this cell is an integer variant (widened to `i64`).
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int16(value) => Some(*value as i64),
            Self::Int32(value) | Self::DateDays(value) => Some(*value as i64),
            Self::Int64(value) | Self::TimestamptzMicros(value) | Self::TimestampMicros(value) => {
                Some(*value)
            }
            _ => None,
        }
    }

    /// Returns a float when this cell is a floating-point variant (widened to `f64`).
    #[must_use]
    pub const fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float32(value) => Some(*value as f64),
            Self::Float64(value) => Some(*value),
            _ => None,
        }
    }

    /// Returns text when this cell is [`Self::Utf8`].
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Utf8(value) => Some(value.as_str()),
            _ => None,
        }
    }

    /// Lossless JSON view for SQL/admin boundaries.
    #[must_use]
    pub fn to_json(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(*value),
            Self::Int16(value) => Value::Number((*value).into()),
            Self::Int32(value) => Value::Number((*value).into()),
            Self::Int64(value) => Value::Number((*value).into()),
            // Temporal cells render as the text PostgreSQL's `to_jsonb` gives the value (timestamptz in
            // UTC), so a key built here compares equal to one the heap or the change-log mirror reports.
            Self::DateDays(value) => crate::temporal::format_date_pg_days(*value)
                .map_or_else(|_| Value::Number((*value).into()), Value::String),
            Self::TimestampMicros(value) => crate::temporal::format_timestamp_pg_micros(*value)
                .map_or_else(|_| Value::Number((*value).into()), Value::String),
            Self::TimestamptzMicros(value) => crate::temporal::format_timestamptz_json_utc(*value)
                .map_or_else(|_| Value::Number((*value).into()), Value::String),
            Self::Float32(value) => float_json(f64::from(*value), &format!("{value:e}")),
            Self::Float64(value) => float_json(*value, &format!("{value:e}")),
            Self::Utf8(value) => Value::String(value.clone()),
        }
    }

    /// The JSON used as primary-key identity. A `numeric` cell is carried as text, but heap and mirror
    /// keys (`to_jsonb`) are JSON numbers, so it becomes a number here (`62.5` and `62.50` are one key).
    #[must_use]
    pub fn to_pk_json(&self, is_numeric: bool) -> Value {
        if let (true, Self::Utf8(text)) = (is_numeric, self) {
            if let Ok(number @ Value::Number(_)) = serde_json::from_str::<Value>(text) {
                return number;
            }
        }
        self.to_json()
    }

    /// Builds a cell from JSON produced at a SQL/admin boundary.
    ///
    /// Numbers prefer the narrowest integer that fits; otherwise `float64`.
    /// Objects and arrays are compacted into [`Self::Utf8`] so jsonb/array
    /// payloads remain round-trippable as text for Parquet Utf8 storage.
    #[must_use]
    pub fn from_json(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(flag) => Self::Bool(*flag),
            Value::Number(number) => {
                if let Some(n) = number.as_i64() {
                    if let Ok(n) = i16::try_from(n) {
                        Self::Int16(n)
                    } else if let Ok(n) = i32::try_from(n) {
                        Self::Int32(n)
                    } else {
                        Self::Int64(n)
                    }
                } else if let Some(n) = number.as_u64() {
                    if let Ok(n) = i16::try_from(n) {
                        Self::Int16(n)
                    } else if let Ok(n) = i32::try_from(n) {
                        Self::Int32(n)
                    } else if let Ok(n) = i64::try_from(n) {
                        Self::Int64(n)
                    } else {
                        Self::Utf8(n.to_string())
                    }
                } else if let Some(n) = number.as_f64() {
                    Self::Float64(n)
                } else {
                    Self::Utf8(number.to_string())
                }
            }
            Value::String(text) => Self::Utf8(text.clone()),
            Value::Array(_) | Value::Object(_) => Self::Utf8(value.to_string()),
        }
    }

    /// Compact display used by residual filter matching (SPI text equality).
    #[must_use]
    pub fn display_text(&self) -> String {
        match self {
            Self::Null => "null".to_string(),
            Self::Bool(value) => value.to_string(),
            Self::Int16(value) => value.to_string(),
            Self::Int32(value) | Self::DateDays(value) => value.to_string(),
            Self::Int64(value) | Self::TimestamptzMicros(value) | Self::TimestampMicros(value) => {
                value.to_string()
            }
            Self::Float32(value) => value.to_string(),
            Self::Float64(value) => value.to_string(),
            Self::Utf8(value) => value.clone(),
        }
    }
}

/// Application column payload for a hot or cold row.
///
/// Cells are keyed by catalog column name. Prefer building from typed decode
/// paths; use [`Self::from_json`] / [`Self::to_json`] only at JSON boundaries.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RowImage {
    cells: HashMap<String, CellValue>,
}

impl RowImage {
    /// Empty image (no application columns).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds an image with pre-sized capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cells: HashMap::with_capacity(capacity),
        }
    }

    /// Returns true when no cells are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Number of stored cells.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Borrows a cell by catalog name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&CellValue> {
        self.cells.get(name)
    }

    /// Inserts or replaces a cell.
    pub fn insert(&mut self, name: impl Into<String>, value: CellValue) -> Option<CellValue> {
        self.cells.insert(name.into(), value)
    }

    /// Removes a cell by name.
    pub fn remove(&mut self, name: &str) -> Option<CellValue> {
        self.cells.remove(name)
    }

    /// Returns true when `name` is present.
    #[must_use]
    pub fn contains_key(&self, name: &str) -> bool {
        self.cells.contains_key(name)
    }

    /// Iterates name/value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &CellValue)> {
        self.cells.iter()
    }

    /// Mutable map access for rename remapping on cold decode.
    #[must_use]
    pub fn cells_mut(&mut self) -> &mut HashMap<String, CellValue> {
        &mut self.cells
    }

    /// Builds from a JSON object (or null → empty).
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is neither null nor an object.
    pub fn from_json(value: &Value) -> Result<Self> {
        match value {
            Value::Null => Ok(Self::new()),
            Value::Object(map) => {
                let mut cells = HashMap::with_capacity(map.len());
                for (name, cell) in map {
                    cells.insert(name.clone(), CellValue::from_json(cell));
                }
                Ok(Self { cells })
            }
            other => Err(KoldstoreError::InvalidIdentifier {
                kind: "row_image",
                value: other.to_string(),
            }),
        }
    }

    /// Convenience for tests and JSON fixtures.
    #[must_use]
    pub fn from_json_value(value: Value) -> Self {
        Self::from_json(&value).unwrap_or_default()
    }

    /// JSON object view for SQL/admin boundaries.
    #[must_use]
    pub fn to_json(&self) -> Value {
        let mut map = Map::with_capacity(self.cells.len());
        for (name, value) in &self.cells {
            map.insert(name.clone(), value.to_json());
        }
        Value::Object(map)
    }
}

impl Serialize for RowImage {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_json().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RowImage {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        Self::from_json(&value).map_err(serde::de::Error::custom)
    }
}

impl Index<&str> for RowImage {
    type Output = CellValue;

    fn index(&self, name: &str) -> &Self::Output {
        &self.cells[name]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips_object_images() {
        let image = RowImage::from_json_value(json!({"id": 7, "body": "hot"}));
        assert_eq!(image["id"].as_i64(), Some(7));
        assert_eq!(image["body"].as_str(), Some("hot"));
        assert_eq!(image.to_json(), json!({"id": 7, "body": "hot"}));
    }

    #[test]
    fn null_json_becomes_empty_image() {
        assert!(RowImage::from_json(&Value::Null).unwrap().is_empty());
    }
}

/// JSON for a float exactly as PostgreSQL's `to_jsonb(float)` renders it.
///
/// `to_jsonb` takes the float's shortest round-trip decimal text (`0.1::real` is `0.1`, not the
/// widened `0.10000000149011612`), reads it as a `numeric` and prints that without an exponent
/// (`1e20` is `100000000000000000000`, `1e-7` is `0.0000001`, `-0` is `0`); `NaN` and the infinities
/// are the strings `"NaN"`, `"Infinity"` and `"-Infinity"`. A float primary key is compared between
/// heap, change-log mirror and cold rows through this value, so the text must match digit for digit.
///
/// `exp_form` is Rust's `{:e}` rendering of the same value at its own width (`f32` or `f64`), which
/// is the shortest round-trip form.
fn float_json(value: f64, exp_form: &str) -> Value {
    if value.is_nan() {
        return Value::String("NaN".to_string());
    }
    if value.is_infinite() {
        return Value::String(if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string());
    }
    let text = plain_decimal_from_exp_form(exp_form);
    text.parse::<Number>().map_or(Value::Null, Value::Number)
}

/// Expands Rust's shortest `{:e}` text (`-1.5e-7`) to a plain decimal (`-0.00000015`).
fn plain_decimal_from_exp_form(exp_form: &str) -> String {
    let (mantissa, exponent) = exp_form.split_once('e').unwrap_or((exp_form, "0"));
    let exponent: i64 = exponent.parse().unwrap_or(0);
    let (negative, mantissa) = mantissa
        .strip_prefix('-')
        .map_or((false, mantissa), |rest| (true, rest));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    if digits.chars().all(|digit| digit == '0') {
        return "0".to_string(); // also folds -0.0 into 0, as numeric does
    }
    let integer_len = i64::try_from(mantissa.split('.').next().map_or(1, str::len)).unwrap_or(1);
    let point = integer_len + exponent; // digits before the decimal point
    let digit_count = i64::try_from(digits.len()).unwrap_or(0);
    let body = if point <= 0 {
        format!("0.{}{digits}", "0".repeat(usize::try_from(-point).unwrap_or(0)))
    } else if point >= digit_count {
        format!("{digits}{}", "0".repeat(usize::try_from(point - digit_count).unwrap_or(0)))
    } else {
        let split = usize::try_from(point).unwrap_or(0);
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        format!("-{body}")
    } else {
        body
    }
}

#[cfg(test)]
mod float_json_tests {
    use super::*;

    fn text(cell: &CellValue) -> String {
        cell.to_json().to_string()
    }

    #[test]
    fn floats_render_like_postgres_to_jsonb() {
        assert_eq!(text(&CellValue::Float64(0.5)), "0.5");
        assert_eq!(text(&CellValue::Float64(0.1)), "0.1");
        assert_eq!(text(&CellValue::Float32(0.1)), "0.1"); // not 0.10000000149011612
        assert_eq!(text(&CellValue::Float32(0.25)), "0.25");
        assert_eq!(text(&CellValue::Float64(1e20)), "100000000000000000000");
        assert_eq!(text(&CellValue::Float64(1e-7)), "0.0000001");
        assert_eq!(text(&CellValue::Float64(-1.5e-7)), "-0.00000015");
        assert_eq!(text(&CellValue::Float64(12345.678)), "12345.678");
        assert_eq!(text(&CellValue::Float64(3.0)), "3");
        assert_eq!(text(&CellValue::Float64(-0.0)), "0");
        assert_eq!(text(&CellValue::Float64(0.0)), "0");
        assert_eq!(text(&CellValue::Float64(f64::NAN)), "\"NaN\"");
        assert_eq!(text(&CellValue::Float64(f64::INFINITY)), "\"Infinity\"");
        assert_eq!(text(&CellValue::Float32(f32::NEG_INFINITY)), "\"-Infinity\"");
    }
}
