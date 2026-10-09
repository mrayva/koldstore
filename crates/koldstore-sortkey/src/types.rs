//! Sort Key V1 type taxonomy and value shapes.

use uuid::Uuid;

/// Persisted codec identity for KoldStore Sort Key V1.
///
/// Stored beside every `cold_segment_index` bound. Changing the encoding must
/// bump this constant; silent Storekey dependency upgrades must not rewrite
/// persisted bytes without an intentional codec bump.
pub const CODEC_VERSION: i16 = 1;

/// Days from the Unix epoch (1970-01-01) to the PostgreSQL epoch (2000-01-01).
pub const PG_EPOCH_DAYS_FROM_UNIX: i32 = 10_957;

/// Microseconds from the Unix epoch to the PostgreSQL epoch.
pub const PG_EPOCH_MICROS_FROM_UNIX: i64 = 946_684_800_000_000;

/// Converts PostgreSQL-epoch microseconds (`timestamp` / `timestamptz`) to Unix-epoch microseconds,
/// the unit Arrow and Parquet store.
///
/// PostgreSQL encodes `infinity` / `-infinity` as the extreme `i64` values; those pass through
/// unchanged so a plain epoch shift cannot turn them into ordinary-looking (and wrong) instants.
/// Finite values saturate, so a (far future) instant that cannot be shifted into `i64` degrades to
/// `infinity` rather than wrapping.
#[must_use]
pub const fn pg_micros_to_unix(pg_micros: i64) -> i64 {
    if pg_micros == i64::MAX || pg_micros == i64::MIN {
        pg_micros
    } else {
        pg_micros.saturating_add(PG_EPOCH_MICROS_FROM_UNIX)
    }
}

/// Inverse of [`pg_micros_to_unix`].
#[must_use]
pub const fn unix_micros_to_pg(unix_micros: i64) -> i64 {
    if unix_micros == i64::MAX || unix_micros == i64::MIN {
        unix_micros
    } else {
        unix_micros.saturating_sub(PG_EPOCH_MICROS_FROM_UNIX)
    }
}

/// Converts PostgreSQL-epoch days (`date`) to Unix-epoch days (Arrow `Date32`); `infinity` passes through.
#[must_use]
pub const fn pg_days_to_unix(pg_days: i32) -> i32 {
    if pg_days == i32::MAX || pg_days == i32::MIN {
        pg_days
    } else {
        pg_days.saturating_add(PG_EPOCH_DAYS_FROM_UNIX)
    }
}

/// Inverse of [`pg_days_to_unix`].
#[must_use]
pub const fn unix_days_to_pg(unix_days: i32) -> i32 {
    if unix_days == i32::MAX || unix_days == i32::MIN {
        unix_days
    } else {
        unix_days.saturating_sub(PG_EPOCH_DAYS_FROM_UNIX)
    }
}

/// Allowlisted PostgreSQL types for Sort Key V1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortKeyType {
    /// `boolean` / OID 16.
    Bool,
    /// `smallint` / OID 21.
    Int2,
    /// `integer` / OID 23.
    Int4,
    /// `bigint` / OID 20.
    Int8,
    /// `date` / OID 1082 (days since PostgreSQL epoch).
    Date,
    /// `timestamp` / OID 1114 (µs since PostgreSQL epoch).
    Timestamp,
    /// `timestamptz` / OID 1184 (UTC µs since PostgreSQL epoch).
    Timestamptz,
    /// `uuid` / OID 2950.
    Uuid,
}

impl SortKeyType {
    /// Maps a PostgreSQL type OID to a Sort Key V1 type.
    #[must_use]
    pub const fn from_type_oid(type_oid: u32) -> Option<Self> {
        match type_oid {
            16 => Some(Self::Bool),
            21 => Some(Self::Int2),
            23 => Some(Self::Int4),
            20 => Some(Self::Int8),
            1082 => Some(Self::Date),
            1114 => Some(Self::Timestamp),
            1184 => Some(Self::Timestamptz),
            2950 => Some(Self::Uuid),
            _ => None,
        }
    }

    /// Returns the canonical PostgreSQL type OID for this sort-key type.
    #[must_use]
    pub const fn type_oid(self) -> u32 {
        match self {
            Self::Bool => 16,
            Self::Int2 => 21,
            Self::Int4 => 23,
            Self::Int8 => 20,
            Self::Date => 1082,
            Self::Timestamp => 1114,
            Self::Timestamptz => 1184,
            Self::Uuid => 2950,
        }
    }

    /// Returns true when this type may be used as `segment_order_column_id`.
    #[must_use]
    pub const fn is_order_column_supported(self) -> bool {
        true
    }
}

/// Canonical in-memory value before Storekey encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortKeyValue {
    /// Boolean.
    Bool(bool),
    /// Signed 16-bit integer.
    Int2(i16),
    /// Signed 32-bit integer.
    Int4(i32),
    /// Signed 64-bit integer.
    Int8(i64),
    /// PostgreSQL date as days since 2000-01-01.
    Date(i32),
    /// PostgreSQL timestamp as µs since 2000-01-01.
    Timestamp(i64),
    /// PostgreSQL timestamptz as UTC µs since 2000-01-01.
    Timestamptz(i64),
    /// UUID.
    Uuid(Uuid),
}

impl SortKeyValue {
    /// Returns the Sort Key V1 type for this value.
    #[must_use]
    pub const fn sort_key_type(&self) -> SortKeyType {
        match self {
            Self::Bool(_) => SortKeyType::Bool,
            Self::Int2(_) => SortKeyType::Int2,
            Self::Int4(_) => SortKeyType::Int4,
            Self::Int8(_) => SortKeyType::Int8,
            Self::Date(_) => SortKeyType::Date,
            Self::Timestamp(_) => SortKeyType::Timestamp,
            Self::Timestamptz(_) => SortKeyType::Timestamptz,
            Self::Uuid(_) => SortKeyType::Uuid,
        }
    }

    /// Converts this value into the JSON shape accepted by [`crate::encode_sort_key_json`].
    ///
    /// Temporal values use PostgreSQL-epoch integer units (days / microseconds).
    /// UUIDs use hyphenated lowercase strings.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Bool(value) => serde_json::Value::Bool(*value),
            Self::Int2(value) => serde_json::Value::Number((*value).into()),
            Self::Int4(value) => serde_json::Value::Number((*value).into()),
            Self::Int8(value) => serde_json::Value::Number((*value).into()),
            Self::Date(value) => serde_json::Value::Number((*value).into()),
            Self::Timestamp(value) => serde_json::Value::Number((*value).into()),
            Self::Timestamptz(value) => serde_json::Value::Number((*value).into()),
            Self::Uuid(value) => serde_json::Value::String(value.to_string()),
        }
    }
}

#[cfg(test)]
mod epoch_tests {
    use super::*;

    #[test]
    fn finite_values_shift_and_round_trip() {
        for pg in [0_i64, 1, -1, 86_400_000_000, -211_813_488_000_000_000, 700_000_000_000_000] {
            assert_eq!(unix_micros_to_pg(pg_micros_to_unix(pg)), pg);
        }
        assert_eq!(pg_micros_to_unix(0), PG_EPOCH_MICROS_FROM_UNIX);
        for pg in [0_i32, 1, -1, 7_000, -2_451_545] {
            assert_eq!(unix_days_to_pg(pg_days_to_unix(pg)), pg);
        }
        assert_eq!(pg_days_to_unix(0), PG_EPOCH_DAYS_FROM_UNIX);
    }

    #[test]
    fn infinity_passes_through_unshifted() {
        for pg in [i64::MAX, i64::MIN] {
            assert_eq!(pg_micros_to_unix(pg), pg);
            assert_eq!(unix_micros_to_pg(pg), pg);
        }
        for pg in [i32::MAX, i32::MIN] {
            assert_eq!(pg_days_to_unix(pg), pg);
            assert_eq!(unix_days_to_pg(pg), pg);
        }
    }
}
