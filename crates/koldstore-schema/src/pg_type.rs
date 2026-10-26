//! Supported PostgreSQL column types for the pg-koldstore MVP.
//!
//! [`PgType`] is the single canonical enum for supported PostgreSQL column types.
//! Catalog-name normalization lives in `koldstore_common::canonical_postgres_type_name`;
//! Arrow and JSON value coercion live in `koldstore_parquet::pg_type_codec`.

use koldstore_common::canonical_postgres_type_name;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::TypeMatrix;

/// Supported PostgreSQL column type for managed tables and cold storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PgType {
    /// `bool`
    Bool = 0,
    /// `int2`
    Int2 = 1,
    /// `int4`
    Int4 = 2,
    /// `int8`
    Int8 = 3,
    /// `float4`
    Float4 = 4,
    /// `float8`
    Float8 = 5,
    /// `text` / `varchar`
    Text = 6,
    /// `numeric`
    Numeric = 7,
    /// `uuid`
    Uuid = 8,
    /// `jsonb`
    Jsonb = 9,
    /// `text[]`
    TextArray = 10,
    /// `bytea`
    Bytea = 11,
    /// `timestamptz`
    Timestamptz = 12,
    /// `date`
    Date = 13,
    /// `timestamp` (without time zone)
    Timestamp = 14,
    /// pgvector `vector(n)`; the type OID is assigned by `CREATE EXTENSION vector`, so it is resolved at runtime
    Vector = 15,
    /// `tsvector`
    Tsvector = 16,
    /// `json`
    Json = 17,
    /// `inet`
    Inet = 18,
    /// `cidr`
    Cidr = 19,
    /// `macaddr`
    Macaddr = 20,
    /// `char(n)` / `bpchar`
    Bpchar = 21,
    /// `time` (without time zone)
    Time = 22,
    /// `timetz`
    Timetz = 23,
    /// `int2[]`
    Int2Array = 24,
    /// `int4[]`
    Int4Array = 25,
    /// `int8[]`
    Int8Array = 26,
    /// `float4[]`
    Float4Array = 27,
    /// `float8[]`
    Float8Array = 28,
    /// `numeric[]`
    NumericArray = 29,
    /// `uuid[]`
    UuidArray = 30,
    /// `bool[]`
    BoolArray = 31,
    /// `interval`, rendered as ISO 8601 so the text form does not depend on `IntervalStyle`
    Interval = 32,
    /// `date[]`
    DateArray = 33,
    /// `timestamp[]`
    TimestampArray = 34,
    /// `timestamptz[]`
    TimestamptzArray = 35,
    /// `interval[]`
    IntervalArray = 36,
    /// `bit(n)`
    Bit = 37,
    /// `bit varying(n)`
    Varbit = 38,
    /// `macaddr8`
    Macaddr8 = 39,
    /// `name`
    Name = 40,
    /// `oid`
    Oid = 41,
    /// A user-defined enum, stored as its label; the type OID is per column.
    Enum = 42,
    /// `xml`, stored as its text.
    Xml = 43,
    /// `money`, stored as its raw int64 (locale independent).
    Money = 44,
}

/// PostgreSQL integer array type OID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgIntegerArrayOid {
    /// `smallint[]`
    Int2 = 1005,
    /// `integer[]`
    Int4 = 1007,
    /// `bigint[]`
    Int8 = 1016,
}

/// Schema/type conversion error.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaError {
    /// Catalog type is outside the MVP support matrix.
    #[error("unsupported PostgreSQL type: {0}")]
    UnsupportedType(String),
}

impl Serialize for PgType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.catalog_type_name())
    }
}

impl<'de> Deserialize<'de> for PgType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let type_name = String::deserialize(deserializer)?;
        Self::from_postgres_name(&type_name).map_err(serde::de::Error::custom)
    }
}

impl PgType {
    /// Parses PostgreSQL catalog type names and common aliases.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::UnsupportedType`] for unsupported or blank types.
    pub fn from_postgres_name(type_name: &str) -> Result<Self, SchemaError> {
        match canonical_postgres_type_name(type_name).as_str() {
            "bool" => Ok(Self::Bool),
            "int2" => Ok(Self::Int2),
            "int4" => Ok(Self::Int4),
            "int8" => Ok(Self::Int8),
            "float4" => Ok(Self::Float4),
            "float8" => Ok(Self::Float8),
            "text" | "varchar" => Ok(Self::Text),
            "numeric" => Ok(Self::Numeric),
            "uuid" => Ok(Self::Uuid),
            "jsonb" => Ok(Self::Jsonb),
            "text[]" => Ok(Self::TextArray),
            "bytea" => Ok(Self::Bytea),
            "timestamptz" => Ok(Self::Timestamptz),
            "date" => Ok(Self::Date),
            "timestamp" => Ok(Self::Timestamp),
            "vector" => Ok(Self::Vector),
            "tsvector" => Ok(Self::Tsvector),
            "json" => Ok(Self::Json),
            "inet" => Ok(Self::Inet),
            "cidr" => Ok(Self::Cidr),
            "macaddr" => Ok(Self::Macaddr),
            "bpchar" => Ok(Self::Bpchar),
            "time" => Ok(Self::Time),
            "timetz" => Ok(Self::Timetz),
            "int2[]" => Ok(Self::Int2Array),
            "int4[]" => Ok(Self::Int4Array),
            "int8[]" => Ok(Self::Int8Array),
            "float4[]" => Ok(Self::Float4Array),
            "float8[]" => Ok(Self::Float8Array),
            "numeric[]" => Ok(Self::NumericArray),
            "uuid[]" => Ok(Self::UuidArray),
            "bool[]" => Ok(Self::BoolArray),
            "interval" => Ok(Self::Interval),
            "date[]" => Ok(Self::DateArray),
            "timestamp[]" => Ok(Self::TimestampArray),
            "timestamptz[]" => Ok(Self::TimestamptzArray),
            "interval[]" => Ok(Self::IntervalArray),
            "bit" => Ok(Self::Bit),
            "varbit" => Ok(Self::Varbit),
            "macaddr8" => Ok(Self::Macaddr8),
            "name" => Ok(Self::Name),
            "oid" => Ok(Self::Oid),
            "enum" => Ok(Self::Enum),
            "xml" => Ok(Self::Xml),
            "money" => Ok(Self::Money),
            normalized => Err(SchemaError::UnsupportedType(normalized.to_string())),
        }
    }

    /// Returns the compact discriminant for this type.
    #[must_use]
    pub const fn discriminant(self) -> u8 {
        self as u8
    }

    /// Maps a compact discriminant back to a supported type.
    #[must_use]
    pub const fn from_discriminant(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Bool),
            1 => Some(Self::Int2),
            2 => Some(Self::Int4),
            3 => Some(Self::Int8),
            4 => Some(Self::Float4),
            5 => Some(Self::Float8),
            6 => Some(Self::Text),
            7 => Some(Self::Numeric),
            8 => Some(Self::Uuid),
            9 => Some(Self::Jsonb),
            10 => Some(Self::TextArray),
            11 => Some(Self::Bytea),
            12 => Some(Self::Timestamptz),
            13 => Some(Self::Date),
            14 => Some(Self::Timestamp),
            15 => Some(Self::Vector),
            16 => Some(Self::Tsvector),
            17 => Some(Self::Json),
            18 => Some(Self::Inet),
            19 => Some(Self::Cidr),
            20 => Some(Self::Macaddr),
            21 => Some(Self::Bpchar),
            22 => Some(Self::Time),
            23 => Some(Self::Timetz),
            24 => Some(Self::Int2Array),
            25 => Some(Self::Int4Array),
            26 => Some(Self::Int8Array),
            27 => Some(Self::Float4Array),
            28 => Some(Self::Float8Array),
            29 => Some(Self::NumericArray),
            30 => Some(Self::UuidArray),
            31 => Some(Self::BoolArray),
            32 => Some(Self::Interval),
            33 => Some(Self::DateArray),
            34 => Some(Self::TimestampArray),
            35 => Some(Self::TimestamptzArray),
            36 => Some(Self::IntervalArray),
            37 => Some(Self::Bit),
            38 => Some(Self::Varbit),
            39 => Some(Self::Macaddr8),
            40 => Some(Self::Name),
            41 => Some(Self::Oid),
            42 => Some(Self::Enum),
            43 => Some(Self::Xml),
            44 => Some(Self::Money),
            _ => None,
        }
    }

    /// Returns the canonical MVP type name for diagnostics and matrix checks.
    #[must_use]
    pub const fn catalog_type_name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int2 => "int2",
            Self::Int4 => "int4",
            Self::Int8 => "int8",
            Self::Float4 => "float4",
            Self::Float8 => "float8",
            Self::Text => "text",
            Self::Numeric => "numeric",
            Self::Uuid => "uuid",
            Self::Jsonb => "jsonb",
            Self::TextArray => "text[]",
            Self::Bytea => "bytea",
            Self::Timestamptz => "timestamptz",
            Self::Date => "date",
            Self::Timestamp => "timestamp",
            Self::Vector => "vector",
            Self::Tsvector => "tsvector",
            Self::Json => "json",
            Self::Inet => "inet",
            Self::Cidr => "cidr",
            Self::Macaddr => "macaddr",
            Self::Bpchar => "bpchar",
            Self::Time => "time",
            Self::Timetz => "timetz",
            Self::Int2Array => "int2[]",
            Self::Int4Array => "int4[]",
            Self::Int8Array => "int8[]",
            Self::Float4Array => "float4[]",
            Self::Float8Array => "float8[]",
            Self::NumericArray => "numeric[]",
            Self::UuidArray => "uuid[]",
            Self::BoolArray => "bool[]",
            Self::Interval => "interval",
            Self::DateArray => "date[]",
            Self::TimestampArray => "timestamp[]",
            Self::TimestamptzArray => "timestamptz[]",
            Self::IntervalArray => "interval[]",
            Self::Bit => "bit",
            Self::Varbit => "varbit",
            Self::Macaddr8 => "macaddr8",
            Self::Name => "name",
            Self::Oid => "oid",
            Self::Enum => "enum",
            Self::Xml => "xml",
            Self::Money => "money",
        }
    }

    /// Returns true when the type is supported by the MVP type matrix.
    #[must_use]
    pub fn is_mvp_supported(self) -> bool {
        TypeMatrix::postgres_15_default_ref()
            .support_for(self.catalog_type_name())
            .supported
    }

    /// Returns true when the type can provide oldest-to-newest migration ordering.
    #[must_use]
    pub fn is_orderable(self) -> bool {
        matches!(
            self,
            Self::Bool
                | Self::Int2
                | Self::Int4
                | Self::Int8
                | Self::Uuid
                | Self::Timestamptz
                | Self::Date
                | Self::Timestamp
        )
    }

    /// Returns true when a raw catalog type name can provide migration ordering.
    #[must_use]
    pub fn is_orderable_catalog_type(type_name: &str) -> bool {
        matches!(
            canonical_postgres_type_name(type_name).as_str(),
            "bool" | "int2" | "int4" | "int8" | "uuid" | "timestamptz" | "timestamp" | "date"
        )
    }

    /// Maps a supported type to its PostgreSQL type OID.
    #[must_use]
    pub const fn type_oid(self) -> u32 {
        match self {
            Self::Bool => 16,
            Self::Int2 => 21,
            Self::Int4 => 23,
            Self::Int8 => 20,
            Self::Float4 => 700,
            Self::Float8 => 701,
            Self::Text => 25,
            Self::Numeric => 1700,
            Self::Uuid => 2950,
            Self::Jsonb => 3802,
            Self::TextArray => 1009,
            Self::Bytea => 17,
            Self::Timestamptz => 1184,
            Self::Date => 1082,
            Self::Timestamp => 1114,
            // Not a fixed OID: look it up from the column (`atttypid`) or `to_regtype('vector')`.
            Self::Vector => 0,
            Self::Tsvector => 3614,
            Self::Json => 114,
            Self::Inet => 869,
            Self::Cidr => 650,
            Self::Macaddr => 829,
            Self::Bpchar => 1042,
            Self::Time => 1083,
            Self::Timetz => 1266,
            Self::Int2Array => 1005,
            Self::Int4Array => 1007,
            Self::Int8Array => 1016,
            Self::Float4Array => 1021,
            Self::Float8Array => 1022,
            Self::NumericArray => 1231,
            Self::UuidArray => 2951,
            Self::BoolArray => 1000,
            Self::Interval => 1186,
            Self::DateArray => 1182,
            Self::TimestampArray => 1115,
            Self::TimestamptzArray => 1185,
            Self::IntervalArray => 1187,
            Self::Bit => 1560,
            Self::Varbit => 1562,
            Self::Macaddr8 => 774,
            Self::Name => 19,
            Self::Oid => 26,
            // Enums have no fixed OID; callers resolve the column's own type.
            Self::Enum => 0,
            Self::Xml => 142,
            Self::Money => 790,
        }
    }

    /// Maps a supported integer type to its PostgreSQL type OID.
    #[must_use]
    pub const fn integer_oid(self) -> Option<u32> {
        match self {
            Self::Int2 => Some(self.type_oid()),
            Self::Int4 => Some(self.type_oid()),
            Self::Int8 => Some(self.type_oid()),
            _ => None,
        }
    }

    /// Maps a PostgreSQL integer type OID to the supported `PgType`.
    #[must_use]
    pub fn from_integer_oid(oid: u32) -> Option<Self> {
        match oid {
            20 => Some(Self::Int8),
            21 => Some(Self::Int2),
            23 => Some(Self::Int4),
            _ => None,
        }
    }

    /// Maps a supported integer type to its PostgreSQL array type OID.
    #[must_use]
    pub const fn integer_array_oid(self) -> Option<PgIntegerArrayOid> {
        match self {
            Self::Int2 => Some(PgIntegerArrayOid::Int2),
            Self::Int4 => Some(PgIntegerArrayOid::Int4),
            Self::Int8 => Some(PgIntegerArrayOid::Int8),
            _ => None,
        }
    }

    /// Maps a PostgreSQL integer array OID to its element type.
    #[must_use]
    pub fn from_integer_array_oid(oid: u32) -> Option<Self> {
        match PgIntegerArrayOid::from_oid(oid)? {
            PgIntegerArrayOid::Int2 => Some(Self::Int2),
            PgIntegerArrayOid::Int4 => Some(Self::Int4),
            PgIntegerArrayOid::Int8 => Some(Self::Int8),
        }
    }

    /// Formats an integer datum as SQL literal text.
    #[must_use]
    pub fn integer_sql_literal(self, value: i64) -> Option<String> {
        match self {
            Self::Int2 => i16::try_from(value).ok().map(|value| value.to_string()),
            Self::Int4 => i32::try_from(value).ok().map(|value| value.to_string()),
            Self::Int8 => Some(value.to_string()),
            _ => None,
        }
    }

    /// Returns true when an equality literal of `literal` may be pushed into a
    /// source predicate typed as `self`.
    ///
    /// PostgreSQL leaves untyped numeric consts as `int4` (or `int2`), so
    /// `bigint_col = 100000` is a cross-type `int8 = int4` operator. Hot SPI
    /// pushdown must accept those integer width promotions the same way cold
    /// prune already does. Demotions and non-integer cross-types stay rejected.
    #[must_use]
    pub const fn accepts_integer_equality_literal(self, literal: Self) -> bool {
        matches!(
            (self, literal),
            (Self::Int8, Self::Int2 | Self::Int4 | Self::Int8)
                | (Self::Int4, Self::Int2 | Self::Int4)
                | (Self::Int2, Self::Int2)
        )
    }
}

impl PgIntegerArrayOid {
    /// Maps a PostgreSQL array type OID to the supported enum.
    #[must_use]
    pub const fn from_oid(oid: u32) -> Option<Self> {
        match oid {
            1005 => Some(Self::Int2),
            1007 => Some(Self::Int4),
            1016 => Some(Self::Int8),
            _ => None,
        }
    }

    /// Returns the PostgreSQL array type OID.
    #[must_use]
    pub const fn oid(self) -> u32 {
        self as u32
    }

    /// Returns the element type OID accepted by `deconstruct_array`.
    #[must_use]
    pub const fn element_oid(self) -> u32 {
        match self {
            Self::Int2 => 21,
            Self::Int4 => 23,
            Self::Int8 => 20,
        }
    }

    /// Returns the element `PgType`.
    #[must_use]
    pub const fn element_type(self) -> PgType {
        match self {
            Self::Int2 => PgType::Int2,
            Self::Int4 => PgType::Int4,
            Self::Int8 => PgType::Int8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PgType;
    use serde_json::json;

    #[test]
    fn pg_type_round_trips_through_json_alias_names() {
        let value = json!("bigint");
        let pg_type: PgType = serde_json::from_value(value).unwrap();
        assert_eq!(pg_type, PgType::Int8);
        assert_eq!(serde_json::to_value(pg_type).unwrap(), json!("int8"));
    }

    #[test]
    fn pg_type_discriminant_round_trip() {
        for pg_type in [
            PgType::Bool,
            PgType::Int2,
            PgType::Int4,
            PgType::Int8,
            PgType::Float4,
            PgType::Float8,
            PgType::Text,
            PgType::Numeric,
            PgType::Uuid,
            PgType::Jsonb,
            PgType::TextArray,
            PgType::Bytea,
            PgType::Timestamptz,
            PgType::Date,
            PgType::Timestamp,
            PgType::Vector,
            PgType::Tsvector,
            PgType::Json,
            PgType::Inet,
            PgType::Cidr,
            PgType::Macaddr,
            PgType::Bpchar,
            PgType::Time,
            PgType::Timetz,
            PgType::Int2Array,
            PgType::Int4Array,
            PgType::Int8Array,
            PgType::Float4Array,
            PgType::Float8Array,
            PgType::NumericArray,
            PgType::UuidArray,
            PgType::BoolArray,
            PgType::Interval,
            PgType::DateArray,
            PgType::TimestampArray,
            PgType::TimestamptzArray,
            PgType::IntervalArray,
            PgType::Bit,
            PgType::Varbit,
            PgType::Macaddr8,
            PgType::Name,
            PgType::Oid,
            PgType::Enum,
            PgType::Xml,
            PgType::Money,
        ] {
            assert_eq!(
                PgType::from_discriminant(pg_type.discriminant()),
                Some(pg_type)
            );
        }
    }

    #[test]
    fn type_oid_matches_known_postgres_oids() {
        assert_eq!(PgType::Bool.type_oid(), 16);
        assert_eq!(PgType::Int8.type_oid(), 20);
        assert_eq!(PgType::Uuid.type_oid(), 2950);
        assert_eq!(PgType::Timestamptz.type_oid(), 1184);
        assert_eq!(PgType::Int8.integer_oid(), Some(20));
    }

    #[test]
    fn integer_equality_accepts_width_promotions_not_demotions() {
        assert!(PgType::Int8.accepts_integer_equality_literal(PgType::Int4));
        assert!(PgType::Int8.accepts_integer_equality_literal(PgType::Int2));
        assert!(PgType::Int8.accepts_integer_equality_literal(PgType::Int8));
        assert!(PgType::Int4.accepts_integer_equality_literal(PgType::Int2));
        assert!(PgType::Int4.accepts_integer_equality_literal(PgType::Int4));
        assert!(PgType::Int2.accepts_integer_equality_literal(PgType::Int2));

        assert!(!PgType::Int4.accepts_integer_equality_literal(PgType::Int8));
        assert!(!PgType::Int2.accepts_integer_equality_literal(PgType::Int4));
        assert!(!PgType::Int8.accepts_integer_equality_literal(PgType::Float8));
        assert!(!PgType::Int8.accepts_integer_equality_literal(PgType::Text));
    }
}
