//! PostgreSQL datum boundary for Sort Key V1 encoding.
//!
//! Segment-order and scan helpers call this polymorphic SQL function after
//! manage-time type validation. The codec itself remains owned by
//! `koldstore-sortkey`.

#[cfg(feature = "pg")]
use koldstore_sortkey::{encode_sort_key, SortKeyValue};
#[cfg(feature = "pg")]
use pgrx::datum::{AnyElement, FromDatum};

/// Encodes one supported PostgreSQL scalar as Sort Key V1 `bytea`.
///
/// SQL contract: the input is non-null and has one of the allowlisted type
/// OIDs validated for `segment_order_column_id`.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(
    name = "internal_encode_sort_key",
    schema = "koldstore",
    immutable,
    parallel_safe
)]
pub fn internal_encode_sort_key(value: AnyElement) -> Vec<u8> {
    // A domain-typed key is encoded as its base type; AnyElement::into would reject the domain OID.
    let base_oid = unsafe { pgrx::pg_sys::getBaseType(value.oid()) };
    let type_oid = u32::from(base_oid);
    let datum = value.datum();
    let encoded = unsafe {
        match type_oid {
            16 => bool::from_polymorphic_datum(datum, false, base_oid).map(SortKeyValue::Bool),
            21 => i16::from_polymorphic_datum(datum, false, base_oid).map(SortKeyValue::Int2),
            23 => i32::from_polymorphic_datum(datum, false, base_oid).map(SortKeyValue::Int4),
            20 => i64::from_polymorphic_datum(datum, false, base_oid).map(SortKeyValue::Int8),
            1082 => pgrx::datum::Date::from_polymorphic_datum(datum, false, base_oid)
                .map(|date| SortKeyValue::Date(date.into_inner())),
            1114 => pgrx::datum::Timestamp::from_polymorphic_datum(datum, false, base_oid)
                .map(|timestamp| SortKeyValue::Timestamp(timestamp.into_inner())),
            1184 => pgrx::datum::TimestampWithTimeZone::from_polymorphic_datum(datum, false, base_oid)
                .map(|timestamp| SortKeyValue::Timestamptz(timestamp.into_inner())),
            2950 => pgrx::datum::Uuid::from_polymorphic_datum(datum, false, base_oid)
                .map(|uuid| SortKeyValue::Uuid(uuid::Uuid::from_bytes(*uuid.as_bytes()))),
            _ => pgrx::error!("unsupported Sort Key V1 PostgreSQL type OID {type_oid}"),
        }
    }
    .unwrap_or_else(|| pgrx::error!("cannot encode NULL as a segment order key"));

    encode_sort_key(&encoded)
        .unwrap_or_else(|error| pgrx::error!("failed to encode Sort Key V1 value: {error}"))
}
