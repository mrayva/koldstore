//! KoldStore Sort Key V1 — order-preserving binary bounds for cold segment indexes.
//!
//! Wraps [`storekey`] behind a pinned codec version so flush writers and query
//! planners share one encoding path. PostgreSQL compares the resulting `bytea`
//! values with ordinary B-tree operators.
//!
//! Must not depend on `pgrx` or any other `koldstore-*` crate.

mod encode;
mod error;
mod summary;
mod types;

pub use encode::{decode_sort_key, encode_sort_key, encode_sort_key_json, encode_sort_key_pg_text};
pub use error::SortKeyError;
pub use summary::{
    summary_hash, summary_may_contain, ValueSummaryBuilder, SUMMARY_MAX_BYTES, SUMMARY_MAX_ROWS,
};
pub use types::{
    pg_days_to_unix, pg_micros_to_unix, unix_days_to_pg, unix_micros_to_pg, SortKeyType, SortKeyValue,
    CODEC_VERSION, PG_EPOCH_DAYS_FROM_UNIX, PG_EPOCH_MICROS_FROM_UNIX,
};
