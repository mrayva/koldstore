//! PostgreSQL SQL entrypoints.
//!
//! Library crates own SQL planning; these modules execute plans through SPI.
//! Layout follows extension domains: migrate, flush, events, storage, plus
//! small shared helpers (`job_lock`, `session`, `sort_key`, `ops`).

#[cfg(feature = "pg")]
pub mod backup;
#[cfg(feature = "pg")]
pub mod cold_dml;
#[cfg(feature = "pg")]
pub mod drift;
#[cfg(feature = "pg")]
pub mod events;
pub mod flush;
#[cfg(feature = "pg")]
pub mod integrity;
#[cfg(feature = "pg")]
pub mod job_lock;
pub mod migrate;
pub mod ops;
pub mod session;
#[cfg(feature = "pg")]
pub mod summaries;
pub mod sort_key;
#[cfg(feature = "pg")]
pub mod table_metadata;
pub mod storage;
#[cfg(feature = "pg")]
pub mod sync;
