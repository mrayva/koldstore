//! Manage-table options, flush policy, and privilege policy helpers.
//!
//! Pure configuration and role/GUC policy — no PostgreSQL execution.

mod move_predicate;
mod options;

pub mod privileges;

pub use move_predicate::{
    MovePredicate, PredicateOp, PredicateTerm, MAX_PREDICATE_IN_VALUES, MAX_PREDICATE_TERMS,
};
pub use options::{
    flush_enabled_from_options, hot_row_limit_from_options, validate_max_rows_per_file,
    FlushPolicy, ManageTableOptions, MigrationStatus, MoveAfter, ParquetBloomFilterFpp,
    ParquetCompression, DEFAULT_MAX_ROWS_PER_FLUSH, DEFAULT_MIN_MAX_ROWS_PER_FILE,
};
