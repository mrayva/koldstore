//! Value summaries: a small fixed-size membership bitmap over Sort Key V1 bytes.
//!
//! A cold segment's `min`/`max` bounds cannot rule it out for a point lookup when its keys are
//! scattered over the whole key range, which is what a table's hydrate-and-reflush churn produces:
//! the lookup then has to open every such segment just to learn the key is not in it. A summary is a
//! one-hash bit set of the segment's key values, stored in the catalog beside the bounds. The catalog
//! query tests one bit per candidate segment and never opens the ones that cannot hold the key.
//!
//! The persisted format is deliberately tiny and frozen:
//!
//! * the input is the Sort Key V1 encoding of the value (the same bytes that back `min_value` and
//!   `max_value`), so the writer and the planner agree by construction;
//! * [`summary_hash`] maps those bytes to a value below `2^63`, which PostgreSQL can carry as a
//!   non-negative `bigint` and reduce with `%`;
//! * the bit for a value is `hash % bits` where `bits = 8 * summary.len()`, stored the way
//!   PostgreSQL's `get_bit(bytea, n)` reads it: bit `n` is `1 << (n % 8)` of byte `n / 8`.
//!
//! There are no false negatives. A missing summary (`NULL`) always means "cannot prune". A summary
//! is skipped for segments too large for the cap (the filter would be saturated and useless), so the
//! catalog stays `O(segments)`: at most [`SUMMARY_MAX_BYTES`] per segment.

/// Largest summary stored for one segment and column.
pub const SUMMARY_MAX_BYTES: usize = 8 * 1024;
/// Segments with more rows than this get no summary (bit load would exceed 50%).
pub const SUMMARY_MAX_ROWS: usize = SUMMARY_MAX_BYTES * 8 / 2;
/// Bits allocated per row for segments that fit under the cap (about 6% false positives).
const BITS_PER_ROW: usize = 16;
/// Smallest summary in bits.
const MIN_BITS: usize = 64;

/// Hashes Sort Key V1 bytes to a value below `2^63`. **Frozen**: the result is persisted.
#[must_use]
pub fn summary_hash(encoded: &[u8]) -> u64 {
    // FNV-1a over the bytes, then a splitmix64 finalizer so low bits mix well for `%`.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in encoded {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash = hash.wrapping_add(0x9e37_79b9_7f4a_7c15);
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (hash ^ (hash >> 31)) >> 1
}

/// Collects the values of one column of one segment and builds its summary.
#[derive(Debug, Default, Clone)]
pub struct ValueSummaryBuilder {
    hashes: Vec<u64>,
    overflowed: bool,
}

impl ValueSummaryBuilder {
    /// Creates an empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one value (its Sort Key V1 bytes). Stops collecting once the segment is too large.
    pub fn insert(&mut self, encoded: &[u8]) {
        if self.overflowed {
            return;
        }
        if self.hashes.len() >= SUMMARY_MAX_ROWS {
            self.overflowed = true;
            self.hashes = Vec::new();
            return;
        }
        self.hashes.push(summary_hash(encoded));
    }

    /// Number of values collected (zero once overflowed).
    #[must_use]
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    /// True when no value is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Builds the bitmap, or `None` for an empty or oversized segment.
    #[must_use]
    pub fn finish(self) -> Option<Vec<u8>> {
        let rows = self.hashes.len();
        if self.overflowed || rows == 0 {
            return None;
        }
        let bits = (rows * BITS_PER_ROW)
            .next_multiple_of(8)
            .clamp(MIN_BITS, SUMMARY_MAX_BYTES * 8);
        let mut summary = vec![0_u8; bits / 8];
        for hash in self.hashes {
            let bit = usize::try_from(hash % bits as u64).expect("bit index fits usize");
            summary[bit / 8] |= 1 << (bit % 8);
        }
        Some(summary)
    }
}

/// True when `summary` may contain the value (never a false negative).
#[must_use]
pub fn summary_may_contain(summary: &[u8], encoded: &[u8]) -> bool {
    if summary.is_empty() {
        return true;
    }
    let bits = summary.len() as u64 * 8;
    let bit = usize::try_from(summary_hash(encoded) % bits).expect("bit index fits usize");
    summary[bit / 8] & (1 << (bit % 8)) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u64) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }

    #[test]
    fn hash_is_frozen_and_below_2_pow_63() {
        // These constants are part of the persisted format: changing the hash silently turns every
        // stored summary into a source of false negatives. If this test fails, do not "fix" the
        // constants; add a new codec version instead.
        assert_eq!(summary_hash(b""), 7_043_838_727_467_204_504);
        assert_eq!(summary_hash(b"koldstore"), 5_495_628_202_174_716_924);
        assert_eq!(
            summary_hash(&[0x80, 0, 0, 0, 0, 0, 0, 42]),
            8_093_551_759_240_755_733
        );
        for sample in [&b""[..], b"a", b"koldstore", &[0u8; 64], &[0xffu8; 17]] {
            assert!(summary_hash(sample) < (1_u64 << 63));
        }
    }

    #[test]
    fn no_false_negatives() {
        for rows in [1_usize, 2, 7, 100, 1_000, 5_000, SUMMARY_MAX_ROWS] {
            let mut builder = ValueSummaryBuilder::new();
            for n in 0..rows {
                builder.insert(&key(n as u64 * 7919));
            }
            let summary = builder.finish().expect("fits under the cap");
            assert!(summary.len() <= SUMMARY_MAX_BYTES);
            for n in 0..rows {
                assert!(
                    summary_may_contain(&summary, &key(n as u64 * 7919)),
                    "rows={rows} n={n}"
                );
            }
        }
    }

    #[test]
    fn false_positive_rate_is_low_for_small_segments() {
        // 5,000 rows want 80,000 bits but the 8 KiB cap is 65,536 bits (load 7.6%, ~7.3% expected).
        let rows = 5_000_usize;
        let mut builder = ValueSummaryBuilder::new();
        for n in 0..rows {
            builder.insert(&key(n as u64));
        }
        let summary = builder.finish().unwrap();
        assert_eq!(summary.len(), SUMMARY_MAX_BYTES);
        let probes = 200_000_u64;
        let hits = (1_000_000..1_000_000 + probes)
            .filter(|n| summary_may_contain(&summary, &key(*n)))
            .count();
        let rate = hits as f64 / probes as f64;
        assert!(rate < 0.10, "false positive rate {rate}");
        // A 1,000-row segment (the default max_rows_per_file) gets the full 16 bits per row (~6%).
        let mut small = ValueSummaryBuilder::new();
        for n in 0..1_000_u64 {
            small.insert(&key(n));
        }
        let small = small.finish().unwrap();
        assert_eq!(small.len() * 8, 16_000);
        let hits = (1_000_000..1_000_000 + probes)
            .filter(|n| summary_may_contain(&small, &key(*n)))
            .count();
        assert!((hits as f64 / probes as f64) < 0.075);
    }

    #[test]
    fn oversized_and_empty_segments_get_no_summary() {
        assert!(ValueSummaryBuilder::new().finish().is_none());
        let mut builder = ValueSummaryBuilder::new();
        for n in 0..=SUMMARY_MAX_ROWS {
            builder.insert(&key(n as u64));
        }
        assert_eq!(builder.len(), 0);
        assert!(builder.finish().is_none());
    }

    #[test]
    fn bit_layout_matches_postgres_get_bit() {
        // PostgreSQL get_bit(bytea, n): bit 0 is the least significant bit of the first byte.
        let mut builder = ValueSummaryBuilder::new();
        builder.insert(&key(42));
        let summary = builder.finish().unwrap();
        assert_eq!(summary.len(), 8, "one row rounds up to the 64-bit minimum");
        let bit = (summary_hash(&key(42)) % 64) as usize;
        assert_eq!(summary[bit / 8], 1 << (bit % 8));
        assert_eq!(summary.iter().map(|b| b.count_ones()).sum::<u32>(), 1);
    }

    #[test]
    fn empty_summary_never_prunes() {
        assert!(summary_may_contain(&[], &key(1)));
    }
}
