//! Per-segment sketches of pgvector `vector` columns, for nearest-neighbour segment skipping.
//!
//! The writer summarises every segment's vectors as a *ball* around a fixed centre: the centre, the
//! largest distance of any vector from it, and how many vectors there are. Two balls are kept, one in
//! plain space (for `<->`, L2) and one over unit-length vectors (for `<=>`, cosine distance, which is
//! `|a/|a| - b/|b||^2 / 2`). By the triangle inequality every vector `v` of a segment satisfies
//! `|q - c| - r <= |q - v| <= |q - c| + r`, so a query vector `q` gets a lower and an upper bound on
//! the distance to *any* row of the segment without reading it.
//!
//! [`knn_excluded`] turns the bounds into a prune: `k` rows are certain to exist within the `k`-th
//! smallest upper bound, so a segment whose lower bound is above it cannot hold a top-`k` row.
//!
//! The encoding below is **frozen** (it is persisted in `cold_segment_index.value_summary`).

use arrow_array::{Array, RecordBatch, StringArray};

const MAGIC: &[u8; 4] = b"VSK1";
/// Vectors buffered to pick the centre; later ones only grow the radius.
const CENTRE_SAMPLE: usize = 1024;
/// Largest dimension a sketch is built for (pgvector's own limit is 16000).
const MAX_DIM: usize = 16_000;
/// Relative slack added to every bound so `f32` arithmetic in PostgreSQL never beats the proof.
const BOUND_SLACK: f64 = 1e-4;

/// Distance function a nearest-neighbour query orders by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnnMetric {
    /// `<->`, Euclidean distance.
    L2,
    /// `<=>`, cosine distance.
    Cosine,
}

/// Parses a pgvector text literal such as `[1,2.5,-3]`.
#[must_use]
pub fn parse_vector_text(text: &str) -> Option<Vec<f32>> {
    let inner = text.trim().strip_prefix('[')?.strip_suffix(']')?;
    if inner.trim().is_empty() {
        return None;
    }
    let mut values = Vec::new();
    for part in inner.split(',') {
        let value: f32 = part.trim().parse().ok()?;
        if !value.is_finite() {
            return None;
        }
        values.push(value);
    }
    (values.len() <= MAX_DIM).then_some(values)
}

fn norm(vector: &[f32]) -> f64 {
    vector
        .iter()
        .map(|&x| f64::from(x) * f64::from(x))
        .sum::<f64>()
        .sqrt()
}

fn unit(vector: &[f32]) -> Option<Vec<f32>> {
    let length = norm(vector);
    (length > 0.0 && length.is_finite())
        .then(|| vector.iter().map(|&x| (f64::from(x) / length) as f32).collect())
}

fn distance(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(&x, &y)| {
            let d = f64::from(x) - f64::from(y);
            d * d
        })
        .sum::<f64>()
        .sqrt()
}

/// A ball of vectors: fixed centre, radius, row count.
#[derive(Debug, Default)]
struct BallBuilder {
    sample: Vec<Vec<f32>>,
    centre: Option<Vec<f32>>,
    radius: f64,
    count: u64,
}

impl BallBuilder {
    fn add(&mut self, vector: Vec<f32>) {
        self.count += 1;
        if let Some(centre) = &self.centre {
            self.radius = self.radius.max(distance(centre, &vector));
        } else {
            self.sample.push(vector);
            if self.sample.len() >= CENTRE_SAMPLE {
                self.settle();
            }
        }
    }

    /// Fixes the centre as the mean of the sample and measures the sample against it.
    fn settle(&mut self) {
        if self.centre.is_some() || self.sample.is_empty() {
            return;
        }
        let dim = self.sample[0].len();
        let mut mean = vec![0.0f64; dim];
        for vector in &self.sample {
            for (slot, &x) in mean.iter_mut().zip(vector) {
                *slot += f64::from(x);
            }
        }
        let n = self.sample.len() as f64;
        let centre: Vec<f32> = mean.iter().map(|sum| (sum / n) as f32).collect();
        for vector in std::mem::take(&mut self.sample) {
            self.radius = self.radius.max(distance(&centre, &vector));
        }
        self.centre = Some(centre);
    }
}

/// Rebuilds the sketch flush would have written for the `vector` column of a segment object that predates
/// the column's listing in `pruning_columns`.
///
/// # Errors
///
/// Returns an error when the object cannot be read or lacks the column.
pub fn rebuild_vector_sketch_from_parquet(
    data: impl Into<bytes::Bytes>,
    column: &str,
) -> Result<crate::term_filter::RebuiltColumnSummary, String> {
    let mut builder = VectorSketchBuilder::new();
    let mut rows = 0_usize;
    crate::term_filter::for_each_column_batch(data, column, |batch| {
        rows += batch.num_rows();
        builder.add_batch(batch, column);
    })?;
    Ok(crate::term_filter::RebuiltColumnSummary {
        summary: builder.finish(),
        rows,
    })
}

/// Accumulates the sketch of the vector column of the segment being written.
#[derive(Debug, Default)]
pub struct VectorSketchBuilder {
    dim: Option<usize>,
    plain: BallBuilder,
    unit: BallBuilder,
    complete: bool,
}

impl VectorSketchBuilder {
    /// A builder for one segment.
    #[must_use]
    pub fn new() -> Self {
        Self {
            complete: true,
            ..Self::default()
        }
    }

    /// Adds the vectors of `column` (a `Utf8` column holding pgvector text literals).
    pub fn add_batch(&mut self, batch: &RecordBatch, column: &str) {
        if !self.complete {
            return;
        }
        let Some(array) = batch
            .column_by_name(column)
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        else {
            self.complete = false;
            return;
        };
        for index in 0..array.len() {
            if array.is_null(index) {
                continue;
            }
            let Some(vector) = parse_vector_text(array.value(index)) else {
                self.complete = false;
                return;
            };
            if *self.dim.get_or_insert(vector.len()) != vector.len() {
                self.complete = false;
                return;
            }
            if let Some(normalised) = unit(&vector) {
                self.unit.add(normalised);
            }
            self.plain.add(vector);
        }
    }

    /// The encoded sketch, or `None` when the segment has no usable vectors.
    #[must_use]
    pub fn finish(mut self) -> Option<Vec<u8>> {
        if !self.complete {
            return None;
        }
        let dim = self.dim?;
        self.plain.settle();
        self.unit.settle();
        let plain_centre = self.plain.centre?;
        let mut out = Vec::with_capacity(32 + dim * 8);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&u32::try_from(dim).ok()?.to_le_bytes());
        out.extend_from_slice(&self.plain.count.to_le_bytes());
        out.extend_from_slice(&self.unit.count.to_le_bytes());
        out.extend_from_slice(&round_up(self.plain.radius).to_le_bytes());
        out.extend_from_slice(&round_up(self.unit.radius).to_le_bytes());
        for x in &plain_centre {
            out.extend_from_slice(&x.to_le_bytes());
        }
        // No unit vectors (all zero): an all-zero centre with a zero count, never used.
        let unit_centre = self.unit.centre.unwrap_or_else(|| vec![0.0; dim]);
        for x in &unit_centre {
            out.extend_from_slice(&x.to_le_bytes());
        }
        Some(out)
    }
}

/// `f32` at or above `value`.
fn round_up(value: f64) -> f32 {
    let narrowed = value as f32;
    if f64::from(narrowed) >= value || !narrowed.is_finite() {
        narrowed
    } else {
        f32::from_bits(narrowed.to_bits() + 1)
    }
}

/// A decoded segment sketch.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSketch {
    dim: usize,
    count_plain: u64,
    count_unit: u64,
    radius_plain: f64,
    radius_unit: f64,
    centre_plain: Vec<f32>,
    centre_unit: Vec<f32>,
}

impl VectorSketch {
    /// Decodes a sketch written by [`VectorSketchBuilder::finish`].
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.get(..4)? != MAGIC {
            return None;
        }
        let dim = u32::from_le_bytes(bytes.get(4..8)?.try_into().ok()?) as usize;
        if dim == 0 || dim > MAX_DIM || bytes.len() != 32 + dim * 8 {
            return None;
        }
        let count_plain = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let count_unit = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        let radius_plain = f64::from(f32::from_le_bytes(bytes[24..28].try_into().ok()?));
        let radius_unit = f64::from(f32::from_le_bytes(bytes[28..32].try_into().ok()?));
        let floats = |from: usize| -> Vec<f32> {
            bytes[from..from + dim * 4]
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("4 bytes")))
                .collect()
        };
        Some(Self {
            dim,
            count_plain,
            count_unit,
            radius_plain,
            radius_unit,
            centre_plain: floats(32),
            centre_unit: floats(32 + dim * 4),
        })
    }

    /// Lower and upper bound on the distance from `query` to any vector of the segment, with the
    /// number of vectors the bounds cover. `None` when the sketch cannot answer (wrong dimension).
    fn bounds(&self, query: &[f32], metric: KnnMetric) -> Option<(f64, f64, u64)> {
        if query.len() != self.dim {
            return None;
        }
        match metric {
            KnnMetric::L2 => {
                let d = distance(query, &self.centre_plain);
                let r = self.radius_plain;
                let slack = BOUND_SLACK * (d + r) + 1e-6;
                Some(((d - r - slack).max(0.0), d + r + slack, self.count_plain))
            }
            KnnMetric::Cosine => {
                if self.count_unit == 0 {
                    return None;
                }
                let unit_query = unit(query)?;
                let d = distance(&unit_query, &self.centre_unit);
                let r = self.radius_unit;
                let slack = BOUND_SLACK * (d + r) + 1e-6;
                let lb = (d - r - slack).max(0.0);
                let ub = d + r + slack;
                Some((lb * lb / 2.0, ub * ub / 2.0, self.count_unit))
            }
        }
    }
}

/// Indexes of `sketches` whose segment cannot hold one of the `needed` nearest rows to `query`.
///
/// `needed` must cover the rows wanted *plus* every row that could be masked (superseded or deleted
/// by a hot row or tombstone), so the `needed`-th smallest upper bound is certain to bound live rows.
/// Segments without a sketch (`None`) are never excluded and never counted.
#[must_use]
pub fn knn_excluded(
    sketches: &[Option<VectorSketch>],
    query: &[f32],
    metric: KnnMetric,
    needed: u64,
) -> Vec<usize> {
    if needed == 0 || query.iter().any(|x| !x.is_finite()) {
        return Vec::new();
    }
    let bounds: Vec<Option<(f64, f64, u64)>> = sketches
        .iter()
        .map(|sketch| sketch.as_ref().and_then(|s| s.bounds(query, metric)))
        .collect();
    let mut by_upper: Vec<(f64, u64)> = bounds
        .iter()
        .flatten()
        .filter(|(_, _, count)| *count > 0)
        .map(|&(_, ub, count)| (ub, count))
        .collect();
    by_upper.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut covered = 0u64;
    let mut threshold = None;
    for (ub, count) in by_upper {
        covered = covered.saturating_add(count);
        if covered >= needed {
            threshold = Some(ub);
            break;
        }
    }
    let Some(threshold) = threshold else {
        return Vec::new();
    };
    bounds
        .iter()
        .enumerate()
        .filter_map(|(index, bound)| match bound {
            Some((lb, _, _)) if *lb > threshold => Some(index),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayRef;
    use std::sync::Arc;

    fn batch(rows: &[Option<&str>]) -> RecordBatch {
        let array: ArrayRef = Arc::new(StringArray::from(rows.to_vec()));
        RecordBatch::try_from_iter([("emb", array)]).unwrap()
    }

    fn sketch(rows: &[Option<&str>]) -> VectorSketch {
        let mut builder = VectorSketchBuilder::new();
        builder.add_batch(&batch(rows), "emb");
        VectorSketch::decode(&builder.finish().expect("sketch")).expect("decode")
    }

    #[test]
    fn parses_pgvector_literals() {
        assert_eq!(parse_vector_text("[1,2.5, -3]"), Some(vec![1.0, 2.5, -3.0]));
        assert_eq!(parse_vector_text("[]"), None);
        assert_eq!(parse_vector_text("1,2"), None);
        assert_eq!(parse_vector_text("[1,nan]"), None);
    }

    #[test]
    fn bounds_enclose_true_distances() {
        let rows = ["[0,0]", "[2,0]", "[0,2]", "[2,2]"];
        let s = sketch(&rows.map(Some));
        let query = [10.0f32, 1.0];
        let (lb, ub, count) = s.bounds(&query, KnnMetric::L2).unwrap();
        assert_eq!(count, 4);
        for row in rows {
            let v = parse_vector_text(row).unwrap();
            let d = distance(&query, &v);
            assert!(lb <= d && d <= ub, "{lb} <= {d} <= {ub}");
        }
    }

    #[test]
    fn cosine_bounds_enclose_true_distances() {
        let rows = ["[1,0]", "[1,1]", "[0.5,0.1]"];
        let s = sketch(&rows.map(Some));
        let query = [-1.0f32, 2.0];
        let (lb, ub, _) = s.bounds(&query, KnnMetric::Cosine).unwrap();
        for row in rows {
            let v = parse_vector_text(row).unwrap();
            let cos = 1.0 - f64::from(query[0] * v[0] + query[1] * v[1]) / (norm(&query) * norm(&v));
            assert!(lb <= cos && cos <= ub, "{lb} <= {cos} <= {ub}");
        }
    }

    #[test]
    fn far_segment_is_excluded_near_ones_kept() {
        let near = sketch(&[Some("[0,0]"), Some("[1,1]"), Some("[1,0]")]);
        let far = sketch(&[Some("[100,100]"), Some("[101,100]")]);
        let sketches = [Some(near), Some(far), None];
        let excluded = knn_excluded(&sketches, &[0.0, 0.0], KnnMetric::L2, 2);
        assert_eq!(excluded, vec![1]);
        // More rows wanted than the near segment holds: nothing may be skipped.
        assert!(knn_excluded(&sketches, &[0.0, 0.0], KnnMetric::L2, 4).is_empty());
    }

    #[test]
    fn nulls_and_bad_rows() {
        let s = sketch(&[None, Some("[1,2]"), None]);
        assert_eq!(s.count_plain, 1);
        let mut builder = VectorSketchBuilder::new();
        builder.add_batch(&batch(&[Some("[1,2]"), Some("[1,2,3]")]), "emb");
        assert!(builder.finish().is_none());
    }

    #[test]
    fn zero_vector_counts_for_l2_only() {
        let s = sketch(&[Some("[0,0]"), Some("[3,4]")]);
        assert_eq!((s.count_plain, s.count_unit), (2, 1));
    }

    #[test]
    fn large_segment_radius_covers_late_rows() {
        let rows: Vec<String> = (0..CENTRE_SAMPLE + 50).map(|i| format!("[{i},0]")).collect();
        let s = sketch(&rows.iter().map(|r| Some(r.as_str())).collect::<Vec<_>>());
        let (lb, ub, _) = s.bounds(&[5000.0, 0.0], KnnMetric::L2).unwrap();
        let farthest = 5000.0 - 0.0;
        let nearest = 5000.0 - (CENTRE_SAMPLE + 49) as f64;
        assert!(lb <= nearest && farthest <= ub);
    }
}
