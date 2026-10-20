//! Compaction planning: which active segments are rewritten together.
//!
//! Pure bookkeeping over each segment's row count and whole-segment key bounds (Sort Key V1 bytes, one
//! `[min, max]` interval per primary-key column); the executor in `pg_koldstore` does the reading and
//! rewriting.
//!
//! Two segments can share a key only if their intervals intersect on *every* key column (a "box"
//! test): per-column bounds say nothing about which tuples exist, so intersecting boxes are treated
//! as possibly sharing a key, and boxes that miss on any one column cannot. For a single-column key
//! this is plain interval overlap.
//!
//! Two kinds of unit are produced. An *overlap component* is a maximal set of segments whose key
//! ranges chain together (touching counts, since one key may sit in both): rewriting it keeps the
//! newest version of every key and restores disjoint ranges, which is what lets scans skip
//! retaining cold identities. A *small run* is a run of adjacent, already disjoint small segments
//! merged into fewer, fuller ones, so many tiny flushes do not leave the table with hundreds of
//! objects.

/// One active segment's size and key box.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRange {
    /// Rows stored in the segment, including superseded versions and delete markers.
    pub row_count: i64,
    /// `(min, max)` Sort Key V1 bytes of each primary-key column, in key order.
    pub bounds: Vec<(Vec<u8>, Vec<u8>)>,
    /// Lexicographic `(min, max)` of the whole composite key tuple, when the segment has it.
    pub tuple: Option<(Vec<u8>, Vec<u8>)>,
}

impl SegmentRange {
    /// A single-column range.
    #[must_use]
    pub fn single(row_count: i64, min: Vec<u8>, max: Vec<u8>) -> Self {
        Self {
            row_count,
            bounds: vec![(min, max)],
            tuple: None,
        }
    }

    /// True when one key may be in both segments: their key boxes intersect on every key column
    /// *and*, if both carry them, their lexicographic key-tuple intervals intersect. Each test is
    /// sound on its own (disjoint boxes, or disjoint tuple intervals, prove no shared key) and they
    /// are independent: boxes see segments that interleave on the leading column but are separated
    /// by another, tuple intervals see segments split mid-way through a leading value.
    #[must_use]
    pub fn may_share_keys(&self, other: &Self) -> bool {
        let boxes = self.bounds.len() == other.bounds.len()
            && self
                .bounds
                .iter()
                .zip(&other.bounds)
                .all(|((min_a, max_a), (min_b, max_b))| min_a <= max_b && min_b <= max_a);
        if !boxes {
            return false;
        }
        match (&self.tuple, &other.tuple) {
            (Some((min_a, max_a)), Some((min_b, max_b))) => min_a <= max_b && min_b <= max_a,
            _ => true,
        }
    }
}

/// Indexes of `segments` grouped into overlap components (segments chained by
/// [`SegmentRange::may_share_keys`]), each component in input order, components ordered by their
/// first member. The input must be sorted by the first key column's minimum.
///
/// Cost is the number of segment pairs whose first-column intervals intersect, so key columns that
/// split segments well (a leading tenant or time column) make this near-linear.
#[must_use]
pub fn overlap_components(segments: &[SegmentRange]) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..segments.len()).collect();
    fn find(parent: &mut [usize], mut node: usize) -> usize {
        while parent[node] != node {
            parent[node] = parent[parent[node]];
            node = parent[node];
        }
        node
    }
    let mut active: Vec<usize> = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        let Some((lead_min, _)) = segment.bounds.first() else {
            continue;
        };
        active.retain(|other| {
            segments[*other]
                .bounds
                .first()
                .is_some_and(|(_, other_max)| other_max >= lead_min)
        });
        for other in &active {
            if segment.may_share_keys(&segments[*other]) {
                let (a, b) = (find(&mut parent, index), find(&mut parent, *other));
                if a != b {
                    parent[a.max(b)] = a.min(b);
                }
            }
        }
        active.push(index);
    }
    let mut order: Vec<usize> = Vec::new();
    let mut groups: std::collections::HashMap<usize, Vec<usize>> = std::collections::HashMap::new();
    for index in 0..segments.len() {
        let root = find(&mut parent, index);
        let group = groups.entry(root).or_default();
        if group.is_empty() {
            order.push(root);
        }
        group.push(index);
    }
    order
        .into_iter()
        .filter_map(|root| groups.remove(&root))
        .collect()
}

/// True when no two segments can share a key (no pair of key boxes intersects), so a key has at
/// most one cold version. `None` when `max_pairs` first-column-overlapping pairs would have to be
/// examined: the caller then treats the layout as possibly overlapping. Input sorted as for
/// [`overlap_components`].
#[must_use]
pub fn keys_disjoint(segments: &[SegmentRange], max_pairs: usize) -> Option<bool> {
    let mut active: Vec<usize> = Vec::new();
    let mut examined = 0_usize;
    for (index, segment) in segments.iter().enumerate() {
        let (lead_min, _) = segment.bounds.first()?;
        active.retain(|other| {
            segments[*other]
                .bounds
                .first()
                .is_some_and(|(_, other_max)| other_max >= lead_min)
        });
        for other in &active {
            examined += 1;
            if examined > max_pairs {
                return None;
            }
            if segment.may_share_keys(&segments[*other]) {
                return Some(false);
            }
        }
        active.push(index);
    }
    Some(true)
}

/// Segments to rewrite together, as indexes into the planner's input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionUnit {
    /// Input indexes, in key order.
    pub members: Vec<usize>,
    /// Sum of the members' row counts (what the rewrite reads).
    pub rows: i64,
    /// True for an overlap component (it may hold several versions of a key); false for a run of
    /// small disjoint segments.
    pub overlapping: bool,
}

/// The planned units plus what was left out.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompactionPlan {
    /// Units that fit the per-unit row cap, in key order.
    pub units: Vec<CompactionUnit>,
    /// Overlap components larger than the cap (left for a larger `max_rows`).
    pub skipped_over_cap: usize,
}

/// Plans units over `segments`, which must be sorted by `min`.
///
/// `max_rows_per_file` is the table's segment size target: a lone segment at or under half of it is
/// "small" and merged with small neighbours up to that target. `max_unit_rows` caps how many rows
/// one unit may read.
#[must_use]
pub fn plan_compaction_units(
    segments: &[SegmentRange],
    max_rows_per_file: i64,
    max_unit_rows: i64,
) -> CompactionPlan {
    let mut plan = CompactionPlan::default();
    let components = overlap_components(segments);

    let target = max_rows_per_file.max(1);
    let cap = max_unit_rows.max(1);
    let mut run: Vec<usize> = Vec::new();
    let mut run_rows = 0_i64;
    let close_run = |run: &mut Vec<usize>, run_rows: &mut i64, plan: &mut CompactionPlan| {
        if run.len() >= 2 {
            plan.units.push(CompactionUnit {
                members: std::mem::take(run),
                rows: *run_rows,
                overlapping: false,
            });
        } else {
            run.clear();
        }
        *run_rows = 0;
    };
    for component in components {
        let rows: i64 = component.iter().map(|i| segments[*i].row_count).sum();
        if component.len() > 1 {
            close_run(&mut run, &mut run_rows, &mut plan);
            if rows > cap {
                plan.skipped_over_cap += 1;
            } else {
                plan.units.push(CompactionUnit {
                    members: component,
                    rows,
                    overlapping: true,
                });
            }
            continue;
        }
        let index = component[0];
        let row_count = segments[index].row_count;
        if row_count.saturating_mul(2) > target {
            close_run(&mut run, &mut run_rows, &mut plan);
            continue;
        }
        if run_rows + row_count > target.min(cap) {
            close_run(&mut run, &mut run_rows, &mut plan);
        }
        run.push(index);
        run_rows += row_count;
    }
    close_run(&mut run, &mut run_rows, &mut plan);
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(rows: i64, min: u8, max: u8) -> SegmentRange {
        SegmentRange::single(rows, vec![min], vec![max])
    }

    fn seg2(rows: i64, a: (u8, u8), b: (u8, u8)) -> SegmentRange {
        SegmentRange {
            row_count: rows,
            bounds: vec![(vec![a.0], vec![a.1]), (vec![b.0], vec![b.1])],
            tuple: None,
        }
    }

    #[test]
    fn disjoint_full_segments_need_no_compaction() {
        let segments = [seg(100, 0, 9), seg(100, 10, 19), seg(100, 20, 29)];
        assert_eq!(plan_compaction_units(&segments, 100, 1000), CompactionPlan::default());
    }

    #[test]
    fn overlapping_segments_form_one_unit_even_when_they_only_touch() {
        let segments = [seg(100, 0, 10), seg(100, 10, 20), seg(100, 30, 40)];
        let plan = plan_compaction_units(&segments, 100, 1000);
        assert_eq!(
            plan.units,
            vec![CompactionUnit {
                members: vec![0, 1],
                rows: 200,
                overlapping: true
            }]
        );
    }

    #[test]
    fn chained_overlaps_join_into_one_component() {
        let segments = [seg(50, 0, 10), seg(50, 5, 20), seg(50, 18, 30), seg(50, 40, 50)];
        let plan = plan_compaction_units(&segments, 100, 1000);
        assert_eq!(plan.units.len(), 1);
        assert_eq!(plan.units[0].members, vec![0, 1, 2]);
    }

    #[test]
    fn a_component_over_the_cap_is_skipped_and_counted() {
        let segments = [seg(600, 0, 10), seg(600, 5, 20)];
        let plan = plan_compaction_units(&segments, 1000, 1000);
        assert!(plan.units.is_empty());
        assert_eq!(plan.skipped_over_cap, 1);
    }

    #[test]
    fn small_disjoint_neighbours_merge_up_to_the_file_target() {
        let segments = [
            seg(30, 0, 1),
            seg(30, 2, 3),
            seg(30, 4, 5),
            seg(30, 6, 7),
            seg(30, 8, 9),
        ];
        let plan = plan_compaction_units(&segments, 100, 1000);
        let members: Vec<_> = plan.units.iter().map(|unit| unit.members.clone()).collect();
        assert_eq!(members, vec![vec![0, 1, 2], vec![3, 4]]);
    }

    #[test]
    fn a_full_segment_separates_two_runs_and_lone_smalls_stay_alone() {
        let segments = [seg(10, 0, 1), seg(100, 2, 3), seg(10, 4, 5)];
        assert_eq!(plan_compaction_units(&segments, 100, 1000), CompactionPlan::default());
    }

    #[test]
    fn composite_boxes_that_miss_on_the_second_column_do_not_overlap() {
        // same leading range, but the second column separates them: no key can be in both
        let segments = [seg2(100, (1, 5), (0, 9)), seg2(100, (1, 5), (10, 19))];
        assert_eq!(keys_disjoint(&segments, 1000), Some(true));
        assert!(overlap_components(&segments).iter().all(|c| c.len() == 1));
        assert_eq!(plan_compaction_units(&segments, 100, 1000), CompactionPlan::default());
    }

    #[test]
    fn composite_boxes_that_intersect_on_every_column_overlap() {
        let segments = [seg2(100, (1, 5), (0, 9)), seg2(100, (3, 8), (5, 12))];
        assert_eq!(keys_disjoint(&segments, 1000), Some(false));
        assert_eq!(overlap_components(&segments), vec![vec![0, 1]]);
    }

    #[test]
    fn disjointness_gives_up_past_the_pair_budget() {
        let segments: Vec<_> = (0..50).map(|i| seg2(10, (0, 255), (i * 2, i * 2 + 1))).collect();
        assert_eq!(keys_disjoint(&segments, 10), None);
        assert_eq!(keys_disjoint(&segments, 100_000), Some(true));
    }

    #[test]
    fn single_column_disjointness_matches_interval_overlap() {
        assert_eq!(keys_disjoint(&[seg(1, 0, 9), seg(1, 10, 19)], 100), Some(true));
        assert_eq!(keys_disjoint(&[seg(1, 0, 10), seg(1, 10, 19)], 100), Some(false));
    }

    fn with_tuple(mut segment: SegmentRange, min: &[u8], max: &[u8]) -> SegmentRange {
        segment.tuple = Some((min.to_vec(), max.to_vec()));
        segment
    }

    #[test]
    fn tuple_intervals_separate_segments_whose_boxes_intersect() {
        // tenant 1 straddles both segments and the id ranges overlap, but the tuples do not
        let segments = [
            with_tuple(seg2(100, (0, 1), (1, 130)), &[0, 1], &[1, 70]),
            with_tuple(seg2(100, (1, 2), (1, 130)), &[1, 71], &[2, 40]),
        ];
        assert_eq!(keys_disjoint(&segments, 100), Some(true));
        // without the tuple bounds the boxes cannot tell
        let boxes_only = [seg2(100, (0, 1), (1, 130)), seg2(100, (1, 2), (1, 130))];
        assert_eq!(keys_disjoint(&boxes_only, 100), Some(false));
    }

    #[test]
    fn boxes_separate_segments_whose_tuple_intervals_overlap() {
        // ids ascend with the load order but every segment spans all tenants: the lexicographic
        // intervals interleave, the id column separates the boxes
        let segments = [
            with_tuple(seg2(100, (0, 3), (1, 20)), &[0, 4], &[3, 19]),
            with_tuple(seg2(100, (0, 3), (21, 40)), &[0, 24], &[3, 39]),
        ];
        assert_eq!(keys_disjoint(&segments, 100), Some(true));
    }

    #[test]
    fn a_shared_key_is_found_when_both_tests_intersect() {
        let segments = [
            with_tuple(seg2(100, (0, 3), (1, 20)), &[0, 4], &[3, 19]),
            with_tuple(seg2(100, (0, 3), (10, 40)), &[0, 12], &[3, 39]),
        ];
        assert_eq!(keys_disjoint(&segments, 100), Some(false));
    }
}
