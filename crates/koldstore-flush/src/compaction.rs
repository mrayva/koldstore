//! Compaction planning: which active segments are rewritten together.
//!
//! Pure bookkeeping over each segment's row count and whole-segment key bounds (Sort Key V1 bytes of
//! the single primary-key column); the executor in `pg_koldstore` does the reading and rewriting.
//!
//! Two kinds of unit are produced. An *overlap component* is a maximal set of segments whose key
//! ranges chain together (touching counts, since one key may sit in both): rewriting it keeps the
//! newest version of every key and restores disjoint ranges, which is what lets scans skip
//! retaining cold identities. A *small run* is a run of adjacent, already disjoint small segments
//! merged into fewer, fuller ones, so many tiny flushes do not leave the table with hundreds of
//! objects.

/// One active segment's size and key range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRange {
    /// Rows stored in the segment, including superseded versions and delete markers.
    pub row_count: i64,
    /// Smallest key (Sort Key V1 bytes).
    pub min: Vec<u8>,
    /// Largest key (Sort Key V1 bytes).
    pub max: Vec<u8>,
}

/// Segments to rewrite together, as indexes into the planner's input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionUnit {
    /// Input indexes, in key order.
    pub members: Vec<usize>,
    /// Sum of the members' row counts (what the rewrite reads).
    pub rows: i64,
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
    let mut components: Vec<Vec<usize>> = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    let mut current_max: Option<&[u8]> = None;
    for (index, segment) in segments.iter().enumerate() {
        match current_max {
            Some(max) if segment.min.as_slice() <= max => {
                current.push(index);
                if segment.max.as_slice() > max {
                    current_max = Some(segment.max.as_slice());
                }
            }
            _ => {
                if !current.is_empty() {
                    components.push(std::mem::take(&mut current));
                }
                current.push(index);
                current_max = Some(segment.max.as_slice());
            }
        }
    }
    if !current.is_empty() {
        components.push(current);
    }

    let target = max_rows_per_file.max(1);
    let cap = max_unit_rows.max(1);
    let mut run: Vec<usize> = Vec::new();
    let mut run_rows = 0_i64;
    let close_run = |run: &mut Vec<usize>, run_rows: &mut i64, plan: &mut CompactionPlan| {
        if run.len() >= 2 {
            plan.units.push(CompactionUnit {
                members: std::mem::take(run),
                rows: *run_rows,
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
        SegmentRange {
            row_count: rows,
            min: vec![min],
            max: vec![max],
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
                rows: 200
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
}
