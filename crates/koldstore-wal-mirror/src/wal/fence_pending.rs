//! Which foreground read-fence progress is committed yet.
//!
//! A hydrate-on-write read fence applies WAL into the mirror inside the caller's own
//! (sub)transaction, so the progress it made is only real once that transaction commits: a
//! subtransaction abort discards the mirror rows its fences wrote, and so must discard the fences.
//! This is the PostgreSQL-free bookkeeping for that; `pg_koldstore` feeds it nesting levels and
//! transaction events.

/// Fence LSNs awaiting the outcome of the (sub)transactions they ran in.
#[derive(Debug, Default)]
pub struct PendingFences {
    /// `(subtransaction nesting level, highest fence LSN applied at that level)`.
    by_level: Vec<(u32, u64)>,
}

impl PendingFences {
    #[must_use]
    pub const fn new() -> Self {
        Self { by_level: Vec::new() }
    }

    /// A fence ran at `level` and applied the mirror through `lsn`.
    pub fn note(&mut self, level: u32, lsn: u64) {
        self.raise(level, lsn);
    }

    /// The subtransaction at `level` committed: its fences now belong to the parent.
    pub fn commit_subtransaction(&mut self, level: u32) {
        if level <= 1 {
            return;
        }
        let mut promoted = None::<u64>;
        self.by_level.retain(|&(l, lsn)| {
            if l == level {
                promoted = Some(promoted.map_or(lsn, |p| p.max(lsn)));
                false
            } else {
                true
            }
        });
        if let Some(lsn) = promoted {
            self.raise(level - 1, lsn);
        }
    }

    /// The subtransaction at `level` (and anything nested in it) aborted: its mirror rows are
    /// rolled back, so its fences did not happen.
    pub fn abort_subtransaction(&mut self, level: u32) {
        self.by_level.retain(|&(l, _)| l < level);
    }

    /// The top-level transaction committed: the highest surviving fence LSN, if any.
    pub fn take_committed(&mut self) -> Option<u64> {
        let max = self.by_level.iter().map(|&(_, lsn)| lsn).max();
        self.by_level.clear();
        max
    }

    /// The top-level transaction aborted or was prepared: nothing is committed.
    pub fn clear(&mut self) {
        self.by_level.clear();
    }

    fn raise(&mut self, level: u32, lsn: u64) {
        match self.by_level.iter_mut().find(|(l, _)| *l == level) {
            Some((_, existing)) => *existing = (*existing).max(lsn),
            None => self.by_level.push((level, lsn)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PendingFences;

    #[test]
    fn top_level_commit_publishes_the_highest_fence() {
        let mut p = PendingFences::new();
        p.note(1, 100);
        p.note(1, 90);
        p.note(1, 120);
        assert_eq!(p.take_committed(), Some(120));
        assert_eq!(p.take_committed(), None);
    }

    #[test]
    fn nothing_noted_publishes_nothing() {
        assert_eq!(PendingFences::new().take_committed(), None);
    }

    #[test]
    fn aborted_subtransaction_fences_are_discarded() {
        let mut p = PendingFences::new();
        p.note(1, 100);
        p.note(2, 500);
        p.abort_subtransaction(2);
        assert_eq!(p.take_committed(), Some(100));
    }

    #[test]
    fn abort_drops_nested_levels_too() {
        let mut p = PendingFences::new();
        p.note(2, 200);
        p.note(3, 300);
        p.note(4, 400);
        p.abort_subtransaction(3);
        assert_eq!(p.take_committed(), Some(200));
    }

    #[test]
    fn committed_subtransaction_promotes_to_parent_and_dies_with_it() {
        let mut p = PendingFences::new();
        p.note(2, 300);
        p.commit_subtransaction(2);
        // Now owned by level 1; aborting a *sibling* level 2 must not drop it.
        p.abort_subtransaction(2);
        assert_eq!(p.take_committed(), Some(300));

        let mut q = PendingFences::new();
        q.note(3, 700);
        q.commit_subtransaction(3); // -> level 2
        q.abort_subtransaction(2); // parent aborts: promoted fence goes with it
        assert_eq!(q.take_committed(), None);
    }

    #[test]
    fn promotion_keeps_the_parents_own_higher_fence() {
        let mut p = PendingFences::new();
        p.note(1, 900);
        p.note(2, 300);
        p.commit_subtransaction(2);
        assert_eq!(p.take_committed(), Some(900));
    }

    #[test]
    fn abort_of_top_level_clears_everything() {
        let mut p = PendingFences::new();
        p.note(1, 100);
        p.note(2, 200);
        p.clear();
        assert_eq!(p.take_committed(), None);
    }
}
