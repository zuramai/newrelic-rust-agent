//! Priority reservoir: keeps the `cap` highest-priority items and counts everything seen.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::ids::Priority;

#[derive(Debug)]
struct Prioritized<T> {
    priority: Priority,
    item: T,
}

impl<T> PartialEq for Prioritized<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl<T> Eq for Prioritized<T> {}

impl<T> PartialOrd for Prioritized<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Prioritized<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

#[derive(Debug)]
pub(crate) struct Reservoir<T> {
    cap: usize,
    heap: BinaryHeap<Reverse<Prioritized<T>>>,
    seen: u64,
}

impl<T> Reservoir<T> {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap,
            heap: BinaryHeap::new(),
            seen: 0,
        }
    }

    pub(crate) fn cap(&self) -> usize {
        self.cap
    }

    pub(crate) fn seen(&self) -> u64 {
        self.seen
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.heap.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub(crate) fn push(&mut self, priority: Priority, item: T) {
        self.seen += 1;
        self.insert(priority, item);
    }

    fn insert(&mut self, priority: Priority, item: T) {
        if self.cap == 0 {
            return;
        }
        if self.heap.len() < self.cap {
            self.heap.push(Reverse(Prioritized { priority, item }));
            return;
        }
        if let Some(mut min) = self.heap.peek_mut()
            && priority >= min.0.priority
        {
            *min = Reverse(Prioritized { priority, item });
        }
    }

    /// Merges a reservoir whose send failed back in.
    pub(crate) fn merge(&mut self, other: Self) {
        self.seen += other.seen;
        for Reverse(p) in other.heap {
            self.insert(p.priority, p.item);
        }
    }

    pub(crate) fn set_cap(&mut self, cap: usize) {
        self.cap = cap;
        while self.heap.len() > cap {
            self.heap.pop();
        }
    }

    /// Empties the reservoir, returning a fresh one with the same cap.
    pub(crate) fn take(&mut self) -> Self {
        std::mem::replace(self, Self::new(self.cap))
    }

    pub(crate) fn items(&self) -> impl Iterator<Item = &T> {
        self.heap.iter().map(|Reverse(p)| &p.item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_keeps_highest_priorities_when_full() {
        let mut r = Reservoir::new(2);
        for (p, v) in [(0.1, 1), (0.9, 2), (0.5, 3), (0.05, 4)] {
            r.push(Priority::new(p), v);
        }
        let mut kept: Vec<_> = r.items().copied().collect();
        kept.sort_unstable();
        assert_eq!(kept, vec![2, 3]);
        assert_eq!(r.seen(), 4);
    }

    #[test]
    fn set_cap_drops_lowest_priorities() {
        let mut r = Reservoir::new(3);
        for (p, v) in [(0.1, 1), (0.9, 2), (0.5, 3)] {
            r.push(Priority::new(p), v);
        }
        r.set_cap(1);
        assert_eq!(r.items().copied().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn merge_sums_seen_counts() {
        let mut a = Reservoir::new(10);
        a.push(Priority::new(0.1), 1);
        let mut b = Reservoir::new(10);
        b.push(Priority::new(0.2), 2);
        b.push(Priority::new(0.3), 3);
        a.merge(b);
        assert_eq!((a.len(), a.seen()), (3, 3));
    }
}
