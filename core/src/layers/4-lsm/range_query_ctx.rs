// Context for range query.
use super::{RecordKey, RecordValue};
use crate::prelude::*;
use crate::util::BitMap;

use core::ops::RangeInclusive;

/// Context for a range query request.
/// It tracks the completing process of each slot within the range.
#[derive(Debug)]
pub struct RangeQueryCtx<K, V> {
    start: K,
    count: usize,
    complete_table: BitMap,
    res: Vec<(K, V)>,
}

impl<K: RecordKey<K>, V: RecordValue> RangeQueryCtx<K, V> {
    pub fn new(start: K, count: usize) -> Self {
        Self {
            start,
            count,
            complete_table: BitMap::repeat(false, count),
            res: Vec::with_capacity(count),
        }
    }

    /// Gets the uncompleted range within the whole, returns `None`
    /// if all slots are already completed.
    pub fn range_uncompleted(&self) -> Option<RangeInclusive<K>> {
        let first_uncompleted = self.start + self.complete_table.first_zero(0)?;
        let last_uncompleted = self.start + self.complete_table.last_zero()?;
        Some(first_uncompleted..=last_uncompleted)
    }

    pub fn contains_uncompleted(&self, key: &K) -> bool {
        let nth = *key - self.start;
        nth < self.count && !self.complete_table[nth]
    }

    pub fn is_completed(&self) -> bool {
        self.complete_table.count_zeros() == 0
    }

    /// Complete one slot within the range.
    pub fn complete(&mut self, key: K, value: V) {
        let nth = key - self.start;
        if self.complete_table[nth] {
            return;
        }

        self.res.push((key, value));
        self.complete_table.set(nth, true);
    }

    pub fn mark_completed(&mut self, key: K) {
        let nth = key - self.start;
        self.complete_table.set(nth, true);
    }

    pub fn as_results(self) -> Vec<(K, V)> {
        debug_assert!(self.is_completed());
        self.res
    }
}
