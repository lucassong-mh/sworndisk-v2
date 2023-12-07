use super::{mem_table::ValueEx, TxEventListener};
use crate::prelude::*;

use alloc::collections::BTreeMap;
use pod::Pod;

pub(super) struct Compactor;

impl Compactor {
    pub fn compact_records<K: Ord + Pod + Debug, V: Pod + Debug>(
        upper_records: impl Iterator<Item = (K, ValueEx<V>)>,
        lower_records: impl Iterator<Item = (K, ValueEx<V>)>,
    ) -> (Vec<(K, ValueEx<V>)>, Vec<(K, V)>) {
        let mut merged_map: BTreeMap<K, ValueEx<V>> = upper_records.collect();
        let mut dropped_records = Vec::new();
        for (key, rv_ex) in lower_records {
            if let Some(lv_ex) = merged_map.get_mut(&key) {
                let replaced_opt = match (&lv_ex, rv_ex) {
                    (ValueEx::Synced(_), ValueEx::Synced(rv)) => {
                        dropped_records.push((key, rv));
                        None
                    }
                    (ValueEx::Unsynced(lv), ValueEx::Synced(rv)) => {
                        Some(ValueEx::SyncedAndUnsynced(rv, *lv))
                    }
                    (ValueEx::Unsynced(_), ValueEx::Unsynced(rv)) => {
                        dropped_records.push((key, rv));
                        None
                    }
                    (ValueEx::Unsynced(lv), ValueEx::SyncedAndUnsynced(rcv, rucv)) => {
                        dropped_records.push((key, rucv));
                        Some(ValueEx::SyncedAndUnsynced(rcv, *lv))
                    }
                    (ValueEx::SyncedAndUnsynced(_, _), ValueEx::Synced(rcv)) => {
                        dropped_records.push((key, rcv));
                        None
                    }
                    _ => {
                        unreachable!()
                    }
                };
                if let Some(replaced) = replaced_opt {
                    *lv_ex = replaced;
                }
            } else {
                let _ = merged_map.insert(key, rv_ex);
            }
        }
        let compacted_records = merged_map.into_iter().collect();
        (compacted_records, dropped_records)
    }

    pub fn on_drop_records<K: Ord + Pod + Debug, V: Pod + Debug>(
        event_listener: &Arc<dyn TxEventListener<K, V>>,
        dropped_records: impl Iterator<Item = (K, V)>,
    ) -> Result<()> {
        for record in dropped_records {
            event_listener.on_drop_record(&record)?;
        }
        Ok(())
    }
}

// // Merge all records arrays into one
// fn merge_sort(
//     mut arrays: Vec<Vec<(K, ValueEx<V>)>>,
//     event_listener: &Arc<dyn TxEventListener<K, V>>,
// ) -> Result<Vec<(K, ValueEx<V>)>> {
//     debug_assert!(!arrays.is_empty());
//     if arrays.len() == 1 {
//         return Ok(arrays.pop().unwrap());
//     }

//     let mid = arrays.len() / 2;

//     let left_arrays = arrays.drain(..mid).collect::<Vec<Vec<(K, ValueEx<V>)>>>();
//     let right_arrays = arrays;

//     let left_sorted = Self::merge_sort(left_arrays, event_listener)?;
//     let right_sorted = Self::merge_sort(right_arrays, event_listener)?;

//     Self::merge(left_sorted, right_sorted, event_listener)
// }

// // Merge two records arrays
// fn merge(
//     mut left: Vec<(K, ValueEx<V>)>,
//     mut right: Vec<(K, ValueEx<V>)>,
//     event_listener: &Arc<dyn TxEventListener<K, V>>,
// ) -> Result<Vec<(K, ValueEx<V>)>> {
//     let mut merged = Vec::with_capacity(left.len() + right.len());

//     while !left.is_empty() && !right.is_empty() {
//         let lkey = left[0].0;
//         let rkey = right[0].0;
//         if lkey == rkey {
//             match (left.remove(0).1, right.remove(0).1) {
//                 (ValueEx::Synced(lv), ValueEx::Synced(rv)) => {
//                     event_listener.on_drop_record(&(rkey, rv))?;
//                     merged.push((lkey, ValueEx::Synced(lv)));
//                 }
//                 (ValueEx::Unsynced(lv), ValueEx::Synced(rv)) => {
//                     merged.push((lkey, ValueEx::SyncedAndUnsynced(rv, lv)))
//                 }
//                 (ValueEx::Unsynced(lv), ValueEx::Unsynced(rv)) => {
//                     event_listener.on_drop_record(&(rkey, rv))?;
//                     merged.push((lkey, ValueEx::Unsynced(lv)));
//                 }
//                 (ValueEx::Unsynced(lv), ValueEx::SyncedAndUnsynced(rcv, rucv)) => {
//                     event_listener.on_drop_record(&(rkey, rucv))?;
//                     merged.push((lkey, ValueEx::SyncedAndUnsynced(rcv, lv)));
//                 }
//                 (ValueEx::SyncedAndUnsynced(lcv, lucv), ValueEx::Synced(rcv)) => {
//                     event_listener.on_drop_record(&(rkey, rcv))?;
//                     merged.push((lkey, ValueEx::SyncedAndUnsynced(lcv, lucv)));
//                 }
//                 _ => {
//                     unreachable!()
//                 }
//             }
//         } else if lkey < rkey {
//             merged.push(left.remove(0));
//         } else {
//             merged.push(right.remove(0));
//         }
//     }

//     merged.extend(left);
//     merged.extend(right);

//     Ok(merged)
// }
