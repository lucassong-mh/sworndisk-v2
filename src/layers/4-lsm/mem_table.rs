//! MemTable.
use super::{AsKv, RangeQueryCtx, RecordKey, RecordValue, SyncID};
use crate::prelude::*;

use alloc::collections::BTreeMap;
use core::fmt::Debug;
use pod::Pod;

/// MemTable for LSM-Tree.
///
/// Manages organized key-value records in memory with a capacity.
/// Each `MemTable` is sync-aware (tagged with current sync ID).
/// Both synced and unsynced records can co-exist.
/// Also supports user-defined callback when a record is dropped.
pub(super) struct MemTable<K, V> {
    table: BTreeMap<K, ValueEx<V>>, // TODO: Try skiplist
    size: usize,
    cap: usize,
    sync_id: SyncID,
    on_drop_record: Option<Arc<dyn Fn(&dyn AsKv<K, V>)>>,
}

/// An extended value which is sync-aware.
/// At most one unsynced and one synced records can coexist at the same time.
#[derive(Clone, Debug)]
pub(super) enum ValueEx<V> {
    Synced(V),
    Unsynced(V),
    SyncedAndUnsynced(V, V),
}

impl<K: RecordKey<K>, V: RecordValue> MemTable<K, V> {
    pub fn new(
        cap: usize,
        sync_id: SyncID,
        on_drop_record: Option<Arc<dyn Fn(&dyn AsKv<K, V>)>>,
    ) -> Self {
        Self {
            table: BTreeMap::new(),
            size: 0,
            cap,
            sync_id,
            on_drop_record,
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let value_ex = self.table.get(key)?;
        Some(value_ex.get())
    }

    /// Range query, returns whether the request is completed.
    pub fn get_range(&self, range_query_ctx: &mut RangeQueryCtx<K, V>) -> bool {
        debug_assert!(!range_query_ctx.is_completed());
        for (k, v_ex) in self
            .table
            .range(range_query_ctx.range_uncompleted().unwrap())
        {
            range_query_ctx.complete(*k, *v_ex.get());
        }
        range_query_ctx.is_completed()
    }

    /// Put a new K-V record to the table, drop the old one.
    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        if let Some(value_ex) = self.table.get_mut(&key) {
            if let Some(dropped) = value_ex.put(value) {
                self.on_drop_record
                    .as_ref()
                    .map(|on_drop_record| on_drop_record(&(key, dropped)));
                return Some(dropped);
            } else {
                self.size += 1;
                return None;
            }
        }

        self.table.insert(key, ValueEx::new(value));
        self.size += 1;
        None
    }

    /// Sync the table, update the sync ID, drop the replaced one.
    pub fn sync(&mut self, sync_id: SyncID) -> Result<()> {
        debug_assert!(self.sync_id <= sync_id);
        if self.sync_id == sync_id {
            return Ok(());
        }

        for (k, v_ex) in &mut self.table {
            if let Some(dropped) = v_ex.sync() {
                self.on_drop_record
                    .as_ref()
                    .map(|on_drop_record| on_drop_record(&(*k, dropped)));
                self.size -= 1;
            }
        }
        self.sync_id = sync_id;
        Ok(())
    }

    pub fn sync_id(&self) -> SyncID {
        self.sync_id
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &ValueEx<V>)> {
        self.table.iter()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn at_capacity(&self) -> bool {
        self.size == self.cap
    }

    pub fn clear(&mut self) {
        self.table.clear();
        self.size = 0;
    }
}

impl<V: RecordValue> ValueEx<V> {
    fn new(value: V) -> Self {
        Self::Unsynced(value)
    }

    /// Gets the most recent value.
    fn get(&self) -> &V {
        match self {
            Self::Synced(v) => v,
            Self::Unsynced(v) => v,
            Self::SyncedAndUnsynced(_, v) => v,
        }
    }

    fn put(&mut self, value: V) -> Option<V> {
        let existed = core::mem::take(self);

        let dropped = match existed {
            ValueEx::Synced(v) => {
                *self = Self::SyncedAndUnsynced(v, value);
                None
            }
            ValueEx::Unsynced(v) => {
                *self = Self::Unsynced(value);
                Some(v)
            }
            ValueEx::SyncedAndUnsynced(sv, usv) => {
                *self = Self::SyncedAndUnsynced(sv, value);
                Some(usv)
            }
        };
        dropped
    }

    fn sync(&mut self) -> Option<V> {
        let existed = core::mem::take(self);

        let dropped = match existed {
            ValueEx::Synced(_v) => None,
            ValueEx::Unsynced(v) => {
                *self = Self::Synced(v);
                None
            }
            ValueEx::SyncedAndUnsynced(sv, usv) => {
                *self = Self::Synced(usv);
                Some(sv)
            }
        };
        dropped
    }
}

impl<V: RecordValue> Default for ValueEx<V> {
    fn default() -> Self {
        Self::Unsynced(V::new_uninit())
    }
}
