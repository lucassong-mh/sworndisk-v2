//! MemTable.
use super::AsKv;
use crate::prelude::*;

use alloc::collections::BTreeMap;
use core::fmt::Debug;

/// MemTable for LSM-Tree.
pub(super) struct MemTable<K, V> {
    // Use `ValueEx<V>` instead `V` to maintain multiple
    // values tagged with sync id for each key
    table: BTreeMap<K, ValueEx<V>>,
    size: usize,
    cap: usize,
    sync_id: u64,
    on_drop_record: Option<Arc<dyn Fn(&dyn AsKv<K, V>)>>,
}

// Value which is sync-aware
// At most one unsynced&one synced records can coexist at the same time
#[derive(Clone, Debug)]
pub(super) enum ValueEx<V> {
    Synced(V),
    Unsynced(V),
    SyncedAndUnsynced(V, V),
}

impl<V: Copy> ValueEx<V> {
    fn new(value: V) -> Self {
        Self::Unsynced(value)
    }

    fn get(&self) -> &V {
        match self {
            ValueEx::Synced(v) => v,
            ValueEx::Unsynced(v) => v,
            ValueEx::SyncedAndUnsynced(_, v) => v,
        }
    }

    fn put(&mut self, value: V) -> Option<V> {
        // TODO: Optimize this by using `mem::take`
        let (updated, replaced) = match self {
            ValueEx::Synced(v) => (Self::SyncedAndUnsynced(*v, value), None),
            ValueEx::Unsynced(v) => (Self::Unsynced(value), Some(*v)),
            ValueEx::SyncedAndUnsynced(cv, _ucv) => {
                (Self::SyncedAndUnsynced(*cv, value), Some(*cv))
            }
        };
        *self = updated;
        replaced
    }

    fn sync(&mut self) -> Option<V> {
        // TODO: Optimize this by using `mem::take`
        let (updated, replaced) = match self {
            ValueEx::Synced(_v) => (None, None),
            ValueEx::Unsynced(v) => (Some(Self::Synced(*v)), None),
            ValueEx::SyncedAndUnsynced(cv, ucv) => (Some(Self::Synced(*cv)), Some(*ucv)),
        };
        updated.map(|updated| *self = updated);
        replaced
    }
}

impl<K: Copy + Ord + Debug, V: Copy> MemTable<K, V> {
    pub fn new(
        cap: usize,
        sync_id: u64,
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
        // Return value which tagged most latest sync id
        Some(value_ex.get())
    }

    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        if let Some(value_ex) = self.table.get_mut(&key) {
            if let Some(replaced) = value_ex.put(value) {
                self.on_drop_record
                    .as_ref()
                    .map(|on_drop_record| on_drop_record(&(key, replaced)));
                self.size -= 1;
                return Some(replaced);
            }
        }
        self.table.insert(key, ValueEx::new(value));
        self.size += 1;
        None
    }

    pub fn sync(&mut self, sync_id: u64) -> Result<()> {
        for (k, v_ex) in &mut self.table {
            if let Some(replaced) = v_ex.sync() {
                self.on_drop_record
                    .as_ref()
                    .map(|on_drop_record| on_drop_record(&(*k, replaced)));
                self.size -= 1;
            }
        }
        self.sync_id = sync_id;
        Ok(())
    }

    // Records should be tagged with sync id
    pub fn keys_values(&self) -> impl Iterator<Item = (&K, &ValueEx<V>)> {
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
