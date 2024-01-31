//! MemTable.
use super::{AsKV, RangeQueryCtx, RecordKey, RecordValue, SyncID};
use crate::os::{BTreeMap, Mutex, RwLock, RwLockReadGuard};
use crate::prelude::*;

use core::fmt;

/// Manager for an active `MemTable` and an immutable `MemTable`
/// in a `TxLsmTree`.
pub(super) struct MemTableManager<K: RecordKey<K>, V> {
    active: Mutex<MemTable<K, V>>,
    immutable: RwLock<MemTable<K, V>>, // Read-only most of the time
}

/// MemTable for LSM-Tree.
///
/// Manages organized key-value records in memory with a capacity.
/// Each `MemTable` is sync-aware (tagged with current sync ID).
/// Both synced and unsynced records can co-exist.
/// Also supports user-defined callback when a record is dropped.
pub(super) struct MemTable<K: RecordKey<K>, V> {
    table: BTreeMap<K, ValueEx<V>>,
    size: usize,
    cap: usize,
    sync_id: SyncID,
    on_drop_record: Option<Arc<dyn Fn(&dyn AsKV<K, V>)>>,
}

/// An extended value which is sync-aware.
/// At most one unsynced and one synced records can coexist at the same time.
#[derive(Clone, Debug)]
pub(super) enum ValueEx<V> {
    Synced(V),
    Unsynced(V),
    SyncedAndUnsynced(V, V),
}

impl<K: RecordKey<K>, V: RecordValue> MemTableManager<K, V> {
    /// Creates a new `MemTableManager` given the current master sync ID,
    /// the capacity and the callback when dropping records.
    pub fn new(
        sync_id: SyncID,
        capacity: usize,
        on_drop_record_in_memtable: Option<Arc<dyn Fn(&dyn AsKV<K, V>)>>,
    ) -> Self {
        let active = Mutex::new(MemTable::new(
            capacity,
            sync_id,
            on_drop_record_in_memtable.clone(),
        ));
        let immutable = RwLock::new(MemTable::new(capacity, sync_id, on_drop_record_in_memtable));

        Self { active, immutable }
    }

    /// Gets the target value of the given key from the `MemTable`s.
    pub fn get(&self, key: &K) -> Option<V> {
        if let Some(value) = self.active.lock().get(key) {
            return Some(value.clone());
        }

        if let Some(value) = self.immutable.read().get(key) {
            return Some(value.clone());
        }

        None
    }

    /// Gets the range of values from the `MemTable`s.
    pub fn get_range(&self, range_query_ctx: &mut RangeQueryCtx<K, V>) -> bool {
        let is_completed = self.active.lock().get_range(range_query_ctx);
        if is_completed {
            return is_completed;
        }

        self.immutable.read().get_range(range_query_ctx)
    }

    /// Puts a key-value pair into the active `MemTable`, and
    /// return whether the active `MemTable` is full.
    pub fn put(&self, key: K, value: V) -> bool {
        let mut active = self.active.lock();
        let _ = active.put(key, value);
        active.at_capacity()
    }

    /// Sync the active `MemTable` with the given sync ID.
    pub fn sync(&self, sync_id: SyncID) -> Result<()> {
        self.active.lock().sync(sync_id)
    }

    /// Switch two `MemTable`s. Should only be called in a situation that
    /// the active `MemTable` becomes full and the immutable `MemTable` is
    /// ready to be cleared.
    pub fn switch(&self) -> Result<()> {
        let mut active = self.active.lock();
        let sync_id = active.sync_id();
        let mut immutable = self.immutable.write();
        immutable.clear();
        debug_assert!(immutable.is_empty());

        core::mem::swap(&mut *active, &mut *immutable);

        // Update sync ID of the switched active `MemTable`
        active.sync(sync_id)
    }

    /// Gets the immutable `MemTable` instance (read-only).
    pub fn immutable_memtable(&self) -> RwLockReadGuard<MemTable<K, V>> {
        self.immutable.read()
    }
}

impl<K: RecordKey<K>, V: RecordValue> MemTable<K, V> {
    /// Creates a new `MemTable`, given the capacity, the current sync ID,
    /// and the callback of dropping record.
    pub fn new(
        cap: usize,
        sync_id: SyncID,
        on_drop_record: Option<Arc<dyn Fn(&dyn AsKV<K, V>)>>,
    ) -> Self {
        Self {
            table: BTreeMap::new(),
            size: 0,
            cap,
            sync_id,
            on_drop_record,
        }
    }

    /// Gets the target value given the key.
    pub fn get(&self, key: &K) -> Option<&V> {
        let value_ex = self.table.get(key)?;
        Some(value_ex.get())
    }

    /// Range query, returns whether the request is completed.
    pub fn get_range(&self, range_query_ctx: &mut RangeQueryCtx<K, V>) -> bool {
        debug_assert!(!range_query_ctx.is_completed());
        let target_range = range_query_ctx.range_uncompleted().unwrap();

        for (k, v_ex) in self.table.range(target_range) {
            range_query_ctx.complete(*k, *v_ex.get());
        }

        range_query_ctx.is_completed()
    }

    /// Puts a new K-V record to the table, drop the old one.
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

        let _ = self.table.insert(key, ValueEx::new(value));
        self.size += 1;
        None
    }

    /// Sync the table, update the sync ID, drop the replaced one.
    pub fn sync(&mut self, sync_id: SyncID) -> Result<()> {
        debug_assert!(self.sync_id <= sync_id);
        if self.sync_id == sync_id {
            return Ok(());
        }

        for (k, v_ex) in self.table.iter_mut() {
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

    /// Return the sync ID of this table.
    pub fn sync_id(&self) -> SyncID {
        self.sync_id
    }

    /// Return an iterator over the table.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &ValueEx<V>)> {
        self.table.iter()
    }

    /// Return the number of records in the table.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Return whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Return whether the table is full.
    pub fn at_capacity(&self) -> bool {
        self.size == self.cap
    }

    /// Clear all records from the table.
    pub fn clear(&mut self) {
        self.table.clear();
        self.size = 0;
    }
}

impl<V: RecordValue> ValueEx<V> {
    /// Creates a new unsynced value.
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

    /// Puts a new value, return the replaced value if any.
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

    /// Sync the value, return the replaced value if any.
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

impl<K: RecordKey<K>, V: RecordValue> Debug for MemTableManager<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemTableManager")
            .field("active_memtable_size", &self.active.lock().size())
            .field("immutable_memtable_size", &self.immutable_memtable().size())
            .finish()
    }
}