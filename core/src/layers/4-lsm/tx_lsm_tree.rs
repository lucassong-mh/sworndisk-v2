//! Transactional LSM-Tree.
//!
//! API: `format()`, `recover()`, `get()`, `put()`, `get_range()`, `sync()`
//!
//! Responsible for managing two `MemTable`s, WAL and SSTs as `TxLog`s
//! backed by a `TxLogStore`. All operations are executed based
//! on internal transactions.
use super::compaction::Compactor;
use super::mem_table::{MemTable, ValueEx};
use super::range_query_ctx::RangeQueryCtx;
use super::sstable::SSTable;
use super::wal::{WalAppendTx, BUCKET_WAL};
use crate::layers::bio::BlockSet;
use crate::layers::log::{TxLogId, TxLogStore};
use crate::os::RwLock;
use crate::prelude::*;
use crate::tx::Tx;

use core::fmt::{self, Debug};
use core::hash::Hash;
use core::ops::{Add, Sub};
use core::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use pod::Pod;
use rbtree::RBTree;

// TODO: Use `Thread` in os module
use std::thread;

/// XXX: Master sync ID should be stored in external trusted storage
pub static MASTER_SYNC_ID: AtomicU64 = AtomicU64::new(0);
pub type SyncID = u64;

/// A transactional LSM-Tree, managing `MemTable`s, WALs and SSTs backed by `TxLogStore` (L3).
///
/// Supports inserting and querying key-value records within transactions.
/// Supports user-defined callbacks in `MemTable`, during compaction and recovery.
pub struct TxLsmTree<K, V, D>(Arc<TreeInner<K, V, D>>);

/// Inner structures of `TxLsmTree`.
pub(super) struct TreeInner<K, V, D> {
    memtable_manager: MemTableManager<K, V>,
    sst_manager: RwLock<SstManager<K, V>>,
    wal_append_tx: WalAppendTx<D>,
    compactor: Compactor<K, V>,
    tx_log_store: Arc<TxLogStore<D>>,
    listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
}

/// Levels in a `TxLsmTree`.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum LsmLevel {
    L0 = 0,
    L1,
    L2,
    L3,
    L4,
    L5, // Include over 300 TB data
}

/// Manager for the active `MemTable` and the immutable `MemTable`.
struct MemTableManager<K, V> {
    mem_tables: [Arc<RwLock<MemTable<K, V>>>; 2],
    immut_idx: AtomicU8,
}

/// Manager of all `SSTable`s from every level in a `TxLsmTree`.
#[derive(Debug)]
struct SstManager<K, V> {
    level_ssts: Vec<RBTree<TxLogId, Arc<SSTable<K, V>>>>,
}

/// A factory of per-transaction event listeners.
pub trait TxEventListenerFactory<K, V> {
    /// Creates a new event listener for a given transaction.
    fn new_event_listener(&self, tx_type: TxType) -> Arc<dyn TxEventListener<K, V>>;
}

/// An event listener that get informed when
/// 1) A new record is added,
/// 2) An existing record is dropped,
/// 3) After a TX began,
/// 4) Before a TX ended,
/// 5) After a TX committed.
/// `tx_type` indicates an internal transaction of `TxLsmTree`.
pub trait TxEventListener<K, V> {
    /// Notify the listener that a new record is added to a LSM-Tree.
    fn on_add_record(&self, record: &dyn AsKV<K, V>) -> Result<()>;

    /// Notify the listener that an existing record is dropped from a LSM-Tree.
    fn on_drop_record(&self, record: &dyn AsKV<K, V>) -> Result<()>;

    /// Notify the listener after a TX just began.
    fn on_tx_begin(&self, tx: &mut Tx) -> Result<()>;

    /// Notify the listener before a tx ended.
    fn on_tx_precommit(&self, tx: &mut Tx) -> Result<()>;

    /// Notify the listener after a TX committed.
    fn on_tx_commit(&self);
}

/// Types of `TxLsmTree`'s internal transactions.
#[derive(Copy, Clone, Debug)]
pub enum TxType {
    /// A Compaction Transaction merges old `SSTable`s into new ones.
    Compaction { to_level: LsmLevel },
    /// A Migration Transaction migrates synced records from old `SSTable`s
    /// new ones and discard unsynced records.
    Migration,
}

/// A trait that represents the key for a record in a `TxLsmTree`.
pub trait RecordKey<K>:
    Ord + Pod + Hash + Add<usize, Output = K> + Sub<K, Output = usize> + Debug + 'static
{
}
/// A trait that represents the value for a record in a `TxLsmTree`.
pub trait RecordValue: Pod + Debug + 'static {}

/// Represent any type that includes a key and a value.
pub trait AsKV<K, V> {
    fn key(&self) -> &K;

    fn value(&self) -> &V;
}

/// Represent any type that includes a key and a sync-aware value.
pub(super) trait AsKVex<K, V> {
    fn key(&self) -> &K;

    fn value_ex(&self) -> &ValueEx<V>;
}

pub(super) const MEMTABLE_CAPACITY: usize = 81920; // TBD
pub(super) const SSTABLE_CAPACITY: usize = MEMTABLE_CAPACITY;

impl<K: RecordKey<K>, V: RecordValue, D: BlockSet + 'static> TxLsmTree<K, V, D> {
    /// Format a `TxLsmTree` from a given `TxLogStore`.
    pub fn format(
        tx_log_store: Arc<TxLogStore<D>>,
        listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
        on_drop_record_in_memtable: Option<Arc<dyn Fn(&dyn AsKV<K, V>)>>,
    ) -> Result<Self> {
        let inner = TreeInner::format(tx_log_store, listener_factory, on_drop_record_in_memtable)?;
        Ok(Self(Arc::new(inner)))
    }

    /// Recover a `TxLsmTree` from a given `TxLogStore`.
    pub fn recover(
        tx_log_store: Arc<TxLogStore<D>>,
        listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
        on_drop_record_in_memtable: Option<Arc<dyn Fn(&dyn AsKV<K, V>)>>,
    ) -> Result<Self> {
        let inner = TreeInner::recover(tx_log_store, listener_factory, on_drop_record_in_memtable)?;
        Ok(Self(Arc::new(inner)))
    }

    /// Gets a target value given a key.
    pub fn get(&self, key: &K) -> Result<V> {
        self.0.get(key)
    }

    /// Gets a range of target values given a range of keys.
    pub fn get_range(&self, range_query_ctx: &mut RangeQueryCtx<K, V>) -> Result<()> {
        self.0.get_range(range_query_ctx)
    }

    /// Puts a key-value pair to the tree.
    pub fn put(&self, key: K, value: V) -> Result<()> {
        let inner = &self.0;
        let record = (key, value);

        // 1. Write WAL
        inner.wal_append_tx.append(&record)?;

        // 2. Put into MemTable
        let at_capacity = inner.memtable_manager.put(key, value);
        if !at_capacity {
            return Ok(());
        }

        // TODO: Think of combining WAL's TX with minor compaction TX?
        inner.wal_append_tx.commit()?;

        inner.compactor.wait_compaction()?;

        // 3. Trigger compaction when MemTable is at capacity
        inner.memtable_manager.switch();

        self.do_compaction_tx()?;

        // Discard current WAL
        inner.wal_append_tx.discard()?;

        Ok(())
    }

    /// Persist all in-memory data of `TxLsmTree` to the backed storage.
    pub fn sync(&self) -> Result<()> {
        self.0.sync()
    }

    fn do_compaction_tx(&self) -> Result<()> {
        let inner = self.0.clone();
        let handle = thread::spawn(move || -> Result<()> {
            // Do major compaction first if needed
            if inner
                .sst_manager
                .read()
                .require_major_compaction(LsmLevel::L0)
            {
                inner.do_compaction_tx(LsmLevel::L1)?;
            }

            // Do minor compaction
            inner.do_compaction_tx(LsmLevel::L0)?;

            Ok(())
        });

        // handle.join().unwrap()?; // synchronous
        self.0.compactor.record_handle(handle); // asynchronous
        Ok(())
    }
}

impl<K: RecordKey<K>, V: RecordValue, D: BlockSet + 'static> TreeInner<K, V, D> {
    pub fn format(
        tx_log_store: Arc<TxLogStore<D>>,
        listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
        on_drop_record_in_memtable: Option<Arc<dyn Fn(&dyn AsKV<K, V>)>>,
    ) -> Result<Self> {
        Ok(Self {
            memtable_manager: MemTableManager::new(MEMTABLE_CAPACITY, on_drop_record_in_memtable),
            sst_manager: RwLock::new(SstManager::new()),
            wal_append_tx: WalAppendTx::new(&tx_log_store),
            compactor: Compactor::new(),
            tx_log_store,
            listener_factory,
        })
    }

    pub fn recover(
        tx_log_store: Arc<TxLogStore<D>>,
        listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
        on_drop_record_in_memtable: Option<Arc<dyn Fn(&dyn AsKV<K, V>)>>,
    ) -> Result<Self> {
        // Only synced records count, all unsynced are discarded
        let synced_records = Self::collect_synced_records_from_wal(&tx_log_store)?;

        let memtable_manager = MemTableManager::new(MEMTABLE_CAPACITY, on_drop_record_in_memtable);
        synced_records.into_iter().for_each(|(k, v)| {
            let _ = memtable_manager.put(k, v);
        });

        // Prepare SST manager (load each SST's index blocks)
        let sst_manager = Self::recover_sst_manager(&tx_log_store)?;

        let recov_self = Self {
            memtable_manager,
            sst_manager: RwLock::new(sst_manager),
            wal_append_tx: WalAppendTx::new(&tx_log_store),
            compactor: Compactor::new(),
            tx_log_store,
            listener_factory,
        };

        recov_self.do_migration_tx()?;

        debug!("[TxLsmTree Recovery] {recov_self:?}");
        Ok(recov_self)
    }

    fn collect_synced_records_from_wal(tx_log_store: &Arc<TxLogStore<D>>) -> Result<Vec<(K, V)>> {
        let mut tx = tx_log_store.new_tx();
        let res: Result<_> = tx.context(|| {
            let wal_res = tx_log_store.open_log_in(BUCKET_WAL, false);
            if let Err(e) = &wal_res
                && e.errno() == NotFound
            {
                return Ok(vec![]);
            }
            let wal = wal_res?;
            WalAppendTx::collect_synced_records::<K, V>(&wal)
        });
        if res.is_ok() {
            tx.commit()?;
            // TODO: Update master sync ID if mismatch
        } else {
            tx.abort();
            return_errno_with_msg!(TxAborted, "recover from WAL failed");
        }
        res
    }

    fn recover_sst_manager(tx_log_store: &Arc<TxLogStore<D>>) -> Result<SstManager<K, V>> {
        let mut manager = SstManager::new();
        let mut tx = tx_log_store.new_tx();
        let res: Result<_> = tx.context(|| {
            for (level, bucket) in LsmLevel::iter() {
                let log_ids = tx_log_store.list_logs_in(bucket);
                if let Err(e) = &log_ids
                    && e.errno() == NotFound
                {
                    continue;
                }

                for id in log_ids? {
                    let log = tx_log_store.open_log(id, false)?;
                    manager.insert(SSTable::from_log(&log)?, level);
                }
            }
            Ok(())
        });
        if res.is_ok() {
            tx.commit()?;
        } else {
            tx.abort();
            return_errno_with_msg!(TxAborted, "recover TxLsmTree failed");
        }
        Ok(manager)
    }

    pub fn get(&self, key: &K) -> Result<V> {
        // 1. Search from MemTables
        if let Some(value) = self.memtable_manager.get(key) {
            return Ok(value);
        }

        // 2. Search from SSTs (do Read TX)
        let value = self.do_read_tx(key)?;

        Ok(value)
    }

    pub fn get_range(&self, range_query_ctx: &mut RangeQueryCtx<K, V>) -> Result<()> {
        let is_completed = self.memtable_manager.get_range(range_query_ctx);
        if is_completed {
            return Ok(());
        }

        self.do_read_range_tx(range_query_ctx)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let master_sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed) + 1;

        self.wal_append_tx.sync(master_sync_id)?;

        self.memtable_manager.sync(master_sync_id)?;

        self.compactor.wait_compaction()?;
        self.tx_log_store.sync()?;

        // XXX: Master sync ID should be updated to trusted storage
        MASTER_SYNC_ID.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// TXs in `TxLsmTree`

    /// Read TX.
    fn do_read_tx(&self, key: &K) -> Result<V> {
        let mut tx = self.tx_log_store.new_tx();

        let read_res: Result<_> = tx.context(|| {
            // Search each level from top to bottom (newer to older)
            let sst_manager = self.sst_manager.read();

            for (level, _bucket) in LsmLevel::iter() {
                for (_id, sst) in sst_manager.list_level(level) {
                    if !sst.is_within_range(key) {
                        continue;
                    }

                    if let Ok(target_value) = sst.access_point(key, &self.tx_log_store) {
                        return Ok(target_value);
                    }
                }
            }

            return_errno_with_msg!(NotFound, "target sst not found");
        });
        if read_res.as_ref().is_err_and(|e| e.errno() != NotFound) {
            tx.abort();
            return_errno_with_msg!(TxAborted, "read TX failed")
        }

        tx.commit()?;

        read_res
    }

    /// Read Range TX.
    fn do_read_range_tx(&self, range_query_ctx: &mut RangeQueryCtx<K, V>) -> Result<()> {
        debug_assert!(!range_query_ctx.is_completed());
        let mut tx = self.tx_log_store.new_tx();

        let read_res: Result<_> = tx.context(|| {
            // Search each level from top to bottom (newer to older)
            let sst_manager = self.sst_manager.read();

            for (level, _bucket) in LsmLevel::iter() {
                for (_id, sst) in sst_manager.list_level(level) {
                    if !sst.overlap_with(&range_query_ctx.range_uncompleted().unwrap()) {
                        continue;
                    }

                    sst.access_range(range_query_ctx, &self.tx_log_store)?;

                    if range_query_ctx.is_completed() {
                        return Ok(());
                    }
                }
            }

            return_errno_with_msg!(NotFound, "target sst not found");
        });
        if read_res.as_ref().is_err_and(|e| e.errno() != NotFound) {
            tx.abort();
            return_errno_with_msg!(TxAborted, "read TX failed")
        }

        tx.commit()?;

        read_res
    }

    /// Compaction TX.
    fn do_compaction_tx(&self, to_level: LsmLevel) -> Result<()> {
        match to_level {
            LsmLevel::L0 => self.do_minor_compaction(),
            LsmLevel::L1 => self.do_major_compaction(to_level),
            _ => unreachable!(),
        }
    }

    /// Minor Compaction TX { to_level: LsmLevel::L0 }.
    fn do_minor_compaction(&self) -> Result<()> {
        let mut tx = self.tx_log_store.new_tx();
        // Prepare TX listener
        let tx_type = TxType::Compaction {
            to_level: LsmLevel::L0,
        };
        let event_listener = self.listener_factory.new_event_listener(tx_type);
        event_listener.on_tx_begin(&mut tx).map_err(|_| {
            tx.abort();
            Error::with_msg(
                TxAborted,
                "minor compaction TX callback 'on_tx_begin' failed",
            )
        })?;

        let res: Result<_> = tx.context(|| {
            let tx_log = self.tx_log_store.create_log(LsmLevel::L0.bucket())?;

            // Cook records in immutable MemTable into a new SST
            let immut_mem_table = self.memtable_manager.immut_mem_table().read();
            let records_iter = immut_mem_table.iter().map(|(k, v_ex)| {
                match v_ex {
                    ValueEx::Synced(v) | ValueEx::Unsynced(v) => {
                        event_listener.on_add_record(&(k, v)).unwrap();
                    }
                    ValueEx::SyncedAndUnsynced(sv, usv) => {
                        event_listener.on_add_record(&(k, sv)).unwrap();
                        event_listener.on_add_record(&(k, usv)).unwrap();
                    }
                }
                (k, v_ex)
            });
            let sync_id = immut_mem_table.sync_id();
            let sst = SSTable::build(records_iter, sync_id, &tx_log)?;
            self.sst_manager.write().insert(sst, LsmLevel::L0);
            Ok(())
        });
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(TxAborted, "minor compaction TX failed");
        }

        event_listener.on_tx_precommit(&mut tx).map_err(|_| {
            tx.abort();
            Error::with_msg(
                TxAborted,
                "minor compaction TX callback 'on_tx_precommit' failed",
            )
        })?;

        tx.commit()?;
        event_listener.on_tx_commit();
        self.memtable_manager.immut_mem_table().write().clear();

        debug!("[TxLsmTree Minor Compaction] {self:?}");
        Ok(())
    }

    /// Major Compaction TX { to_level: LsmLevel::L1~LsmLevel::L5 }.
    fn do_major_compaction(&self, to_level: LsmLevel) -> Result<()> {
        let from_level = to_level.upper_level();
        let mut tx = self.tx_log_store.new_tx();

        // Prepare TX listener
        let tx_type = TxType::Compaction { to_level };
        let event_listener = self.listener_factory.new_event_listener(tx_type);
        event_listener.on_tx_begin(&mut tx).map_err(|_| {
            tx.abort();
            Error::with_msg(
                TxAborted,
                "major compaction TX callback 'on_tx_begin' failed",
            )
        })?;

        let master_sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
        let tx_log_store = self.tx_log_store.clone();
        let listener = event_listener.clone();
        let res: Result<_> = tx.context(move || {
            let (mut created_ssts, mut deleted_ssts) = (vec![], vec![]);

            // Collect overlapped SSTs
            let sst_manager = self.sst_manager.read();
            let (upper_sst_id, upper_sst) = sst_manager
                .list_level(from_level)
                .last()
                .map(|(id, sst)| (*id, sst.clone()))
                .unwrap();
            let lower_ssts: Vec<(TxLogId, Arc<SSTable<K, V>>)> = sst_manager
                .list_level(to_level)
                .filter_map(|(id, sst)| {
                    if sst.overlap_with(&upper_sst.range()) {
                        Some((*id, sst.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            drop(sst_manager);

            // If there are no overlapped SSTs, just move the upper SST to the lower level
            if lower_ssts.is_empty() {
                tx_log_store.move_log(upper_sst_id, from_level.bucket(), to_level.bucket())?;
                self.sst_manager
                    .write()
                    .move_sst(upper_sst_id, from_level, to_level);
                return Ok((created_ssts, deleted_ssts));
            }

            let upper_records = {
                let tx_log = tx_log_store.open_log(upper_sst_id, false)?;
                let (records, dropped_records) =
                    upper_sst.access_scan(&tx_log, master_sync_id, false)?;
                for record in dropped_records {
                    listener.on_drop_record(&record)?;
                }
                records
            };

            let lower_records_vec = {
                let mut records_vec = Vec::with_capacity(lower_ssts.len());
                for (id, sst) in &lower_ssts {
                    let tx_log = tx_log_store.open_log(*id, false)?;
                    let (records, dropped_records) =
                        sst.access_scan(&tx_log, master_sync_id, false)?;
                    for record in dropped_records {
                        listener.on_drop_record(&record)?;
                    }
                    records_vec.push(records);
                }
                records_vec
            };

            // Compact records then build new SSTs
            created_ssts = Compactor::compact_records_and_build_ssts(
                upper_records.into_iter(),
                lower_records_vec
                    .into_iter()
                    .flat_map(|records| records.into_iter()),
                &tx_log_store,
                &listener,
                to_level,
                master_sync_id,
            )?;

            // Delete the old SSTs
            tx_log_store.delete_log(upper_sst_id)?;
            deleted_ssts.push((upper_sst_id, from_level));
            for (id, _) in lower_ssts {
                tx_log_store.delete_log(id)?;
                deleted_ssts.push((id, to_level));
            }
            Ok((created_ssts, deleted_ssts))
        });
        let (created_ssts, deleted_ssts) = res.map_err(|_| {
            tx.abort();
            Error::with_msg(TxAborted, "major compaction TX failed")
        })?;

        event_listener.on_tx_precommit(&mut tx).map_err(|_| {
            tx.abort();
            Error::with_msg(
                TxAborted,
                "major compaction TX callback 'on_tx_precommit' failed",
            )
        })?;

        tx.commit()?;
        event_listener.on_tx_commit();

        self.update_sst_manager(
            created_ssts.into_iter().map(|sst| (sst, to_level)),
            deleted_ssts.into_iter(),
        );

        // Continue to do major compaction if necessary
        if self.sst_manager.read().require_major_compaction(to_level) {
            self.do_major_compaction(to_level.lower_level())?;
        }

        debug!("[TxLsmTree Major Compaction] {self:?}");
        Ok(())
    }

    /// Migration TX, primarily to discard all unsynced records in SSTs.
    fn do_migration_tx(&self) -> Result<()> {
        let mut tx = self.tx_log_store.new_tx();

        // Prepare TX listener
        let tx_type = TxType::Migration;
        let event_listener = self.listener_factory.new_event_listener(tx_type);
        event_listener.on_tx_begin(&mut tx).map_err(|_| {
            tx.abort();
            Error::with_msg(TxAborted, "migration TX callback 'on_tx_begin' failed")
        })?;

        let master_sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
        let tx_log_store = self.tx_log_store.clone();
        let listener = event_listener.clone();
        let res: Result<_> = tx.context(move || {
            let (mut created_ssts, mut deleted_ssts) = (vec![], vec![]);

            let sst_manager = self.sst_manager.read();
            for (level, bucket) in LsmLevel::iter() {
                let ssts = sst_manager.list_level(level);
                // Iterate SSTs whose sync ID is equal to the
                // master id, who may have unsynced records
                for (&id, sst) in ssts.filter(|(_, sst)| sst.sync_id() == master_sync_id) {
                    // Collect synced records only
                    let log = tx_log_store.open_log(id, false)?;
                    let (synced_records, dropped_records) =
                        sst.access_scan(&log, master_sync_id, true)?;
                    for (k, v) in dropped_records {
                        listener.on_drop_record(&(k, v))?;
                    }

                    if !synced_records.is_empty() {
                        // Create new migrated SST
                        let new_log = tx_log_store.create_log(bucket)?;
                        let new_sst =
                            SSTable::build(synced_records.into_iter(), master_sync_id, &new_log)?;
                        created_ssts.push((new_sst, level));
                        continue;
                    }

                    deleted_ssts.push((id, level));
                    // Delete old SST
                    tx_log_store.delete_log(id)?;
                }
            }

            Ok((created_ssts, deleted_ssts))
        });
        let (created_ssts, deleted_ssts) = res.map_err(|_| {
            tx.abort();
            Error::with_msg(TxAborted, "migration TX failed")
        })?;

        event_listener.on_tx_precommit(&mut tx).map_err(|_| {
            tx.abort();
            Error::with_msg(TxAborted, "migration TX callback 'on_tx_precommit' failed")
        })?;

        tx.commit()?;
        event_listener.on_tx_commit();

        self.update_sst_manager(created_ssts.into_iter(), deleted_ssts.into_iter());
        Ok(())
    }

    fn update_sst_manager(
        &self,
        created: impl Iterator<Item = (SSTable<K, V>, LsmLevel)>,
        deleted: impl Iterator<Item = (TxLogId, LsmLevel)>,
    ) {
        let mut sst_manager = self.sst_manager.write();
        created.for_each(|(sst, level)| {
            let _ = sst_manager.insert(sst, level);
        });
        deleted.for_each(|(id, level)| {
            let _ = sst_manager.remove(id, level);
        });
    }
}

impl<K: RecordKey<K>, V: RecordValue, D: BlockSet + 'static> Debug for TreeInner<K, V, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxLsmTree")
            .field("memtable_manager", &self.memtable_manager)
            .field("sst_manager", &self.sst_manager.read())
            .field("tx_log_store", &self.tx_log_store)
            .finish()
    }
}

impl<K: RecordKey<K>, V: RecordValue, D: BlockSet + 'static> TxLsmTree<K, V, D> {
    fn drop(&mut self) {
        MASTER_SYNC_ID.store(0, Ordering::Release);
    }
}

impl LsmLevel {
    const LEVEL0_RATIO: u16 = 4;
    const LEVELI_RATIO: u16 = 10;

    const MAX_NUM_LEVELS: usize = 6;
    const LEVEL_BUCKETS: [(LsmLevel, &'static str); Self::MAX_NUM_LEVELS] = [
        (LsmLevel::L0, LsmLevel::L0.bucket()),
        (LsmLevel::L1, LsmLevel::L1.bucket()),
        (LsmLevel::L2, LsmLevel::L2.bucket()),
        (LsmLevel::L3, LsmLevel::L3.bucket()),
        (LsmLevel::L4, LsmLevel::L4.bucket()),
        (LsmLevel::L5, LsmLevel::L5.bucket()),
    ];

    pub fn iter() -> impl Iterator<Item = (LsmLevel, &'static str)> {
        Self::LEVEL_BUCKETS.iter().cloned()
    }

    pub fn upper_level(&self) -> LsmLevel {
        debug_assert!(*self != LsmLevel::L0);
        LsmLevel::from(*self as u8 - 1)
    }

    pub fn lower_level(&self) -> LsmLevel {
        debug_assert!(*self != LsmLevel::L5);
        LsmLevel::from(*self as u8 + 1)
    }

    pub const fn bucket(&self) -> &str {
        match self {
            LsmLevel::L0 => "L0",
            LsmLevel::L1 => "L1",
            LsmLevel::L2 => "L2",
            LsmLevel::L3 => "L3",
            LsmLevel::L4 => "L4",
            LsmLevel::L5 => "L5",
        }
    }
}

impl From<u8> for LsmLevel {
    fn from(value: u8) -> Self {
        match value {
            0 => LsmLevel::L0,
            1 => LsmLevel::L1,
            2 => LsmLevel::L2,
            3 => LsmLevel::L3,
            4 => LsmLevel::L4,
            5 => LsmLevel::L5,
            _ => unreachable!(),
        }
    }
}

// FIXME: Should changes support abort?
impl<K: RecordKey<K>, V: RecordValue> SstManager<K, V> {
    pub fn new() -> Self {
        let level_ssts = (0..LsmLevel::MAX_NUM_LEVELS)
            .map(|_| RBTree::new())
            .collect();
        Self { level_ssts }
    }

    pub fn list_level(
        &self,
        level: LsmLevel,
    ) -> impl Iterator<Item = (&TxLogId, &Arc<SSTable<K, V>>)> {
        self.level_ssts[level as usize].iter().rev()
    }

    pub fn insert(&mut self, sst: SSTable<K, V>, level: LsmLevel) -> Option<Arc<SSTable<K, V>>> {
        let nth_level = level as usize;
        debug_assert!(nth_level < self.level_ssts.len());
        let level_ssts = &mut self.level_ssts[nth_level];
        level_ssts.replace_or_insert(sst.id(), Arc::new(sst))
    }

    pub fn remove(&mut self, id: TxLogId, level: LsmLevel) -> Option<Arc<SSTable<K, V>>> {
        let level_ssts = &mut self.level_ssts[level as usize];
        level_ssts.remove(&id)
    }

    pub fn move_sst(&mut self, id: TxLogId, from: LsmLevel, to: LsmLevel) {
        let moved = self.level_ssts[from as usize].remove(&id).unwrap();
        self.level_ssts[to as usize].insert(id, moved);
    }

    pub fn require_major_compaction(&self, from_level: LsmLevel) -> bool {
        debug_assert!(from_level != LsmLevel::L5);

        if from_level == LsmLevel::L0 {
            return self.list_level(LsmLevel::L0).count() >= LsmLevel::LEVEL0_RATIO as _;
        }
        self.list_level(from_level).count() >= LsmLevel::LEVELI_RATIO.pow(from_level as _) as _
    }
}

impl<K: RecordKey<K>, V: RecordValue> MemTableManager<K, V> {
    pub fn new(
        capacity: usize,
        on_drop_record_in_memtable: Option<Arc<dyn Fn(&dyn AsKV<K, V>)>>,
    ) -> Self {
        let mem_tables = {
            let sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
            let mem_table = Arc::new(RwLock::new(MemTable::new(
                capacity,
                sync_id,
                on_drop_record_in_memtable.clone(),
            )));
            let immut_mem_table = Arc::new(RwLock::new(MemTable::new(
                capacity,
                sync_id,
                on_drop_record_in_memtable,
            )));
            [mem_table, immut_mem_table]
        };

        Self {
            mem_tables,
            immut_idx: AtomicU8::new(1),
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        if let Some(value) = self.active_mem_table().read().get(key) {
            return Some(value.clone());
        }

        if let Some(value) = self.immut_mem_table().read().get(key) {
            return Some(value.clone());
        }

        None
    }

    pub fn get_range(&self, range_query_ctx: &mut RangeQueryCtx<K, V>) -> bool {
        let is_completed = self.active_mem_table().read().get_range(range_query_ctx);
        if is_completed {
            return is_completed;
        }

        self.immut_mem_table().read().get_range(range_query_ctx)
    }

    pub fn put(&self, key: K, value: V) -> bool {
        let mut mem_table = self.active_mem_table().write();
        let _ = mem_table.put(key, value);
        mem_table.at_capacity()
    }

    pub fn sync(&self, sync_id: SyncID) -> Result<()> {
        self.active_mem_table().write().sync(sync_id)
    }

    pub fn switch(&self) {
        let active_idx = self.immut_idx.fetch_xor(1, Ordering::Release);
        // Update sync ID of the active MemTable
        let mut active_mem_table = self.mem_tables[active_idx as usize].write();
        debug_assert!(active_mem_table.is_empty());
        let _ = active_mem_table.sync(MASTER_SYNC_ID.load(Ordering::Relaxed));
    }

    pub fn active_mem_table(&self) -> &Arc<RwLock<MemTable<K, V>>> {
        &self.mem_tables[(self.immut_idx.load(Ordering::Relaxed) as usize) ^ 1]
    }

    pub fn immut_mem_table(&self) -> &Arc<RwLock<MemTable<K, V>>> {
        &self.mem_tables[self.immut_idx.load(Ordering::Relaxed) as usize]
    }
}

impl<K: RecordKey<K>, V: RecordValue> Debug for MemTableManager<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemTableManager")
            .field(
                "active_memtable_size",
                &self.active_mem_table().read().size(),
            )
            .field("immut_memtable_size", &self.immut_mem_table().read().size())
            .finish()
    }
}

impl<K, V> AsKV<K, V> for (K, V) {
    fn key(&self) -> &K {
        &self.0
    }
    fn value(&self) -> &V {
        &self.1
    }
}

impl<K, V> AsKV<K, V> for (&K, &V) {
    fn key(&self) -> &K {
        self.0
    }
    fn value(&self) -> &V {
        self.1
    }
}

impl<K, V> AsKVex<K, V> for (K, ValueEx<V>) {
    fn key(&self) -> &K {
        &self.0
    }
    fn value_ex(&self) -> &ValueEx<V> {
        &self.1
    }
}

impl<K, V> AsKVex<K, V> for (&K, &ValueEx<V>) {
    fn key(&self) -> &K {
        self.0
    }
    fn value_ex(&self) -> &ValueEx<V> {
        self.1
    }
}

// SAFETY: `TxLsmTree` is concurrency-safe.
unsafe impl<K, V, D: BlockSet> Send for TreeInner<K, V, D> {}
unsafe impl<K, V, D: BlockSet> Sync for TreeInner<K, V, D> {}

#[cfg(test)]
mod tests {
    use super::super::RangeQueryCtx;
    use super::*;
    use crate::{layers::bio::MemDisk, os::AeadKey as Key, os::AeadMac as Mac};

    struct Factory;
    struct Listener;

    impl<K, V> TxEventListenerFactory<K, V> for Factory {
        fn new_event_listener(&self, _tx_type: TxType) -> Arc<dyn TxEventListener<K, V>> {
            Arc::new(Listener)
        }
    }
    impl<K, V> TxEventListener<K, V> for Listener {
        fn on_add_record(&self, _record: &dyn AsKV<K, V>) -> Result<()> {
            Ok(())
        }
        fn on_drop_record(&self, _record: &dyn AsKV<K, V>) -> Result<()> {
            Ok(())
        }
        fn on_tx_begin(&self, _tx: &mut Tx) -> Result<()> {
            Ok(())
        }
        fn on_tx_precommit(&self, _tx: &mut Tx) -> Result<()> {
            Ok(())
        }
        fn on_tx_commit(&self) {}
    }

    #[repr(C)]
    #[derive(Copy, Clone, Pod, Debug)]
    struct Value {
        pub hba: BlockId,
        pub key: Key,
        pub mac: Mac,
    }

    impl RecordKey<BlockId> for BlockId {}
    impl RecordValue for Value {}

    #[test]
    fn tx_lsm_tree_fns() -> Result<()> {
        env_logger::init();

        let nblocks = 16 * 1024;
        let mem_disk = MemDisk::create(nblocks)?;
        let tx_log_store = Arc::new(TxLogStore::format(mem_disk, Key::random())?);
        let tx_lsm_tree: TxLsmTree<BlockId, Value, MemDisk> =
            TxLsmTree::format(tx_log_store.clone(), Arc::new(Factory), None)?;

        // Put sufficient records which can trigger compaction before a sync command
        let cap = MEMTABLE_CAPACITY;
        let start = 0;
        for i in start..start + cap {
            let (k, v) = (
                i as BlockId,
                Value {
                    hba: i as BlockId,
                    key: Key::random(),
                    mac: Mac::random(),
                },
            );
            tx_lsm_tree.put(k, v)?;
        }
        let target_value = tx_lsm_tree.get(&5).unwrap();
        assert_eq!(target_value.hba, 5);

        tx_lsm_tree.sync()?;

        let target_value = tx_lsm_tree.get(&1000).unwrap();
        assert_eq!(target_value.hba, 1000);

        // Put sufficient records which can trigger compaction after a sync command
        let start = 500;
        for i in start..start + cap {
            let (k, v) = (
                i as BlockId,
                Value {
                    hba: (i * 2) as BlockId,
                    key: Key::random(),
                    mac: Mac::random(),
                },
            );
            tx_lsm_tree.put(k, v)?;
        }

        let target_value = tx_lsm_tree.get(&500).unwrap();
        assert_eq!(target_value.hba, 1000);

        let start = 700;
        for i in start..start + cap {
            let (k, v) = (
                i as BlockId,
                Value {
                    hba: (i * 3) as BlockId,
                    key: Key::random(),
                    mac: Mac::random(),
                },
            );
            tx_lsm_tree.put(k, v)?;
        }

        let target_value = tx_lsm_tree.get(&700).unwrap();
        assert_eq!(target_value.hba, 2100);
        let target_value = tx_lsm_tree.get(&600).unwrap();
        assert_eq!(target_value.hba, 1200);
        let target_value = tx_lsm_tree.get(&25).unwrap();
        assert_eq!(target_value.hba, 25);

        // Recover the `TxLsmTree`, all unsynced records should be discarded
        drop(tx_lsm_tree);
        let tx_lsm_tree: TxLsmTree<BlockId, Value, MemDisk> =
            TxLsmTree::recover(tx_log_store.clone(), Arc::new(Factory), None)?;

        assert!(tx_lsm_tree.get(&(600 + cap)).is_err());

        // FIXME: Buggy
        // let cnt = 16;
        // let mut range_query_ctx = RangeQueryCtx::new(500, cnt);
        // tx_lsm_tree.get_range(&mut range_query_ctx).unwrap();
        // let res = range_query_ctx.as_results();
        // assert_eq!(res[0].1.hba, 500);
        // assert_eq!(res[cnt - 1].1.hba, 500 + cnt - 1);
        Ok(())
    }
}