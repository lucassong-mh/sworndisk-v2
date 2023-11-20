//! Transactional LSM-Tree.
//!
//! API: format(), recover(), get(), put(), get_range(), put_range(), sync(), discard()
//!
//! Responsible for managing two `MemTable`s, a `TxLogStore` to
//! manage TX logs (WAL and SSTs). Operations are executed based
//! on internal transactions.
use super::mem_table::{MemTable, ValueEx};
use super::sstable::SSTable;
use super::wal::{WalAppendTx, BUCKET_WAL};
use crate::layers::bio::BlockSet;
use crate::layers::log::{TxLogId, TxLogStore};
use crate::os::RwLock;
use crate::prelude::*;
use crate::tx::Tx;

use alloc::collections::BTreeMap;
use core::fmt::Debug;
use core::ops::Range;
use core::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use pod::Pod;

static MASTER_SYNC_ID: AtomicU64 = AtomicU64::new(0);

/// A LSM-Tree built upon `TxLogStore`.
pub struct TxLsmTree<K, V, D>
where
    D: BlockSet,
{
    mem_tables: [Arc<RwLock<MemTable<K, V>>>; 2],
    immut_idx: AtomicU8,
    tx_log_store: Arc<TxLogStore<D>>,
    wal_append_tx: WalAppendTx<D>,
    sst_manager: RwLock<SstManager<K, V>>,
    listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
}

/// Represent any type that includes a key and a value.
pub trait AsKv<K, V> {
    fn key(&self) -> &K;

    fn value(&self) -> &V;
}

/// Levels in Lsm-tree.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum LsmLevel {
    L0 = 0,
    L1,
    L2,
    L3,
    L4,
    L5, // Indicate over 300 TB data
}

/// SST manager of the `TxLsmTree`,
/// cache SST's index blocks of every level in memory.
// FIXME: Should changes support abort?
#[derive(Debug)]
struct SstManager<K, V> {
    level_ssts: Vec<BTreeMap<TxLogId, Arc<SSTable<K, V>>>>,
}

/// A factory of per-transaction event listeners.
pub trait TxEventListenerFactory<K, V> {
    /// Creates a new event listener for a given transaction.
    fn new_event_listener(&self, tx_type: TxType) -> Arc<dyn TxEventListener<K, V>>;
}

/// An event listener that get informed when
/// 1) A new record is added,
/// 2) An existing record is dropped,
/// 3) After a TX begined,
/// 4) Before a TX ended,
/// 5) After a TX committed.
/// `tx_type` indicates an internal transaction of `TxLsmTree`.
pub trait TxEventListener<K, V> {
    /// Notify the listener that a new record is added to a LSM-Tree.
    fn on_add_record(&self, record: &dyn AsKv<K, V>) -> Result<()>;

    /// Notify the listener that an existing record is dropped from a LSM-Tree.
    fn on_drop_record(&self, record: &dyn AsKv<K, V>) -> Result<()>;

    /// Notify the listener after a TX just begined.
    fn on_tx_begin(&self, tx: &mut Tx) -> Result<()>;

    /// Notify the listener before a tx ended.
    fn on_tx_precommit(&self, tx: &mut Tx) -> Result<()>;

    /// Notify the listener after a TX committed.
    fn on_tx_commit(&self);
}

/// Types of `TxLsmTree`'s internal transactions.
#[derive(Copy, Clone, Debug)]
pub enum TxType {
    /// A Compaction Transaction merges old SSTables into new ones.
    Compaction { to_level: LsmLevel },
    /// A Migration Transaction migrates synced records from old SSTables
    /// (WAL) to new ones during recovery.
    Migration,
}

impl<K: Ord + Pod + Debug, V: Pod + Debug, D: BlockSet + 'static> TxLsmTree<K, V, D> {
    const MEMTABLE_CAPACITY: usize = 1024;
    const SSTABLE_CAPACITY: usize = Self::MEMTABLE_CAPACITY;

    /// Format a `TxLsmTree` from a given `TxLogStore`.
    pub fn format(
        tx_log_store: Arc<TxLogStore<D>>,
        listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
        on_drop_record_in_memtable: Option<Arc<dyn Fn(&dyn AsKv<K, V>)>>,
    ) -> Result<Self> {
        let mem_tables = {
            let sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
            let mem_table = Arc::new(RwLock::new(MemTable::new(
                Self::MEMTABLE_CAPACITY,
                sync_id,
                on_drop_record_in_memtable.clone(),
            )));
            let immut_mem_table = Arc::new(RwLock::new(MemTable::new(
                Self::MEMTABLE_CAPACITY,
                sync_id,
                on_drop_record_in_memtable,
            )));
            [mem_table, immut_mem_table]
        };

        Ok(Self {
            mem_tables,
            immut_idx: AtomicU8::new(1),
            wal_append_tx: WalAppendTx::new(&tx_log_store),
            tx_log_store,
            sst_manager: RwLock::new(SstManager::new()),
            listener_factory,
        })
    }

    /// Recover a `TxLsmTree` from a given `TxLogStore`.
    pub fn recover(
        tx_log_store: Arc<TxLogStore<D>>,
        listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
        on_drop_record_in_memtable: Option<Arc<dyn Fn(&dyn AsKv<K, V>)>>,
    ) -> Result<Self> {
        // Only synced records count, all unsynced are discarded
        let synced_records = {
            let mut tx = tx_log_store.new_tx();
            let res: Result<_> = tx.context(|| {
                let wal_res = tx_log_store.open_log_in(BUCKET_WAL);
                if let Err(e) = &wal_res && e.errno() == NotFound {
                    return Ok(vec![]);
                }
                let wal = wal_res?;
                WalAppendTx::collect_synced_records::<K, V>(&wal)
            });
            if res.is_ok() {
                tx.commit()?;
                // TODO: Update master sync id if mismatch
            } else {
                tx.abort();
                return_errno_with_msg!(TxAborted, "recover from WAL failed");
            }
            res.unwrap()
        };
        let mem_tables = {
            let sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
            let mut mem_table = MemTable::new(
                Self::MEMTABLE_CAPACITY,
                sync_id,
                on_drop_record_in_memtable.clone(),
            );
            for (k, v) in synced_records {
                mem_table.put(k, v);
            }

            let immut_mem_table = Arc::new(RwLock::new(MemTable::new(
                Self::MEMTABLE_CAPACITY,
                sync_id,
                on_drop_record_in_memtable,
            )));
            [Arc::new(RwLock::new(mem_table)), immut_mem_table]
        };

        // Prepare SST manager (load index block to cache)
        let sst_manager = {
            let mut manager = SstManager::new();
            let mut tx = tx_log_store.new_tx();
            let res: Result<_> = tx.context(|| {
                for (level, bucket) in LsmLevel::iter() {
                    let log_ids = tx_log_store.list_logs(bucket);
                    if let Err(e) = &log_ids && e.errno() == NotFound {
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
            res.unwrap();
            RwLock::new(manager)
        };

        let recov_self = Self {
            mem_tables,
            immut_idx: AtomicU8::new(1),
            wal_append_tx: WalAppendTx::new(&tx_log_store),
            tx_log_store,
            sst_manager,
            listener_factory,
        };

        recov_self.do_migration_tx()?;

        println!("===after recovery {:?}\n", recov_self.sst_manager.read());
        recov_self.tx_log_store.debug_state();

        Ok(recov_self)
    }

    pub fn get(&self, key: &K) -> Option<V> {
        // 1. Search from MemTables
        if let Some(value) = self.active_mem_table().read().get(key) {
            return Some(value.clone());
        }
        if let Some(value) = self.immut_mem_table().read().get(key) {
            return Some(value.clone());
        }

        // 2. Search from SSTs (do Read TX)
        self.do_read_tx(key).ok()
    }

    pub fn get_range(&self, range: &Range<K>) -> Vec<V> {
        todo!()
    }

    pub fn put(&self, key: K, value: V) -> Result<()> {
        let record = (key, value);
        // 1. Write WAL
        self.wal_append_tx.append(&record)?;

        // 2. Put into MemTable
        let _ = self.active_mem_table().write().put(key, value);
        if !self.active_mem_table().read().at_capacity() {
            return Ok(());
        }

        self.wal_append_tx.commit()?;

        // 3. Trigger compaction if needed
        self.immut_idx.fetch_xor(1, Ordering::Release);
        // Do major compaction first if needed
        if self.sst_manager.read().require_minor_compaction() {
            self.do_compaction_tx(LsmLevel::L1)?;
        }
        // Do minor compaction
        self.do_compaction_tx(LsmLevel::L0)
    }

    // FIXME: Do we actually need this API?
    // pub fn put_range(&self, range: &Range<K>) -> Result<()> {}

    /// Persist all in-memory data of `TxLsmTree`.
    pub fn sync(&self) -> Result<()> {
        // TODO: Store master sync id to trusted storage
        let master_sync_id = MASTER_SYNC_ID.fetch_add(1, Ordering::Release) + 1;
        self.wal_append_tx.sync(master_sync_id)?;
        self.active_mem_table().write().sync(master_sync_id)
    }

    /// TXs in `TxLsmTree`

    /// Read TX
    fn do_read_tx(&self, key: &K) -> Result<V> {
        let mut tx = self.tx_log_store.new_tx();

        let read_res: Result<_> = tx.context(|| {
            // Search each level from top to bottom (newer to older)
            let sst_manager = self.sst_manager.read();
            for (level, bucket) in LsmLevel::iter() {
                for (id, sst) in sst_manager.list_level(level) {
                    let tx_log = self.tx_log_store.open_log(*id, false)?;
                    debug_assert!(tx_log.bucket() == bucket);
                    if let Some(target_value) = sst.search(key, &tx_log) {
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

    /// Compaction TX
    fn do_compaction_tx(&self, to_level: LsmLevel) -> Result<()> {
        match to_level {
            LsmLevel::L0 => self.do_minor_compaction(),
            LsmLevel::L1 => self.do_major_compaction(to_level),
            _ => unreachable!(),
        }
    }

    /// Compaction TX { to_level: LsmLevel::L0 }.
    fn do_minor_compaction(&self) -> Result<()> {
        println!(
            "===do_minor_compaction before {:?}\n",
            self.sst_manager.read()
        );
        self.tx_log_store.debug_state();

        let mut tx = self.tx_log_store.new_tx();
        let tx_type = TxType::Compaction {
            to_level: LsmLevel::L0,
        };
        let event_listener = self.listener_factory.new_event_listener(tx_type);
        let res = event_listener.on_tx_begin(&mut tx);
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(
                TxAborted,
                "minor compaction TX callback 'on_tx_begin' failed"
            );
        }

        let res: Result<_> = tx.context(|| {
            let tx_log = self.tx_log_store.create_log(LsmLevel::L0.bucket())?;

            let immut_mem_table = self.immut_mem_table().read();
            let records: Vec<_> = immut_mem_table
                .keys_values()
                .map(|(k, v_ex)| (*k, v_ex.clone()))
                .collect();
            for (k, v_ex) in records.iter() {
                match v_ex {
                    ValueEx::Synced(v) | ValueEx::Unsynced(v) => {
                        event_listener.on_add_record(&(*k, *v))?;
                    }
                    ValueEx::SyncedAndUnsynced(cv, ucv) => {
                        event_listener.on_add_record(&(*k, *cv))?;
                        event_listener.on_add_record(&(*k, *ucv))?;
                    }
                }
            }
            let sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
            let sst = SSTable::build(&records, sync_id, &tx_log)?;
            self.sst_manager.write().insert(sst, LsmLevel::L0);
            Ok(())
        });
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(TxAborted, "minor compaction TX failed");
        }

        let res = event_listener.on_tx_precommit(&mut tx);
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(
                TxAborted,
                "minor compaction TX callback 'on_tx_precommit' failed"
            );
        }

        tx.commit()?;
        event_listener.on_tx_commit();
        self.immut_mem_table().write().clear();

        // Discard current WAL
        self.wal_append_tx.discard()?;

        println!(
            "===do_minor_compaction after {:?}\n",
            self.sst_manager.read()
        );
        self.tx_log_store.debug_state();

        Ok(())
    }

    /// Compaction TX { to_level: LsmLevel::L1~LsmLevel::L5 }.
    fn do_major_compaction(&self, to_level: LsmLevel) -> Result<()> {
        println!(
            "===do_major_compaction before {:?}\n",
            self.sst_manager.read()
        );
        self.tx_log_store.debug_state();

        let from_level = to_level.upper_level().unwrap();
        let mut tx = self.tx_log_store.new_tx();
        let tx_type = TxType::Compaction { to_level };
        let event_listener = self.listener_factory.new_event_listener(tx_type);
        let res = event_listener.on_tx_begin(&mut tx);
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(
                TxAborted,
                "major compaction TX callback 'on_tx_begin' failed"
            );
        }

        let tx_log_store = self.tx_log_store.clone();
        let mut compacted_sst_ids = Vec::new();
        let mut compacted_ssts = Vec::new();
        let mut compacted_records = Vec::new();
        let master_sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
        let listener = event_listener.clone();
        let res: Result<_> = tx.context(move || {
            // Collect overlapped SSTs
            let sst_manager = self.sst_manager.read();

            let upper_range = {
                let (id, sst) = sst_manager.list_level(from_level).last().unwrap();
                compacted_sst_ids.push((*id, from_level));
                compacted_ssts.push(sst.clone());
                sst.range()
            };
            let lower_ssts = sst_manager.list_level(to_level);

            let _: Vec<()> = lower_ssts
                .filter(|(_, sst)| sst.overlap_with(&upper_range))
                .map(|(id, sst)| {
                    compacted_sst_ids.push((*id, to_level));
                    compacted_ssts.push(sst.clone());
                })
                .collect();
            drop(sst_manager);

            for sst in compacted_ssts {
                let sync_id = sst.sync_id();
                let mut records =
                    sst.collect_all_records(&tx_log_store.open_log(sst.id(), false)?)?;
                debug_assert!(sync_id <= master_sync_id);
                if sync_id < master_sync_id {
                    for (k, v_ex) in records.iter_mut() {
                        match v_ex {
                            ValueEx::Unsynced(v) => *v_ex = ValueEx::Synced(*v),
                            ValueEx::SyncedAndUnsynced(cv, ucv) => {
                                listener.on_drop_record(&(*k, *cv))?;
                                *v_ex = ValueEx::Synced(*ucv);
                            }
                            _ => {}
                        }
                    }
                }
                compacted_records.push(records);
            }

            // Collect and sort records during compaction
            let sorted_records = Self::merge_sort(compacted_records, &listener)?;

            let (mut created_ssts, mut deleted_ssts) = (vec![], vec![]);
            // Create new SSTs
            for records in sorted_records.chunks(Self::SSTABLE_CAPACITY) {
                let new_log = tx_log_store.create_log(to_level.bucket())?;
                let new_sst = SSTable::build(records, master_sync_id, &new_log)?;
                created_ssts.push((new_sst, to_level));
            }

            // Delete old SSTs
            for (id, level) in compacted_sst_ids {
                tx_log_store.delete_log(id)?;
                deleted_ssts.push((id, level));
            }
            Ok((created_ssts, deleted_ssts))
        });
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(TxAborted, "major compaction TX failed");
        }
        let (created_ssts, deleted_ssts) = res.unwrap();

        let res = event_listener.on_tx_precommit(&mut tx);
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(
                TxAborted,
                "major compaction TX callback 'on_tx_precommit' failed"
            );
        }

        tx.commit()?;
        event_listener.on_tx_commit();
        // Update SST cache
        let mut sst_manager = self.sst_manager.write();
        created_ssts
            .into_iter()
            .for_each(|(sst, level)| sst_manager.insert(sst, level));
        deleted_ssts
            .into_iter()
            .for_each(|(id, level)| sst_manager.remove(id, level));
        drop(sst_manager);

        println!(
            "===do_major_compaction after {:?}\n",
            self.sst_manager.read()
        );
        self.tx_log_store.debug_state();

        if self.sst_manager.read().require_major_compaction(to_level) {
            self.do_major_compaction(to_level.lower_level().unwrap())?;
        }
        Ok(())
    }

    // Merge all records arrays into one
    fn merge_sort(
        mut arrays: Vec<Vec<(K, ValueEx<V>)>>,
        event_listener: &Arc<dyn TxEventListener<K, V>>,
    ) -> Result<Vec<(K, ValueEx<V>)>> {
        debug_assert!(!arrays.is_empty());
        if arrays.len() == 1 {
            return Ok(arrays.pop().unwrap());
        }

        let mid = arrays.len() / 2;

        let left_arrays = arrays.drain(..mid).collect::<Vec<Vec<(K, ValueEx<V>)>>>();
        let right_arrays = arrays;

        let left_sorted = Self::merge_sort(left_arrays, event_listener)?;
        let right_sorted = Self::merge_sort(right_arrays, event_listener)?;

        Self::merge(left_sorted, right_sorted, event_listener)
    }

    // Merge two records arrays
    fn merge(
        mut left: Vec<(K, ValueEx<V>)>,
        mut right: Vec<(K, ValueEx<V>)>,
        event_listener: &Arc<dyn TxEventListener<K, V>>,
    ) -> Result<Vec<(K, ValueEx<V>)>> {
        let mut merged = Vec::new();

        while !left.is_empty() && !right.is_empty() {
            let lkey = left[0].0;
            let rkey = right[0].0;
            if lkey == rkey {
                match (left.remove(0).1, right.remove(0).1) {
                    (ValueEx::Synced(lv), ValueEx::Synced(rv)) => {
                        event_listener.on_drop_record(&(rkey, rv))?;
                        merged.push((lkey, ValueEx::Synced(lv)));
                    }
                    (ValueEx::Synced(_), ValueEx::Unsynced(_)) => unreachable!(),
                    (ValueEx::Synced(_), ValueEx::SyncedAndUnsynced(_, _)) => {
                        unreachable!()
                    }
                    (ValueEx::Unsynced(lv), ValueEx::Synced(rv)) => {
                        merged.push((lkey, ValueEx::SyncedAndUnsynced(rv, lv)))
                    }
                    (ValueEx::Unsynced(lv), ValueEx::Unsynced(rv)) => {
                        event_listener.on_drop_record(&(rkey, rv))?;
                        merged.push((lkey, ValueEx::Unsynced(lv)));
                    }
                    (ValueEx::Unsynced(lv), ValueEx::SyncedAndUnsynced(rcv, rucv)) => {
                        event_listener.on_drop_record(&(rkey, rucv))?;
                        merged.push((lkey, ValueEx::SyncedAndUnsynced(rcv, lv)));
                    }
                    (ValueEx::SyncedAndUnsynced(lcv, lucv), ValueEx::Synced(rcv)) => {
                        event_listener.on_drop_record(&(rkey, rcv))?;
                        merged.push((lkey, ValueEx::SyncedAndUnsynced(lcv, lucv)));
                    }
                    (ValueEx::SyncedAndUnsynced(_, _), ValueEx::Unsynced(_)) => {
                        unreachable!()
                    }
                    (ValueEx::SyncedAndUnsynced(_, _), ValueEx::SyncedAndUnsynced(_, _)) => {
                        unreachable!()
                    }
                }
            } else if lkey < rkey {
                merged.push(left.remove(0));
            } else {
                merged.push(right.remove(0));
            }
        }

        merged.extend(left);
        merged.extend(right);

        Ok(merged)
    }

    /// Migration TX.
    fn do_migration_tx(&self) -> Result<()> {
        // Discard all unsynced records in SSTs
        let mut tx = self.tx_log_store.new_tx();
        let tx_type = TxType::Migration;
        let event_listener = self.listener_factory.new_event_listener(tx_type);
        let res = event_listener.on_tx_begin(&mut tx);
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(TxAborted, "migration TX callback 'on_tx_begin' failed");
        }

        let master_sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
        let tx_log_store = self.tx_log_store.clone();
        let listener = event_listener.clone();
        let res: Result<_> = tx.context(move || {
            let (mut created_ssts, mut deleted_ssts) = (vec![], vec![]);
            let sst_manager = self.sst_manager.read();
            for (level, bucket) in LsmLevel::iter() {
                let ssts = sst_manager.list_level(level);
                for (&id, sst) in ssts.filter(|(_, sst)| sst.sync_id() == master_sync_id) {
                    // Collect records
                    let log = tx_log_store.open_log(id, false)?;
                    let records = sst.collect_all_records(&log)?;
                    let records: Vec<_> = records
                        .into_iter()
                        .filter_map(|(k, v)| match v {
                            ValueEx::Synced(v) => Some((k, ValueEx::Synced(v))),
                            ValueEx::Unsynced(v) => {
                                listener.on_drop_record(&(k, v)).unwrap();
                                None
                            }
                            ValueEx::SyncedAndUnsynced(cv, ucv) => {
                                listener.on_drop_record(&(k, ucv)).unwrap();
                                Some((k, ValueEx::Synced(cv)))
                            }
                        })
                        .collect();
                    if records.is_empty() {
                        continue;
                    }
                    // Create new migrated SST
                    let new_log = tx_log_store.create_log(bucket)?;
                    let new_sst = SSTable::build(&records, master_sync_id, &new_log)?;
                    created_ssts.push((new_sst, level));
                    deleted_ssts.push((id, level));
                    // Delete old SST
                    tx_log_store.delete_log(id)?;
                }
            }
            Ok((created_ssts, deleted_ssts))
        });
        if res.is_err() {
            tx.abort();
            res.unwrap();
            return_errno_with_msg!(TxAborted, "migration TX failed");
        }
        let (created_ssts, deleted_ssts) = res.unwrap();

        let res = event_listener.on_tx_precommit(&mut tx);
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(TxAborted, "migration TX callback 'on_tx_precommit' failed");
        }

        tx.commit()?;
        event_listener.on_tx_commit();
        // Update SST cache
        let mut sst_manager = self.sst_manager.write();
        created_ssts
            .into_iter()
            .for_each(|(sst, level)| sst_manager.insert(sst, level));
        deleted_ssts
            .into_iter()
            .for_each(|(id, level)| sst_manager.remove(id, level));
        Ok(())
    }

    fn active_mem_table(&self) -> &Arc<RwLock<MemTable<K, V>>> {
        &self.mem_tables[(self.immut_idx.load(Ordering::Relaxed) as usize) ^ 1]
    }

    fn immut_mem_table(&self) -> &Arc<RwLock<MemTable<K, V>>> {
        &self.mem_tables[self.immut_idx.load(Ordering::Relaxed) as usize]
    }
}

impl LsmLevel {
    const LEVEL0_RATIO: u16 = 1; // 4
    const LEVELI_RATIO: u16 = 10;

    const LEVEL_BUCKETS: [(LsmLevel, &str); 6] = [
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

    // TODO: Use `TryFrom`
    pub fn upper_level(&self) -> Option<LsmLevel> {
        match self {
            LsmLevel::L0 => None,
            LsmLevel::L1 => Some(LsmLevel::L0),
            LsmLevel::L2 => Some(LsmLevel::L1),
            LsmLevel::L3 => Some(LsmLevel::L2),
            LsmLevel::L4 => Some(LsmLevel::L3),
            LsmLevel::L5 => Some(LsmLevel::L4),
        }
    }

    pub fn lower_level(&self) -> Option<LsmLevel> {
        match self {
            LsmLevel::L0 => Some(LsmLevel::L1),
            LsmLevel::L1 => Some(LsmLevel::L2),
            LsmLevel::L2 => Some(LsmLevel::L3),
            LsmLevel::L3 => Some(LsmLevel::L4),
            LsmLevel::L4 => Some(LsmLevel::L5),
            LsmLevel::L5 => None,
        }
    }

    const fn bucket(&self) -> &str {
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

impl<K: Ord + Pod + Debug, V: Pod> SstManager<K, V> {
    const MAX_NUM_LEVELS: usize = 6;

    pub fn new() -> Self {
        let level_ssts = (0..Self::MAX_NUM_LEVELS).map(|_| BTreeMap::new()).collect();
        Self { level_ssts }
    }

    pub fn list_level(
        &self,
        level: LsmLevel,
    ) -> impl Iterator<Item = (&TxLogId, &Arc<SSTable<K, V>>)> {
        self.level_ssts[level as usize].iter().rev()
    }

    pub fn insert(&mut self, sst: SSTable<K, V>, level: LsmLevel) {
        let nth_level = level as usize;
        debug_assert!(nth_level < self.level_ssts.len());
        let level_ssts = &mut self.level_ssts[nth_level];
        let replaced = level_ssts.insert(sst.id(), Arc::new(sst));
        debug_assert!(replaced.is_none());
    }

    pub fn remove(&mut self, id: TxLogId, level: LsmLevel) {
        let level_ssts = &mut self.level_ssts[level as usize];
        let removed = level_ssts.remove(&id);
        debug_assert!(removed.is_some());
    }

    pub fn require_minor_compaction(&self) -> bool {
        self.list_level(LsmLevel::L0).count() >= LsmLevel::LEVEL0_RATIO as _
    }

    pub fn require_major_compaction(&self, from_level: LsmLevel) -> bool {
        debug_assert!(from_level != LsmLevel::L0 && from_level != LsmLevel::L5);
        self.list_level(from_level).count() >= LsmLevel::LEVELI_RATIO.pow(from_level as _) as _
    }
}

impl<K, V> AsKv<K, V> for (K, V) {
    fn key(&self) -> &K {
        &self.0
    }

    fn value(&self) -> &V {
        &self.1
    }
}

#[cfg(test)]
mod tests {
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
        fn on_add_record(&self, _record: &dyn AsKv<K, V>) -> Result<()> {
            Ok(())
        }
        fn on_drop_record(&self, _record: &dyn AsKv<K, V>) -> Result<()> {
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
    #[derive(Clone, Copy, Pod, Debug)]
    struct RecordValue {
        pub hba: BlockId,
        pub key: Key,
        pub mac: Mac,
    }

    #[test]
    fn tx_lsm_tree_fns() -> Result<()> {
        let nblocks = 16 * 1024;
        let mem_disk = MemDisk::create(nblocks)?;
        let tx_log_store = Arc::new(TxLogStore::format(mem_disk)?);
        let tx_lsm_tree: TxLsmTree<BlockId, RecordValue, MemDisk> =
            TxLsmTree::format(tx_log_store.clone(), Arc::new(Factory), None)?;

        let cap = 1024;
        let start = 0;
        for i in start..start + cap {
            let (k, v) = (
                i as BlockId,
                RecordValue {
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

        let start = 500;
        for i in start..start + cap {
            let (k, v) = (
                i as BlockId,
                RecordValue {
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
                RecordValue {
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

        drop(tx_lsm_tree);
        let tx_lsm_tree: TxLsmTree<BlockId, RecordValue, MemDisk> =
            TxLsmTree::recover(tx_log_store.clone(), Arc::new(Factory), None)?;

        assert!(tx_lsm_tree.get(&1500).is_none());
        let target_value = tx_lsm_tree.get(&500).unwrap();
        assert_eq!(target_value.hba, 500);
        Ok(())
    }
}
