//! Transactional LSM-tree.
//!
//! API: format(), recover(), get(), put(), get_range(), put_range(), commit(), discard()
//!
//! Responsible for managing two `MemTable`s, a `TxLogStore` to
//! manage tx logs (WAL and SSTs). Operations are executed based
//! on internal transactions.
use super::mem_table::{MemTable, ValueEx};
use super::sstable::SSTable;
use crate::layers::bio::{BlockId, BlockSet, Buf};
use crate::layers::log::{TxLog, TxLogId, TxLogStore};
use crate::prelude::*;
use crate::tx::{Tx, TxStatus};

use alloc::collections::BTreeMap;
use core::fmt::Debug;
use core::mem::size_of;
use core::ops::Range;
use core::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use pod::Pod;
use spin::{Mutex, RwLock};

const BUCKET_WAL: &str = "WAL";
const BUCKET_L0: &str = "L0";
const BUCKET_L1: &str = "L1";

const LEVEL0_RATIO: u16 = 1;
const LEVELI_RATIO: u16 = 10;

static MASTER_SYNC_ID: AtomicU64 = AtomicU64::new(0);

/// A LSM-tree built upon `TxLogStore`.
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

/// A factory of per-transaction event listeners.
pub trait TxEventListenerFactory<K, V> {
    /// Creates a new event listener for a given transaction.
    fn new_event_listener(&self, tx_type: TxType) -> Arc<dyn TxEventListener<K, V>>;
}

/// An event listener that get informed when
/// 1. A new record is added,
/// 2. An existing record is dropped,
/// 3. After a tx begined,
/// 4. Before a tx ended,
/// 5. After a tx committed.
/// `tx_type` indicates an internal transaction of `TxLsmTree`.
pub trait TxEventListener<K, V> {
    /// Notify the listener that a new record is added to a LSM-tree.
    fn on_add_record(&self, record: &dyn AsKv<K, V>) -> Result<()>;

    /// Notify the listener that an existing record is dropped from a LSM-tree.
    fn on_drop_record(&self, record: &dyn AsKv<K, V>) -> Result<()>;

    /// Notify the listener after a tx just begined.
    fn on_tx_begin(&self, tx: &mut Tx) -> Result<()>;

    /// Notify the listener before a tx ended.
    fn on_tx_precommit(&self, tx: &mut Tx) -> Result<()>;

    /// Notify the listener after a tx committed.
    fn on_tx_commit(&self);
}

/// Types of `TxLsmTree`'s internal transactions.
#[derive(Copy, Clone, Debug)]
pub enum TxType {
    /// Operations in memtable, not a tx strictly (ACPI)
    // MemTable, // deprecated for now
    /// A Read Transaction reads record from Lsm-tree
    // Read, // deprecated for now
    /// An Append Transaction writes records to WAL.
    // Append, // deprecated for now
    /// A Compaction Transaction merges old SSTables into new ones.
    Compaction { to_level: LsmLevel },
    /// A Migration Transaction migrates committed records from old SSTables
    /// (WAL) to new ones during recovery.
    Migration,
}

/// Levels in Lsm-tree.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum LsmLevel {
    L0 = 0,
    L1,
    // TODO: Support variable levels
}

impl LsmLevel {
    pub fn upper_level(&self) -> Option<LsmLevel> {
        match self {
            LsmLevel::L0 => None,
            LsmLevel::L1 => Some(LsmLevel::L0),
        }
    }
}

impl<K: Ord + Pod + Debug, V: Pod + Debug, D: BlockSet + 'static> TxLsmTree<K, V, D> {
    const MEMTABLE_CAPACITY: usize = 1024;

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
            sst_manager: RwLock::new(SstManager::new(2)),
            listener_factory,
        })
    }

    /// Recover a `TxLsmTree` from a given `TxLogStore`.
    pub fn recover(
        tx_log_store: Arc<TxLogStore<D>>,
        listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
        on_drop_record_in_memtable: Option<Arc<dyn Fn(&dyn AsKv<K, V>)>>,
    ) -> Result<Self> {
        // Only committed records count, all uncommitted are discarded
        let committed_records = {
            let mut tx = tx_log_store.new_tx();
            let res: Result<_> = tx.context(|| {
                let wal = tx_log_store.open_log_in(BUCKET_WAL)?;
                WalAppendTx::collect_committed_records::<K, V>(&wal)
            });
            if res.is_ok() {
                tx.commit()?;
                // TODO: Update master commit id if mismatch
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
            for (k, v) in committed_records {
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
            let mut manager = SstManager::new(2);
            let mut tx = tx_log_store.new_tx();
            let res: Result<_> = tx.context(|| {
                let buckets = [(BUCKET_L0, LsmLevel::L0), (BUCKET_L1, LsmLevel::L1)];
                for (bucket, level) in buckets {
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
        Ok(recov_self)
    }

    pub fn get(&self, key: &K) -> Option<V> {
        // 1. Get from MemTables
        if let Some(value) = self.active_mem_table().read().get(key) {
            return Some(value.clone());
        }
        if let Some(value) = self.immut_mem_table().read().get(key) {
            return Some(value.clone());
        }

        // 2. Get from SSTs (do Read Tx)
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
        let to_be_replaced = self.active_mem_table().write().put(key, value);
        if !self.active_mem_table().read().at_capacity() {
            return Ok(());
        }

        println!("here");
        self.wal_append_tx.commit()?;
        println!("here 2");

        // 3. Trigger compaction if needed
        self.immut_idx.fetch_xor(1, Ordering::Release);
        // Do major compaction first if needed
        if self.sst_manager.read().list_level(LsmLevel::L0).count() == LEVEL0_RATIO as _ {
            self.do_compaction_tx(LsmLevel::L1)?;
        }
        // Do minor compaction
        self.do_compaction_tx(LsmLevel::L0)
    }

    // FIXME: Should we need this API?
    //     pub fn put_range(&self, range: &Range<K>) -> Result<()> {
    //         todo!()
    //     }

    /// User-called flush
    pub fn sync(&self) -> Result<()> {
        // TODO: Store master sync id to trusted storage
        let master_sync_id = MASTER_SYNC_ID.fetch_add(1, Ordering::Release) + 1;
        self.wal_append_tx.sync(master_sync_id)?;
        self.active_mem_table().write().commit(master_sync_id)
    }

    /// TXs in TxLsmTree

    /// Read TX
    fn do_read_tx(&self, key: &K) -> Result<V> {
        let mut tx = self.tx_log_store.new_tx();

        let read_res: Result<_> = tx.context(|| {
            // Search each level from top to bottom (newer to older)
            let buckets = [(BUCKET_L0, LsmLevel::L0), (BUCKET_L1, LsmLevel::L1)];
            let sst_manager = self.sst_manager.read();
            for (bucket, level) in buckets {
                for (id, sst) in sst_manager.list_level(level) {
                    let tx_log = self.tx_log_store.open_log(*id, false)?;
                    debug_assert!(tx_log.bucket() == bucket);
                    if let Some(target_value) = sst.search(key, &tx_log) {
                        return Ok(target_value);
                    }
                }
            }
            return_errno!(NotFound);
        });
        if read_res.as_ref().is_err_and(|e| e.errno() != NotFound) {
            tx.abort();
            return_errno_with_msg!(TxAborted, "read TX failed")
        }

        tx.commit()?;
        read_res
    }

    /// Append TX
    fn do_append_tx(&self, record: &dyn AsKv<K, V>) -> Result<()> {
        self.wal_append_tx.append(record)
    }

    /// Compaction TX
    fn do_compaction_tx(&self, to_level: LsmLevel) -> Result<()> {
        match to_level {
            LsmLevel::L0 => self.do_minor_compaction(),
            LsmLevel::L1 => self.do_major_compaction(to_level),
            _ => unreachable!(),
        }
    }

    /// Compaction TX { to_level: LsmLevel::L0 }
    fn do_minor_compaction(&self) -> Result<()> {
        println!("do_minor_compaction before {:?}", self.sst_manager.read());
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
            let tx_log = self.tx_log_store.create_log(BUCKET_L0)?;

            let immut_mem_table = self.immut_mem_table().read();
            let records: Vec<_> = immut_mem_table
                .keys_values()
                .map(|(k, v_ex)| (*k, v_ex.clone()))
                .collect();
            for (k, v_ex) in records.iter() {
                match v_ex {
                    ValueEx::Committed(v) | ValueEx::Uncommitted(v) => {
                        event_listener.on_add_record(&(*k, *v))?;
                    }
                    ValueEx::CommittedAndUncommitted(cv, ucv) => {
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

        self.tx_log_store.debug_state();

        // Discard current WAL
        self.wal_append_tx.discard()?;

        println!("do_minor_compaction after {:?}", self.sst_manager.read());
        Ok(())
    }

    /// Compaction TX { to_level: LsmLevel::L1 }
    fn do_major_compaction(&self, to_level: LsmLevel) -> Result<()> {
        println!("do_major_compaction before {:?}", self.sst_manager.read());
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
            // TODO: Make number of upper level SSTs for compaction configurable

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
                            ValueEx::Uncommitted(v) => *v_ex = ValueEx::Committed(*v),
                            ValueEx::CommittedAndUncommitted(cv, ucv) => {
                                listener.on_drop_record(&(*k, *cv))?;
                                *v_ex = ValueEx::Committed(*ucv);
                            }
                            _ => {}
                        }
                    }
                }
                compacted_records.push(records);
            }

            // Collect records during compaction
            let sorted_records = Self::merge_sort(compacted_records, &listener)?;
            // println!(
            //     "sorted records: {:?}, {:?}, {:?}",
            //     &sorted_records[0],
            //     &sorted_records[1],
            //     &sorted_records.last().unwrap()
            // );

            let (mut created_ssts, mut deleted_ssts) = (vec![], vec![]);
            // Create new SSTs
            for records in sorted_records.chunks(Self::MEMTABLE_CAPACITY) {
                let new_log = tx_log_store.create_log(BUCKET_L1)?;
                let new_sst = SSTable::build(records, master_sync_id, &new_log)?;
                created_ssts.push((new_sst, to_level));
            }

            // Delete old SSTs
            println!("delete log: {:?}", compacted_sst_ids);
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

        println!("do_major_compaction after {:?}", self.sst_manager.read());
        self.tx_log_store.debug_state();

        Ok(())
    }

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
                    (ValueEx::Committed(lv), ValueEx::Committed(rv)) => {
                        event_listener.on_drop_record(&(rkey, rv))?;
                        merged.push((lkey, ValueEx::Committed(lv)));
                    }
                    (ValueEx::Committed(_), ValueEx::Uncommitted(_)) => unreachable!(),
                    (ValueEx::Committed(_), ValueEx::CommittedAndUncommitted(_, _)) => {
                        unreachable!()
                    }
                    (ValueEx::Uncommitted(lv), ValueEx::Committed(rv)) => {
                        merged.push((lkey, ValueEx::CommittedAndUncommitted(rv, lv)))
                    }
                    (ValueEx::Uncommitted(lv), ValueEx::Uncommitted(rv)) => {
                        event_listener.on_drop_record(&(rkey, rv))?;
                        merged.push((lkey, ValueEx::Uncommitted(lv)));
                    }
                    (ValueEx::Uncommitted(lv), ValueEx::CommittedAndUncommitted(rcv, rucv)) => {
                        event_listener.on_drop_record(&(rkey, rucv))?;
                        merged.push((lkey, ValueEx::CommittedAndUncommitted(rcv, lv)));
                    }
                    (ValueEx::CommittedAndUncommitted(lcv, lucv), ValueEx::Committed(rcv)) => {
                        event_listener.on_drop_record(&(rkey, rcv))?;
                        merged.push((lkey, ValueEx::CommittedAndUncommitted(lcv, lucv)));
                    }
                    (ValueEx::CommittedAndUncommitted(_, _), ValueEx::Uncommitted(_)) => {
                        unreachable!()
                    }
                    (
                        ValueEx::CommittedAndUncommitted(_, _),
                        ValueEx::CommittedAndUncommitted(_, _),
                    ) => unreachable!(),
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

    /// Migration TX
    fn do_migration_tx(&self) -> Result<()> {
        // Discard all uncommitted records in SSTs
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
            let ssts = sst_manager.list_level(LsmLevel::L0);
            for (&id, sst) in ssts.filter(|(_, sst)| sst.sync_id() == master_sync_id) {
                // Collect records
                let log = tx_log_store.open_log(id, false)?;
                let records = sst.collect_all_records(&log)?;
                let records: Vec<_> = records
                    .into_iter()
                    .filter_map(|(k, v)| match v {
                        ValueEx::Committed(v) => Some((k, ValueEx::Committed(v))),
                        ValueEx::Uncommitted(v) => {
                            listener.on_drop_record(&(k, v));
                            None
                        }
                        ValueEx::CommittedAndUncommitted(cv, ucv) => {
                            listener.on_drop_record(&(k, ucv));
                            Some((k, ValueEx::Committed(cv)))
                        }
                    })
                    .collect();
                if records.is_empty() {
                    continue;
                }
                // Create new migrated SST
                let new_log = tx_log_store.create_log(BUCKET_L0)?;
                let new_sst = SSTable::build(&records, master_sync_id, &new_log)?;
                created_ssts.push((new_sst, LsmLevel::L0));
                deleted_ssts.push((id, LsmLevel::L0));
                // Delete old SST
                tx_log_store.delete_log(id)?;
            }

            // TODO: Do migration in every SST, from newer to older.
            // If one SST has no uncommitted record at all, stops scanning.
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

/// An append tx in `TxLsmTree`.
struct WalAppendTx<D>
where
    D: BlockSet,
{
    inner: Mutex<WalAppendTxInner<D>>,
}

struct WalAppendTxInner<D> {
    wal_tx_and_log: Option<(Tx, Arc<TxLog<D>>)>, // Cache append tx and wal log
    log_id: Option<TxLogId>,
    record_buf: Vec<u8>, // Cache appended wal record
    tx_log_store: Arc<TxLogStore<D>>,
}

impl<D: BlockSet + 'static> WalAppendTxInner<D> {
    /// Prepare phase for Append Tx, mainly to create new tx and wal log.
    fn perpare(&mut self) -> Result<()> {
        debug_assert!(self.wal_tx_and_log.is_none());
        let wal_tx_and_log = {
            let store = &self.tx_log_store;
            let mut wal_tx = store.new_tx();
            let log_id_opt = self.log_id.clone();
            let res = wal_tx.context(|| {
                if log_id_opt.is_some() {
                    println!("open log");
                    store.open_log(log_id_opt.unwrap(), true)
                } else {
                    store.create_log(BUCKET_WAL)
                }
            });
            if res.is_err() {
                wal_tx.abort();
            }
            let wal_log = res?;
            self.log_id.insert(wal_log.id());
            (wal_tx, wal_log)
        };
        let _ = self.wal_tx_and_log.insert(wal_tx_and_log);
        Ok(())
    }
}

#[derive(PartialEq, Eq, Debug)]
enum WalAppendFlag {
    Record = 13,
    Sync = 23,
}

impl TryFrom<u8> for WalAppendFlag {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            13 => Ok(WalAppendFlag::Record),
            23 => Ok(WalAppendFlag::Sync),
            _ => Err(Error::new(InvalidArgs)),
        }
    }
}

impl<D: BlockSet + 'static> WalAppendTx<D> {
    const BUF_CAP: usize = 128 * BLOCK_SIZE;

    pub fn new(store: &Arc<TxLogStore<D>>) -> Self {
        Self {
            inner: Mutex::new(WalAppendTxInner {
                wal_tx_and_log: None,
                log_id: None,
                record_buf: Vec::with_capacity(Self::BUF_CAP),
                tx_log_store: store.clone(),
            }),
        }
    }

    /// Append phase for Append Tx, mainly to append newly records to wal log.
    pub fn append<K: Pod, V: Pod>(&self, record: &dyn AsKv<K, V>) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.wal_tx_and_log.is_none() {
            inner.perpare()?;
        }

        let record_buf = &mut inner.record_buf;
        record_buf.push(WalAppendFlag::Record as u8);
        record_buf.extend_from_slice(record.key().as_bytes());
        record_buf.extend_from_slice(record.value().as_bytes());

        if record_buf.len() < Self::BUF_CAP {
            return Ok(());
        }

        let buf = record_buf.clone();
        record_buf.clear();

        let (wal_tx, wal_log) = inner.wal_tx_and_log.as_mut().unwrap();
        self.flush_buf(buf, wal_tx, wal_log)?;
        Ok(())
    }

    /// Commit phase for Append Tx, mainly to commit(or abort) the tx.
    pub fn commit(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.wal_tx_and_log.is_none() {
            return Ok(());
        }

        let record_buf = &mut inner.record_buf;
        println!("record buf len: {}", record_buf.len());
        let buf = record_buf.clone();
        record_buf.clear();
        let (mut wal_tx, wal_log) = inner.wal_tx_and_log.take().unwrap();
        // Append cached records
        // Append master sync id
        self.flush_buf(buf, &mut wal_tx, &wal_log)?;

        drop(wal_log);
        wal_tx.commit()
    }

    pub fn sync(&self, sync_id: u64) -> Result<()> {
        println!("synccccccc");
        let mut inner = self.inner.lock();
        if inner.wal_tx_and_log.is_none() {
            inner.perpare()?;
        }
        let record_buf = &mut inner.record_buf;
        record_buf.push(WalAppendFlag::Sync as u8);
        record_buf.extend_from_slice(&sync_id.to_le_bytes());
        let buf = record_buf.clone();
        record_buf.clear();

        let (mut wal_tx, wal_log) = inner.wal_tx_and_log.take().unwrap();
        // Append cached records
        // Append master sync id
        self.flush_buf(buf, &mut wal_tx, &wal_log)?;

        drop(wal_log);
        wal_tx.commit()
    }

    fn flush_buf(&self, record_buf: Vec<u8>, tx: &mut Tx, log: &Arc<TxLog<D>>) -> Result<()> {
        println!("flush buf");
        let mut buf = Buf::alloc(align_up(record_buf.len(), BLOCK_SIZE) / BLOCK_SIZE)?;
        buf.as_mut_slice()[..record_buf.len()].copy_from_slice(&record_buf);
        let res = tx.context(|| {
            // Append cached records
            // Append master sync id
            log.append(buf.as_ref())
        });
        if res.is_err() {
            tx.abort();
        }
        res
    }

    pub fn discard(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        debug_assert!(inner.record_buf.is_empty());
        let store = inner.tx_log_store.clone();
        let _ = inner.wal_tx_and_log.take();
        let log_id = inner.log_id.take().unwrap();
        let mut wal_tx = store.new_tx();
        let res = wal_tx.context(move || store.delete_log(log_id));
        if res.is_err() {
            wal_tx.abort();
            return res;
        }
        wal_tx.commit()
    }

    pub fn collect_committed_records<K: Pod, V: Pod>(wal: &TxLog<D>) -> Result<Vec<(K, V)>> {
        let nblocks = wal.nblocks();
        let mut records = Vec::new();
        // TODO: Load master sync id from trusted storage

        let mut buf = Buf::alloc(nblocks)?;
        wal.read(0 as BlockId, buf.as_mut())?;
        let buf_slice = buf.as_slice();

        let k_size = size_of::<K>();
        let v_size = size_of::<V>();
        let mut offset = 0;
        let (mut max_sync_id, mut committed_len) = (None, 0);
        loop {
            let flag = WalAppendFlag::try_from(buf_slice[offset]);
            if flag.is_err() || offset >= nblocks * BLOCK_SIZE - 1 {
                break;
            }
            offset += 1;
            match flag.unwrap() {
                WalAppendFlag::Record => {
                    let record = {
                        let k = K::from_bytes(&buf_slice[offset..offset + k_size]);
                        let v =
                            V::from_bytes(&buf_slice[offset + k_size..offset + k_size + v_size]);
                        offset += k_size + v_size;
                        (k, v)
                    };
                    records.push(record);
                }
                WalAppendFlag::Sync => {
                    let sync_id =
                        u64::from_le_bytes(buf_slice[offset..offset + 8].try_into().unwrap());
                    max_sync_id.insert(sync_id);
                    committed_len = records.len();
                    offset += 8;
                }
            }
        }
        if let Some(max_sync_id) = max_sync_id {
            // TODO: Compare read sync id with master sync id
            records.truncate(committed_len);
            Ok(records)
        } else {
            Ok(vec![])
        }
    }
}

impl<D: BlockSet> Drop for WalAppendTx<D> {
    fn drop(&mut self) {
        // self.commit().unwrap()
    }
}

/// Represent any type that includes a key and a value.
pub trait AsKv<K, V> {
    fn key(&self) -> &K;

    fn value(&self) -> &V;
}

impl<K, V> AsKv<K, V> for (K, V) {
    fn key(&self) -> &K {
        &self.0
    }

    fn value(&self) -> &V {
        &self.1
    }
}

/// SST manager of the `TxLsmTree`,
/// cache SST's index blocks of every level in memory.
// TODO: Support variable levels
// Issue: Should changes support abort?
#[derive(Debug)]
struct SstManager<K, V> {
    levels: Vec<BTreeMap<TxLogId, Arc<SSTable<K, V>>>>,
}

impl<K: Ord + Pod + Debug, V: Pod> SstManager<K, V> {
    fn new(num_levels: usize) -> Self {
        let mut levels = Vec::with_capacity(num_levels);
        for _ in 0..num_levels {
            levels.push(BTreeMap::new());
        }
        Self { levels }
    }

    fn list_level(&self, level: LsmLevel) -> impl Iterator<Item = (&TxLogId, &Arc<SSTable<K, V>>)> {
        self.levels[level as usize].iter().rev()
    }

    fn insert(&mut self, sst: SSTable<K, V>, level: LsmLevel) {
        let nth_level = level as usize;
        debug_assert!(nth_level < self.levels.len());
        let level_ssts = &mut self.levels[nth_level];
        let replaced = level_ssts.insert(sst.id(), Arc::new(sst));
        debug_assert!(replaced.is_none());
    }

    fn remove(&mut self, id: TxLogId, level: LsmLevel) {
        let level_ssts = &mut self.levels[level as usize];
        let removed = level_ssts.remove(&id);
        debug_assert!(removed.is_some());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{layers::bio::MemDisk, os::AeadKey as Key, os::AeadMac as Mac};

    struct Factory;
    struct Listener;

    impl<K, V> TxEventListenerFactory<K, V> for Factory {
        fn new_event_listener(&self, tx_type: TxType) -> Arc<dyn TxEventListener<K, V>> {
            Arc::new(Listener)
        }
    }
    impl<K, V> TxEventListener<K, V> for Listener {
        fn on_add_record(&self, record: &dyn AsKv<K, V>) -> Result<()> {
            Ok(())
        }
        fn on_drop_record(&self, record: &dyn AsKv<K, V>) -> Result<()> {
            Ok(())
        }
        fn on_tx_begin(&self, tx: &mut Tx) -> Result<()> {
            Ok(())
        }
        fn on_tx_precommit(&self, tx: &mut Tx) -> Result<()> {
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
        println!("here?");

        let target_value = tx_lsm_tree.get(&1023).unwrap();
        assert_eq!(target_value.hba, 1023);

        let start = 725;
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

        // let target_value = tx_lsm_tree.get(&725).unwrap();
        // assert_eq!(target_value.hba, 1450);

        // let start = 512;
        // for i in start..start + cap {
        //     let (k, v) = (
        //         i as BlockId,
        //         RecordValue {
        //             hba: (i * 2) as BlockId,
        //             key: Key::random(),
        //             mac: Mac::random(),
        //         },
        //     );
        //     tx_lsm_tree.put(k, v)?;
        // }

        // let target_value = tx_lsm_tree.get(&1023).unwrap();
        // assert_eq!(target_value.hba, 2046);
        // let target_value = tx_lsm_tree.get(&5).unwrap();
        // assert_eq!(target_value.hba, 5);

        //

        // let c1 = 225577;
        // let (k, v) = (
        //     c1 as BlockId,
        //     RecordValue {
        //         hba: c1 as BlockId,
        //         key: Key::random(),
        //         mac: Mac::random(),
        //     },
        // );
        // tx_lsm_tree.put(k, v)?;

        // drop(tx_lsm_tree);
        // let tx_lsm_tree: TxLsmTree<BlockId, RecordValue, MemDisk> =
        //     TxLsmTree::recover(tx_log_store.clone(), Arc::new(Factory), None)?;

        // let target_value = tx_lsm_tree.get(&1748).unwrap();
        // assert_eq!(target_value.hba, 1748);

        // let target_value = tx_lsm_tree.get(&c1);
        // assert!(target_value.is_none());

        // tx_lsm_tree.put(k, v)?;

        // tx_lsm_tree.sync()?;

        // let c2 = 775522;
        // let (k, v) = (
        //     c2 as BlockId,
        //     RecordValue {
        //         hba: c2 as BlockId,
        //         key: Key::random(),
        //         mac: Mac::random(),
        //     },
        // );
        // tx_lsm_tree.put(k, v)?;

        // drop(tx_lsm_tree);
        // let tx_lsm_tree: TxLsmTree<BlockId, RecordValue, MemDisk> =
        //     TxLsmTree::recover(tx_log_store, Arc::new(Factory), None)?;

        // let target_value = tx_lsm_tree.get(&c1).unwrap();
        // assert_eq!(target_value.hba, c1);
        // let target_value = tx_lsm_tree.get(&c2);
        // assert!(target_value.is_none());

        Ok(())
    }
}
