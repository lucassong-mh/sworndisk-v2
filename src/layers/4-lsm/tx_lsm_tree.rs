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
use crate::layers::lsm::compaction::Compactor;
use crate::os::RwLock;
use crate::prelude::*;
use crate::tx::Tx;

use alloc::collections::BTreeMap;
use core::fmt::{self, Debug};
use core::hash::Hash;
use core::ops::Range;
use core::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use pod::Pod;

static MASTER_SYNC_ID: AtomicU64 = AtomicU64::new(0);

const ENABLE_DEBUG: bool = false;

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
    L5, // Include over 300 TB data
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
/// 3) After a TX began,
/// 4) Before a TX ended,
/// 5) After a TX committed.
/// `tx_type` indicates an internal transaction of `TxLsmTree`.
pub trait TxEventListener<K, V> {
    /// Notify the listener that a new record is added to a LSM-Tree.
    fn on_add_record(&self, record: &dyn AsKv<K, V>) -> Result<()>;

    /// Notify the listener that an existing record is dropped from a LSM-Tree.
    fn on_drop_record(&self, record: &dyn AsKv<K, V>) -> Result<()>;

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
    /// A Compaction Transaction merges old SSTables into new ones.
    Compaction { to_level: LsmLevel },
    /// A Migration Transaction migrates synced records from old SSTables
    /// (WAL) to new ones during recovery.
    Migration,
}

impl<K: Ord + Pod + Hash + Debug, V: Pod + Debug, D: BlockSet + 'static> TxLsmTree<K, V, D> {
    pub(super) const MEMTABLE_CAPACITY: usize = 81920;
    pub(super) const SSTABLE_CAPACITY: usize = Self::MEMTABLE_CAPACITY;

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
                let wal_res = tx_log_store.open_log_in(BUCKET_WAL, false);
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

        if ENABLE_DEBUG {
            println!("===== TxLsmTree Recovery =====");
            println!("{:?}", recov_self);
            println!("===== TxLsmTree Recovery =====");
        }

        Ok(recov_self)
    }

    pub fn get(&self, key: &K) -> Result<V> {
        // 1. Search from MemTables
        if let Some(value) = self.active_mem_table().read().get(key) {
            return Ok(value.clone());
        }
        if let Some(value) = self.immut_mem_table().read().get(key) {
            return Ok(value.clone());
        }

        // let timer = LatencyMetrics::start_timer(ReqType::Read, "read_tx", "lsmtree");

        // 2. Search from SSTs (do Read TX)
        self.do_read_tx(key)
        // LatencyMetrics::stop_timer(timer);
    }

    pub fn get_range(&self, range: &Range<K>) -> Vec<V> {
        todo!()
    }

    pub fn put(&self, key: K, value: V) -> Result<()> {
        let record = (key, value);
        let timer = LatencyMetrics::start_timer(ReqType::Write, "wal", "lsmtree");

        // 1. Write WAL
        self.wal_append_tx.append(&record)?;

        // 2. Put into MemTable
        let _ = self.active_mem_table().write().put(key, value);
        if !self.active_mem_table().read().at_capacity() {
            LatencyMetrics::stop_timer(timer);
            return Ok(());
        }

        // TODO: Think of combining WAL's TX with minor compaction TX?
        self.wal_append_tx.commit()?;

        LatencyMetrics::stop_timer(timer);

        // 3. Trigger compaction if needed
        self.immut_idx.fetch_xor(1, Ordering::Release);
        // Do major compaction first if needed
        if self
            .sst_manager
            .read()
            .require_major_compaction(LsmLevel::L0)
        {
            let timer = LatencyMetrics::start_timer(ReqType::Write, "major_compaction", "lsmtree");

            self.do_compaction_tx(LsmLevel::L1)?;

            LatencyMetrics::stop_timer(timer);
        }

        let timer = LatencyMetrics::start_timer(ReqType::Write, "minor_compaction", "lsmtree");

        // Do minor compaction
        self.do_compaction_tx(LsmLevel::L0)?;

        LatencyMetrics::stop_timer(timer);

        Ok(())
    }

    // FIXME: Do we actually need this API?
    // pub fn put_range(&self, range: &Range<K>) -> Result<()> {}

    /// Persist all in-memory data of `TxLsmTree`.
    pub fn sync(&self) -> Result<()> {
        // TODO: Store master sync id to trusted storage
        let master_sync_id = MASTER_SYNC_ID.fetch_add(1, Ordering::Release) + 1;

        let timer = LatencyMetrics::start_timer(ReqType::Sync, "wal", "lsmtree");

        self.wal_append_tx.sync(master_sync_id)?;

        LatencyMetrics::stop_timer(timer);

        let timer = LatencyMetrics::start_timer(ReqType::Sync, "memtable", "lsmtree");

        self.active_mem_table().write().sync(master_sync_id)?;

        LatencyMetrics::stop_timer(timer);

        self.tx_log_store.sync()?;

        Ok(())
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
                    if !sst.is_within_range(key) {
                        continue;
                    }
                    if let Some(value) = sst.search_in_cache(key) {
                        return Ok(value);
                    }

                    // let timer = LatencyMetrics::start_timer(ReqType::Read, "open_log", "read_tx");
                    let tx_log = self.tx_log_store.open_log(*id, false)?;
                    // LatencyMetrics::stop_timer(timer);

                    debug_assert!(tx_log.bucket() == bucket);

                    // let timer = LatencyMetrics::start_timer(ReqType::Read, "sst", "read_tx");

                    if let Ok(target_value) = sst.search(key, &tx_log) {
                        // LatencyMetrics::stop_timer(timer);

                        return Ok(target_value);
                    }

                    // LatencyMetrics::stop_timer(timer);
                }
            }

            return_errno_with_msg!(NotFound, "target sst not found");
        });
        if read_res.as_ref().is_err_and(|e| e.errno() != NotFound) {
            tx.abort();
            return_errno_with_msg!(TxAborted, "read TX failed")
        }

        // let timer = LatencyMetrics::start_timer(ReqType::Read, "tx_commit", "read_tx");
        tx.commit()?;
        // LatencyMetrics::stop_timer(timer);

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
            let records_iter = immut_mem_table.iter().map(|(k, v_ex)| {
                match v_ex {
                    ValueEx::Synced(v) | ValueEx::Unsynced(v) => {
                        event_listener.on_add_record(&(k, v)).unwrap();
                    }
                    ValueEx::SyncedAndUnsynced(cv, ucv) => {
                        event_listener.on_add_record(&(k, cv)).unwrap();
                        event_listener.on_add_record(&(k, ucv)).unwrap();
                    }
                }
                (k, v_ex)
            });
            let sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
            let sst = SSTable::build(records_iter, sync_id, &tx_log)?;
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

        if ENABLE_DEBUG {
            println!("===== TxLsmTree Minor Compaction =====",);
            println!("{:?}", self);
            println!("===== TxLsmTree Minor Compaction =====",);
        }
        Ok(())
    }

    /// Compaction TX { to_level: LsmLevel::L1~LsmLevel::L5 }.
    fn do_major_compaction(&self, to_level: LsmLevel) -> Result<()> {
        let from_level = to_level.upper_level();

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

        let master_sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
        let tx_log_store = self.tx_log_store.clone();
        let listener = event_listener.clone();
        let res: Result<_> = tx.context(move || {
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

            let upper_records = {
                let tx_log = tx_log_store.open_log(upper_sst_id, false)?;
                let (records, dropped_records) =
                    upper_sst.collect_all_records(&tx_log, master_sync_id, false)?;
                Compactor::on_drop_records(&listener, dropped_records.into_iter())?;
                records
            };

            let lower_records_vec = {
                let mut records_vec = Vec::with_capacity(lower_ssts.len());
                for (id, sst) in &lower_ssts {
                    let tx_log = tx_log_store.open_log(*id, false)?;
                    let (records, dropped_records) =
                        sst.collect_all_records(&tx_log, master_sync_id, false)?;
                    Compactor::on_drop_records(&listener, dropped_records.into_iter())?;
                    records_vec.push(records);
                }
                records_vec
            };

            // Collect and sort records during compaction
            let compacted_records = {
                if !lower_records_vec.is_empty() {
                    let (compacted_records, dropped_records) = Compactor::compact_records(
                        upper_records.into_iter(),
                        lower_records_vec
                            .into_iter()
                            .flat_map(|records| records.into_iter()),
                    );
                    Compactor::on_drop_records(&listener, dropped_records.into_iter())?;
                    compacted_records
                } else {
                    upper_records
                }
            };
            debug_assert!(compacted_records.is_sorted_by_key(|(k, _)| k));

            let (mut created_ssts, mut deleted_ssts) = (vec![], vec![]);
            // Create new SSTs
            for records in compacted_records.chunks(Self::SSTABLE_CAPACITY) {
                let new_log = tx_log_store.create_log(to_level.bucket())?;
                let new_sst = SSTable::build(
                    records.iter().map(|(k, v)| (k, v)),
                    master_sync_id,
                    &new_log,
                )?;
                created_ssts.push((new_sst, to_level));
            }

            // Delete old SSTs
            tx_log_store.delete_log(upper_sst_id)?;
            deleted_ssts.push((upper_sst_id, from_level));
            for id in lower_ssts.iter().map(|(id, _)| *id) {
                tx_log_store.delete_log(id)?;
                deleted_ssts.push((id, to_level));
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

        if self.sst_manager.read().require_major_compaction(to_level) {
            self.do_major_compaction(to_level.lower_level())?;
        }

        if ENABLE_DEBUG {
            println!("===== TxLsmTree Major Compaction =====",);
            println!("{:?}", self);
            println!("===== TxLsmTree Major Compaction =====",);
        }
        Ok(())
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
                    // Only collect synced records
                    let log = tx_log_store.open_log(id, false)?;
                    let (synced_records, dropped_records) =
                        sst.collect_all_records(&log, master_sync_id, true)?;
                    for (k, v) in dropped_records {
                        listener.on_drop_record(&(k, v))?;
                    }
                    if !synced_records.is_empty() {
                        // Create new migrated SST
                        let new_log = tx_log_store.create_log(bucket)?;
                        let new_sst = SSTable::build(
                            synced_records.iter().map(|(k, v)| (k, v)),
                            master_sync_id,
                            &new_log,
                        )?;
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

impl<K: Ord + Pod + Hash + Debug, V: Pod + Debug, D: BlockSet + 'static> Debug
    for TxLsmTree<K, V, D>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxLsmTree")
            .field(
                "active_memtable_size",
                &self.active_mem_table().read().size(),
            )
            .field("immut_memtable_size", &self.immut_mem_table().read().size())
            .field("sst_manager", &self.sst_manager.read())
            .field("tx_log_store", &self.tx_log_store)
            .finish()
    }
}

impl LsmLevel {
    const LEVEL0_RATIO: u16 = 4;
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

    pub fn upper_level(&self) -> LsmLevel {
        debug_assert!(*self != LsmLevel::L0);
        LsmLevel::from(*self as u8 - 1)
    }

    pub fn lower_level(&self) -> LsmLevel {
        debug_assert!(*self != LsmLevel::L5);
        LsmLevel::from(*self as u8 + 1)
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

impl<K: Ord + Pod + Hash + Debug, V: Pod + Debug> SstManager<K, V> {
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

    pub fn require_major_compaction(&self, from_level: LsmLevel) -> bool {
        debug_assert!(from_level != LsmLevel::L5);

        if from_level == LsmLevel::L0 {
            return self.list_level(LsmLevel::L0).count() >= LsmLevel::LEVEL0_RATIO as _;
        }
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

impl<K, V> AsKv<K, V> for (&K, &V) {
    fn key(&self) -> &K {
        self.0
    }

    fn value(&self) -> &V {
        self.1
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

        let cap = TxLsmTree::<BlockId, RecordValue, MemDisk>::MEMTABLE_CAPACITY;
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

        assert!(tx_lsm_tree.get(&(600 + cap)).is_err());
        let target_value = tx_lsm_tree.get(&500).unwrap();
        assert_eq!(target_value.hba, 500);
        Ok(())
    }
}
