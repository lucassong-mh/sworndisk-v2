//! Transactional LSM-tree.
//!
//! API: format(), recover(), get(), put(), get_range(), put_range(), commit(), discard()
//!
//! Responsible for managing two `MemTable`s, a `TxLogStore` to
//! manage tx logs (WAL and SSTs). Operations are executed based
//! on internal transactions.
use crate::layers::bio::{BlockId, BlockSet, Buf};
use crate::layers::log::{TxLog, TxLogId, TxLogStore};
use crate::prelude::*;
use crate::tx::{Tx, TxStatus};

use alloc::collections::BTreeMap;
use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem::size_of;
use core::ops::Range;
use core::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use pod::Pod;
use spin::{Mutex, RwLock};

const BUCKET_WAL: &str = "WAL";
const BUCKET_L0: &str = "L0";
const BUCKET_L1: &str = "L1";

const LEVEL_RATIO: u16 = 10;

static MASTER_SYNC_ID: AtomicU64 = AtomicU64::new(0);

/// A LSM-tree built upon `TxLogStore`.
pub struct TxLsmTree<K, V, D> {
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
    fn on_tx_begin(&self, tx: &Tx) -> Result<()>;

    /// Notify the listener before a tx ended.
    fn on_tx_precommit(&self, tx: &Tx) -> Result<()>;

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
#[derive(Copy, Clone, Debug)]
pub enum LsmLevel {
    L0 = 0,
    L1,
    // TODO: Support variable levels
}

impl<K: Ord + Pod, V: Pod, D: BlockSet + 'static> TxLsmTree<K, V, D> {
    const MEMTABLE_CAPACITY: usize = 1024;

    /// Format a `TxLsmTree` from a given `TxLogStore`.
    pub fn format(
        tx_log_store: Arc<TxLogStore<D>>,
        listener_factory: Arc<dyn TxEventListenerFactory<K, V>>,
        on_drop_record_in_memtable: Option<Arc<dyn Fn(&dyn AsKv<K, V>)>>,
    ) -> Result<Self> {
        let mem_tables = {
            let mem_table = Arc::new(RwLock::new(MemTable::new(
                Self::MEMTABLE_CAPACITY,
                on_drop_record_in_memtable.clone(),
            )));
            let immut_mem_table = Arc::new(RwLock::new(MemTable::new(
                Self::MEMTABLE_CAPACITY,
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
        // Only committed records count, all uncommitted are discarded
        let committed_records = {
            // let tx = tx_log_store.new_tx();
            // let res = tx.context(|| {
            //     let wal = tx_log_store.open_log_in(BUCKET_WAL)?;
            //     let latest_sync_id: u64 = wal.find_latest_sync_id()?;
            //     wal.collect_committed_records()
            // });
            // if res.is_ok() {
            //     tx.commit()?;
            //     // TODO: Update master commit id if mismatch
            // } else {
            //     tx.abort();
            //     return_errno_with_msg!();
            // }
            // res.unwrap()
        };
        let mem_tables = {
            let mut mem_table =
                MemTable::new(Self::MEMTABLE_CAPACITY, on_drop_record_in_memtable.clone());
            // for record in committed_records {
            //     mem_table.put(record.key(), record.value());
            // }

            let immut_mem_table = Arc::new(RwLock::new(MemTable::new(
                Self::MEMTABLE_CAPACITY,
                on_drop_record_in_memtable,
            )));
            [Arc::new(RwLock::new(mem_table)), immut_mem_table]
        };

        // Prepare SST manager (load index block to cache)
        let sst_manager = {
            let mut manager = SstManager::new();
            let mut tx = tx_log_store.new_tx();
            let res: Result<_> = tx.context(|| {
                let buckets = [(BUCKET_L0, LsmLevel::L0), (BUCKET_L1, LsmLevel::L1)];
                for (bucket, level) in buckets {
                    let log_ids = tx_log_store.list_logs(bucket)?;
                    for id in log_ids {
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

        // recov_self.do_migration_tx()?;
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

        // 3. Trigger compaction if needed
        self.immut_idx.fetch_xor(1, Ordering::Release);
        // Do major compaction first if needed
        // if self.sst_manager.read().list_level(LsmLevel::L0).count() == LEVEL_RATIO as _ {
        //     self.do_compaction_tx(LsmLevel::L1)?;
        // }
        // Do minor compaction
        self.do_compaction_tx(LsmLevel::L0)
    }

    //     pub fn put_range(&self, range: &Range<K>) -> Result<()> {
    //         todo!()
    //     }

    /// User-called flush
    pub fn sync(&self) -> Result<()> {
        self.wal_append_tx.commit()
    }

    /// Txs in TxLsmTree

    /// Read Tx
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

    /// Append Tx
    fn do_append_tx(&self, record: &dyn AsKv<K, V>) -> Result<()> {
        self.wal_append_tx.append(record)
    }

    /// Compaction Tx
    fn do_compaction_tx(&self, to_level: LsmLevel) -> Result<()> {
        match to_level {
            LsmLevel::L0 => self.do_minor_compaction(),
            // _ => self.do_major_compaction(to_level),
            _ => unreachable!(),
        }
    }

    /// Compaction Tx { to_level: LsmLevel::L0 }
    fn do_minor_compaction(&self) -> Result<()> {
        let mut tx = self.tx_log_store.new_tx();
        let tx_type = TxType::Compaction {
            to_level: LsmLevel::L0,
        };
        let event_listener = self.listener_factory.new_event_listener(tx_type);
        let res = event_listener.on_tx_begin(&tx);
        if res.is_err() {
            tx.abort();
            return_errno!(TxAborted);
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
            let sst = SSTable::build(records, sync_id, &tx_log)?;
            self.sst_manager.write().insert(sst, LsmLevel::L0);
            Ok(())
        });
        if res.is_err() {
            tx.abort();
            return_errno!(TxAborted);
        }

        let res = event_listener.on_tx_precommit(&tx);
        if res.is_err() {
            tx.abort();
            return_errno!(TxAborted);
        }

        if res.is_ok() {
            tx.commit()?;
            event_listener.on_tx_commit();
            self.immut_mem_table().write().clear();
            // Discard current WAL
            self.wal_append_tx.reset();
        } else {
            tx.abort();
        }
        Ok(())
    }

    //     /// Compaction Tx { to_level: LsmLevel::L1 }
    //     fn do_major_compaction(&self, to_level: LsmLevel) -> Result<()> {
    //         let tx_log_store = self.tx_log_store;
    //         let tx = tx_log_store.new_tx();
    //         let tx_type = TxType::Compaction {
    //             to_level: LsmLevel::L1,
    //         };
    //         let event_listener = self.listener_factory.new_event_listener(tx_type);
    //         let res = event_listener.on_tx_begin(&tx);
    //         if res.is_err() {
    //             tx.abort();
    //             return Err(());
    //         }

    //         let res = tx.context(move || {
    //             // Collect overlapped SSTs
    //             let sst_manager = self.sst_manager.read();
    //             let l0_sst = sst_manager.get(LsmLevel::L0)[0];
    //             let l0_range = l0_sst.range();
    //             let l1_ssts = sst_manager.get(LsmLevel::L1);
    //             let overlapped_l1_ssts = l1_ssts
    //                 .iter()
    //                 .filter_map(|sst| sst.range().overlap_with(l0_range))
    //                 .collect();
    //             drop(sst_manager);

    //             // Collect records during compaction
    //             let l0_log = tx_log_store.open_log(l0_sst.id())?;
    //             let compacted_records: BTreeMap<K, V> = l0_sst.collect_all_records(l0_log) as _;
    //             let mut dropped_records = vec![];
    //             for l1_sst in overlapped_l1_ssts {
    //                 let l1_log = tx_log_store.open_log(l1_sst.id())?;
    //                 let records = l1_sst.collect_all_records(l1_log)?;
    //                 for record in records {
    //                     if compacted_records.contains_key(record.key()) {
    //                         event_listener.on_drop_record(record);
    //                         dropped_records.push(record.clone());
    //                     } else {
    //                         compacted_records.insert(record.key(), record.value());
    //                     }
    //                 }
    //             }

    //             let (mut created_ssts, mut deleted_ssts) = (vec![], vec![]);
    //             // Create new SSTs
    //             for records in compacted_records.chunks(SST_MAX_RECORD_CAP) {
    //                 let new_log = tx_log_store.create_log(BUCKET_L1)?;
    //                 let new_sst = SSTable::build(records, new_log)?;
    //                 created_ssts.push((new_sst, LsmLevel::L1));
    //             }

    //             // Delete old SSTs
    //             tx_log_store.delete_log(l0_sst.id())?;
    //             deleted_ssts.push((l0_sst, LsmLevel::L0));
    //             for l1_sst in overlapped_l1_ssts {
    //                 tx_log_store.delete_log(l1_sst.id())?;
    //                 deleted_ssts.push((l1_sst.id(), LsmLevel::L1));
    //             }
    //             (created_ssts, deleted_ssts)
    //         });

    //         let res = event_listener.on_tx_precommit(&tx);
    //         if res.is_err() {
    //             tx.abort();
    //             return Err(());
    //         }

    //         if res.is_ok() {
    //             tx.commit().ok()?;
    //             event_listener.on_tx_commit();
    //             let (created_ssts, deleted_ssts) = res.unwrap();
    //             // Update SST cache
    //             let mut sst_manager = self.sst_manager.write();
    //             created_ssts.for_each(|(sst, level)| sst_manager.put(sst, level));
    //             deleted_ssts.for_each(|(id, level)| sst_manager.remove(id, level));
    //             Ok(())
    //         } else {
    //             tx.abort();
    //             Err(())
    //         }
    //     }

    //     /// Migration Tx
    //     fn do_migration_tx(&self) -> Result<()> {
    //         // Discard all uncommitted records in SSTs
    //         let tx_log_store = self.tx_log_store;
    //         let tx = tx_log_store.new_tx();
    //         let tx_type = TxType::Migration;
    //         let event_listener = self.listener_factory.new_event_listener(tx_type);
    //         let res = event_listener.on_tx_begin(&tx);
    //         if res.is_err() {
    //             tx.abort();
    //             return Err(());
    //         }

    //         let res = tx.context(move || {
    //             // Collect overlapped SSTs
    //             let sst_manager = self.sst_manager.read();
    //             let l0_sst = sst_manager.get(LsmLevel::L0)[0];
    //             drop(sst_manager);

    //             // Collect records
    //             let l0_log = tx_log_store.open_log(l0_sst.id())?;
    //             let mut records = l0_sst.collect_all_records(l0_log);
    //             records.filter(|r| {
    //                 if r.is_uncommitted() {
    //                     event_listener.on_drop_record(r);
    //                 } else {
    //                     false
    //                 }
    //             });

    //             let (mut created_ssts, mut deleted_ssts) = (vec![], vec![]);
    //             // Create new migrated SST
    //             let new_log = tx_log_store.create_log(BUCKET_L0)?;
    //             let new_sst = SSTable::build(records, new_log)?;
    //             created_ssts.push((new_sst, LsmLevel::L0));

    //             // Delete old SST
    //             tx_log_store.delete_log(l0_sst.id())?;
    //             deleted_ssts.push((l0_sst, LsmLevel::L0));

    //             // TODO: Do migration in every SST, from newer to older.
    //             // If one SST has no uncommitted record at all, stops scanning.
    //             (created_ssts, deleted_ssts)
    //         });

    //         let res = event_listener.on_tx_precommit(&tx);
    //         if res.is_err() {
    //             tx.abort();
    //             return Err(());
    //         }

    //         if res.is_ok() {
    //             tx.commit().ok()?;
    //             event_listener.on_tx_commit();
    //             let (created_ssts, deleted_ssts) = res.unwrap();
    //             // Update SST cache
    //             let mut sst_manager = self.sst_manager.write();
    //             created_ssts.for_each(|(sst, level)| sst_manager.put(sst, level));
    //             deleted_ssts.for_each(|(id, level)| sst_manager.remove(id, level));
    //             Ok(())
    //         } else {
    //             tx.abort();
    //             Err(())
    //         }
    //     }

    fn active_mem_table(&self) -> &Arc<RwLock<MemTable<K, V>>> {
        &self.mem_tables[(self.immut_idx.load(Ordering::Relaxed) as usize) ^ 1]
    }

    fn immut_mem_table(&self) -> &Arc<RwLock<MemTable<K, V>>> {
        &self.mem_tables[self.immut_idx.load(Ordering::Relaxed) as usize]
    }
}

/// An append tx in `TxLsmTree`.
struct WalAppendTx<D> {
    inner: Mutex<WalAppendTxInner<D>>,
}

struct WalAppendTxInner<D> {
    wal_tx_and_log: Option<(Tx, TxLog<D>)>, // Cache append tx and wal log
    record_buf: Vec<u8>,                    // Cache appended wal record
    tx_log_store: Arc<TxLogStore<D>>,
}

impl<D: BlockSet + 'static> WalAppendTxInner<D> {
    /// Prepare phase for Append Tx, mainly to create new tx and wal log.
    fn perpare(&mut self) -> Result<()> {
        debug_assert!(self.wal_tx_and_log.is_none());
        let wal_tx_and_log = {
            let store = &self.tx_log_store;
            let mut wal_tx = store.new_tx();
            let res = wal_tx.context(|| store.create_log(BUCKET_WAL));
            if res.is_err() {
                wal_tx.abort();
            }
            let wal_log = res?;
            (wal_tx, wal_log)
        };
        self.wal_tx_and_log.insert(wal_tx_and_log);
        Ok(())
    }
}

enum WalAppendType {
    Record = 1,
    Commit,
}

impl<D: BlockSet + 'static> WalAppendTx<D> {
    const BUF_CAP: usize = BLOCK_SIZE;

    pub fn new(store: &Arc<TxLogStore<D>>) -> Self {
        Self {
            inner: Mutex::new(WalAppendTxInner {
                wal_tx_and_log: None,
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
        record_buf.push(WalAppendType::Record as u8);
        record_buf.extend_from_slice(record.key().as_bytes());
        record_buf.extend_from_slice(record.value().as_bytes());

        if record_buf.len() < Self::BUF_CAP {
            return Ok(());
        }
        let mut buf = Buf::alloc(align_up(Self::BUF_CAP, BLOCK_SIZE) / BLOCK_SIZE)?;
        buf.as_mut_slice().copy_from_slice(&record_buf);

        let (wal_tx, wal_log) = inner.wal_tx_and_log.as_mut().unwrap();

        let res = wal_tx.context(|| wal_log.append(buf.as_ref()));
        if res.is_err() {
            wal_tx.abort();
        }
        res
    }

    /// Commit phase for Append Tx, mainly to commit(or abort) the tx.
    pub fn commit(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        let record_buf = &mut inner.record_buf;
        record_buf.push(WalAppendType::Commit as u8);

        // TODO: Store master commit id to trusted storage
        let pre_master_sync_id = MASTER_SYNC_ID.fetch_add(1, Ordering::Release);

        record_buf.extend_from_slice(&(pre_master_sync_id + 1).to_le_bytes());
        let mut buf = Buf::alloc(align_up(record_buf.len(), BLOCK_SIZE) / BLOCK_SIZE)?;
        buf.as_mut_slice()[..record_buf.len()].copy_from_slice(&record_buf);

        let (wal_tx, wal_log) = inner.wal_tx_and_log.as_mut().unwrap();
        let res = wal_tx.context(|| {
            // Append cached records
            // Append master commit id
            wal_log.append(buf.as_ref())
        });
        if res.is_err() {
            wal_tx.abort();
            return res;
        }
        wal_tx.commit()?;
        drop(inner);

        self.reset();
        Ok(())
    }

    pub fn reset(&self) {
        let mut inner = self.inner.lock();
        // TODO: delete the wal log
        inner
            .wal_tx_and_log
            .take()
            .map(|(wal_tx, _)| debug_assert!(wal_tx.status() == TxStatus::Committed));
    }
}

/// Represent any type that includes a key and a value.
pub trait AsKv<K, V> {
    fn key(&self) -> &K;

    fn value(&self) -> &V;
}

/// MemTable for LSM-tree.
pub(super) struct MemTable<K, V> {
    // Use `ValueEx<V>` instead `V` to maintain multiple
    // values tagged with commit id for each key
    table: BTreeMap<K, ValueEx<V>>,
    size: usize,
    cap: usize,
    sync_id: u64,
    on_drop_record: Option<Arc<dyn Fn(&dyn AsKv<K, V>)>>,
}

// Value which is commit-aware
// At most one uncommitted&one committed records can coexist at the same time
#[derive(Clone)]
pub(super) enum ValueEx<V> {
    Committed(V),
    Uncommitted(V),
    CommittedAndUncommitted(V, V),
}

impl<V: Copy> ValueEx<V> {
    fn new(value: V) -> Self {
        Self::Uncommitted(value)
    }

    fn get(&self) -> &V {
        match self {
            ValueEx::Committed(v) => v,
            ValueEx::Uncommitted(v) => v,
            ValueEx::CommittedAndUncommitted(_, v) => v,
        }
    }

    fn put(&mut self, value: V) -> Option<V> {
        // TODO: Optimize this by using `mem::take`
        let (updated, replaced) = match self {
            ValueEx::Committed(v) => (Self::CommittedAndUncommitted(*v, value), None),
            ValueEx::Uncommitted(v) => (Self::Uncommitted(value), Some(*v)),
            ValueEx::CommittedAndUncommitted(cv, ucv) => {
                (Self::CommittedAndUncommitted(*cv, value), Some(*cv))
            }
        };
        *self = updated;
        replaced
    }

    fn commit(&mut self) -> Option<V> {
        // TODO: Optimize this by using `mem::take`
        let (updated, replaced) = match self {
            ValueEx::Committed(v) => (None, None),
            ValueEx::Uncommitted(v) => (Some(Self::Committed(*v)), None),
            ValueEx::CommittedAndUncommitted(cv, ucv) => (Some(Self::Committed(*cv)), Some(*ucv)),
        };
        updated.map(|updated| *self = updated);
        replaced
    }
}

impl<K: Copy + Ord, V: Copy> MemTable<K, V> {
    pub fn new(cap: usize, on_drop_record: Option<Arc<dyn Fn(&dyn AsKv<K, V>)>>) -> Self {
        Self {
            table: BTreeMap::new(),
            size: 0,
            cap,
            sync_id: MASTER_SYNC_ID.load(Ordering::Relaxed),
            on_drop_record,
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let value_ex = self.table.get(key)?;
        // Return value which tagged most latest commit id
        Some(value_ex.get())
    }

    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        if let Some(value_ex) = self.table.get_mut(&key) {
            if let Some(replaced) = value_ex.put(value) {
                self.on_drop_record
                    .as_ref()
                    .map(|on_drop_record| on_drop_record(&(key, replaced)));
                return Some(replaced);
            }
        }
        self.table.insert(key, ValueEx::new(value));
        self.size += 1;
        None
    }

    pub fn commit(&mut self) -> Result<()> {
        for (k, v_ex) in &mut self.table {
            if let Some(replaced) = v_ex.commit() {
                self.on_drop_record
                    .as_ref()
                    .map(|on_drop_record| on_drop_record(&(*k, replaced)));
                self.size -= 1;
            }
        }
        self.sync_id = MASTER_SYNC_ID.load(Ordering::Relaxed);
        Ok(())
    }

    // Records should be tagged with commit id
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

impl<K, V> AsKv<K, V> for (K, V) {
    fn key(&self) -> &K {
        &self.0
    }

    fn value(&self) -> &V {
        &self.1
    }
}

static SST_VERSION: AtomicU64 = AtomicU64::new(0);

/// SST manager of the `TxLsmTree`,
/// cache SST's index blocks of every level in memory.
// TODO: Support variable levels
// Issue: Should changes support abort?
struct SstManager<K, V> {
    levels: Vec<BTreeMap<TxLogId, SSTable<K, V>>>,
}

impl<K: Ord + Pod, V: Pod> SstManager<K, V> {
    fn new() -> Self {
        Self {
            levels: Vec::with_capacity(2),
        }
    }

    fn list_level(&self, level: LsmLevel) -> impl Iterator<Item = (&TxLogId, &SSTable<K, V>)> {
        self.levels[level as usize].iter().rev()
    }

    fn insert(&mut self, sst: SSTable<K, V>, level: LsmLevel) {
        let level_ssts = &mut self.levels[level as usize];
        let replaced = level_ssts.insert(sst.id(), sst);
        debug_assert!(replaced.is_none());
    }

    fn remove(&mut self, id: &TxLogId, level: LsmLevel) {
        let level_ssts = &mut self.levels[level as usize];
        let removed = level_ssts.remove(id);
        debug_assert!(removed.is_some());
    }
}

/// Sorted String Table (SST) for LSM-tree
///
/// format:
/// ```text
/// |   [record]    |   [record]    |...|         Footer           |
/// |K|flag|V(V)|...|   [record]    |...| [IndexEntry] | FooterMeta|
/// |  BLOCK_SIZE   |  BLOCK_SIZE   |...|                          |
/// ```
///
// TODO: Add bloom filter and second-level index
// TODO: Constrain encoded KV's length
struct SSTable<K, V> {
    // Cache txlog id, and footer block
    id: TxLogId,
    footer: Footer<K>,
    phantom: PhantomData<(K, V)>,
}

struct Footer<K> {
    meta: FooterMeta,
    index: Vec<IndexEntry<K>>,
}

#[repr(C)]
#[derive(Clone, Copy, Pod)]
struct FooterMeta {
    index_nblocks: u16,
    num_index: u16,
    total_records: u32,
    sync_id: u64,
}
const FOOTER_META_SIZE: usize = size_of::<FooterMeta>();

struct IndexEntry<K> {
    pos: BlockId,
    first: K,
}

#[derive(PartialEq, Eq, Debug)]
enum RecordFlag {
    Invalid = 0,
    Committed = 7,
    Uncommitted = 11,
    CommittedAndUncommitted = 19,
}

impl From<u8> for RecordFlag {
    fn from(value: u8) -> Self {
        match value {
            7 => RecordFlag::Committed,
            11 => RecordFlag::Uncommitted,
            19 => RecordFlag::CommittedAndUncommitted,
            _ => RecordFlag::Invalid,
        }
    }
}

impl<K: Ord + Pod, V: Pod> SSTable<K, V> {
    const BID_SIZE: usize = size_of::<BlockId>();
    const K_SIZE: usize = size_of::<K>();
    const V_SIZE: usize = size_of::<V>();
    const MAX_RECORD_SIZE: usize = Self::BID_SIZE + 1 + 2 * Self::V_SIZE;
    const INDEX_ENTRY_SIZE: usize = Self::BID_SIZE + Self::K_SIZE;

    fn id(&self) -> TxLogId {
        self.id
    }

    fn footer(&self) -> &Footer<K> {
        &self.footer
    }

    fn search<D: BlockSet>(&self, key: &K, tx_log: &Arc<TxLog<D>>) -> Option<V> {
        let target_block_pos = self.search_in_cache(key)?;
        self.search_in_log(key, target_block_pos, tx_log).ok()
    }

    /// Search a target records block position in the SST (from cache).
    fn search_in_cache(&self, key: &K) -> Option<BlockId> {
        for (i, entry) in self.footer.index.iter().enumerate() {
            if &entry.first > key && i > 0 {
                return Some(self.footer.index[i - 1].pos);
            }
        }
        None
    }

    /// Search a target record in the SST (from log).
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn search_in_log<D: BlockSet>(
        &self,
        key: &K,
        target_pos: BlockId,
        tx_log: &Arc<TxLog<D>>,
    ) -> Result<V> {
        debug_assert!(tx_log.id() == self.id());
        let mut rbuf = Buf::alloc(1)?;
        tx_log.read(target_pos, rbuf.as_mut())?;
        // Search in the records block
        let rbuf_slice = rbuf.as_slice();
        let mut offset = 0;
        loop {
            if offset + Self::MAX_RECORD_SIZE > BLOCK_SIZE {
                break;
            }
            let k = K::from_bytes(&rbuf_slice[offset..offset + Self::K_SIZE]);
            offset += Self::K_SIZE;
            let flag = RecordFlag::from(rbuf_slice[offset]);
            offset += 1;
            if flag == RecordFlag::Invalid {
                break;
            }
            if k != *key {
                continue;
            }
            let target_value = match flag {
                RecordFlag::Committed | RecordFlag::Uncommitted => {
                    let v = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                    offset += Self::V_SIZE;
                    v
                }
                RecordFlag::CommittedAndUncommitted => {
                    let v = V::from_bytes(
                        &rbuf_slice[offset + Self::V_SIZE..offset + 2 * Self::V_SIZE],
                    );
                    offset += 2 * Self::V_SIZE;
                    v
                }
                _ => unreachable!(),
            };
            return Ok(target_value);
        }

        return_errno_with_msg!(NotFound, "record not existed in the tx log");
    }

    /// Build a SST given a bunch of records, after build, the SST sealed.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn build<D: BlockSet>(
        records: Vec<(K, ValueEx<V>)>,
        sync_id: u64,
        tx_log: &TxLog<D>,
    ) -> Result<Self> {
        let total_records = records.len();
        let mut index = Vec::new();
        let mut buf = Vec::with_capacity(BLOCK_SIZE);
        let mut append_buf = Buf::alloc(1)?;
        let mut pos = 0 as BlockId;
        let mut first_k = None;
        for (i, record) in records.iter().enumerate() {
            if buf.is_empty() {
                let _ = first_k.insert(record.0);
            }
            buf.extend_from_slice(record.0.as_bytes());
            match record.1 {
                ValueEx::Committed(v) => {
                    buf.push(RecordFlag::Committed as u8);
                    buf.extend_from_slice(v.as_bytes());
                }
                ValueEx::Uncommitted(v) => {
                    buf.push(RecordFlag::Uncommitted as u8);
                    buf.extend_from_slice(v.as_bytes());
                }
                ValueEx::CommittedAndUncommitted(cv, ucv) => {
                    buf.push(RecordFlag::CommittedAndUncommitted as u8);
                    buf.extend_from_slice(cv.as_bytes());
                    buf.extend_from_slice(ucv.as_bytes());
                }
            }
            if BLOCK_SIZE - buf.len() < Self::MAX_RECORD_SIZE || i == total_records - 1 {
                append_buf.as_mut_slice()[..buf.len()].copy_from_slice(&buf);
                tx_log.append(append_buf.as_ref())?;
                index.push(IndexEntry {
                    pos,
                    first: first_k.unwrap(),
                });
                pos += 1;
                buf.clear();
                append_buf.as_mut_slice().fill(0);
            }
        }

        debug_assert!(buf.is_empty());
        for entry in &index {
            buf.extend_from_slice(&entry.pos.to_le_bytes());
            buf.extend_from_slice(entry.first.as_bytes());
        }
        let index_nblocks = {
            let nblocks = align_up(buf.len(), BLOCK_SIZE) / BLOCK_SIZE;
            if nblocks * BLOCK_SIZE - buf.len() <= FOOTER_META_SIZE {
                nblocks
            } else {
                nblocks + 1
            }
        };
        let meta = FooterMeta {
            index_nblocks: index_nblocks as _,
            num_index: index.len() as _,
            total_records: total_records as _,
            sync_id,
        };
        let mut append_buf = Buf::alloc(index_nblocks)?;
        append_buf.as_mut_slice()[..buf.len()].copy_from_slice(&buf);
        append_buf.as_mut_slice()[index_nblocks * BLOCK_SIZE - FOOTER_META_SIZE..]
            .copy_from_slice(meta.as_bytes());
        tx_log.append(append_buf.as_ref())?;

        Ok(Self {
            id: tx_log.id(),
            footer: Footer { meta, index },
            phantom: PhantomData,
        })
    }

    /// Build a SST from a tx log, load index block to cache.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn from_log<D: BlockSet>(tx_log: &Arc<TxLog<D>>) -> Result<Self> {
        let nblocks = tx_log.nblocks();
        let mut rbuf = Buf::alloc(1)?;
        // Load footer block (last block)
        tx_log.read(nblocks - 1, rbuf.as_mut())?;

        let meta = FooterMeta::from_bytes(&rbuf.as_slice()[BLOCK_SIZE - FOOTER_META_SIZE..]);
        let mut rbuf = Buf::alloc(meta.index_nblocks as _)?;
        tx_log.read(nblocks - meta.index_nblocks as usize, rbuf.as_mut())?;
        let mut index = Vec::with_capacity(meta.num_index as _);
        for i in 0..meta.num_index as _ {
            let buf =
                &rbuf.as_slice()[i * Self::INDEX_ENTRY_SIZE..(i + 1) * Self::INDEX_ENTRY_SIZE];
            let pos = BlockId::from_le_bytes(buf[..Self::BID_SIZE].try_into().unwrap());
            let first = K::from_bytes(&buf[Self::BID_SIZE..]);
            index.push(IndexEntry { pos, first })
        }

        let footer = Footer { meta, index };
        Ok(Self {
            id: tx_log.id(),
            footer,
            phantom: PhantomData,
        })
    }

    /// Collect all records from a SST.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn collect_all_records<D: BlockSet>(
        &self,
        tx_log: &Arc<TxLog<D>>,
    ) -> Result<Vec<(K, ValueEx<V>)>> {
        let mut records = Vec::with_capacity(self.footer.meta.total_records as _);
        let mut rbuf = Buf::alloc(1)?;
        for entry in self.footer.index.iter() {
            tx_log.read(entry.pos, rbuf.as_mut())?;
            let rbuf_slice = rbuf.as_slice();

            let mut offset = 0;
            loop {
                if offset + Self::MAX_RECORD_SIZE > BLOCK_SIZE {
                    break;
                }

                let k = K::from_bytes(&rbuf_slice[offset..offset + Self::K_SIZE]);
                offset += Self::K_SIZE;
                let v_ex = {
                    let flag = RecordFlag::from(rbuf_slice[offset]);
                    offset += 1;
                    if flag == RecordFlag::Invalid {
                        break;
                    }
                    match flag {
                        RecordFlag::Committed => {
                            let v = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                            offset += Self::V_SIZE;
                            ValueEx::Committed(v)
                        }
                        RecordFlag::Uncommitted => {
                            let v = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                            offset += Self::V_SIZE;
                            ValueEx::Uncommitted(v)
                        }
                        RecordFlag::CommittedAndUncommitted => {
                            let cv = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                            offset += Self::V_SIZE;
                            let ucv = V::from_bytes(&rbuf_slice[offset..offset + Self::V_SIZE]);
                            offset += Self::V_SIZE;
                            ValueEx::CommittedAndUncommitted(cv, ucv)
                        }
                        _ => unreachable!(),
                    }
                };
                records.push((k, v_ex));
            }
        }

        Ok(records)
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
        fn on_tx_begin(&self, tx: &Tx) -> Result<()> {
            Ok(())
        }
        fn on_tx_precommit(&self, tx: &Tx) -> Result<()> {
            Ok(())
        }
        fn on_tx_commit(&self) {}
    }

    #[repr(C)]
    #[derive(Clone, Copy, Pod)]
    struct RecordValue {
        pub hba: BlockId,
        pub key: Key,
        pub mac: Mac,
    }

    #[test]
    fn tx_lsm_tree_fns() -> Result<()> {
        let nblocks = 4096;
        let mem_disk = MemDisk::create(nblocks)?;
        let disk = mem_disk.clone();
        let tx_log_store = TxLogStore::format(mem_disk)?;

        let tx_lsm_tree: TxLsmTree<BlockId, RecordValue, MemDisk> =
            TxLsmTree::format(Arc::new(tx_log_store), Arc::new(Factory), None)?;
        let (key, value) = (
            1 as BlockId,
            RecordValue {
                hba: 2 as BlockId,
                key: Key::random(),
                mac: Mac::random(),
            },
        );
        tx_lsm_tree.put(key, value)?;
        tx_lsm_tree.sync().unwrap();

        let target_value = tx_lsm_tree.get(&key).unwrap();
        assert_eq!(target_value.hba, value.hba);
        assert_eq!(target_value.key.as_bytes(), value.key.as_bytes());
        assert_eq!(target_value.mac.as_bytes(), value.mac.as_bytes());
        Ok(())
    }
}
