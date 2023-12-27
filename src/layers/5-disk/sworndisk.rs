//! SwornDisk as a block device.
//!
//! API: read(), write(), sync(), create(), open()
//!
//! Responsible for managing a `TxLsmTree`, an untrusted disk
//! storing user data, a `BlockAlloc` for managing data block
//! validity manage tx logs (WAL and SSTs). `TxLsmTree` and
//! `BlockAlloc` are manipulated based on internal transactions.
use super::block_alloc::{AllocBitmap, BlockAlloc};
use crate::layers::bio::{BlockId, BlockSet, Buf, BufMut, BufRef};
use crate::layers::log::TxLogStore;
use crate::layers::lsm::{
    AsKv, LsmLevel, TxEventListener, TxEventListenerFactory, TxLsmTree, TxType,
};
use crate::os::{Aead as OsAead, AeadIv as Iv, AeadKey as Key, AeadMac as Mac, RwLock};
use crate::prelude::*;
use crate::tx::Tx;
use crate::util::Aead;

use alloc::collections::BTreeMap;
use lending_iterator::LendingIterator;
use pod::Pod;

pub type Lba = BlockId;
pub type Hba = BlockId;

/// SwornDisk.
pub struct SwornDisk<D: BlockSet> {
    logical_block_table: TxLsmTree<RecordKey, RecordValue, D>,
    user_data_disk: D,
    block_validity_bitmap: Arc<AllocBitmap>,
    data_buf: DataBuf,
    root_key: Key,
}

// TODO: Support queue-based API
impl<D: BlockSet + 'static> SwornDisk<D> {
    const BUF_CAP: usize = 1024;

    /// Read a specified number of block contents at a logical block address on the device.
    // TODO: Support range query
    pub fn read(&self, mut lba: Lba, mut buf: BufMut) -> Result<usize> {
        let mut iter_mut = buf.iter_mut();
        while let Some(block_buf) = iter_mut.next() {
            self.read_one_block(lba, block_buf)?;
            lba += 1;
        }

        AmplificationMetrics::acc_data_amount(AmpType::Read, buf.nblocks());
        Ok(buf.nblocks())
    }

    fn read_one_block(&self, lba: Lba, mut buf: BufMut) -> Result<()> {
        debug_assert_eq!(buf.nblocks(), 1);
        if self.data_buf.get(RecordKey { lba }, &mut buf).is_some() {
            return Ok(());
        }

        let timer = LatencyMetrics::start_timer(ReqType::Read, "lsmtree", "");

        let record = self.logical_block_table.get(&RecordKey { lba })?;

        LatencyMetrics::stop_timer(timer);

        let timer = LatencyMetrics::start_timer(ReqType::Read, "data", "");

        let mut cipher = Buf::alloc(1)?;
        self.user_data_disk.read(record.hba, cipher.as_mut())?;
        OsAead::new().decrypt(
            cipher.as_slice(),
            &record.key,
            &Iv::new_zeroed(),
            &[],
            &record.mac,
            buf.as_mut_slice(),
        )?;

        LatencyMetrics::stop_timer(timer);
        Ok(())
    }

    /// Write a specified number of block contents at a logical block address on the device.
    pub fn write(&self, mut lba: Lba, buf: BufRef) -> Result<usize> {
        for block_buf in buf.iter() {
            self.data_buf.put(RecordKey { lba }, block_buf);
            lba += 1;
        }

        if self.data_buf.len() < Self::BUF_CAP {
            return Ok(buf.nblocks());
        }

        let mut cipher = Buf::alloc(1)?;
        for (lba, data_block) in self.data_buf.all_blocks() {
            let timer = LatencyMetrics::start_timer(ReqType::Write, "data", "");
            let hba = self
                .block_validity_bitmap
                .alloc()
                .ok_or(Error::with_msg(OutOfMemory, "block allocation failed"))?;

            let key = Key::random();
            let mac = OsAead::new().encrypt(
                &data_block.0,
                &key,
                &Iv::new_zeroed(),
                &[],
                cipher.as_mut_slice(),
            )?;
            self.user_data_disk.write(hba, cipher.as_ref())?;

            LatencyMetrics::stop_timer(timer);

            AmplificationMetrics::acc_data_amount(AmpType::Write, buf.nblocks());

            let timer = LatencyMetrics::start_timer(ReqType::Write, "lsmtree", "");

            self.logical_block_table
                .put(lba, RecordValue { hba, key, mac })?;

            LatencyMetrics::stop_timer(timer);
        }

        self.data_buf.clear();
        Ok(buf.nblocks())
    }

    /// Sync all cached data in the device to the storage medium for durability.
    pub fn sync(&self) -> Result<()> {
        let timer = LatencyMetrics::start_timer(ReqType::Sync, "lsmtree", "");

        self.logical_block_table.sync()?;

        LatencyMetrics::stop_timer(timer);

        let timer = LatencyMetrics::start_timer(ReqType::Sync, "data", "");

        self.user_data_disk.flush()?;

        LatencyMetrics::stop_timer(timer);

        Ok(())
    }

    /// Return the total number of blocks in the device.
    pub fn total_blocks(&self) -> usize {
        self.user_data_disk.nblocks()
    }

    /// Create the device on a disk, given the root cryption key.
    pub fn create(disk: D, root_key: Key) -> Result<Self> {
        let data_disk = Self::subdisk_for_data(&disk)?;
        let lsm_tree_disk = Self::subdisk_for_logical_block_table(&disk)?;

        let tx_log_store = Arc::new(TxLogStore::format(lsm_tree_disk, root_key.clone())?);
        let block_validity_bitmap = Arc::new(AllocBitmap::new(data_disk.nblocks()));
        let listener_factory = Arc::new(TxLsmTreeListenerFactory::new(
            tx_log_store.clone(),
            block_validity_bitmap.clone(),
        ));

        let bitmap = block_validity_bitmap.clone();
        let on_drop_record_in_memtable = move |record: &dyn AsKv<RecordKey, RecordValue>| {
            // Dealloc block
            bitmap.set_deallocated(record.value().hba);
        };
        let logical_block_table = TxLsmTree::format(
            tx_log_store,
            listener_factory,
            Some(Arc::new(on_drop_record_in_memtable)),
        )?;

        Ok(Self {
            logical_block_table,
            user_data_disk: data_disk,
            block_validity_bitmap,
            data_buf: DataBuf::new(),
            root_key,
        })
    }

    /// Open the device on a disk, given the root cryption key.
    pub fn open(disk: D, root_key: Key) -> Result<Self> {
        let data_disk = Self::subdisk_for_data(&disk)?;
        let index_disk = Self::subdisk_for_logical_block_table(&disk)?;

        let tx_log_store = Arc::new(TxLogStore::recover(index_disk, root_key)?);
        let block_validity_bitmap = Arc::new(AllocBitmap::new(data_disk.nblocks()));
        let listener_factory = Arc::new(TxLsmTreeListenerFactory::new(
            tx_log_store.clone(),
            block_validity_bitmap.clone(),
        ));
        block_validity_bitmap.recover(&tx_log_store)?;

        let bitmap = block_validity_bitmap.clone();
        let on_drop_record_in_memtable = move |record: &dyn AsKv<RecordKey, RecordValue>| {
            // Dealloc block
            bitmap.set_deallocated(record.value().hba);
        };
        let logical_block_table = TxLsmTree::recover(
            tx_log_store,
            listener_factory,
            Some(Arc::new(on_drop_record_in_memtable)),
        )?;

        Ok(Self {
            logical_block_table,
            user_data_disk: data_disk,
            block_validity_bitmap,
            data_buf: DataBuf::new(),
            root_key,
        })
    }

    fn subdisk_for_data(disk: &D) -> Result<D> {
        disk.subset(0..disk.nblocks() / 10 * 5) // TBD
    }

    fn subdisk_for_logical_block_table(disk: &D) -> Result<D> {
        disk.subset(disk.nblocks() / 10 * 5..disk.nblocks()) // TBD
    }
}

unsafe impl<D: BlockSet> Send for SwornDisk<D> {}
unsafe impl<D: BlockSet> Sync for SwornDisk<D> {}

struct TxLsmTreeListenerFactory<D> {
    store: Arc<TxLogStore<D>>,
    alloc_bitmap: Arc<AllocBitmap>,
}

impl<D> TxLsmTreeListenerFactory<D> {
    fn new(store: Arc<TxLogStore<D>>, alloc_bitmap: Arc<AllocBitmap>) -> Self {
        Self {
            store,
            alloc_bitmap,
        }
    }
}

impl<D: BlockSet + 'static> TxEventListenerFactory<RecordKey, RecordValue>
    for TxLsmTreeListenerFactory<D>
{
    fn new_event_listener(
        &self,
        tx_type: TxType,
    ) -> Arc<dyn TxEventListener<RecordKey, RecordValue>> {
        Arc::new(TxLsmTreeListener::new(
            tx_type,
            Arc::new(BlockAlloc::new(
                self.alloc_bitmap.clone(),
                self.store.clone(),
            )),
        ))
    }
}

/// Event listener for `TxLsmTree`.
struct TxLsmTreeListener<D> {
    tx_type: TxType,
    block_alloc: Arc<BlockAlloc<D>>,
}

impl<D> TxLsmTreeListener<D> {
    fn new(tx_type: TxType, block_alloc: Arc<BlockAlloc<D>>) -> Self {
        Self {
            tx_type,
            block_alloc,
        }
    }
}

/// Register callbacks for different txs in `TxLsmTree`.
impl<D: BlockSet + 'static> TxEventListener<RecordKey, RecordValue> for TxLsmTreeListener<D> {
    fn on_add_record(&self, record: &dyn AsKv<RecordKey, RecordValue>) -> Result<()> {
        match self.tx_type {
            TxType::Compaction { to_level } if to_level == LsmLevel::L0 => {
                self.block_alloc.alloc_block(record.value().hba)
            }
            // Major Compaction TX and Migration TX do not add new records
            TxType::Compaction { .. } | TxType::Migration => {
                unreachable!();
            }
        }
    }

    fn on_drop_record(&self, record: &dyn AsKv<RecordKey, RecordValue>) -> Result<()> {
        match self.tx_type {
            // Minor Compaction TX doesn't compact records
            TxType::Compaction { to_level } if to_level == LsmLevel::L0 => {
                unreachable!();
            }
            TxType::Compaction { .. } | TxType::Migration => {
                self.block_alloc.dealloc_block(record.value().hba)
            }
        }
    }

    fn on_tx_begin(&self, tx: &mut Tx) -> Result<()> {
        match self.tx_type {
            TxType::Compaction { .. } | TxType::Migration => {
                tx.context(|| self.block_alloc.prepare_diff_log().unwrap())
            }
        }
        Ok(())
    }

    fn on_tx_precommit(&self, tx: &mut Tx) -> Result<()> {
        match self.tx_type {
            TxType::Compaction { .. } | TxType::Migration => {
                tx.context(|| self.block_alloc.update_diff_log().unwrap())
            }
        }
        Ok(())
    }

    fn on_tx_commit(&self) {
        match self.tx_type {
            TxType::Compaction { .. } | TxType::Migration => self.block_alloc.update_bitmap(),
        }
    }
}

struct DataBuf {
    buf: RwLock<BTreeMap<RecordKey, Arc<DataBlock>>>,
}

struct DataBlock([u8; BLOCK_SIZE]);

impl DataBuf {
    pub fn new() -> Self {
        Self {
            buf: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn get(&self, key: RecordKey, buf: &mut BufMut) -> Option<()> {
        if let Some(block) = self.buf.read().get(&key) {
            buf.as_mut_slice().copy_from_slice(&block.0);
            Some(())
        } else {
            None
        }
    }

    pub fn put(&self, key: RecordKey, buf: BufRef) {
        let _ = self.buf.write().insert(key, DataBlock::from_buf(buf));
    }

    pub fn len(&self) -> usize {
        self.buf.read().len()
    }

    pub fn clear(&self) {
        self.buf.write().clear();
    }

    pub fn all_blocks(&self) -> Vec<(RecordKey, Arc<DataBlock>)> {
        self.buf
            .read()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }
}

impl DataBlock {
    pub fn from_buf(buf: BufRef) -> Arc<Self> {
        debug_assert_eq!(buf.nblocks(), 1);
        Arc::new(DataBlock(buf.as_slice().try_into().unwrap()))
    }
}

/// K-V record for `TxLsmTree`.
struct Record {
    key: RecordKey,
    value: RecordValue,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct RecordKey {
    pub lba: Lba,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Debug)]
struct RecordValue {
    pub hba: Hba,
    pub key: Key,
    pub mac: Mac,
}

impl AsKv<RecordKey, RecordValue> for Record {
    fn key(&self) -> &RecordKey {
        &self.key
    }

    fn value(&self) -> &RecordValue {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::bio::MemDisk;

    use std::thread::{self, JoinHandle};

    #[test]
    fn sworndisk_fns() -> Result<()> {
        let nblocks = 8 * 1024;
        let mem_disk = MemDisk::create(nblocks)?;
        let root_key = Key::random();
        let sworndisk = SwornDisk::create(mem_disk.clone(), root_key)?;

        let num_rw = 1024;
        let mut rw_buf = Buf::alloc(1)?;
        for i in 0..num_rw {
            rw_buf.as_mut_slice().fill(i as u8);
            sworndisk.write(i as Lba, rw_buf.as_ref())?;
        }
        sworndisk.sync()?;
        for i in 0..num_rw {
            sworndisk.read(i as Lba, rw_buf.as_mut())?;
            assert_eq!(rw_buf.as_slice()[0], i as u8);
        }

        drop(sworndisk);
        thread::spawn(move || -> Result<()> {
            let opened_sworndisk = SwornDisk::open(mem_disk, root_key)?;
            opened_sworndisk.read(5 as Lba, rw_buf.as_mut())?;
            assert_eq!(rw_buf.as_slice()[0], 5u8);
            Ok(())
        })
        .join()
        .unwrap()
    }
}
