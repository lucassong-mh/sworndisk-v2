//! SwornDisk as a block device.
//!
//! API: read(), write(), sync(), create(), open()
//!
//! Responsible for managing a `TxLsmTree`, whereas the TX logs (WAL and SSTs)
//! are stored; an untrusted disk storing user data, a `BlockAlloc` for managing data blocks'
//! allocation metadata. `TxLsmTree` and `BlockAlloc` are manipulated
//! based on internal transactions.
use super::block_alloc::{AllocBitmap, BlockAlloc};
use crate::layers::bio::{BlockId, BlockSet, Buf, BufMut, BufRef};
use crate::layers::log::TxLogStore;
use crate::layers::lsm::{
    AsKv, LsmLevel, RangeQueryCtx, RecordKey as RecordK, RecordValue as RecordV, TxEventListener,
    TxEventListenerFactory, TxLsmTree, TxType,
};
use crate::os::{Aead as OsAead, AeadIv as Iv, AeadKey as Key, AeadMac as Mac, RwLock};
use crate::prelude::*;
use crate::tx::Tx;
use crate::util::Aead;

use alloc::collections::BTreeMap;
use core::ops::{Add, RangeInclusive, Sub};
use lending_iterator::LendingIterator;
use pod::Pod;

/// Logical Block Address.
pub type Lba = BlockId;
/// Host Block Address.
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
    const DATA_BUF_CAP: usize = 1024;

    /// Creates a new `SwornDisk` on the given disk, with the root encryption key.
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
            // Deallocate the host block while the corresponding record is dropped in `MemTable`
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

    /// Opens the `SwornDisk` on the given disk, with the root encryption key.
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
            // Deallocate the host block while the corresponding record is dropped in `MemTable`
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

    /// Read a specified number of block contents at a logical block address on the device.
    pub fn read(&self, lba: Lba, mut buf: BufMut) -> Result<usize> {
        let nblocks = buf.nblocks();
        AmplificationMetrics::acc_data_amount(AmpType::Read, nblocks);

        if nblocks == 1 {
            self.read_one_block(lba, buf)?;
        } else {
            self.read_multi_blocks(lba, buf)?;
        }

        Ok(nblocks)
    }

    fn read_one_block(&self, lba: Lba, mut buf: BufMut) -> Result<()> {
        debug_assert_eq!(buf.nblocks(), 1);
        // Search in `DataBuf` first
        if self.data_buf.get(RecordKey { lba }, &mut buf).is_some() {
            return Ok(());
        }

        let timer = LatencyMetrics::start_timer(ReqType::Read, "lsmtree", "");

        // Search in `TxLsmTree` then
        let value = self.logical_block_table.get(&RecordKey { lba })?;

        LatencyMetrics::stop_timer(timer);

        let timer = LatencyMetrics::start_timer(ReqType::Read, "data", "");

        // Perform disk read and decryption
        let mut cipher = Buf::alloc(1)?;
        self.user_data_disk.read(value.hba, cipher.as_mut())?;
        OsAead::new().decrypt(
            cipher.as_slice(),
            &value.key,
            &Iv::new_zeroed(),
            &[],
            &value.mac,
            buf.as_mut_slice(),
        )?;

        LatencyMetrics::stop_timer(timer);
        Ok(())
    }

    fn read_multi_blocks(&self, lba: Lba, mut buf: BufMut) -> Result<()> {
        let nblocks = buf.nblocks();
        let mut range_query_ctx =
            RangeQueryCtx::<RecordKey, RecordValue>::new(RecordKey { lba }, nblocks);

        // Search in `DataBuf` first
        for (key, data_block) in self
            .data_buf
            .get_range(range_query_ctx.range_uncompleted().unwrap())
        {
            buf.as_mut_slice()[(key.lba - lba) * BLOCK_SIZE..(key.lba - lba + 1) * BLOCK_SIZE]
                .copy_from_slice(data_block.as_slice());
            range_query_ctx.mark_completed(key);
        }
        if range_query_ctx.is_completed() {
            return Ok(());
        }

        // Search in `TxLsmTree` then
        self.logical_block_table.get_range(&mut range_query_ctx)?;
        assert!(range_query_ctx.is_completed()); // debug_assert, allow empty read

        let mut res = range_query_ctx.as_results();
        let record_batches = {
            res.sort_by(|(_, v1), (_, v2)| v1.hba.cmp(&v2.hba));
            res.group_by(|(_, v1), (_, v2)| v2.hba - v1.hba == 1)
        };

        // Perform disk read in batches and decryption
        let mut buf_slice = buf.as_mut_slice();
        let mut cipher_buf = Buf::alloc(nblocks)?;
        let mut cipher_slice = cipher_buf.as_mut_slice();
        for record_batch in record_batches {
            self.user_data_disk.read(
                record_batch.first().unwrap().1.hba,
                BufMut::try_from(&mut cipher_slice[..record_batch.len() * BLOCK_SIZE]).unwrap(),
            )?;

            for (nth, (key, value)) in record_batch.iter().enumerate() {
                OsAead::new().decrypt(
                    &cipher_slice[nth * BLOCK_SIZE..(nth + 1) * BLOCK_SIZE],
                    &value.key,
                    &Iv::new_zeroed(),
                    &[],
                    &value.mac,
                    &mut buf_slice[(key.lba - lba) * BLOCK_SIZE..(key.lba - lba + 1) * BLOCK_SIZE],
                )?;
            }
        }
        Ok(())
    }

    /// Write a specified number of block contents at a logical block address on the device.
    pub fn write(&self, mut lba: Lba, buf: BufRef) -> Result<usize> {
        let nblocks = buf.nblocks();
        // Write block contents to `DataBuf` directly
        for block_buf in buf.iter() {
            self.data_buf.put(RecordKey { lba }, block_buf);
            lba += 1;
        }
        AmplificationMetrics::acc_data_amount(AmpType::Write, nblocks);

        if self.data_buf.len() < Self::DATA_BUF_CAP {
            return Ok(nblocks);
        }

        // Flush all data blocks in `DataBuf` to disk if it's full
        self.write_blocks_from_data_buf()?;
        Ok(nblocks)
    }

    fn write_blocks_from_data_buf(&self) -> Result<()> {
        let num_write = self.data_buf.len();
        if num_write == 0 {
            return Ok(());
        }
        let timer = LatencyMetrics::start_timer(ReqType::Write, "data", "");

        let hbas = self
            .block_validity_bitmap
            .alloc_batch(num_write)
            .ok_or(Error::with_msg(OutOfDisk, "block allocation failed"))?;
        debug_assert_eq!(hbas.len(), num_write);
        let hba_batches = hbas.group_by(|hba1, hba2| hba2 - hba1 == 1);

        let data_blocks = self.data_buf.all_blocks();
        let mut records = Vec::with_capacity(num_write);

        // Perform encryption and batch disk write
        let mut cipher_buf = Buf::alloc(num_write)?;
        let mut cipher_slice = cipher_buf.as_mut_slice();
        let mut nth = 0;
        for hba_batch in hba_batches {
            for (i, &hba) in hba_batch.iter().enumerate() {
                let (lba, data_block) = &data_blocks[nth];
                let key = Key::random();
                let mac = OsAead::new().encrypt(
                    data_block.as_slice(),
                    &key,
                    &Iv::new_zeroed(),
                    &[],
                    &mut cipher_slice[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE],
                )?;

                records.push((*lba, RecordValue { hba, key, mac }));
                nth += 1;
            }

            self.user_data_disk.write(
                *hba_batch.first().unwrap(),
                BufRef::try_from(&cipher_slice[..hba_batch.len() * BLOCK_SIZE]).unwrap(),
            )?;
            cipher_slice = &mut cipher_slice[hba_batch.len() * BLOCK_SIZE..];
        }
        LatencyMetrics::stop_timer(timer);

        let timer = LatencyMetrics::start_timer(ReqType::Write, "lsmtree", "");
        // Insert new records of data blocks to `TxLsmTree`
        for (key, value) in records {
            self.logical_block_table.put(key, value)?;
        }
        LatencyMetrics::stop_timer(timer);

        self.data_buf.clear();
        Ok(())
    }

    /// Sync all cached data in the device to the storage medium for durability.
    pub fn sync(&self) -> Result<()> {
        self.write_blocks_from_data_buf()?;
        debug_assert!(self.data_buf.is_empty());

        let timer = LatencyMetrics::start_timer(ReqType::Sync, "lsmtree", "");

        self.logical_block_table.sync()?;

        LatencyMetrics::stop_timer(timer);

        let timer = LatencyMetrics::start_timer(ReqType::Sync, "data", "");

        self.user_data_disk.flush()?;

        LatencyMetrics::stop_timer(timer);

        Ok(())
    }

    /// Returns the total number of blocks in the device.
    pub fn total_blocks(&self) -> usize {
        self.user_data_disk.nblocks()
    }

    fn subdisk_for_data(disk: &D) -> Result<D> {
        disk.subset(0..disk.nblocks() / 10 * 5) // TBD
    }

    fn subdisk_for_logical_block_table(disk: &D) -> Result<D> {
        disk.subset(disk.nblocks() / 10 * 5..disk.nblocks()) // TBD
    }
}

// Safety.
unsafe impl<D: BlockSet> Send for SwornDisk<D> {}
unsafe impl<D: BlockSet> Sync for SwornDisk<D> {}

/// Listener factory for `TxLsmTree`.
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

/// Register callbacks for different TXs in `TxLsmTree`.
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

/// A buffer to cache data blocks before they are written to disk.
struct DataBuf {
    buf: RwLock<BTreeMap<RecordKey, Arc<DataBlock>>>,
}

/// User data block.
struct DataBlock([u8; BLOCK_SIZE]);

impl DataBuf {
    pub fn new() -> Self {
        Self {
            buf: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn get(&self, key: RecordKey, buf: &mut BufMut) -> Option<()> {
        if let Some(block) = self.buf.read().get(&key) {
            buf.as_mut_slice().copy_from_slice(block.as_slice());
            Some(())
        } else {
            None
        }
    }

    pub fn get_range(&self, range: RangeInclusive<RecordKey>) -> Vec<(RecordKey, Arc<DataBlock>)> {
        self.buf
            .read()
            .range(range)
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    pub fn put(&self, key: RecordKey, buf: BufRef) {
        let _ = self.buf.write().insert(key, DataBlock::from_buf(buf));
    }

    pub fn len(&self) -> usize {
        self.buf.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

/// Key-Value record for `TxLsmTree`.
struct Record {
    key: RecordKey,
    value: RecordValue,
}

/// The key of a `Record`.
#[repr(C)]
#[derive(Clone, Copy, Pod, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct RecordKey {
    /// Logical block address of user data block.
    pub lba: Lba,
}

/// The value of a `Record`.
#[repr(C)]
#[derive(Clone, Copy, Pod, Debug)]
struct RecordValue {
    /// Host block address of user data block.
    pub hba: Hba,
    /// Encryption key of the data block.
    pub key: Key,
    /// Encrypted MAC of the data block.
    pub mac: Mac,
}

impl Add<usize> for RecordKey {
    type Output = Self;

    fn add(self, other: usize) -> Self::Output {
        Self {
            lba: self.lba + other,
        }
    }
}

impl Sub<RecordKey> for RecordKey {
    type Output = usize;

    fn sub(self, other: RecordKey) -> Self::Output {
        self.lba - other.lba
    }
}

impl RecordK<RecordKey> for RecordKey {}
impl RecordV for RecordValue {}

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
        // Create a new `SwornDisk` then do some writes
        let sworndisk = SwornDisk::create(mem_disk.clone(), root_key)?;

        let num_rw = 1024;
        let mut rw_buf = Buf::alloc(1)?;
        for i in 0..num_rw {
            rw_buf.as_mut_slice().fill(i as u8);
            sworndisk.write(i as Lba, rw_buf.as_ref())?;
        }
        // Sync the `SwornDisk` then do some reads
        sworndisk.sync()?;
        for i in 0..num_rw {
            sworndisk.read(i as Lba, rw_buf.as_mut())?;
            assert_eq!(rw_buf.as_slice()[0], i as u8);
        }

        // Open the closed `SwornDisk` then test its data's existence
        drop(sworndisk);
        thread::spawn(move || -> Result<()> {
            let opened_sworndisk = SwornDisk::open(mem_disk, root_key)?;
            let mut rw_buf = Buf::alloc(2)?;
            opened_sworndisk.read(5 as Lba, rw_buf.as_mut())?;
            assert_eq!(rw_buf.as_slice()[0], 5u8);
            assert_eq!(rw_buf.as_slice()[4096], 6u8);
            Ok(())
        })
        .join()
        .unwrap()
    }
}
