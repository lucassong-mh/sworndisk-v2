//! SwornDisk as a block device.
//!
//! API: read(), write(), flush(), flush_blocks(), discard(), total_blocks(), open(), create()
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
use crate::os::{Aead as OsAead, AeadIv as Iv, AeadKey as Key, AeadMac as Mac};
use crate::prelude::*;
use crate::tx::Tx;
use crate::util::Aead;

use hashbrown::HashMap;
use lending_iterator::LendingIterator;
use pod::Pod;

pub type Lba = BlockId;
pub type Hba = BlockId;

/// SwornDisk.
pub struct SwornDisk<D: BlockSet> {
    tx_lsm_tree: TxLsmTree<RecordKey, RecordValue, D>,
    user_data_disk: D,
    block_validity_bitmap: Arc<AllocBitmap>,
    // data_buf: DataBuf, // TODO: Support data buffering.
    root_key: Key,
}

impl<D: BlockSet + 'static> SwornDisk<D> {
    const BUF_CAP: usize = 1024;

    /// Read a specified number of block contents at a logical block address on the device.
    pub fn read(&self, mut lba: Lba, mut buf: BufMut) -> Result<usize> {
        let mut iter_mut = buf.iter_mut();
        while let Some(block_buf) = iter_mut.next() {
            self.read_one_block(lba, block_buf)?;
            lba += 1;
        }
        Ok(buf.nblocks())
    }

    fn read_one_block(&self, lba: Lba, mut buf: BufMut) -> Result<()> {
        debug_assert_eq!(buf.nblocks(), 1);
        let timer = LatencyMetrics::start_timer(ReqType::Read, "lsmtree", "");

        let record = self.tx_lsm_tree.get(&RecordKey { lba })?;

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
    pub fn write(&self, lba: Lba, buf: BufRef) -> Result<usize> {
        let timer = LatencyMetrics::start_timer(ReqType::Write, "data", "");

        // TODO: batch write
        let hba = self
            .block_validity_bitmap
            .alloc()
            .ok_or(Error::with_msg(OutOfMemory, "block allocation failed"))?;

        let key = Key::random();
        let mut cipher = Buf::alloc(1)?;
        let mac = OsAead::new().encrypt(
            buf.as_slice(),
            &key,
            &Iv::new_zeroed(),
            &[],
            cipher.as_mut_slice(),
        )?;
        self.user_data_disk.write(hba, cipher.as_ref())?;

        LatencyMetrics::stop_timer(timer);

        AmplificationMetrics::acc_data_amount(AmpType::Write, buf.nblocks());

        let timer = LatencyMetrics::start_timer(ReqType::Write, "lsmtree", "");

        self.tx_lsm_tree
            .put(RecordKey { lba }, RecordValue { hba, key, mac })?;

        LatencyMetrics::stop_timer(timer);

        Ok(buf.nblocks())
    }

    /// Sync all cached data in the device to the storage medium for durability.
    pub fn sync(&self) -> Result<()> {
        let timer = LatencyMetrics::start_timer(ReqType::Sync, "lsmtree", "");

        self.tx_lsm_tree.sync()?;

        LatencyMetrics::stop_timer(timer);

        let timer = LatencyMetrics::start_timer(ReqType::Sync, "data", "");

        self.user_data_disk.flush()?;

        LatencyMetrics::stop_timer(timer);

        Ok(())
    }

    pub fn display_metrics(&self) {
        Metrics::display();
        Metrics::reset();
    }

    /// Return the total number of blocks in the device.
    pub fn total_blocks(&self) -> usize {
        self.user_data_disk.nblocks()
    }

    /// Create the device on a disk, given the root cryption key.
    pub fn create(disk: D, root_key: Key) -> Result<Self> {
        let data_disk = Self::subdisk_for_data(&disk)?;
        let lsm_tree_disk = Self::subdisk_for_tx_lsm_tree(&disk)?;

        let tx_log_store = Arc::new(TxLogStore::format(lsm_tree_disk)?);
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
        let tx_lsm_tree = TxLsmTree::format(
            tx_log_store,
            listener_factory,
            Some(Arc::new(on_drop_record_in_memtable)),
        )?;

        Ok(Self {
            tx_lsm_tree,
            user_data_disk: data_disk,
            root_key,
            block_validity_bitmap,
        })
    }

    /// Open the device on a disk, given the root cryption key.
    pub fn open(disk: D, root_key: Key) -> Result<Self> {
        let data_disk = Self::subdisk_for_data(&disk)?;
        let index_disk = Self::subdisk_for_tx_lsm_tree(&disk)?;

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
        let tx_lsm_tree = TxLsmTree::recover(
            tx_log_store,
            listener_factory,
            Some(Arc::new(on_drop_record_in_memtable)),
        )?;

        Ok(Self {
            tx_lsm_tree,
            user_data_disk: data_disk,
            root_key,
            block_validity_bitmap,
        })
    }

    fn subdisk_for_data(disk: &D) -> Result<D> {
        disk.subset(0..disk.nblocks() / 10 * 5) // TBD
    }

    fn subdisk_for_tx_lsm_tree(disk: &D) -> Result<D> {
        disk.subset(disk.nblocks() / 10 * 5..disk.nblocks()) // TBD
    }
}

unsafe impl<D: BlockSet> Send for SwornDisk<D> {}
unsafe impl<D: BlockSet> Sync for SwornDisk<D> {}

// struct DataBuf {
//     buf: HashMap<RecordKey, RecordValue>,
// }

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

    #[test]
    fn sworndisk_fns() -> Result<()> {
        let nblocks = 32 * 1024;
        let mem_disk = MemDisk::create(nblocks)?;
        let root_key = Key::random();
        let sworndisk = SwornDisk::create(mem_disk, root_key)?;

        let num_rw = 4 * 1024;
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

        Ok(())
    }
}
