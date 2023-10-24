//! SwornDisk as a BlockDevice.
//!
//! API: read(), write(), flush(), flush_blocks(), discard(), total_blocks(), open(), create()
//!
//! Responisble for managing a `TxLsmTree`, an untrusted disk
//! storing user data, a `BlockAlloc` for managing data block
//! validity manage tx logs (WAL and SSTs). `TxLsmTree` and
//! `BlockAlloc` are manipulated based on internal transactions.
use super::bio::{BlockId, BlockSet, Buf, BufMut, BufRef};
use super::log::{TxLog, TxLogStore};
use super::lsm::{AsKv, LsmLevel, TxEventListener, TxEventListenerFactory, TxLsmTree, TxType};
use crate::os::{Aead as OsAead, AeadIv as Iv, AeadKey as Key, AeadMac as Mac, Mutex};
use crate::prelude::*;
use crate::tx::Tx;
use crate::util::Aead;

use alloc::collections::BTreeMap;
use pod::Pod;
use serde::{Deserialize, Serialize};

type Lba = BlockId;
type Hba = BlockId;

type BitMap = bitvec::prelude::BitVec<u8, bitvec::prelude::Lsb0>;

// ID: Index Disk, DD: Data Disk
// D: one disk generic
pub struct SwornDisk<D: BlockSet> {
    tx_lsm_tree: TxLsmTree<RecordKey, RecordValue, D>,
    user_data_disk: D,
    block_validity_bitmap: Arc<AllocBitmap>,
    root_key: Key,
}

impl<D: BlockSet + 'static> SwornDisk<D> {
    /// Read a specified number of block contents at a logical block address on the device.
    pub fn read(&self, lba: Lba, mut buf: BufMut) -> Result<usize> {
        // TODO: batch read
        let record = self.tx_lsm_tree.get(&RecordKey { lba }).unwrap();
        let mut rbuf = Buf::alloc(1)?;
        self.user_data_disk.read(record.hba, rbuf.as_mut())?;
        OsAead::new().decrypt(
            rbuf.as_slice(),
            &record.key,
            &Iv::new_zeroed(),
            &[],
            &record.mac,
            buf.as_mut_slice(),
        )?;
        Ok(buf.nblocks())
    }

    /// Write a specified number of block contents at a logical block address on the device.
    pub fn write(&self, lba: Lba, buf: BufRef) -> Result<usize> {
        // TODO: batch write
        let hba = self
            .alloc_block()
            .ok_or(Error::with_msg(NoMemory, "block allocation failed"))?;
        let key = Key::random();
        let mut wbuf = Buf::alloc(1)?;
        let mac = OsAead::new().encrypt(
            buf.as_slice(),
            &key,
            &Iv::new_zeroed(),
            &[],
            wbuf.as_mut_slice(),
        )?;

        self.user_data_disk.write(hba, wbuf.as_ref())?;
        self.tx_lsm_tree
            .put(RecordKey { lba }, RecordValue { hba, key, mac })?;
        Ok(buf.nblocks())
    }

    /// Sync all cached data in the device to the storage medium for durability.
    pub fn sync(&self) -> Result<()> {
        self.tx_lsm_tree.sync()?;
        self.user_data_disk.flush()
    }

    /// Return the total number of blocks in the device.
    pub fn total_blocks(&self) -> usize {
        self.user_data_disk.nblocks()
    }

    /// Create the device on a disk, given the root cryption key.
    pub fn create(disk: D, root_key: Key) -> Result<Self> {
        let data_disk = Self::subdisk_for_data(&disk)?;
        let index_disk = Self::subdisk_for_tx_lsm_tree(&disk)?;

        let tx_log_store = Arc::new(TxLogStore::format(index_disk)?);
        let block_validity_bitmap = Arc::new(Mutex::new(BitMap::repeat(true, data_disk.nblocks())));
        let listener_factory = Arc::new(TxLsmTreeListenerFactory::new(
            tx_log_store.clone(),
            block_validity_bitmap.clone(),
        ));

        let bitmap = block_validity_bitmap.clone();
        let on_drop_record_in_memtable = move |record: &dyn AsKv<RecordKey, RecordValue>| {
            // Dealloc block
            bitmap.lock().set(record.value().hba, true);
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
        let block_validity_bitmap = Arc::new(Mutex::new(BitMap::repeat(true, data_disk.nblocks())));
        let listener_factory = Arc::new(TxLsmTreeListenerFactory::new(
            tx_log_store.clone(),
            block_validity_bitmap.clone(),
        ));
        Self::recover_bitmap(&tx_log_store, &block_validity_bitmap)?;

        let bitmap = block_validity_bitmap.clone();
        let on_drop_record_in_memtable = move |record: &dyn AsKv<RecordKey, RecordValue>| {
            // Dealloc block
            bitmap.lock().set(record.value().hba, true);
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

    fn recover_bitmap(store: &Arc<TxLogStore<D>>, bitmap: &Arc<AllocBitmap>) -> Result<()> {
        let mut tx = store.new_tx();
        let res: Result<_> = tx.context(|| {
            let diff_log = store.open_log_in(BUCKET_BLOCK_ALLOC_LOG)?;
            let mut buf = Buf::alloc(diff_log.nblocks())?;
            diff_log.read(0, buf.as_mut())?;
            let buf_slice = buf.as_slice();
            let mut offset = 0;
            while offset <= buf.nblocks() * BLOCK_SIZE {
                let diff = AllocDiff::try_from(buf_slice[offset]);
                offset += 1;
                if diff.is_err() {
                    continue;
                }
                let bid = BlockId::from_bytes(&buf_slice[offset..offset + 8]);
                offset += 8;
                match diff? {
                    AllocDiff::Alloc => bitmap.lock().set(bid, false),
                    AllocDiff::Dealloc => bitmap.lock().set(bid, true),
                }
            }
            Ok(())
        });
        if res.is_err() {
            tx.abort();
            return_errno!(TxAborted);
        }
        tx.commit()
    }

    fn alloc_block(&self) -> Option<Hba> {
        let mut bitmap = self.block_validity_bitmap.lock();
        for (nth, mut is_valid) in bitmap.iter_mut().enumerate() {
            if *is_valid {
                *is_valid = false;
                return Some(nth as Hba);
            }
        }
        None
    }

    fn subdisk_for_data(disk: &D) -> Result<D> {
        disk.subset(0..disk.nblocks() / 10 * 9) // TBD
    }

    fn subdisk_for_tx_lsm_tree(disk: &D) -> Result<D> {
        disk.subset(disk.nblocks() / 10 * 9..disk.nblocks()) // TBD
    }
}

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
                tx.context(|| self.block_alloc.prepare_diff_log())
            }
        }
    }

    fn on_tx_precommit(&self, tx: &mut Tx) -> Result<()> {
        match self.tx_type {
            TxType::Compaction { .. } | TxType::Migration => {
                tx.context(|| self.block_alloc.update_diff_log())
            }
        }
    }

    fn on_tx_commit(&self) {
        match self.tx_type {
            TxType::Compaction { .. } | TxType::Migration => self.block_alloc.update_bitmap(),
        }
    }
}

const BUCKET_BLOCK_VALIDITY_TABLE: &str = "BVT";
const BUCKET_BLOCK_ALLOC_LOG: &str = "BAL";

/// Block allocator, manages user-data blocks validity.
// TODO: Distinguish snapshot diff log (during compaction) and regular diff log
struct BlockAlloc<D> {
    bitmap: Arc<AllocBitmap>,               // In memory
    diff_table: AllocDiffTable,             // In memory
    store: Arc<TxLogStore<D>>,              // On disk
    diff_log: Mutex<Option<Arc<TxLog<D>>>>, // Cache opened diff log // TODO: Support multiple diff logs
}

/// Block validity bitmap.
type AllocBitmap = Mutex<BitMap>;
/// Incremental changes of block validity bitmap.
type AllocDiffTable = Mutex<BTreeMap<Hba, AllocDiff>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum AllocDiff {
    Alloc = 3,
    Dealloc = 7,
}

impl TryFrom<u8> for AllocDiff {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            3 => Ok(AllocDiff::Alloc),
            7 => Ok(AllocDiff::Dealloc),
            _ => Err(Error::new(InvalidArgs)),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum AllocDiffRecord {
    Diff(Hba, AllocDiff),
    Checkpoint,
}

impl<D: BlockSet + 'static> BlockAlloc<D> {
    fn new(bitmap: Arc<AllocBitmap>, store: Arc<TxLogStore<D>>) -> Self {
        Self {
            bitmap,
            diff_table: Mutex::new(BTreeMap::new()),
            store,
            diff_log: Mutex::new(None),
        }
    }

    /// Allocate a specifiied block, means update in-memory metadata.
    fn alloc_block(&self, block_id: Hba) -> Result<()> {
        let mut diff_table = self.diff_table.lock();
        let replaced = diff_table.insert(block_id, AllocDiff::Alloc);
        if replaced == Some(AllocDiff::Alloc) {
            panic!("cannot allocate a block twice");
        }
        Ok(())
    }

    /// Deallocate a specifiied block, means update in-memory metadata.
    fn dealloc_block(&self, block_id: Hba) -> Result<()> {
        let mut diff_table = self.diff_table.lock();
        let replaced = diff_table.insert(block_id, AllocDiff::Dealloc);
        if replaced == Some(AllocDiff::Dealloc) {
            panic!("cannot deallocate a block twice");
        }
        Ok(())
    }

    /// Open the block validity diff log.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn prepare_diff_log(&self) -> Result<()> {
        let mut diff_log = self.store.open_log_in(BUCKET_BLOCK_ALLOC_LOG);
        if let Err(e) = &diff_log && e.errno() == NotFound {
            diff_log = self.store.create_log(BUCKET_BLOCK_ALLOC_LOG);
        }
        let diff_log = diff_log?;
        self.diff_log.lock().insert(diff_log.clone());
        Ok(())
    }

    /// Update cached diff table to the block validity diff log.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    fn update_diff_log(&self) -> Result<()> {
        let diff_table = self.diff_table.lock();
        let mut diff_buf = Vec::new();
        for (block_id, block_diff) in diff_table.iter() {
            diff_buf.push(*block_diff as u8);
            diff_buf.extend_from_slice(block_id.as_bytes());
        }
        let mut buf = Buf::alloc(align_up(diff_buf.len(), BLOCK_SIZE) / BLOCK_SIZE)?;
        buf.as_mut_slice()[..diff_buf.len()].copy_from_slice(&diff_buf);

        let diff_log = self.diff_log.lock();
        let diff_log = diff_log.as_ref().unwrap();
        diff_log.append(buf.as_ref())
    }

    fn update_bitmap(&self) {
        let diff_table = self.diff_table.lock();
        let mut bitmap = self.bitmap.lock();
        for (block_id, block_diff) in diff_table.iter() {
            let validity = match block_diff {
                AllocDiff::Alloc => false,
                AllocDiff::Dealloc => true,
            };
            bitmap.set(*block_id, validity);
        }
        drop(bitmap);
    }

    // /// Checkpoint to seal a bitmap log snapshot, diff logs before
    // /// checkpoint can be deleted.
    // ///
    // /// # Panics
    // ///
    // /// This method must be called within a TX. Otherwise, this method panics.
    // // TODO: Use snapshot diff log instead bitmap log
    // fn checkpoint(&self, diff_log: Arc<TxLog>, bitmap_log: Arc<TxLog<D>>) -> Result<()> {
    //     let inner = self.inner.lock();
    //     bitmap_log.append(inner.validity_bitmap.to_bytes())?;
    //     diff_log.append(AllocDiffRecord::Checkpoint)
    // }

    fn recover(store: Arc<TxLogStore<D>>) -> Result<Self> {
        // Open the latest bitmap log, apply each `AllocDiffRecord::Diff` after the newest `AllocDiffRecord::Checkpoint` to the bitmap
        todo!()
    }

    fn do_compaction(&self) {
        // Open the diff log, Migrate `AllocDiffRecord::Diff`s after newest `AllocDiffRecord::Checkpoint` to a new log, delete the old one
        todo!()
    }
}

/// K-V record for `TxLsmTree`.
struct Record {
    key: RecordKey,
    value: RecordValue,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, PartialEq, Eq, PartialOrd, Ord, Debug)]
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
        let nblocks = 20 * 1024;
        let mem_disk = MemDisk::create(nblocks)?;
        let root_key = Key::random();
        let sworndisk = SwornDisk::create(mem_disk, root_key)?;

        let lba = 5 as Lba;
        let mut wbuf = Buf::alloc(1)?;
        wbuf.as_mut_slice().fill(5u8);
        sworndisk.write(lba, wbuf.as_ref())?;
        sworndisk.sync()?;
        let mut rbuf = Buf::alloc(1)?;
        sworndisk.read(lba, rbuf.as_mut())?;
        assert_eq!(rbuf.as_slice(), wbuf.as_slice());

        Ok(())
    }
}
