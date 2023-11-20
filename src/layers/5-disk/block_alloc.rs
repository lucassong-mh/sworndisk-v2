//! Block allocation.

use super::sworndisk::Hba;
use crate::layers::bio::{BlockSet, Buf};
use crate::layers::log::{TxLog, TxLogStore};
use crate::os::Mutex;
use crate::prelude::*;

use alloc::collections::BTreeMap;
use pod::Pod;
use serde::{Deserialize, Serialize};

type BitMap = bitvec::prelude::BitVec<u8, bitvec::prelude::Lsb0>;

const BUCKET_BLOCK_VALIDITY_TABLE: &str = "BVT";
const BUCKET_BLOCK_ALLOC_LOG: &str = "BAL";

/// Block allocator, manages user-data blocks validity.
// TODO: Distinguish snapshot diff log (during compaction) and regular diff log
pub(super) struct BlockAlloc<D> {
    bitmap: Arc<AllocBitmap>,               // In memory
    diff_table: AllocDiffTable,             // In memory
    store: Arc<TxLogStore<D>>,              // On disk
    diff_log: Mutex<Option<Arc<TxLog<D>>>>, // Cache opened diff log // TODO: Support multiple diff logs
}
type AllocDiffTable = Mutex<BTreeMap<Hba, AllocDiff>>;

/// Block validity bitmap.
pub(super) struct AllocBitmap(Mutex<BitMap>);

impl<D: BlockSet + 'static> BlockAlloc<D> {
    pub fn new(bitmap: Arc<AllocBitmap>, store: Arc<TxLogStore<D>>) -> Self {
        Self {
            bitmap,
            diff_table: Mutex::new(BTreeMap::new()),
            store,
            diff_log: Mutex::new(None),
        }
    }

    /// Allocate a specifiied block, means update in-memory metadata.
    pub fn alloc_block(&self, block_id: Hba) -> Result<()> {
        let mut diff_table = self.diff_table.lock();
        let replaced = diff_table.insert(block_id, AllocDiff::Alloc);
        if replaced == Some(AllocDiff::Alloc) {
            panic!("can't allocate a block twice");
        }
        Ok(())
    }

    /// Deallocate a specifiied block, means update in-memory metadata.
    pub fn dealloc_block(&self, block_id: Hba) -> Result<()> {
        let mut diff_table = self.diff_table.lock();
        let replaced = diff_table.insert(block_id, AllocDiff::Dealloc);
        if replaced == Some(AllocDiff::Dealloc) {
            panic!("can't deallocate a block twice");
        }
        Ok(())
    }

    /// Open the block validity diff log.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn prepare_diff_log(&self) -> Result<()> {
        // FIXME: not in append mode
        let mut diff_log = self.store.open_log_in(BUCKET_BLOCK_ALLOC_LOG);
        if let Err(e) = &diff_log && e.errno() == NotFound {
            diff_log = self.store.create_log(BUCKET_BLOCK_ALLOC_LOG);
        }
        let diff_log = diff_log?;
        let _ = self.diff_log.lock().insert(diff_log.clone());
        Ok(())
    }

    /// Update cached diff table to the block validity diff log.
    ///
    /// # Panics
    ///
    /// This method must be called within a TX. Otherwise, this method panics.
    pub fn update_diff_log(&self) -> Result<()> {
        let diff_table = self.diff_table.lock();
        if diff_table.is_empty() {
            return Ok(());
        }
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

    pub fn update_bitmap(&self) {
        let diff_table = self.diff_table.lock();
        let mut bitmap = self.bitmap.0.lock();
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

impl AllocBitmap {
    pub fn new(nblocks: usize) -> Self {
        Self(Mutex::new(BitMap::repeat(true, nblocks)))
    }

    pub fn recover<D: BlockSet + 'static>(&self, store: &Arc<TxLogStore<D>>) -> Result<()> {
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
                    AllocDiff::Alloc => self.set_allocated(bid),
                    AllocDiff::Dealloc => self.set_deallocated(bid),
                }
            }
            Ok(())
        });
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(TxAborted, "recover block validity bitmap TX aborted");
        }
        tx.commit()
    }

    pub fn alloc(&self) -> Option<Hba> {
        let mut bitmap = self.0.lock();
        for (nth, mut is_valid) in bitmap.iter_mut().enumerate() {
            if *is_valid {
                *is_valid = false;
                return Some(nth as Hba);
            }
        }
        None
    }

    pub fn set_allocated(&self, nth: usize) {
        self.0.lock().set(nth, false);
    }

    pub fn set_deallocated(&self, nth: usize) {
        self.0.lock().set(nth, true);
    }
}

/// Incremental changes of block validity bitmap.
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
