//! Block allocation.
use super::sworndisk::Hba;
use crate::layers::bio::{BlockSet, Buf, BufRef, BID_SIZE};
use crate::layers::log::{TxLog, TxLogStore};
use crate::os::Mutex;
use crate::prelude::*;

use alloc::collections::BTreeMap;
use core::sync::atomic::{AtomicUsize, Ordering};
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
pub(super) struct AllocBitmap {
    bitmap: Mutex<BitMap>,
    min_avail: AtomicUsize,
}

impl<D: BlockSet + 'static> BlockAlloc<D> {
    pub fn new(bitmap: Arc<AllocBitmap>, store: Arc<TxLogStore<D>>) -> Self {
        Self {
            bitmap,
            diff_table: Mutex::new(BTreeMap::new()),
            store,
            diff_log: Mutex::new(None),
        }
    }

    /// Allocate a specified block, means update in-memory metadata.
    pub fn alloc_block(&self, block_id: Hba) -> Result<()> {
        let mut diff_table = self.diff_table.lock();
        let replaced = diff_table.insert(block_id, AllocDiff::Alloc);
        if replaced == Some(AllocDiff::Alloc) {
            panic!("can't allocate a block twice");
        }
        Ok(())
    }

    /// Deallocate a specified block, means update in-memory metadata.
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
        // Do nothing for now
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

        let diff_log = if let Ok(log) = self.store.open_log_in(BUCKET_BLOCK_ALLOC_LOG, true) {
            log
        } else {
            self.store.create_log(BUCKET_BLOCK_ALLOC_LOG)?
        };

        let mut diff_buf = Vec::with_capacity(BLOCK_SIZE);
        for (block_id, block_diff) in diff_table.iter() {
            diff_buf.push(*block_diff as u8);
            diff_buf.extend_from_slice(block_id.as_bytes());
        }
        diff_buf.resize(align_up(diff_buf.len(), BLOCK_SIZE), 0);
        let buf = BufRef::try_from(&diff_buf[..]).unwrap();
        diff_log.append(buf)
    }

    pub fn update_bitmap(&self) {
        let diff_table = self.diff_table.lock();
        let mut bitmap = self.bitmap.bitmap.lock();
        let mut min_avail = self.bitmap.min_avail.load(Ordering::Relaxed);
        for (block_id, block_diff) in diff_table.iter() {
            let validity = match block_diff {
                AllocDiff::Alloc => false,
                AllocDiff::Dealloc => {
                    min_avail = min_avail.min(*block_id);
                    true
                }
                AllocDiff::Invalid => unreachable!(),
            };
            bitmap.set(*block_id, validity);
        }
        self.bitmap.min_avail.store(min_avail, Ordering::Release);
        drop(bitmap);
    }

    fn do_compaction(&self) {
        // Seal the bitmap log snapshot to log `BVT`, discard all diff logs (`BAL``)
        todo!()
    }
}

impl AllocBitmap {
    pub fn new(nblocks: usize) -> Self {
        Self {
            bitmap: Mutex::new(BitMap::repeat(true, nblocks)),
            min_avail: AtomicUsize::new(0),
        }
    }

    // Open and recover the latest `BVT` log, apply `AllocDiff`s in `BAL` logs to the bitmap.
    // TODO: Refine the recovery process
    pub fn recover<D: BlockSet + 'static>(&self, store: &Arc<TxLogStore<D>>) -> Result<()> {
        let mut tx = store.new_tx();
        let res: Result<_> = tx.context(|| {
            let diff_log_res = store.open_log_in(BUCKET_BLOCK_ALLOC_LOG, false);
            if let Err(e) = &diff_log_res && e.errno() == NotFound {
                return Ok(());
            }
            let diff_log = diff_log_res?;

            let mut buf = Buf::alloc(diff_log.nblocks())?;
            diff_log.read(0, buf.as_mut())?;
            let buf_slice = buf.as_slice();
            let mut offset = 0;
            while offset <= buf.nblocks() * BLOCK_SIZE {
                let diff = AllocDiff::from(buf_slice[offset]);
                offset += 1;
                if diff == AllocDiff::Invalid {
                    break;
                }
                let bid = BlockId::from_bytes(&buf_slice[offset..offset + BID_SIZE]);
                offset += BID_SIZE;
                match diff {
                    AllocDiff::Alloc => self.set_allocated(bid),
                    AllocDiff::Dealloc => self.set_deallocated(bid),
                    _ => unreachable!(),
                }
            }
            Ok(())
        });
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(TxAborted, "recover block validity table TX aborted");
        }
        tx.commit()
    }

    pub fn alloc(&self) -> Option<Hba> {
        let mut bitmap = self.bitmap.lock();
        let min_avail = self.min_avail.load(Ordering::Relaxed);
        let hba = bitmap[min_avail..].first_one()? + min_avail;
        bitmap.set(hba, false);
        self.min_avail.store(hba + 1, Ordering::Release);
        Some(hba as Hba)
    }

    pub fn alloc_batch(&self, count: usize) -> Option<Vec<Hba>> {
        let mut bitmap = self.bitmap.lock();
        let mut hbas = Vec::with_capacity(count);
        let mut min_avail = self.min_avail.load(Ordering::Relaxed);
        for _ in 0..count {
            let hba = bitmap[min_avail..].first_one()? + min_avail;
            hbas.push(hba);

            bitmap.set(hba, false);
            min_avail += 1;
        }
        self.min_avail.store(min_avail, Ordering::Release);
        Some(hbas)
    }

    pub fn set_allocated(&self, nth: usize) {
        self.bitmap.lock().set(nth, false);
    }

    pub fn set_deallocated(&self, nth: usize) {
        self.bitmap.lock().set(nth, true);
    }

    pub fn sync<D: BlockSet + 'static>(&self, store: &Arc<TxLogStore<D>>) -> Result<()> {
        let bitmap = self.bitmap.lock();
        let mut buf = postcard::to_vec::<BitMap, BLOCK_SIZE>(bitmap.as_ref())
            .map_err(|_| Error::with_msg(InvalidArgs, "serialize block validity table failed"))?;
        buf.resize(align_up(buf.len(), BLOCK_SIZE), 0);
        let mut tx = store.new_tx();
        let res: Result<_> = tx.context(|| {
            let log = store.create_log(BUCKET_BLOCK_VALIDITY_TABLE)?;
            log.append(BufRef::try_from(&buf[..]).unwrap())
        });
        if res.is_err() {
            tx.abort();
            return_errno_with_msg!(TxAborted, "persist block validity table TX aborted");
        }
        tx.commit()
    }
}

/// Incremental changes of block validity bitmap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum AllocDiff {
    Alloc = 3,
    Dealloc = 7,
    Invalid,
}

impl From<u8> for AllocDiff {
    fn from(value: u8) -> Self {
        match value {
            3 => AllocDiff::Alloc,
            7 => AllocDiff::Dealloc,
            _ => AllocDiff::Invalid,
        }
    }
}

#[derive(Serialize, Deserialize)]
enum AllocDiffRecord {
    Diff(Hba, AllocDiff),
    Checkpoint,
}
