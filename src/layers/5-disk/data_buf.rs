//! Data buffering.
use super::sworndisk::RecordKey;
use crate::layers::bio::{BufMut, BufRef};
use crate::os::RwLock;
use crate::prelude::*;

use core::fmt::{self, Debug};
use core::ops::RangeInclusive;
use core::sync::atomic::{AtomicUsize, Ordering};
use rbtree::RBTree;

/// A buffer to cache data blocks before they are written to disk.
#[derive(Debug)]
pub(super) struct DataBuf {
    buf: RwLock<RBTree<RecordKey, Arc<DataBlock>>>,
    size: AtomicUsize,
}

/// User data block.
pub(super) struct DataBlock([u8; BLOCK_SIZE]);

impl DataBuf {
    /// Create a new empty data buffer.
    pub fn new() -> Self {
        Self {
            buf: RwLock::new(RBTree::new()),
            size: AtomicUsize::new(0),
        }
    }

    /// Get the buffered data block with the key and copy
    /// the content into `buf`.
    pub fn get(&self, key: RecordKey, buf: &mut BufMut) -> Option<()> {
        debug_assert_eq!(buf.nblocks(), 1);
        if let Some(block) = self.buf.read().get(&key) {
            buf.as_mut_slice().copy_from_slice(block.as_slice());
            Some(())
        } else {
            None
        }
    }

    /// Get the buffered data blocks which keys are within the given range.
    pub fn get_range(&self, range: RangeInclusive<RecordKey>) -> Vec<(RecordKey, Arc<DataBlock>)> {
        self.buf
            .read()
            .iter()
            .filter_map(|(k, v)| {
                if range.contains(k) {
                    Some((*k, v.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Put the data block in `buf` into the buffer.
    pub fn put(&self, key: RecordKey, buf: BufRef) {
        debug_assert_eq!(buf.nblocks(), 1);
        self.buf.write().insert(key, DataBlock::from_buf(buf));
        self.size.fetch_add(1, Ordering::Release);
    }

    /// Return the number of data blocks of the buffer.
    pub fn nblocks(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Return whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.nblocks() == 0
    }

    /// Empty the buffer.
    pub fn clear(&self) {
        self.buf.write().clear();
        self.size.store(0, Ordering::Release);
    }

    /// Return all the buffered data blocks.
    pub fn all_blocks(&self) -> Vec<(RecordKey, Arc<DataBlock>)> {
        self.buf
            .read()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }
}

impl DataBlock {
    /// Create a new data block from the given `buf`.
    pub fn from_buf(buf: BufRef) -> Arc<Self> {
        debug_assert_eq!(buf.nblocks(), 1);
        Arc::new(DataBlock(buf.as_slice().try_into().unwrap()))
    }

    /// Return the immutable slice of the data block.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataBlock")
            .field("first 16 bytes", &&self.0[..16])
            .finish()
    }
}
