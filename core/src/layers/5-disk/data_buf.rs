//! Data buffering.
use super::sworndisk::RecordKey;
use crate::layers::bio::{BufMut, BufRef};
use crate::os::{BTreeMap, RwLock};
use crate::prelude::*;

use core::fmt::{self, Debug};
use core::ops::RangeInclusive;

/// A buffer to cache data blocks before they are written to disk.
#[derive(Debug)]
pub(super) struct DataBuf {
    buf: RwLock<BTreeMap<RecordKey, Arc<DataBlock>>>,
    cap: usize,
}

/// User data block.
pub(super) struct DataBlock([u8; BLOCK_SIZE]);

impl DataBuf {
    /// Create a new empty data buffer with a given capacity.
    pub fn new(cap: usize) -> Self {
        Self {
            buf: RwLock::new(BTreeMap::new()),
            cap,
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

    /// Put the data block in `buf` into the buffer. Return
    /// whether the buffer is full after insertion.
    pub fn put(&self, key: RecordKey, buf: BufRef) -> bool {
        debug_assert_eq!(buf.nblocks(), 1);
        let mut data_buf = self.buf.write();
        let _ = data_buf.insert(key, DataBlock::from_buf(buf));
        data_buf.len() >= self.cap
    }

    /// Return the number of data blocks of the buffer.
    pub fn nblocks(&self) -> usize {
        self.buf.read().len()
    }

    /// Return whether the buffer is full.
    pub fn at_capacity(&self) -> bool {
        self.nblocks() >= self.cap
    }

    /// Return whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.nblocks() == 0
    }

    /// Empty the buffer.
    pub fn clear(&self) {
        self.buf.write().clear();
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
