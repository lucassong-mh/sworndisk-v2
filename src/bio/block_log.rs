use super::BlockBuf;
use crate::prelude::*;

use inherit_methods_macro::inherit_methods;

/// A log of data blocks that can support random reads and append-only
/// writes.
pub trait BlockLog {
    /// Read one or multiple blocks at a specified position.
    fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;

    /// Append one or multiple blocks at the end,
    /// returning the ID of the newly-appended block.
    fn append(&self, buf: &impl BlockBuf) -> Result<BlockId>;

    /// Ensure that blocks are persisted to the disk.
    fn flush(&self) -> Result<()>;

    /// Returns the number of blocks.
    fn num_blocks(&self) -> usize;
}

macro_rules! impl_blocklog_pointer {
    ($typ:ty,$from:tt) => {
        #[inherit_methods(from = $from)]
        impl<T: BlockLog> BlockLog for $typ {
            fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()>;
            fn append(&self, buf: &impl BlockBuf) -> Result<BlockId>;
            fn flush(&self) -> Result<()>;
            fn num_blocks(&self) -> usize;
        }
    };
}

impl_blocklog_pointer!(&T, "(**self)");
// impl_blocklog_pointer!(&mut T, "(**self)");
impl_blocklog_pointer!(Box<T>, "(**self)");
impl_blocklog_pointer!(Arc<T>, "(**self)");
