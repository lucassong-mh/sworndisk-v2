//! The layer of secure virtual disk.

mod block_alloc;
mod sworndisk;

pub use self::sworndisk::SwornDisk;
