#![feature(new_uninit)]

mod bio;
mod layers;
mod prelude;

pub type BlockId = u64;
pub type Result<T> = std::result::Result<T, std::io::Error>;

pub const BLOCK_SIZE: usize = 0x1000;
